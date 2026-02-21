package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "golang.org/x/crypto/x509roots/fallback"
)

type writeFlusher struct {
	w io.Writer
}

func (wf *writeFlusher) Write(p []byte) (int, error) {
	fmt.Println(string(p))
	if !bytes.HasSuffix(p, []byte("\n")) {
		return wf.w.Write(append(p, byte('\n')))
	}

	return wf.w.Write(p)
}

type ircWriter struct {
	io.Writer
}

func (ircw *ircWriter) Privmsg(to, format string, args ...any) (int, error) {
	return ircw.Writer.Write(fmt.Appendf([]byte{}, fmt.Sprintf("PRIVMSG %v :", to)+format, args...))
}

func (ic *ircClient) connect() error {
	fmt.Println("client connecting")

	conn, err := tls.Dial("tcp", "irc.rizon.net:6697", nil)

	if err != nil {
		return err
	}

	outScan := bufio.NewScanner(conn)
	messageChan := make(chan []byte)

	fmt.Println("start channel goroutine")
	go func() {
		for outScan.Scan() {
			msgData := bytes.Clone(outScan.Bytes())
			fmt.Println(string(msgData))
			messageChan <- msgData
		}
	}()

	fmt.Println("send handshake")
	in := &ircWriter{&writeFlusher{w: conn}}
	in.Write(fmt.Appendf([]byte{}, "USER fulminata * 0 :%s\n", ic.nickname))
	in.Write(fmt.Appendf([]byte{}, "NICK %s\n", ic.nickname))

	// channel handshake loop (must be synchronous)
	for message := range messageChan {
		// first ping
		if ts, ok := bytes.CutPrefix(message, []byte("PING")); ok {
			in.Write([]byte("PONG" + string(ts) + "\n"))
		}

		// MOTD finished: join now
		if bytes.Contains(message, []byte("End of /MOTD command.")) {
			// nick registration
			in.Privmsg("NickServ", "IDENTIFY %s", registrationPassword)
			// leave time for roundtrip
			time.Sleep(time.Second)

			fmt.Println("send join")
			in.Write(fmt.Appendf([]byte{}, "JOIN %s\n", ic.channel))

			break
		}

		// received NAMES list end: ok, we're connected to the chan, break out of handshake loop
		if bytes.Contains(message, []byte("End of /NAMES list")) {
			break
		}
	}

	// irc side background handler loop
	go func() {
		for {
			select {
			case message := <-messageChan: // message received on serv, process accordingly
				ic.ircMessageHandler(in, message)
			case <-ic.shutdownChan:
				return
			}
		}
	}()
	// dcc side background handler loop
	go func() {
		for {
			select {
			case req := <-ic.downloadRequestsChan:
				if err := ic.processDownloadRequest(in, req); err != nil {
					ic.dequeue(req.Hostname, req)
					in.Privmsg(req.Nick, "Cannot process your download request: %s", err.Error())
				}
			case <-ic.shutdownChan:
				return
			}
		}
	}()

	return nil
}

func (ic *ircClient) ircMessageHandler(in *ircWriter, message []byte) {
	// handle PONG
	if ts, ok := bytes.CutPrefix(message, []byte("PING")); ok {
		in.Write([]byte("PONG" + string(ts) + "\n"))

		return
	}

	// handle CTCP calls
	if bytes.Contains(message, []byte("PRIVMSG")) {
		querier, hostname := extractQH(message)

		switch {
		case bytes.Contains(message, []byte("\x01PING")):
			payload := bytes.Split(message, []byte("\x01PING "))[1]

			in.Privmsg(querier, "\x01PONG %sn", payload)
		case bytes.Contains(message, []byte("\x01VERSION\x01")):
			in.Privmsg(querier, "\x01VERSION fulminata-v0.0.1\x01")
			//		case bytes.Contains(message, []byte("\x01WEBSITE\x01")):
			//			in.Write(fmt.Appendf([]byte{}, "PRIVMSG %v :\x01WEBSITE https://github.com/ina-exter/irc-downloader\x01\n", querier))
		case bytes.Contains(bytes.ToUpper(message), []byte("DCC SEND")):
			// Handle DCC message reception
			// SAFETY: only accept it if it's a private message to us
			if bytes.Contains(message, fmt.Append([]byte("PRIVMSG "), ic.nickname)) {
				ic.enqueueDownloadRequest(in, message)
			}
		case bytes.Contains(bytes.ToUpper(message), []byte("DCC LIST")):
			// Handle DCC message reception
			// SAFETY: only accept it if it's a private message to us
			if bytes.Contains(message, fmt.Append([]byte("PRIVMSG "), ic.nickname)) {
				in.Privmsg(querier, "XDCC LIST Denied: too big for irc. Use /msg %s XDCC SEND list", ic.nickname)
			}
		case bytes.Contains(bytes.ToUpper(message), []byte("DCC CANCEL")):
			// Handle DCC message reception
			// SAFETY: only accept it if it's a private message to us
			if bytes.Contains(message, fmt.Append([]byte("PRIVMSG "), ic.nickname)) {
				ic.cancel(hostname)
			}
		case bytes.Contains(bytes.ToUpper(message), []byte("DCC RESUME")):
			// Handle DCC message reception
			// SAFETY: only accept it if it's a private message to us
			if bytes.Contains(message, fmt.Append([]byte("PRIVMSG "), ic.nickname)) {
				ic.resume(in, querier, message)
			}
		case bytes.Contains(bytes.ToUpper(message), []byte("DCC")):
			if bytes.Contains(message, fmt.Append([]byte("PRIVMSG "), ic.nickname)) {
				in.Privmsg(querier, "Unimplemented DCC action.")
			}
		case bytes.Contains(message, []byte("\x01")):
			in.Privmsg(querier, "Unimplemented CTCP action, don't hesitate to open a github issue (link with CTCP WEBSITE)")
		}

		return
	}
}

func (ic *ircClient) enqueueDownloadRequest(w *ircWriter, message []byte) error {
	req := decodeSend(message)

	if req.Nick == "" || req.Pack == "" {
		return nil
	}

	if !ic.fileFetcher.Exists(req.Pack) {
		w.Privmsg(req.Nick, "Pack not found")

		return nil
	}

	// slot check for extra message
	// remove?
	if ic.portsPool.Free() == 0 {
		w.Privmsg(req.Nick, "Slots are full, please be patient")
	}

	ic.downloadCountMu.Lock()

	defer ic.downloadCountMu.Unlock()

	ic.downloadProcessesByHostname[req.Hostname] = append(
		ic.downloadProcessesByHostname[req.Hostname],
		downloadProcess{
			downloadRequest: req,
		},
	)

	if len(ic.downloadProcessesByHostname[req.Hostname]) > ic.rateLimit {
		w.Privmsg(req.Nick, "Request queued: you have too many transfer pendings. Please finish or cancel pending transfers.")
		ic.downloadQueue[req.Hostname] = append(ic.downloadQueue[req.Hostname], req)
	} else {
		ic.downloadRequestsChan <- req
	}

	return nil
}

func (ic *ircClient) cancel(hostname string) {
	ic.downloadCountMu.Lock()

	defer ic.downloadCountMu.Unlock()

	requests := ic.downloadProcessesByHostname[hostname]
	fmt.Println("cancel first req for hostname ", hostname)

	if len(requests) > 0 {
		fmt.Printf("cancel req %v\n", requests[0])
		if requests[0].cancel != nil {
			fmt.Println("cancel called")
			requests[0].cancel()
		}
		ic.portsPool.Give(requests[0].port)
		ic.downloadProcessesByHostname[hostname] = requests[1:]
	}

	queue := ic.downloadQueue[hostname]

	if len(queue) == 0 {
		return
	}

	nextReq := queue[0]
	queue = queue[1:]

	ic.downloadQueue[hostname] = queue

	ic.downloadRequestsChan <- nextReq
}

func (ic *ircClient) resume(w *ircWriter, querier string, message []byte) {
	w.Privmsg(querier, "DCC RESUME is experimental, be warned.")
	dlr, filename, port, readAt := ic.decodeResume(message)
	fmt.Println("obtained resume request: ", dlr, filename, port, readAt)

	if dlr.Pack == "" || dlr.Nick == "" || filename == "" || readAt == 0 {
		w.Privmsg(querier, "DCC RESUME request is invalid or was improperly passed, please XDCC CANCEL and try again.")
		return
	}

	ic.downloadCountMu.Lock()

	requests := ic.downloadProcessesByHostname[dlr.Hostname]

	if len(requests) > 0 {
		found := false
		index := 0
		process := downloadProcess{}

		for i, p := range requests {
			if p.downloadRequest.Pack == dlr.Pack && p.downloadRequest.Nick == dlr.Nick && !found {
				found = true

				index = i
				process = p

				continue
			}
		}

		if found {
			req := process.downloadRequest
			req.offset = int64(readAt)
			process.downloadRequest = req
			requests[index] = process
			ic.downloadProcessesByHostname[dlr.Hostname] = requests
		}
	}

	ic.downloadCountMu.Unlock()

	// send accept
	w.Privmsg(dlr.Nick, "\x01DCC ACCEPT \"%v\" %d %d\x01", filename, port, readAt)
}

func (ic *ircClient) dequeue(hostname string, request downloadRequest) {
	ic.downloadCountMu.Lock()
	defer ic.downloadCountMu.Unlock()

	processes := ic.downloadProcessesByHostname[hostname]

	if len(processes) > 0 {
		once := true
		newProcesses := make([]downloadProcess, 0, len(processes)-1)

		for _, p := range processes {
			if p.downloadRequest.Pack == request.Pack && once {
				once = false
				continue
			}

			newProcesses = append(newProcesses, p)
		}

		ic.downloadProcessesByHostname[hostname] = newProcesses
	}

	queue := ic.downloadQueue[hostname]

	if len(queue) == 0 {
		return
	}

	nextReq := queue[0]
	queue = queue[1:]

	ic.downloadQueue[hostname] = queue

	ic.downloadRequestsChan <- nextReq
}

const defaultTimeout = time.Second * 180

func (ic *ircClient) processDownloadRequest(w *ircWriter, req downloadRequest) error {
	if req.Nick == "" || req.Pack == "" {
		return errors.New("request is invalid")
	}
	fmt.Printf("processing request %v\n", req)

	// open file before conn
	f, err := ic.fileFetcher.Get(req.Pack)

	if err != nil {
		return err
	}

	stats, err := f.Stat()

	if err != nil {
		f.Close()
		return err
	}

	port := ic.portsPool.Take()

	go func() {
		defer func() {
			if err := recover(); err != nil {
				if strErr, ok := err.(string); ok && strings.Contains(strErr, "transfer reaped") {
					fmt.Println("transfer reaped")

					return
				}
				fmt.Println("panicked during dcc serve: ", err)
				w.Privmsg(req.Nick, "Download request errored, please restart it.")
			}
		}()
		defer f.Close()
		defer ic.portsPool.Give(port)

		ctx, cancelContext := context.WithTimeout(context.Background(), defaultTimeout)

		defer cancelContext()

		lc := net.ListenConfig{
			Control: func(network, address string, c syscall.RawConn) error {
				log.Printf("listening on network %v, address %v\n", network, address)

				return nil
			},
		}

		listener, err := lc.Listen(ctx, "tcp", fmt.Sprintf("%v:%v", ic.localIP, port))

		if err != nil {
			panic(err)
		}

		defer listener.Close()

		if l, ok := listener.(*net.TCPListener); ok {
			l.SetDeadline(time.Now().Add(defaultTimeout))
		}

		ic.alterProcess(
			req,
			func(dp *downloadProcess) {
				if dp == nil {
					return
				}
				dp.cancel = func() {
					fmt.Printf("cancel called for req %v\n", req)
					cancelContext()
					listener.Close()
				}
				dp.port = port
			},
		)

		w.Privmsg(req.Nick, "\x01DCC SEND \"%v\" %d %d %d\x01", stats.Name(), ic.myIP, port, stats.Size())

		conn, err := listener.Accept()

		var offset int64 = 0
		ic.alterProcess(
			req,
			func(dp *downloadProcess) {
				if dp == nil {
					return
				}
				oldCancel := dp.cancel
				dp.cancel = func() {
					fmt.Printf("kill conn req %v\n", req)
					oldCancel()
					if conn != nil {
						conn.Close()
					}
				}
				offset = dp.offset
			},
		)

		// "reaper"
		defer func() {
			time.Sleep(time.Minute * 60)
			ic.portsPool.Give(port)
			ic.dequeue(req.Hostname, req)
			conn.Close()
			listener.Close()
			panic("transfer reaped")
		}()

		if err != nil {
			fmt.Printf("accept fucked up with error %v\n", err.Error())

			if err, ok := err.(*net.OpError); ok && err.Timeout() {
				w.Privmsg(req.Nick, "Download request timed out")
			}

			if strings.Contains(err.Error(), "use of closed network connection") {
				return
			}

			w.Privmsg(req.Nick, "Download request errored, please restart it.")
			return
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetLinger(5)
		}

		fmt.Println("sending with offset ", offset, " and bound ", stats.Size()-req.offset)
		transferred, err := io.Copy(
			conn,
			io.NewSectionReader(f.(io.ReaderAt), offset, stats.Size()-req.offset),
		)

		fmt.Printf("transferred %d bytes\n", transferred)

		if err != nil {
			fmt.Printf("copy fucked up with error %v\n", err.Error())

			w.Privmsg(req.Nick, "Download request errored, please restart it.")
		}

		time.Sleep(time.Second * 2)
		listener.Close()
		conn.Close()
		ic.dequeue(req.Hostname, req)
		ic.portsPool.Give(port)

		w.Privmsg(req.Nick, "This bot is experimental. If you encountered any issue with your download, please reach out to Exterminata on IRC or Ina on the nibl discord.")
	}()

	return nil
}

func (ic *ircClient) alterProcess(req downloadRequest, callback func(*downloadProcess)) {
	ic.downloadCountMu.Lock()
	defer ic.downloadCountMu.Unlock()

	var process *downloadProcess

	processes := ic.downloadProcessesByHostname[req.Hostname]
	index := 0

	for i, p := range processes {
		if p.downloadRequest.Hostname == req.Hostname && p.downloadRequest.Nick == req.Nick && p.downloadRequest.Pack == req.Pack {
			index = i
			process = &p
		}
	}

	callback(process)

	if process != nil {
		processes[index] = *process
		ic.downloadProcessesByHostname[req.Hostname] = processes
	}
}

func extractQH(message []byte) (string, string) {
	pieces := bytes.Split(message, []byte(" "))

	if len(pieces) <= 1 {
		return "", ""
	}

	nodots := bytes.TrimPrefix(pieces[0], []byte(":"))

	usernameBits := bytes.Split(nodots, []byte("!"))

	if len(usernameBits) < 1 {
		return "", ""
	}

	hostnameBits := bytes.Split(nodots, []byte("@"))

	if len(hostnameBits) <= 1 {
		return "", ""
	}

	return string(usernameBits[0]), string(hostnameBits[1])
}

func decodeSend(message []byte) downloadRequest {
	// :Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :XDCC SEND
	noDots, found := bytes.CutPrefix(message, []byte(":"))

	if !found {
		return downloadRequest{}
	}

	username, hostname := extractQH(message)

	if username == "" || hostname == "" {
		return downloadRequest{}
	}

	packNumberPieces := bytes.Split(bytes.ToUpper(noDots), []byte("DCC SEND "))

	if len(packNumberPieces) <= 1 {
		return downloadRequest{}
	}

	return downloadRequest{
		Nick:     string(username),
		Hostname: string(hostname),
		Pack:     string(packNumberPieces[1]),
	}
}

func (ic *ircClient) decodeResume(message []byte) (downloadRequest, string, int, int) {
	noDots, found := bytes.CutPrefix(message, []byte(":"))

	if !found {
		return downloadRequest{}, "", 0, 0
	}

	username, hostname := extractQH(message)

	if username == "" || hostname == "" {
		return downloadRequest{}, "", 0, 0
	}

	resumeDataPieces := bytes.Split(noDots, []byte("DCC RESUME "))

	if len(resumeDataPieces) <= 1 {
		return downloadRequest{}, "", 0, 0
	}

	resumeData := bytes.Split(
		bytes.TrimSuffix(resumeDataPieces[1], []byte("\x01")),
		[]byte(" "),
	)

	if len(resumeData) < 3 {
		return downloadRequest{}, "", 0, 0
	}

	filename := strings.Trim(string(bytes.Join(resumeData[0:len(resumeData)-2], []byte(" "))), "\"")

	portStr := string(resumeData[len(resumeData)-2])
	port, err := strconv.Atoi(portStr)

	if err != nil {
		return downloadRequest{}, "", 0, 0
	}

	readAtStr := string(resumeData[len(resumeData)-1])
	readAt, err := strconv.Atoi(readAtStr)

	if err != nil {
		return downloadRequest{}, "", 0, 0
	}

	fmt.Println("lookup filename ", filename)
	packID, err := ic.fileFetcher.Lookup(filename)

	if err != nil {
		return downloadRequest{}, "", 0, 0
	}

	return downloadRequest{
		Nick:     string(username),
		Hostname: string(hostname),
		Pack:     packID,
	}, filename, port, readAt
}

type downloadRequest struct {
	Nick     string
	Hostname string
	Pack     string
	offset   int64
}

type downloadProcess struct {
	downloadRequest
	cancel func()
	port   int
}

// one client for one server
type ircClient struct {
	fileFetcher fsHandler

	connMutex sync.Mutex
	nickname  string
	channel   string
	myIP      uint32
	localIP   string

	// channel stores requests by clients, they will be processed when a slot is free
	downloadRequestsChan chan downloadRequest

	// store ports in a sequential chan, pick first one
	portsPool *portsPool

	// ratelimit mechanism
	rateLimit                   int
	downloadCountMu             sync.Mutex
	downloadProcessesByHostname map[string][]downloadProcess
	// maps a currently running DL request to a port
	downloadSlots map[int]downloadProcess
	// stores requests by clients, they will be processed when a slot is free, key is hostname
	downloadQueue map[string][]downloadRequest

	shutdownChan chan struct{}
}

func ip2int(ip net.IP) uint32 {
	if len(ip) == 16 {
		return binary.BigEndian.Uint32(ip[12:16])
	}

	return binary.BigEndian.Uint32(ip)
}

var rootSavePath = getEnvFallback("ROOT_FOLDER", "/anime")

func getEnvFallback(key, def string) string {
	val, ok := os.LookupEnv(key)

	if ok {
		return val
	}

	return def
}

func mustExtractIP() uint32 {
	req, err := http.NewRequest(http.MethodGet, "https://ifconfig.me", http.NoBody)

	if err != nil {
		panic(err)
	}

	req.Header.Add("User-Agent", "curl")

	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	strIP, err := io.ReadAll(resp.Body)

	if err != nil {
		panic(err)
	}

	ip := net.ParseIP(string(strIP))

	if ip == nil {
		panic("invalid self ip")
	}

	return ip2int(ip)
}

const (
	registrationPassword = ""
)

func main() {
	fsAccess := os.DirFS(rootSavePath)
	lookupID, lookupPack := mustBuildLookups(fsAccess)

	joinChan := getEnvFallback("CHANNEL", "#exter-periment")
	localIP := getEnvFallback("LOCAL_IP", "192.168.1.68")
	portsStr := strings.Split(getEnvFallback("PORTS", "38542,38543"), ",")

	ports := make([]int, 0, len(portsStr))

	for _, ps := range portsStr {
		pint, _ := strconv.Atoi(ps)

		ports = append(ports, pint)
	}

	pool := newPortsPool(ports)

	c := ircClient{
		nickname:                    "Fulminata|Experiment",
		channel:                     joinChan,
		localIP:                     localIP,
		myIP:                        mustExtractIP(),
		portsPool:                   pool,
		rateLimit:                   1,
		downloadProcessesByHostname: make(map[string][]downloadProcess, 65535),
		downloadRequestsChan:        make(chan downloadRequest, 65535),
		downloadQueue:               make(map[string][]downloadRequest, 65535),
		fileFetcher: fsHandler{
			fsAccess:   fsAccess,
			pool:       pool,
			lookupID:   lookupID,
			lookupPack: lookupPack,
		},
	}

	reportingTicker := time.NewTicker(time.Minute)

	c.connect()

	for {
		<-reportingTicker.C
		fmt.Println("Open ports: ", slices.Collect(maps.Keys(c.portsPool.openPorts)))
		fmt.Println("Requests by hostname:")
		for hn, reqs := range c.downloadProcessesByHostname {
			fmt.Println(hn, reqs)
		}
	}
}
