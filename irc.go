package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

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
	fileFetcher *fsHandler

	nickname                  string
	registrationPassword      string
	channel                   string
	myIP                      uint32
	localIP                   string
	handshakeAndJoinCompleted bool

	// channel stores requests by clients, they will be processed when a slot is free
	downloadRequestsChan chan downloadRequest

	// store ports in a sequential chan, pick first one
	portsPool *portsPool

	// ratelimit mechanism
	rateLimit                   int
	downloadCountMu             sync.Mutex
	downloadProcessesByHostname map[string][]downloadProcess
	// stores requests by clients, they will be processed when a slot is free, key is hostname
	downloadQueue map[string][]downloadRequest

	shutdownChans map[string]chan struct{}
	errChan       chan error
}

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

func (ircw *ircWriter) Notice(to, format string, args ...any) (int, error) {
	return ircw.Writer.Write(fmt.Appendf([]byte{}, fmt.Sprintf("NOTICE %v :", to)+format, args...))
}

func (ic *ircClient) connect() error {
	fmt.Println("client connecting")

	// 6667 for tcp, 6697 for tls over tcp
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

			if err := outScan.Err(); err != nil {
				ic.errChan <- err
			}
		}
	}()

	fmt.Println("send handshake")
	in := &ircWriter{&writeFlusher{w: conn}}
	in.Write(fmt.Appendf([]byte{}, "USER fulminata * 0 :%s\n", ic.nickname))
	in.Write(fmt.Appendf([]byte{}, "NICK %s\n", ic.nickname))

	// irc side background handler loop
	go func() {
		for {
			select {
			case message := <-messageChan: // message received on serv, process accordingly
				ic.ircMessageHandler(in, message)
			case <-ic.shutdownChans["irc"]:
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
			case <-ic.shutdownChans["dcc"]:
				return
			}
		}
	}()

	return nil
}

func (ic *ircClient) ircMessageHandler(in *ircWriter, message []byte) {
	ifMsg := Parse(message)

	switch msg := ifMsg.(type) {
	case PingMessage:
		in.Write([]byte("PONG " + string(msg.Payload)))
	case VerbMsg:
		if msg.SystemMessage() {
			ic.handleSystemMessage(in, msg)

			return
		}

		if !msg.IsPrivmsgToMe(ic.nickname) {
			// only direct privmsg supported yet
			return
		}

		ic.handlePrivMsg(in, msg)
	}
}

func (ic *ircClient) handleSystemMessage(in *ircWriter, msg VerbMsg) {
	// MOTD finished: join now
	if bytes.Contains([]byte(msg.Message), []byte("End of /MOTD command.")) {
		if !ic.handshakeAndJoinCompleted {
			// nick registration
			if ic.registrationPassword != "" {
				in.Privmsg("NickServ", "IDENTIFY %s", ic.registrationPassword)
			}
			// leave time for roundtrip
			time.Sleep(time.Second)

			fmt.Println("send join")
			in.Write(fmt.Appendf([]byte{}, "JOIN %s\n", ic.channel))

			ic.handshakeAndJoinCompleted = true
		}
	}
}

func (ic *ircClient) handlePrivMsg(in *ircWriter, msg VerbMsg) {
	payload := strings.Trim(msg.Message, "\x01")
	// for header purposes
	upperPayload := strings.ToUpper(payload)

	switch {
	case strings.HasPrefix(upperPayload, "PING "):
		data, _ := strings.CutPrefix(payload, "PING ")

		in.Notice(msg.Nick, "\x01PONG %s\x01", data)
	case strings.HasPrefix(upperPayload, "VERSION"):
		in.Notice(msg.Nick, "\x01VERSION fulminata-v0.0.1\x01")
	case strings.HasPrefix(upperPayload, "CLIENTINFO"):
		in.Notice(msg.Nick, "\x01DCC XDCC WEBSITE CLIENTINFO PING VERSION\x01")
	case strings.HasPrefix(upperPayload, "WEBSITE"):
		in.Notice(msg.Nick, "\x01WEBSITE https://github.com/ina-exter/fulminata\x01")
	case strings.HasPrefix(upperPayload, "SOURCE"):
		in.Notice(msg.Nick, "\x01SOURCE https://github.com/ina-exter/fulminata\x01")
	case strings.Contains(upperPayload, "DCC "):
		ic.handleDCCMessage(in, msg)
	default:
		in.Notice(msg.Nick, "Unimplemented CTCP action, don't hesitate to open a github issue (link with CTCP WEBSITE)")
	}
}

var dccRegexp = regexp.MustCompile("((?:S|s)?(?:X|x)?(?:DCC|dcc)) +([^ ]*) ?(.*)")

func (ic *ircClient) handleDCCMessage(in *ircWriter, msg VerbMsg) {
	matches := dccRegexp.FindStringSubmatch(
		strings.Trim(msg.Message, "\x01"),
	)

	if len(matches) < 3 {
		in.Privmsg(msg.Nick, "Unrecognized DCC action.")
		return
	}

	// matches[1] contains (S)(X)DCC
	// matches[2] is the verb

	switch strings.ToUpper(matches[2]) {
	case "SEND":
		ic.send(in, msg)
	case "LIST":
		in.Privmsg(msg.Nick, "XDCC LIST Denied: too big for irc. Use /msg %s XDCC SEND list", ic.nickname)
	case "CANCEL":
		ic.cancel(msg.Hostname)
	case "RESUME":
		ic.resume(in, msg)
	case "BATCH":
		ic.batch(in, msg)
	default:
		in.Privmsg(msg.Nick, "Unimplemented DCC action.")
	}
}

func trimPackName(r rune) bool {
	return r == '\x01' || r == '#' || r == ' '
}

func (ic *ircClient) send(w *ircWriter, message VerbMsg) error {
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
	ic.enqueueDownloadRequests(w, []downloadRequest{req})

	return nil
}

func (ic *ircClient) enqueueDownloadRequests(w *ircWriter, reqs []downloadRequest) {
	if len(reqs) == 0 {
		return
	}

	nick := reqs[0].Nick
	hostname := reqs[0].Hostname

	if ic.portsPool.Free() == 0 {
		w.Privmsg(nick, "Slots are full, please be patient")
	}

	ic.downloadCountMu.Lock()

	defer ic.downloadCountMu.Unlock()

	processes := ic.downloadProcessesByHostname[hostname]
	messageSent := false

	for _, req := range reqs {
		processes = append(
			processes,
			downloadProcess{
				downloadRequest: req,
			},
		)

		if len(ic.downloadProcessesByHostname[hostname]) > ic.rateLimit {
			if !messageSent {
				messageSent = true
				w.Privmsg(nick, "Request queued: you have too many transfer pendings. Please finish or cancel pending transfers.")
			}

			ic.downloadQueue[hostname] = append(ic.downloadQueue[hostname], req)
		} else {
			ic.downloadRequestsChan <- req
		}
	}

	ic.downloadProcessesByHostname[hostname] = processes
}

func (ic *ircClient) batch(w *ircWriter, message VerbMsg) {
	packNumbersPieces := bytes.Split(
		bytes.ToUpper([]byte(message.Message)), []byte("DCC BATCH "),
	)

	if len(packNumbersPieces) < 2 {
		w.Privmsg(message.Nick, "Invalid batch cmd.")

		return
	}

	packNumbers := strings.Split(string(packNumbersPieces[1]), ",")

	reqs := make([]downloadRequest, 0, len(packNumbers))

	for _, spn := range packNumbers {
		packID := strings.TrimFunc(
			string(spn),
			trimPackName,
		)

		if !ic.fileFetcher.Exists(packID) {
			w.Privmsg(message.Nick, "Pack not found")

			return
		}

		reqs = append(
			reqs,
			downloadRequest{
				Nick:     message.Nick,
				Hostname: message.Hostname,
				Pack:     packID,
			},
		)
	}

	ic.enqueueDownloadRequests(w, reqs)
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

func (ic *ircClient) resume(w *ircWriter, message VerbMsg) {
	w.Privmsg(message.Nick, "DCC RESUME is experimental, be warned.")
	dlr, filename, port, readAt := ic.decodeResume(message)
	fmt.Println("obtained resume request: ", dlr, filename, port, readAt)

	if dlr.Pack == "" || dlr.Nick == "" || filename == "" || readAt == 0 {
		w.Privmsg(message.Nick, "DCC RESUME request is invalid or was improperly passed, please XDCC CANCEL and try again.")
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

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.r.Read(p)
	}
}

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

	ctx, cancelContext := context.WithTimeout(context.Background(), defaultTimeout)

	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			log.Printf("listening on network %v, address %v\n", network, address)

			return nil
		},
	}

	listener, err := lc.Listen(ctx, "tcp", fmt.Sprintf("%v:%v", ic.localIP, port))

	if err != nil {
		cancelContext()

		return err
	}

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
				ic.portsPool.Give(port)
				listener.Close()
			}
			dp.port = port
		},
	)

	go func(ctx context.Context) {
		defer func() {
			if err := recover(); err != nil {
				if strErr, ok := err.(string); ok && strings.Contains(strErr, "transfer reaped") {
					fmt.Println("transfer reaped")

					return
				}
				fmt.Println("panicked during dcc serve: ", err)
			}
		}()
		defer f.Close()
		defer ic.portsPool.Give(port)
		defer cancelContext()
		defer listener.Close()

		w.Privmsg(req.Nick, "\x01DCC SEND \"%v\" %d %d %d\x01", stats.Name(), ic.myIP, port, stats.Size())

		conn, err := listener.Accept()

		var offset int64 = 0

		// only used to obtain offset in case of RESUME
		ic.alterProcess(
			req,
			func(dp *downloadProcess) {
				if dp == nil {
					return
				}
				offset = dp.offset
			},
		)

		// "reaper"
		defer func() {
			time.Sleep(time.Minute * 60)
			ic.portsPool.Give(port)
			ic.dequeue(req.Hostname, req)
			cancelContext()
			conn.Close()
			listener.Close()
			panic("transfer reaped")
		}()

		if err != nil {
			fmt.Printf("accept fucked up with error %v\n", err.Error())

			return
		}

		fmt.Println("sending with offset", offset, "and bound", stats.Size()-req.offset)
		transferred, err := io.Copy(
			conn,
			&contextReader{
				ctx: ctx,
				r:   io.NewSectionReader(f.(io.ReaderAt), offset, stats.Size()-req.offset),
			},
		)

		fmt.Printf("transferred %d bytes\n", transferred)

		if err != nil {
			fmt.Printf("copy fucked up with error %v\n", err.Error())
		}

		time.Sleep(time.Second * 2)
		listener.Close()
		conn.Close()
		ic.dequeue(req.Hostname, req)
		ic.portsPool.Give(port)

		w.Privmsg(req.Nick, "This bot is experimental. If you encountered any issue with your download, please reach out to Exterminata on IRC or Ina on the nibl discord.")
	}(ctx)

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

func decodeSend(message VerbMsg) downloadRequest {
	// :Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :XDCC SEND
	packNumberPieces := bytes.Split(
		bytes.ToUpper([]byte(message.Message)), []byte("DCC SEND "),
	)

	if len(packNumberPieces) < 2 {
		return downloadRequest{}
	}

	return downloadRequest{
		Nick:     message.Nick,
		Hostname: message.Hostname,
		Pack: strings.TrimFunc(
			string(packNumberPieces[1]),
			trimPackName,
		),
	}
}

func (ic *ircClient) decodeResume(message VerbMsg) (downloadRequest, string, int, int) {
	resumeDataPieces := bytes.Split([]byte(message.Message), []byte("DCC RESUME "))

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
		Nick:     message.Nick,
		Hostname: message.Hostname,
		Pack:     packID,
	}, filename, port, readAt
}
