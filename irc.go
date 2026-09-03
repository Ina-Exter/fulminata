package main

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type Protocol string
type Verb string

const (
	DCC   Protocol = "DCC"
	XDCC  Protocol = "XDCC"
	SDCC  Protocol = "SDCC"
	SXDCC Protocol = "SXDCC"

	maxIRCLineBytes = 510
)

var errIRCLineTooLong = errors.New("irc line exceeds size limit")

type downloadRequestWithID struct {
	downloadRequest
	uuid.UUID
}

type downloadRequest struct {
	Protocol Protocol
	Nick     string
	Hostname string
	Pack     string
}

type downloadProcess struct {
	id uuid.UUID
	*downloadRequest
	*downloadTransfer
}

func newDownloadProcess(req *downloadRequest) downloadProcess {
	return downloadProcess{
		id:               uuid.New(),
		downloadRequest:  req,
		downloadTransfer: &downloadTransfer{},
	}
}

type downloadTransfer struct {
	contextCancel context.CancelFunc
	listener      net.Listener
	conn          net.Conn
	file          io.ReadCloser
	port          int

	offsetMu sync.Mutex
	offset   int64

	cleanupOnce sync.Once
	cleanupMu   sync.Mutex
	cleaned     atomic.Bool
}

func (dt *downloadTransfer) cleanup() {
	dt.cleanupOnce.Do(
		func() {
			dt.cleanupMu.Lock()

			if dt.contextCancel != nil {
				dt.contextCancel()
			}

			for _, o := range []interface {
				Close() error
			}{
				dt.conn,
				dt.listener,
				dt.file,
			} {
				if o != nil {
					o.Close()
				}
			}

			dt.cleaned.Store(true)

			dt.cleanupMu.Unlock()
		},
	)
}

func (dt *downloadTransfer) registerConn(conn net.Conn) {
	dt.cleanupMu.Lock()

	dt.conn = conn

	if dt.cleaned.Load() {
		conn.Close()
	}

	dt.cleanupMu.Unlock()
}

func (dt *downloadTransfer) registerOffset(offset int64) {
	dt.offsetMu.Lock()
	defer dt.offsetMu.Unlock()

	dt.offset = offset
}

func (dt *downloadTransfer) getOffset() int64 {
	dt.offsetMu.Lock()
	defer dt.offsetMu.Unlock()

	return dt.offset
}

// one client for one server
type ircClient struct {
	fileFetcher FileHandler

	nickname                  string
	registrationPassword      string
	channel                   string
	handshakeAndJoinCompleted bool

	listenerBuilderFactory listenerBuilderFactory

	// channel stores requests by clients, they will be processed when a slot is free
	downloadRequestsChan chan downloadRequestWithID

	// store ports in a sequential chan, pick first one
	portsPool *portsPool

	// ratelimit mechanism
	rateLimit                   int
	downloadCountMu             sync.Mutex
	downloadProcessesByHostname map[string][]downloadProcess
	// stores requests by clients, they will be processed when a slot is free, key is hostname
	downloadQueue map[string][]downloadRequest

	ircErrChan chan error
}

type formatterWriter struct {
	w io.Writer
}

func (fw *formatterWriter) Write(p []byte) (int, error) {
	fmt.Println(string(p))

	cleaned := cleanIRCMessage(p)

	if len(cleaned) > maxIRCLineBytes {
		return 0, errIRCLineTooLong
	}

	return fw.w.Write(append(cleaned, []byte("\r\n")...))
}

type ircWriter struct {
	io.Writer
}

func cleanIRCMessage(in []byte) []byte {
	out := in

	for _, b := range [][]byte{{'\r'}, {'\n'}} {
		out = bytes.ReplaceAll(out, b, []byte{})
	}

	return out
}

func (ircw *ircWriter) Privmsg(to, format string, args ...any) (int, error) {
	return ircw.Writer.Write(
		fmt.Appendf(
			[]byte{},
			fmt.Sprintf("PRIVMSG %v :", to)+format,
			args...,
		),
	)
}

func (ircw *ircWriter) Notice(to, format string, args ...any) (int, error) {
	return ircw.Writer.Write(
		fmt.Appendf(
			[]byte{},
			fmt.Sprintf("NOTICE %v :", to)+format,
			args...,
		),
	)
}

func (ic *ircClient) connect(ctx context.Context, address string) (func() error, error) {
	fmt.Println("client connecting")

	// 6667 for tcp, 6697 for tls over tcp
	dialer := &tls.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", address)

	if err != nil {
		return nil, err
	}

	outScan := bufio.NewScanner(conn)
	in := &ircWriter{&formatterWriter{w: conn}}

	fmt.Println("start irc goroutine")
	// irc side background handler loop
	go func() {
	outer:
		for outScan.Scan() {
			select {
			case <-ctx.Done():
				conn.Close()

				break outer
			default:
				message := bytes.Clone(outScan.Bytes())
				fmt.Println(string(message))
				ic.ircMessageHandler(in, message)
			}
		}
		ic.ircErrChan <- cmp.Or(outScan.Err(), errors.New("call context cancelled or irc EOF"))
	}()

	fmt.Println("send handshake")

	if _, err := in.Write(fmt.Appendf([]byte{}, "USER fulminata * 0 :%s\n", ic.nickname)); err != nil {
		conn.Close()

		return nil, err
	}
	if _, err := in.Write(fmt.Appendf([]byte{}, "NICK %s\n", ic.nickname)); err != nil {
		conn.Close()

		return nil, err
	}

	// dcc side background handler loop
	go func() {
		for {
			select {
			case req := <-ic.downloadRequestsChan:
				if err := ic.processDownloadRequest(in, req); err != nil {
					ic.terminateDownloadProcess(in, req.Hostname, req.UUID)
					in.Privmsg(req.Nick, "Cannot process your download request: %s", err.Error())
				}
			case <-ctx.Done():
				ic.downloadCountMu.Lock()

				// flush all processes upon ctx done
				for hostname, process := range ic.downloadProcessesByHostname {
					for _, p := range process {
						trans := ic.extractProcess(hostname, p.id)
						trans.cleanup()
						ic.portsPool.Give(trans.port)
					}
				}

				ic.downloadCountMu.Unlock()

				return
			}
		}
	}()

	return conn.Close, nil
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

var dccRegexp = regexp.MustCompile("((?:S|s)?(?:X|x)?(?:DCC|dcc)) +([^ ]*) *(.*)")

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
	case "SEND", "SSEND":
		ic.send(in, msg)
	case "LIST":
		in.Privmsg(msg.Nick, "XDCC LIST Denied: too big for irc. Use /msg %s XDCC SEND list", ic.nickname)
	case "CANCEL":
		ic.cancel(in, msg)
	case "RESUME":
		ic.resume(in, msg)
	case "BATCH":
		ic.batch(in, msg)
	case "REMOVE":
		ic.remove(in, msg)
	default:
		in.Privmsg(msg.Nick, "Unimplemented DCC action.")
	}
}

func trimPackName(r rune) bool {
	return r == '\x01' || r == '#' || r == ' '
}

func (ic *ircClient) send(w *ircWriter, message VerbMsg) {
	req, err := ic.decodeSend(message)

	if err != nil {
		w.Privmsg(message.Nick, "DCC SEND request is invalid: %s", err.Error())

		return
	}

	ic.enqueueDownloadRequests(w, []downloadRequest{req})
}

func (ic *ircClient) decodeSend(message VerbMsg) (downloadRequest, error) {
	// :Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :XDCC SEND #5
	packNumberPieces := strings.Fields(
		strings.ToUpper(
			strings.Trim(message.Message, "\x01"),
		),
	)

	if len(packNumberPieces) < 3 {
		return downloadRequest{}, errors.New("invalid request structure")
	}

	proto := Protocol(packNumberPieces[0])

	if strings.ToUpper(packNumberPieces[1]) == "SSEND" {
		proto = SDCC
	}

	pack := strings.TrimFunc(
		string(packNumberPieces[2]),
		trimPackName,
	)

	if !ic.fileFetcher.Exists(pack) {
		return downloadRequest{}, errors.New("pack not found")
	}

	return downloadRequest{
		Protocol: proto,
		Nick:     message.Nick,
		Hostname: message.Hostname,
		Pack:     pack,
	}, nil
}

func (ic *ircClient) batch(w *ircWriter, message VerbMsg) {
	packNumbersPieces := strings.Fields(
		strings.ToUpper(message.Message),
	)

	if len(packNumbersPieces) < 3 {
		w.Privmsg(message.Nick, "Invalid batch cmd.")

		return
	}

	proto := Protocol(packNumbersPieces[0])
	packNumbers := strings.Split(string(packNumbersPieces[2]), ",")

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
				Protocol: proto,
				Nick:     message.Nick,
				Hostname: message.Hostname,
				Pack:     packID,
			},
		)
	}

	ic.enqueueDownloadRequests(w, reqs)
}

func (ic *ircClient) cancel(w *ircWriter, message VerbMsg) {
	ic.downloadCountMu.Lock()

	processes := ic.downloadProcessesByHostname[message.Hostname]

	if len(processes) == 0 {
		w.Privmsg(message.Nick, "You do not have a transfer running.")
		ic.downloadCountMu.Unlock()

		return
	}

	var targetProcessID, zero uuid.UUID

	for _, p := range processes {
		if p.Nick == message.Nick {
			targetProcessID = p.id

			break
		}
	}

	ic.downloadCountMu.Unlock()

	if targetProcessID == zero {
		w.Privmsg(message.Nick, "You do not have a transfer running.")

		return
	}

	fmt.Println("cancel first running req for hostname ", message.Hostname, "and nick", message.Nick)

	ic.terminateDownloadProcess(w, message.Hostname, targetProcessID)
}

type resumeRequest struct {
	downloadRequest
	filename string
	port     int
	readAt   int64
}

func (ic *ircClient) resume(w *ircWriter, message VerbMsg) {
	rreq, err := ic.decodeResume(message)
	fmt.Println("obtained resume request: ", rreq.downloadRequest, rreq.filename, rreq.port, rreq.readAt)

	if err != nil {
		w.Privmsg(message.Nick, "DCC RESUME request is invalid or was improperly passed, please XDCC CANCEL and try again: %s", err.Error())

		return
	}

	ic.downloadCountMu.Lock()

	requests := ic.downloadProcessesByHostname[rreq.Hostname]

	found := false

	if len(requests) > 0 {
		index := 0
		process := downloadProcess{}

		for i, p := range requests {
			if p.downloadRequest.Pack == rreq.Pack && p.downloadRequest.Nick == rreq.Nick && p.port == rreq.port && !found {
				found = true

				index = i
				process = p

				continue
			}
		}

		if found {
			process.registerOffset(rreq.readAt)
			requests[index] = process
			ic.downloadProcessesByHostname[rreq.Hostname] = requests
		}
	}

	ic.downloadCountMu.Unlock()

	if !found {
		w.Privmsg(rreq.Nick, "No request matches RESUME request. Either repeat request or download from start.")

		return
	}

	// send accept
	w.Privmsg(rreq.Nick, "\x01DCC ACCEPT \"%v\" %d %d\x01", rreq.filename, rreq.port, rreq.readAt)
}

func (ic *ircClient) decodeResume(message VerbMsg) (resumeRequest, error) {
	resumeData := strings.Fields(
		strings.Trim(message.Message, "\x01"),
	)

	if len(resumeData) <= 4 {
		return resumeRequest{}, errors.New("missing request element")
	}

	if !strings.EqualFold(resumeData[1], "resume") {
		return resumeRequest{}, errors.New("invalid verb")
	}

	filename := strings.Trim(strings.Join(resumeData[2:len(resumeData)-2], " "), "\"")

	portStr := resumeData[len(resumeData)-2]
	port, err := strconv.Atoi(portStr)

	if err != nil {
		return resumeRequest{}, errors.Wrap(err, "port is not a valid int")
	}

	if port <= 0 {
		return resumeRequest{}, errors.New("invalid port")
	}

	readAtStr := resumeData[len(resumeData)-1]
	readAt, err := strconv.ParseInt(readAtStr, 10, 64)

	if err != nil {
		return resumeRequest{}, errors.Wrap(err, "offset is not a valid int")
	}

	packID, err := ic.fileFetcher.Lookup(filename)

	if err != nil {
		return resumeRequest{}, errors.Wrap(err, "cannot lookup pack")
	}

	pack, err := ic.fileFetcher.Get(packID, false)

	if err != nil {
		return resumeRequest{}, errors.Wrap(err, "cannot get pack")
	}

	defer pack.Close()

	stats, err := pack.Stat()

	if err != nil {
		return resumeRequest{}, errors.Wrap(err, "cannot get pack stats")
	}

	if readAt < 0 || readAt >= stats.Size() {
		return resumeRequest{}, errors.New("requested offset is out of bounds")
	}

	return resumeRequest{
		Nick:     message.Nick,
		Hostname: message.Hostname,
		Pack:     packID,
		filename: filename,
		port:     port,
		readAt:   readAt,
	}, nil
}

func (ic *ircClient) remove(w *ircWriter, message VerbMsg) {
	w.Privmsg(message.Nick, "Unimplemented yet!")
}
