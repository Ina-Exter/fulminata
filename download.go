package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
)

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

	totalOccupied := len(ic.downloadProcessesByHostname[hostname]) + len(ic.downloadQueue[hostname])
	allowed := max(0, 100-totalOccupied)

	if allowed < len(reqs) {
		w.Privmsg(nick, "You may only have up to 100 transfers running and queued at a time. Packs above this limit will be ignored")

		reqs = reqs[:allowed]
	}

	free := ic.rateLimit - len(ic.downloadProcessesByHostname[hostname])

	toSend := reqs[:min(free, len(reqs))]
	toQueue := reqs[min(free, len(reqs)):]

	ic.downloadQueue[hostname] = append(ic.downloadQueue[hostname], toQueue...)
	toShip := make([]downloadRequestWithID, 0, len(toSend))
	processes := ic.downloadProcessesByHostname[hostname]

	for _, req := range toSend {
		p := newDownloadProcess(&req)
		processes = append(processes, p)
		toShip = append(toShip, downloadRequestWithID{downloadRequest: req, UUID: p.id})
	}

	ic.downloadProcessesByHostname[hostname] = processes

	ic.downloadCountMu.Unlock()

	if len(toQueue) > 0 {
		w.Privmsg(nick, "Request queued: you have too many transfer pendings. Please finish or cancel pending transfers.")
	}

	for _, req := range toShip {
		ic.downloadRequestsChan <- req
	}
}

func (ic *ircClient) terminateDownloadProcess(w *ircWriter, hostname string, id uuid.UUID) {
	ic.downloadCountMu.Lock()

	targetTransfer := ic.extractProcess(hostname, id)

	queue := ic.downloadQueue[hostname]

	if targetTransfer == nil || targetTransfer.cleaned.Load() {
		ic.downloadCountMu.Unlock()

		return
	}

	delete(ic.downloadQueue, hostname)
	ic.downloadCountMu.Unlock()

	// avoid cleaning up under lock
	targetTransfer.cleanup()
	ic.portsPool.Give(targetTransfer.port)
	ic.enqueueDownloadRequests(w, queue)
}

func (ic *ircClient) extractProcess(hostname string, id uuid.UUID) *downloadTransfer {
	var targetTransfer *downloadTransfer

	processes := ic.downloadProcessesByHostname[hostname]
	newProcesses := make([]downloadProcess, 0, max(len(processes)-1, 0))

	if len(processes) > 0 {
		for _, p := range processes {
			if p.id == id {
				if p.downloadTransfer != nil {
					targetTransfer = p.downloadTransfer
				}

				continue
			}

			newProcesses = append(newProcesses, p)
		}

		ic.downloadProcessesByHostname[hostname] = newProcesses
	}

	return targetTransfer
}

func (ic *ircClient) processDownloadRequest(w *ircWriter, req downloadRequestWithID) error {
	var zero uuid.UUID

	if req.Nick == "" || req.Pack == "" || req.UUID == zero {
		return errors.New("request is invalid")
	}
	fmt.Printf("processing request %v\n", req)

	// open file before conn
	f, err := ic.fileFetcher.Get(req.Pack, true)

	if err != nil {
		return err
	}

	stats, err := f.Stat()

	if err != nil {
		f.Close()

		return err
	}

	port := ic.portsPool.Take()

	listenerBuilder := ic.listenerBuilderFactory.build(req.Protocol, port)

	// TODO investigate context cancellations over listeners if it takes more than defaultTimeout
	acceptCtx, cancelAcceptCtx := context.WithTimeout(context.Background(), defaultTimeout)

	listener, err := listenerBuilder.listener(acceptCtx)

	if err != nil {
		cancelAcceptCtx()
		f.Close()
		ic.portsPool.Give(port)

		return err
	}

	ic.downloadCountMu.Lock()

	var (
		index   int
		process downloadProcess
		found   bool
	)

	processes := ic.downloadProcessesByHostname[req.Hostname]

	for i, p := range processes {
		if p.id == req.UUID {
			found = true
			index = i
			process = p
		}
	}

	if !found {
		ic.downloadCountMu.Unlock()
		cancelAcceptCtx()
		f.Close()
		ic.portsPool.Give(port)
		listener.Close()

		return errors.New("process not registered")
	}

	downloadCtx, cancelDownloadCtx := context.WithCancel(context.Background())

	process.contextCancel = func() {
		cancelAcceptCtx()
		cancelDownloadCtx()
	}
	process.listener = listener
	process.file = f
	process.port = port

	processes[index] = process

	ic.downloadProcessesByHostname[req.Hostname] = processes

	ic.downloadCountMu.Unlock()

	go func(ctx context.Context) {
		// "reaper"
		timer := time.AfterFunc(
			time.Minute*60,
			func() { ic.terminateDownloadProcess(w, req.Hostname, process.id) },
		)
		defer timer.Stop()

		defer func() {
			if err := recover(); err != nil {
				fmt.Println("panicked during dcc serve: ", err)
			}
			ic.terminateDownloadProcess(w, req.Hostname, process.id)
		}()

		if _, err := listenerBuilder.advertise(w, req.Nick, stats); err != nil {
			fmt.Printf("cannot build listener: %s\n", err.Error())
			w.Privmsg(req.Nick, "We could not prepare your transfer, please try again.")

			return
		}

		conn, err := listener.Accept()

		if err != nil {
			fmt.Printf("accept fucked up with error %v\n", err.Error())

			return
		}

		process.registerConn(conn)

		// only used to obtain offset in case of RESUME
		var offset int64 = 0

		if process.downloadTransfer != nil {
			offset = process.getOffset()
		}

		if offset >= stats.Size() {
			w.Privmsg(req.Nick, "Invalid offset requested")

			return
		}

		fmt.Println("sending with offset", offset, "and bound", stats.Size()-offset)

		fAt, ok := f.(io.ReaderAt)

		if !ok {
			fmt.Printf("file %v does not implement ReaderAt\n", stats.Name())

			return
		}

		transferred, err := io.Copy(
			conn,
			&contextReader{
				ctx: ctx,
				r:   io.NewSectionReader(fAt, offset, stats.Size()-offset),
			},
		)

		fmt.Printf("transferred %d bytes\n", transferred)

		if err != nil {
			fmt.Printf("copy fucked up with error %v\n", err.Error())

			return
		}

		time.Sleep(time.Second * 2)

		w.Privmsg(req.Nick, "This bot is experimental. If you encountered any issue with your download, please reach out to Exterminata on IRC or Ina on the nibl discord.")
	}(downloadCtx)

	return nil
}
