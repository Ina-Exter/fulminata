package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"syscall"
	"time"
)

type listenerBuilderFactory struct {
	externalIP   uint32
	localIP      string
	certificates []tls.Certificate
}

func (lbf listenerBuilderFactory) build(prot Protocol, port int) listenerBuilder {
	switch prot {
	case DCC, XDCC:
		return tcpListenerBuilder{
			externalIP: lbf.externalIP,
			localIP:    lbf.localIP,
			port:       port,
		}
	case SDCC:
		return tcpOverTLSListenerBuilder{
			externalIP: lbf.externalIP,
			localIP:    lbf.localIP,
			port:       port,
			certs:      lbf.certificates,
		}
	}

	return erroredListenerBuilder{}
}

type listenerBuilder interface {
	listener(context.Context) (net.Listener, error)
	advertise(*ircWriter, string, fs.FileInfo) (int, error)
}

type erroredListenerBuilder struct{}

func (erroredListenerBuilder) listener(context.Context) (net.Listener, error) {
	return nil, errors.New("invalid DCC protocol")
}

func (erroredListenerBuilder) advertise(*ircWriter, string, fs.FileInfo) (int, error) {
	return 0, errors.New("invalid DCC protocol")
}

type tcpListenerBuilder struct {
	externalIP uint32
	localIP    string
	port       int
}

func (tlb tcpListenerBuilder) listener(ctx context.Context) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			log.Printf("listening for TCP on network %v, address %v\n", network, address)

			return nil
		},
	}

	listener, err := lc.Listen(ctx, "tcp", fmt.Sprintf("%v:%v", tlb.localIP, tlb.port))

	if err != nil {
		return nil, err
	}

	if l, ok := listener.(*net.TCPListener); ok {
		l.SetDeadline(time.Now().Add(defaultTimeout))
	}

	return listener, nil
}

func (tlb tcpListenerBuilder) advertise(w *ircWriter, nick string, stats fs.FileInfo) (int, error) {
	return w.Privmsg(nick, "\x01DCC SEND \"%v\" %d %d %d\x01", stats.Name(), tlb.externalIP, tlb.port, stats.Size())
}

type tcpOverTLSListenerBuilder struct {
	externalIP uint32
	localIP    string
	port       int

	certs []tls.Certificate
}

func (totlb tcpOverTLSListenerBuilder) listener(ctx context.Context) (net.Listener, error) {
	if len(totlb.certs) == 0 {
		return nil, errors.New("no tls certificates given")
	}

	config := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: totlb.certs,
	}

	tcpListener, err := tcpListenerBuilder{
		externalIP: totlb.externalIP,
		localIP:    totlb.localIP,
		port:       totlb.port,
	}.listener(ctx)

	if err != nil {
		return nil, err
	}

	return tls.NewListener(tcpListener, config), nil
}

func (totlb tcpOverTLSListenerBuilder) advertise(w *ircWriter, nick string, stats fs.FileInfo) (int, error) {
	return w.Privmsg(nick, "\x01DCC SSEND \"%v\" %d %d %d\x01", stats.Name(), totlb.externalIP, totlb.port, stats.Size())
}
