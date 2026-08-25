package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecodeSend(t *testing.T) {
	for _, tt := range []struct {
		name         string
		haveMsg      []byte
		wantNick     string
		wantHost     string
		wantPack     string
		wantProtocol Protocol
	}{
		{
			name:         "with sharp",
			haveMsg:      []byte(":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :\x01XDCC SEND #1\x01"),
			wantNick:     "Exterminata",
			wantHost:     "AFC539C.716A7309.A137196A.IP",
			wantPack:     "1",
			wantProtocol: XDCC,
		},
		{
			name:         "without sharp",
			haveMsg:      []byte(":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :\x01XDCC SEND 1\x01"),
			wantNick:     "Exterminata",
			wantHost:     "AFC539C.716A7309.A137196A.IP",
			wantPack:     "1",
			wantProtocol: XDCC,
		},
		{
			name:         "without x01 headers",
			haveMsg:      []byte(":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :XDCC SEND 1"),
			wantNick:     "Exterminata",
			wantHost:     "AFC539C.716A7309.A137196A.IP",
			wantPack:     "1",
			wantProtocol: XDCC,
		},
		{
			name:         "with DCC",
			haveMsg:      []byte(":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :\x01DCC SEND #1\x01"),
			wantNick:     "Exterminata",
			wantHost:     "AFC539C.716A7309.A137196A.IP",
			wantPack:     "1",
			wantProtocol: DCC,
		},
		{
			name:         "with SDCC",
			haveMsg:      []byte(":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :\x01SDCC SEND #1\x01"),
			wantNick:     "Exterminata",
			wantHost:     "AFC539C.716A7309.A137196A.IP",
			wantPack:     "1",
			wantProtocol: SDCC,
		},
		{
			name:         "DCC SSEND parsed as SDCC SEND",
			haveMsg:      []byte(":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :\x01DCC SSEND #1\x01"),
			wantNick:     "Exterminata",
			wantHost:     "AFC539C.716A7309.A137196A.IP",
			wantPack:     "1",
			wantProtocol: SDCC,
		},
		{
			name:         "XDCC SSEND parsed as SDCC SEND",
			haveMsg:      []byte(":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :\x01XDCC SSEND #1\x01"),
			wantNick:     "Exterminata",
			wantHost:     "AFC539C.716A7309.A137196A.IP",
			wantPack:     "1",
			wantProtocol: SDCC,
		},
		{
			name:         "with SXDCC",
			haveMsg:      []byte(":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :\x01SXDCC SEND #10\x01"),
			wantNick:     "Exterminata",
			wantHost:     "AFC539C.716A7309.A137196A.IP",
			wantPack:     "10",
			wantProtocol: SXDCC,
		},
		{
			name:         "with extra spaces",
			haveMsg:      []byte(":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP    PRIVMSG    Fulminata|Experiment   :\x01DCC   SEND   #1\x01"),
			wantNick:     "Exterminata",
			wantHost:     "AFC539C.716A7309.A137196A.IP",
			wantPack:     "1",
			wantProtocol: DCC,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			msg := Parse(tt.haveMsg)

			req := decodeSend(msg.(VerbMsg))

			assert.Equal(t, tt.wantNick, req.Nick)
			assert.Equal(t, tt.wantHost, req.Hostname)
			assert.Equal(t, tt.wantPack, req.Pack)
			assert.Equal(t, tt.wantProtocol, req.Protocol)
		})
	}
}

func TestIRCClient_DecodeResume(t *testing.T) {
	for _, tt := range []struct {
		name         string
		haveMsg      []byte
		wantNick     string
		wantHost     string
		wantPack     string
		wantFilename string
		wantPort     int
		wantReadAt   int
	}{
		{
			name:         "quoted",
			haveMsg:      []byte(":Exterminata!~Extermina@Rizon-96DE5C73.rev.sfr.net PRIVMSG Fulminata|Experiment :\x01DCC RESUME \"[SubsPlease] 16bit Sensation - Another Layer - 01 (1080p) [C13E9494].mkv\" 38545 524844064\x01"),
			wantNick:     "Exterminata",
			wantHost:     "Rizon-96DE5C73.rev.sfr.net",
			wantPack:     "1",
			wantFilename: "[SubsPlease] 16bit Sensation - Another Layer - 01 (1080p) [C13E9494].mkv",
			wantPort:     38545,
			wantReadAt:   524844064,
		},
		{
			name:         "unquoted",
			haveMsg:      []byte(":Exterminata!~Extermina@Rizon-96DE5C73.rev.sfr.net PRIVMSG Fulminata|Experiment :\x01DCC RESUME fulminata.txt 38546 94120\x01"),
			wantNick:     "Exterminata",
			wantHost:     "Rizon-96DE5C73.rev.sfr.net",
			wantPack:     listPack,
			wantFilename: listFilename,
			wantPort:     38546,
			wantReadAt:   94120,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ic := &ircClient{
				fileFetcher: &fsHandler{
					lookupID: map[string]PackInfo{
						"1": {Name: "[SubsPlease] 16bit Sensation - Another Layer - 01 (1080p) [C13E9494].mkv"},
					},
				},
			}

			msg := Parse(tt.haveMsg)

			req, filename, port, readAt := ic.decodeResume(msg.(VerbMsg))

			assert.Equal(t, tt.wantNick, req.Nick)
			assert.Equal(t, tt.wantHost, req.Hostname)
			assert.Equal(t, tt.wantPack, req.Pack)
			assert.Equal(t, tt.wantFilename, filename)
			assert.Equal(t, tt.wantPort, port)
			assert.Equal(t, tt.wantReadAt, readAt)
		})
	}
}

func TestDCCRegexp(t *testing.T) {
	for _, tt := range []struct {
		name      string
		haveMsg   string
		wantProto string
		wantVerb  string
	}{
		{
			name:      "xdcc send, caps",
			haveMsg:   "XDCC SEND #1",
			wantProto: "XDCC",
			wantVerb:  "SEND",
		},
		{
			name:      "dcc send, caps",
			haveMsg:   "DCC SEND #1",
			wantProto: "DCC",
			wantVerb:  "SEND",
		},
		{
			name:      "dcc list, no caps",
			haveMsg:   "dcc list",
			wantProto: "DCC",
			wantVerb:  "LIST",
		},
		{
			name:      "xdcc list, no caps",
			haveMsg:   "xdcc list",
			wantProto: "XDCC",
			wantVerb:  "LIST",
		},
		{
			name:      "dcc cancel, mixed caps",
			haveMsg:   "DCC cAnCEl",
			wantProto: "DCC",
			wantVerb:  "CANCEL",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			matches := dccRegexp.FindStringSubmatch(tt.haveMsg)
			assert.Equal(t, tt.wantProto, strings.ToUpper(matches[1]))
			assert.Equal(t, tt.wantVerb, strings.ToUpper(matches[2]))
		})
	}
}
