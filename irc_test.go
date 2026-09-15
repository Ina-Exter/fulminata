package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanIRCMessage(t *testing.T) {
	for _, tt := range []struct {
		name, in, out string
	}{
		{
			name: "no clean",
			in:   ":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :\x01XDCC SEND #1\x01",
			out:  ":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :\x01XDCC SEND #1\x01",
		},
		{
			name: "clean LF",
			in:   ":Exterminata!~Extermina@AFC539C.716A7309\n.A137196A.IP PRIVMSG Fulminata|Experiment :\x01XDCC SEND #1\x01",
			out:  ":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :\x01XDCC SEND #1\x01",
		},
		{
			name: "clean CR",
			in:   ":Exterminata!~Extermina@AFC539C.716A7309\r.A137196A.IP PRIVMSG Fulminata|Experiment :\x01XDCC SEND #1\x01",
			out:  ":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :\x01XDCC SEND #1\x01",
		},
		{
			name: "clean CRLF everywhere",
			in:   ":Ext\rerminata!~Extermina@AFC539C.716A7309\r\n.A137196A.IP PRIVMSG\n\r Fulminata|\nExperiment :\x01XDCC SEND #1\x01",
			out:  ":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :\x01XDCC SEND #1\x01",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, []byte(tt.out), cleanIRCMessage([]byte(tt.in)))
		})
	}
}

func TestIRCClient_DecodeSend(t *testing.T) {
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
			ic := &ircClient{
				fileFetcher: &fakeFileHandler{
					filesByPackID: map[string]*fakeFile{
						"1":  {},
						"10": {},
					},
				},
			}

			msg := Parse(tt.haveMsg)

			req, err := ic.decodeSend(msg.(VerbMsg))

			require.NoError(t, err)
			assert.Equal(t, tt.wantNick, req.Nick)
			assert.Equal(t, tt.wantHost, req.Hostname)
			assert.Equal(t, tt.wantPack, req.Pack)
			assert.Equal(t, tt.wantProtocol, req.Protocol)
		})
	}
}

func TestIRCClient_DecodeBatch(t *testing.T) {
	for _, tt := range []struct {
		name         string
		haveMsg      []byte
		wantProtocol Protocol
		wantNick     string
		wantHost     string
		wantPacks    []string
	}{
		{
			name:         "no spaces",
			haveMsg:      []byte(":Exterminata!~Extermina@Rizon-96DE5C73.rev.sfr.net PRIVMSG Fulminata|Experiment :XDCC BATCH 1,2,3,4,5"),
			wantProtocol: XDCC,
			wantNick:     "Exterminata",
			wantHost:     "Rizon-96DE5C73.rev.sfr.net",
			wantPacks:    []string{"1", "2", "3", "4", "5"},
		},
		{
			name:         "spaces galore",
			haveMsg:      []byte(":Exterminata!~Extermina@Rizon-96DE5C73.rev.sfr.net PRIVMSG Fulminata|Experiment :XDCC BATCH  1,   2,3, 4,   5   "),
			wantProtocol: XDCC,
			wantNick:     "Exterminata",
			wantHost:     "Rizon-96DE5C73.rev.sfr.net",
			wantPacks:    []string{"1", "2", "3", "4", "5"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ic := &ircClient{
				fileFetcher: &fakeFileHandler{
					filesByPackID: map[string]*fakeFile{
						"list": {
							info: &fakeInfo{
								name: "fulminata.txt",
								size: 777777,
							},
						},
						"1": {},
						"2": {},
						"3": {},
						"4": {},
						"5": {},
					},
				},
			}

			msg := Parse(tt.haveMsg)

			reqs, err := ic.decodeBatch(msg.(VerbMsg))

			require.NoError(t, err)
			require.NotEmpty(t, reqs)

			req := reqs[0]

			packs := make([]string, 0, len(reqs))

			for _, r := range reqs {
				packs = append(packs, r.Pack)
			}

			assert.Equal(t, tt.wantProtocol, req.Protocol)
			assert.Equal(t, tt.wantNick, req.Nick)
			assert.Equal(t, tt.wantHost, req.Hostname)
			assert.Equal(t, tt.wantPacks, packs)
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
		wantReadAt   int64
	}{
		{
			name:         "quoted",
			haveMsg:      []byte(":Exterminata!~Extermina@Rizon-96DE5C73.rev.sfr.net PRIVMSG Fulminata|Experiment :\x01DCC RESUME \"[SubsPlease] 16bit Sensation - Another Layer - 01 (1080p) [C13E9494].mkv\" 38545 5248\x01"),
			wantNick:     "Exterminata",
			wantHost:     "Rizon-96DE5C73.rev.sfr.net",
			wantPack:     "1",
			wantFilename: "[SubsPlease] 16bit Sensation - Another Layer - 01 (1080p) [C13E9494].mkv",
			wantPort:     38545,
			wantReadAt:   5248,
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
				fileFetcher: &fakeFileHandler{
					filesByPackID: map[string]*fakeFile{
						"list": {
							info: &fakeInfo{
								name: "fulminata.txt",
								size: 777777,
							},
						},
						"1": {
							info: &fakeInfo{
								name: "[SubsPlease] 16bit Sensation - Another Layer - 01 (1080p) [C13E9494].mkv",
								size: 777777,
							},
						},
					},
				},
			}

			msg := Parse(tt.haveMsg)

			req, err := ic.decodeResume(msg.(VerbMsg))

			require.NoError(t, err)
			assert.Equal(t, tt.wantNick, req.Nick)
			assert.Equal(t, tt.wantHost, req.Hostname)
			assert.Equal(t, tt.wantPack, req.Pack)
			assert.Equal(t, tt.wantFilename, req.filename)
			assert.Equal(t, tt.wantPort, req.port)
			assert.Equal(t, tt.wantReadAt, req.readAt)
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

func TestIRCClient_Remove(t *testing.T) {
	for _, tt := range []struct {
		name string

		haveMsg           []byte
		haveDownloadQueue map[string][]downloadRequest

		wantLeftInQueue map[string][]downloadRequest
		wantInWritten   []string
	}{
		{
			name: "remove some",

			haveMsg: []byte(":Nick!~Phoenix@anything-agency PRIVMSG bot :XDCC REMOVE 1, 5, 8,13"),
			haveDownloadQueue: map[string][]downloadRequest{
				"anything-agency": {
					{
						Protocol: XDCC,
						Nick:     "Nick",
						Hostname: "anything-agency",
						Pack:     "1",
					},
					{
						Protocol: XDCC,
						Nick:     "Nick",
						Hostname: "anything-agency",
						Pack:     "2",
					},
					{
						Protocol: XDCC,
						Nick:     "Nick",
						Hostname: "anything-agency",
						Pack:     "5",
					},
					{
						Protocol: XDCC,
						Nick:     "Nick",
						Hostname: "anything-agency",
						Pack:     "8",
					},
					{
						Protocol: XDCC,
						Nick:     "Nick",
						Hostname: "anything-agency",
						Pack:     "3",
					},
				},
				"wololo": {
					{
						Protocol: XDCC,
						Nick:     "ayaya",
						Hostname: "wololo",
						Pack:     "5",
					},
				},
			},

			wantLeftInQueue: map[string][]downloadRequest{
				"anything-agency": {
					{
						Protocol: XDCC,
						Nick:     "Nick",
						Hostname: "anything-agency",
						Pack:     "2",
					},
					{
						Protocol: XDCC,
						Nick:     "Nick",
						Hostname: "anything-agency",
						Pack:     "3",
					},
				},
				"wololo": {
					{
						Protocol: XDCC,
						Nick:     "ayaya",
						Hostname: "wololo",
						Pack:     "5",
					},
				},
			},
			wantInWritten: []string{
				"Unqueued selected packs",
			},
		},
		{
			name: "remove all",

			haveMsg: []byte(":Nick!~Phoenix@anything-agency PRIVMSG bot :XDCC REMOVE"),
			haveDownloadQueue: map[string][]downloadRequest{
				"anything-agency": {
					{
						Protocol: XDCC,
						Nick:     "Nick",
						Hostname: "anything-agency",
						Pack:     "1",
					},
					{
						Protocol: XDCC,
						Nick:     "Nick",
						Hostname: "anything-agency",
						Pack:     "2",
					},
					{
						Protocol: XDCC,
						Nick:     "Nick",
						Hostname: "anything-agency",
						Pack:     "5",
					},
					{
						Protocol: XDCC,
						Nick:     "Nick",
						Hostname: "anything-agency",
						Pack:     "8",
					},
					{
						Protocol: XDCC,
						Nick:     "Nick",
						Hostname: "anything-agency",
						Pack:     "3",
					},
				},
				"wololo": {
					{
						Protocol: XDCC,
						Nick:     "ayaya",
						Hostname: "wololo",
						Pack:     "5",
					},
				},
			},

			wantLeftInQueue: map[string][]downloadRequest{
				"wololo": {
					{
						Protocol: XDCC,
						Nick:     "ayaya",
						Hostname: "wololo",
						Pack:     "5",
					},
				},
			},
			wantInWritten: []string{
				"Download queue cleared",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ic := &ircClient{
				downloadQueue: tt.haveDownloadQueue,
			}
			w := &bytes.Buffer{}

			msg := Parse(tt.haveMsg)

			ic.remove(&ircWriter{w}, msg.(VerbMsg))

			assert.Equal(t, tt.wantLeftInQueue, ic.downloadQueue)

			for _, s := range tt.wantInWritten {
				assert.Contains(t, w.String(), s)
			}
		})
	}
}
