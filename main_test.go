package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
				fileFetcher: fsHandler{
					lookupID: map[string]PackInfo{
						"1": {Name: "[SubsPlease] 16bit Sensation - Another Layer - 01 (1080p) [C13E9494].mkv"},
					},
				},
			}

			req, filename, port, readAt := ic.decodeResume(tt.haveMsg)

			assert.Equal(t, tt.wantNick, req.Nick)
			assert.Equal(t, tt.wantHost, req.Hostname)
			assert.Equal(t, tt.wantPack, req.Pack)
			assert.Equal(t, tt.wantFilename, filename)
			assert.Equal(t, tt.wantPort, port)
			assert.Equal(t, tt.wantReadAt, readAt)
		})
	}
}
