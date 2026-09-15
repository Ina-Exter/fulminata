package main

import (
	"bytes"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRequest(nick, host, pack string) downloadRequest {
	return downloadRequest{
		Protocol: XDCC,
		Nick:     nick,
		Hostname: host,
		Pack:     pack,
	}
}

func TestIRCClient_EnqueueDownloadRequest(t *testing.T) {
	nick1 := "Phoenix"
	hostname1 := "anything@agency"
	hostname2 := "krampus@judge"

	for _, tt := range []struct {
		name string

		haveExistingRequests map[string][]downloadProcess
		haveExistingQueue    map[string][]downloadRequest
		haveReqs             []downloadRequest

		assertProcesses func(*testing.T, map[string][]downloadProcess)
		wantInQueue     map[string][]downloadRequest
		wantInChan      []downloadRequest
		wantWritten     []string
	}{
		{
			name:            "empty queue, no reqs",
			wantInChan:      []downloadRequest{},
			assertProcesses: func(*testing.T, map[string][]downloadProcess) {},
		},
		{
			name: "queue one",

			haveExistingRequests: make(map[string][]downloadProcess),
			haveExistingQueue:    make(map[string][]downloadRequest),
			haveReqs: []downloadRequest{
				makeRequest(nick1, hostname1, "123"),
			},

			assertProcesses: func(t *testing.T, processes map[string][]downloadProcess) {
				prc, ok := processes["anything@agency"]

				require.True(t, ok)
				assert.Len(t, prc, 1)
				assert.Equal(
					t,
					makeRequest(nick1, hostname1, "123"),
					*prc[0].downloadRequest,
				)
			},
			wantInChan: []downloadRequest{
				makeRequest(nick1, hostname1, "123"),
			},
			wantInQueue: map[string][]downloadRequest{hostname1: nil},
		},
		{
			name: "queue several into existing process",

			haveExistingRequests: map[string][]downloadProcess{
				hostname1: {
					{
						downloadRequest: new(makeRequest(nick1, hostname1, "1")),
					},
				},
				hostname2: {
					{
						downloadRequest: new(makeRequest(nick1, hostname2, "77")),
					},
				},
			},
			haveExistingQueue: map[string][]downloadRequest{
				hostname2: {
					makeRequest(nick1, hostname2, "88"),
				},
			},
			haveReqs: []downloadRequest{
				makeRequest(nick1, hostname1, "123"),
				makeRequest(nick1, hostname1, "456"),
				makeRequest(nick1, hostname1, "789"),
			},

			assertProcesses: func(t *testing.T, processes map[string][]downloadProcess) {
				prc, ok := processes[hostname1]

				require.True(t, ok)
				assert.Len(t, prc, 3)
				assert.Equal(
					t,
					makeRequest(nick1, hostname1, "1"),
					*prc[0].downloadRequest,
				)
				assert.Equal(
					t,
					makeRequest(nick1, hostname1, "123"),
					*prc[1].downloadRequest,
				)
				assert.Equal(
					t,
					makeRequest(nick1, hostname1, "456"),
					*prc[2].downloadRequest,
				)

				assert.Len(t, processes[hostname2], 1)
			},
			wantInChan: []downloadRequest{
				makeRequest(nick1, hostname1, "123"),
				makeRequest(nick1, hostname1, "456"),
			},
			wantInQueue: map[string][]downloadRequest{
				hostname1: {
					makeRequest(nick1, hostname1, "789"),
				},
				hostname2: {
					makeRequest(nick1, hostname2, "88"),
				},
			},
			wantWritten: []string{
				"PRIVMSG Phoenix :Request queued: you have too many transfer pendings. Please finish or cancel pending transfers.",
			},
		},
		{
			name: "full queue dump",

			haveExistingRequests: map[string][]downloadProcess{
				hostname1: {
					{
						downloadRequest: new(makeRequest(nick1, hostname1, "1")),
					},
					{
						downloadRequest: new(makeRequest(nick1, hostname1, "3")),
					},
					{
						downloadRequest: new(makeRequest(nick1, hostname1, "2")),
					},
				},
				hostname2: {
					{
						downloadRequest: new(makeRequest(nick1, hostname2, "77")),
					},
				},
			},
			haveExistingQueue: map[string][]downloadRequest{
				hostname1: {
					makeRequest(nick1, hostname1, "4"),
				},
				hostname2: {
					makeRequest(nick1, hostname2, "88"),
				},
			},
			haveReqs: []downloadRequest{
				makeRequest(nick1, hostname1, "123"),
				makeRequest(nick1, hostname1, "456"),
				makeRequest(nick1, hostname1, "789"),
			},

			assertProcesses: func(t *testing.T, processes map[string][]downloadProcess) {
				prc, ok := processes[hostname1]

				require.True(t, ok)
				assert.Len(t, prc, 3)
				assert.Equal(
					t,
					makeRequest(nick1, hostname1, "1"),
					*prc[0].downloadRequest,
				)
				assert.Equal(
					t,
					makeRequest(nick1, hostname1, "3"),
					*prc[1].downloadRequest,
				)
				assert.Equal(
					t,
					makeRequest(nick1, hostname1, "2"),
					*prc[2].downloadRequest,
				)

				assert.Len(t, processes[hostname2], 1)
			},
			wantInChan: []downloadRequest{},
			wantInQueue: map[string][]downloadRequest{
				hostname1: {
					makeRequest(nick1, hostname1, "4"),
					makeRequest(nick1, hostname1, "123"),
					makeRequest(nick1, hostname1, "456"),
					makeRequest(nick1, hostname1, "789"),
				},
				hostname2: {
					makeRequest(nick1, hostname2, "88"),
				},
			},
			wantWritten: []string{
				"PRIVMSG Phoenix :Request queued: you have too many transfer pendings. Please finish or cancel pending transfers.",
			},
		},
		{
			name: "queue several, filter some",

			haveExistingRequests: map[string][]downloadProcess{
				hostname1: {
					{
						downloadRequest: new(makeRequest(nick1, hostname1, "123")),
					},
					{
						downloadRequest: new(makeRequest(nick1, hostname1, "456")),
					},
				},
				hostname2: {
					{
						downloadRequest: new(makeRequest(nick1, hostname2, "77")),
					},
				},
			},
			haveExistingQueue: map[string][]downloadRequest{
				hostname2: {
					makeRequest(nick1, hostname2, "88"),
				},
			},
			haveReqs: []downloadRequest{
				makeRequest(nick1, hostname1, "123"),
				makeRequest(nick1, hostname1, "456"),
				makeRequest(nick1, hostname1, "789"),
			},

			assertProcesses: func(t *testing.T, processes map[string][]downloadProcess) {
				prc, ok := processes[hostname1]

				require.True(t, ok)
				assert.Len(t, prc, 3)
				assert.Equal(
					t,
					makeRequest(nick1, hostname1, "123"),
					*prc[0].downloadRequest,
				)
				assert.Equal(
					t,
					makeRequest(nick1, hostname1, "456"),
					*prc[1].downloadRequest,
				)
				assert.Equal(
					t,
					makeRequest(nick1, hostname1, "789"),
					*prc[2].downloadRequest,
				)

				assert.Len(t, processes[hostname2], 1)
			},
			wantInChan: []downloadRequest{
				makeRequest(nick1, hostname1, "789"),
			},
			wantInQueue: map[string][]downloadRequest{
				hostname1: nil,
				hostname2: {
					makeRequest(nick1, hostname2, "88"),
				},
			},
		},
		{
			name: "queue one already queued",

			haveExistingRequests: map[string][]downloadProcess{
				hostname1: {
					{
						downloadRequest: new(makeRequest(nick1, hostname1, "123")),
					},
					{
						downloadRequest: new(makeRequest(nick1, hostname1, "456")),
					},
				},
				hostname2: {
					{
						downloadRequest: new(makeRequest(nick1, hostname2, "77")),
					},
				},
			},
			haveExistingQueue: map[string][]downloadRequest{
				hostname2: {
					makeRequest(nick1, hostname2, "88"),
				},
			},
			haveReqs: []downloadRequest{
				makeRequest(nick1, hostname1, "123"),
			},

			assertProcesses: func(t *testing.T, processes map[string][]downloadProcess) {
				prc, ok := processes[hostname1]

				require.True(t, ok)
				assert.Len(t, prc, 2)
				assert.Equal(
					t,
					makeRequest(nick1, hostname1, "123"),
					*prc[0].downloadRequest,
				)
				assert.Equal(
					t,
					makeRequest(nick1, hostname1, "456"),
					*prc[1].downloadRequest,
				)

				assert.Len(t, processes[hostname2], 1)
			},
			wantInChan: []downloadRequest{},
			wantInQueue: map[string][]downloadRequest{
				hostname2: {
					makeRequest(nick1, hostname2, "88"),
				},
			},
			wantWritten: []string{
				"PRIVMSG Phoenix :Pack already requested.",
			},
		},
		{
			name: "queue several already queued",

			haveExistingRequests: map[string][]downloadProcess{
				hostname1: {
					{
						downloadRequest: new(makeRequest(nick1, hostname1, "123")),
					},
					{
						downloadRequest: new(makeRequest(nick1, hostname1, "456")),
					},
				},
				hostname2: {
					{
						downloadRequest: new(makeRequest(nick1, hostname2, "77")),
					},
				},
			},
			haveExistingQueue: map[string][]downloadRequest{
				hostname2: {
					makeRequest(nick1, hostname2, "88"),
				},
			},
			haveReqs: []downloadRequest{
				makeRequest(nick1, hostname1, "123"),
				makeRequest(nick1, hostname1, "456"),
			},

			assertProcesses: func(t *testing.T, processes map[string][]downloadProcess) {
				prc, ok := processes[hostname1]

				require.True(t, ok)
				assert.Len(t, prc, 2)
				assert.Equal(
					t,
					makeRequest(nick1, hostname1, "123"),
					*prc[0].downloadRequest,
				)
				assert.Equal(
					t,
					makeRequest(nick1, hostname1, "456"),
					*prc[1].downloadRequest,
				)

				assert.Len(t, processes[hostname2], 1)
			},
			wantInChan: []downloadRequest{},
			wantInQueue: map[string][]downloadRequest{
				hostname2: {
					makeRequest(nick1, hostname2, "88"),
				},
			},
			wantWritten: []string{
				"PRIVMSG Phoenix :Packs already requested.",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ic := ircClient{
				downloadRequestsChan:        make(chan downloadRequestWithID, 16),
				downloadProcessesByHostname: maps.Clone(tt.haveExistingRequests),
				downloadQueue:               maps.Clone(tt.haveExistingQueue),
				portsPool:                   newPortsPool([]int{1, 2, 3}),
				rateLimit:                   3,
			}
			writeBuff := bytes.Buffer{}

			ic.enqueueDownloadRequests(&ircWriter{Writer: &writeBuff}, tt.haveReqs)

			close(ic.downloadRequestsChan)

			reqsInChan := make([]downloadRequest, 0, len(ic.downloadRequestsChan))

			for r := range ic.downloadRequestsChan {
				reqsInChan = append(reqsInChan, r.downloadRequest)
			}

			assert.Equal(t, tt.wantInChan, reqsInChan)
			assert.Equal(t, tt.wantInQueue, ic.downloadQueue)
			tt.assertProcesses(t, ic.downloadProcessesByHostname)

			for _, s := range tt.wantWritten {
				assert.Contains(t, writeBuff.String(), s)
			}
		})
	}
}
