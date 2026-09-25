package main

import (
	"bytes"
	"fmt"
	"io"
	"maps"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()

	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()

	defer b.mu.Unlock()

	return b.buf.String()
}

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
			name:                 "empty queue, no reqs",
			haveExistingQueue:    map[string][]downloadRequest{},
			haveExistingRequests: map[string][]downloadProcess{},
			wantInChan:           []downloadRequest{},
			assertProcesses:      func(*testing.T, map[string][]downloadProcess) {},
			wantInQueue:          map[string][]downloadRequest{},
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
			wantInQueue: map[string][]downloadRequest{},
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

			ic.enqueueDownloadRequests(&ircWriter{Writer: &writeBuff}, nick1, hostname1, tt.haveReqs)

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

func TestIRCClient_ProcessDownloadRequest(t *testing.T) {
	testUUID1 := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

	for _, tt := range []struct {
		name string

		req       downloadRequestWithID
		processes map[string][]downloadProcess

		wantWritten        []string
		wantDownloadedFile []byte
	}{
		{
			name: "success",

			req: downloadRequestWithID{
				UUID:     testUUID1,
				Protocol: XDCC,
				Nick:     "Phoenix",
				Hostname: "krampus.judge",
				Pack:     "1",
			},
			processes: map[string][]downloadProcess{
				"krampus.judge": {
					{
						id: testUUID1,
						downloadRequest: &downloadRequest{
							Protocol: XDCC,
							Nick:     "Phoenix",
							Hostname: "krampus.judge",
							Pack:     "1",
						},
						downloadTransfer: &downloadTransfer{},
					},
				},
			},

			wantWritten: []string{
				"PRIVMSG Phoenix :This bot is experimental. If you encountered any issue with your download, please reach out to Exterminata on IRC or Ina on the nibl discord.",
			},
			wantDownloadedFile: []byte("this is file"),
		},
		{
			name: "offset",

			req: downloadRequestWithID{
				UUID:     testUUID1,
				Protocol: XDCC,
				Nick:     "Phoenix",
				Hostname: "krampus.judge",
				Pack:     "1",
			},
			processes: map[string][]downloadProcess{
				"krampus.judge": {
					{
						id: testUUID1,
						downloadRequest: &downloadRequest{
							Protocol: XDCC,
							Nick:     "Phoenix",
							Hostname: "krampus.judge",
							Pack:     "1",
						},
						downloadTransfer: &downloadTransfer{
							offset: 6,
						},
					},
				},
			},

			wantWritten: []string{
				"PRIVMSG Phoenix :This bot is experimental. If you encountered any issue with your download, please reach out to Exterminata on IRC or Ina on the nibl discord.",
			},
			wantDownloadedFile: []byte("s file"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			port := freeTCPPort(t)
			ic := &ircClient{
				portsPool: newPortsPool([]int{port}),
				listenerBuilderFactory: &defaultListenerBuilderFactory{
					externalIP: 2130706433,
					localIP:    "127.0.0.1",
				},
				fileFetcher: &fakeFileHandler{
					filesByPackID: map[string]*fakeFile{
						"1": {
							info: &fakeInfo{
								name: "fake_file.mkv",
								size: 12,
							},
							file: bytes.NewReader([]byte("this is file")),
						},
					},
				},
				downloadProcessesByHostname: tt.processes,
				downloadQueue:               make(map[string][]downloadRequest),
			}
			w := &syncBuffer{}

			err := ic.processDownloadRequest(&ircWriter{Writer: w}, tt.req)

			require.NoError(t, err)

			conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))

			require.NoError(t, err)

			defer conn.Close()

			fileBuff, err := io.ReadAll(conn)

			require.NoError(t, err)

			require.NoError(t, conn.Close())

			assert.Equal(t, tt.wantDownloadedFile, fileBuff)

			// ensures the listener is closed
			require.Eventually(
				t,
				func() bool {
					listener, err := net.Listen(
						"tcp",
						net.JoinHostPort("127.0.0.1", strconv.Itoa(port)),
					)
					if err != nil {
						return false
					}

					return listener.Close() == nil
				},
				2*time.Second,
				10*time.Millisecond,
			)

			for _, s := range append(
				[]string{
					fmt.Sprintf("PRIVMSG Phoenix :\x01DCC SEND \"fake_file.mkv\" 2130706433 %d 12\x01", port),
				},
				tt.wantWritten...,
			) {
				assert.Contains(t, w.String(), s)
			}
		})
	}
}

// freeTCPPort picks a random free port for the downloading test
// the OS is responsible for assigning a working port.
// There's a slight risk that the port we picked will have been used
// in the meantime (TOCTOU) but in that case, just rerun the test.
func freeTCPPort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())

	return port
}
