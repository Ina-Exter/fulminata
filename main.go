package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	_ "golang.org/x/crypto/x509roots/fallback"
)

func ip2int(ip net.IP) uint32 {
	if len(ip) == 16 {
		return binary.BigEndian.Uint32(ip[12:16])
	}

	return binary.BigEndian.Uint32(ip)
}

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

func main() {
	registrationPassword := getEnvFallback("REGISTRATION_PASSWORD", "")
	rootSavePath := getEnvFallback("ROOT_FOLDER", "/anime")
	joinChan := getEnvFallback("CHANNEL", "#exter-periment")
	localIP := getEnvFallback("LOCAL_IP", "192.168.1.68")
	portsStr := strings.Split(getEnvFallback("PORTS", "38542,38543"), ",")
	nick := getEnvFallback("NICK", "Fulminata|Experiment")
	counterFile := getEnvFallback("DOWNLOADS_COUNTER_FILE_PATH", "downloads_count.json")

	ports := make([]int, 0, len(portsStr))

	for _, ps := range portsStr {
		pint, _ := strconv.Atoi(ps)

		ports = append(ports, pint)
	}

	pool := newPortsPool(ports)

	fsHandler := MustNewFSHandler(rootSavePath, counterFile, pool)

	ic := ircClient{
		nickname:                    nick,
		registrationPassword:        registrationPassword,
		channel:                     joinChan,
		localIP:                     localIP,
		myIP:                        mustExtractIP(),
		portsPool:                   pool,
		rateLimit:                   1,
		downloadProcessesByHostname: make(map[string][]downloadProcess, 65535),
		downloadRequestsChan:        make(chan downloadRequest, 65535),
		downloadQueue:               make(map[string][]downloadRequest, 65535),
		errChan:                     make(chan error, 10),
		shutdownChans: map[string]chan struct{}{
			"dcc": make(chan struct{}),
			"irc": make(chan struct{}),
		},
		fileFetcher: fsHandler,
	}

	reportingTicker := time.NewTicker(time.Minute)

	ic.connect()

	for {
		select {
		case <-reportingTicker.C:
			fmt.Println("Open ports: ", slices.Collect(maps.Keys(ic.portsPool.openPorts)))
			fmt.Println("Requests by hostname:")
			for hn, reqs := range ic.downloadProcessesByHostname {
				fmt.Println(hn, reqs)
			}
		case err := <-ic.errChan:
			fmt.Println("fatal error received:", err)

			for _, c := range ic.shutdownChans {
				c <- struct{}{}
			}

			return
		}
	}
}
