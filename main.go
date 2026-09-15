package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/netip"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	_ "golang.org/x/crypto/x509roots/fallback"
)

func getEnvFallback[T any](key string, def T, mapper func(string) T) T {
	val, ok := os.LookupEnv(key)

	if !ok {
		return def
	}

	return mapper(val)
}

func stringNotEmpty(name string) func(string) string {
	return func(s string) string {
		if s == "" {
			panic(name + "cannot be empty")
		}

		return s
	}
}

func extractIPv4() (uint32, error) {
	// should always return ipv4
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)

	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.ipify.org", http.NoBody)

	if err != nil {
		return 0, err
	}

	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("invalid status code for ip discovery: %d", resp.StatusCode)
	}

	strIP, err := io.ReadAll(resp.Body)

	if err != nil {
		return 0, err
	}

	ip, err := netip.ParseAddr(string(strIP))

	if err != nil {
		return 0, err
	}

	if !ip.Is4() {
		return 0, errors.New("ip is not v4")
	}

	ipBytes := ip.As4()

	return binary.BigEndian.Uint32(ipBytes[:]), nil
}

func spinTLSConfig() ([]tls.Certificate, error) {
	tlsCertPath := getEnvFallback("TLS_CERT_PATH", "", func(s string) string { return s })
	tlsKeyPath := getEnvFallback("TLS_KEY_PATH", "", func(s string) string { return s })

	if tlsCertPath == "" || tlsKeyPath == "" {
		return nil, errors.New("no certificate paths provided")
	}

	certData, err := os.ReadFile(tlsCertPath)

	if err != nil {
		return nil, err
	}

	keyData, err := os.ReadFile(tlsKeyPath)

	if err != nil {
		return nil, err
	}

	cert, err := tls.X509KeyPair(certData, keyData)

	if err != nil {
		return nil, err
	}

	return []tls.Certificate{cert}, nil
}

func main() {
	nick := getEnvFallback("NICK", "Fulminata|Experiment", stringNotEmpty("NICK"))
	registrationPassword := getEnvFallback("REGISTRATION_PASSWORD", "", func(s string) string { return s })
	rootSavePath := getEnvFallback("ROOT_FOLDER", "/anime", stringNotEmpty("ROOT_FOLDER"))
	joinChan := getEnvFallback("CHANNEL", "#exter-periment", stringNotEmpty("CHANNEL"))
	localIP := getEnvFallback(
		"LOCAL_IP",
		"192.168.1.68",
		func(s string) string {
			if s == "" {
				panic("LOCAL_IP cannot be empty")
			}

			parsedIP, err := netip.ParseAddr(s)

			if err != nil {
				panic("cannot parse ip" + err.Error())
			}

			if !parsedIP.Is4() {
				panic("IP is not a v4 ip")
			}

			return s
		},
	)
	ports := getEnvFallback("PORTS", []int{38542, 38543}, func(s string) []int {
		portsStr := strings.Split(s, ",")
		ports := make([]int, 0, len(portsStr))

		for _, ps := range portsStr {
			pint, err := strconv.Atoi(ps)

			if err != nil {
				panic("invalid port" + ps + ":" + err.Error())
			}

			if pint <= 0 || pint > 65535 {
				panic("invalid port" + strconv.Itoa(pint))
			}

			ports = append(ports, pint)
		}

		return ports
	})
	counterFile := getEnvFallback("DOWNLOADS_COUNTER_FILE_PATH", path.Join(rootSavePath, "downloads_count.json"), stringNotEmpty("DOWNLOADS_COUNTER_FILE_PATH"))
	rateLimit := getEnvFallback("RATE_LIMIT", 1, func(val string) int {
		ival, err := strconv.Atoi(val)

		if err != nil || ival <= 0 {
			panic(fmt.Sprintf("Invalid value %s given to key RATE_LIMIT, expected int", val))
		}

		return ival
	})
	remoteIP, err := extractIPv4()

	if err != nil {
		panic("could not extract remote IP:" + err.Error())
	}

	certs, err := spinTLSConfig()

	if err != nil {
		fmt.Println("cannot get tls certificate:", err, ", bot starting without SDCC capabilities")
	}

	pool := newPortsPool(ports)

	fsHandler := MustNewFSHandler(rootSavePath, counterFile, pool)

	ic := ircClient{
		nickname:             nick,
		registrationPassword: registrationPassword,
		channel:              joinChan,
		listenerBuilderFactory: &defaultListenerBuilderFactory{
			localIP:      localIP,
			externalIP:   remoteIP,
			certificates: certs,
		},
		portsPool:                   pool,
		rateLimit:                   rateLimit,
		downloadProcessesByHostname: make(map[string][]downloadProcess, 65535),
		downloadRequestsChan:        make(chan downloadRequestWithID, 65535),
		downloadQueue:               make(map[string][]downloadRequest, 65535),
		ircErrChan:                  make(chan error, 10),
		fileFetcher:                 fsHandler,
	}

	reportingTicker := time.NewTicker(time.Minute)

	ctx, cancel := context.WithCancel(context.Background())

	disconnect, err := ic.connect(ctx, "irc.rizon.net:6697")

	if err != nil {
		cancel()
		panic("could not connect to IRC server: " + err.Error())
	}

	for {
		select {
		case <-reportingTicker.C:
			fmt.Println("Open ports: ", ic.portsPool.OpenPorts())
			fmt.Println("Requests by hostname:")

			ic.downloadProcessesMu.Lock()
			snapshot := maps.Clone(ic.downloadProcessesByHostname)
			ic.downloadProcessesMu.Unlock()

			for hn, reqs := range snapshot {
				fmt.Println(hn, reqs)
			}
		case err := <-ic.ircErrChan:
			fmt.Println("fatal error received:", err)

			disconnect()
			cancel()

			return
		case <-ctx.Done():
			fmt.Println("context done received")

			disconnect()
			cancel()

			return
		}
	}
}
