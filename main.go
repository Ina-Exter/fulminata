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
	c := MustLoadConfigFromEnv()

	remoteIP, err := extractIPv4()

	if err != nil {
		panic("could not extract remote IP:" + err.Error())
	}

	certs, err := spinTLSConfig()

	if err != nil {
		fmt.Println("cannot get tls certificate:", err, ", bot starting without SDCC capabilities")
	}

	pool := newPortsPool(c.Ports)

	fsHandler := MustNewFSHandler(c.RootFolder, c.CounterFilePath, pool)

	ic := ircClientFromConfig(c, pool, fsHandler, remoteIP, certs)

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
