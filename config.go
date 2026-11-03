package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

type Config struct {
	Nick                 string   `json:"nick"`
	RegistrationPassword string   `json:"registration_password"`
	RootFolder           string   `json:"root_folder"`
	Channel              string   `json:"channel"`
	LocalIP              string   `json:"local_ip"`
	Ports                []int    `json:"ports"`
	CounterFilePath      string   `json:"downloads_counter_file_path"`
	RateLimit            int      `json:"rate_limit"`
	BlacklistedNicks     []string `json:"blacklisted_nicks"`
	BlacklistedHostnames []string `json:"blacklisted_hostnames"`
}

func LoadConfigFromFile(configPath string) (*Config, error) {
	f, err := os.Open(configPath)

	switch {
	case err == nil:
	case errors.Is(err, os.ErrNotExist):
		return nil, nil
	default:
		return nil, err
	}

	defer f.Close()

	var c Config

	return &c, json.NewDecoder(f).Decode(&c)
}

func SaveConfig(configPath string, config *Config) error {
	tempFile, err := os.CreateTemp(filepath.Dir(configPath), "counts-file-temp")

	if err != nil {
		return err
	}

	if err := json.NewEncoder(tempFile).Encode(config); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())

		return err
	}

	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())

		return err
	}

	if err := tempFile.Close(); err != nil {
		os.Remove(tempFile.Name())

		return err
	}

	if err := os.Rename(tempFile.Name(), configPath); err != nil {
		os.Remove(tempFile.Name())

		return err
	}

	return nil
}

func MustLoadConfigFromEnv() *Config {
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
	blacklistedNicks := getEnvFallback("BLACKLISTED_NICKS", nil, func(val string) []string { return strings.Split(val, ",") })
	blacklistedHostnames := getEnvFallback("BLACKLISTED_HOSTNAMES", nil, func(val string) []string { return strings.Split(val, ",") })

	return &Config{
		Nick:                 nick,
		RegistrationPassword: registrationPassword,
		RootFolder:           rootSavePath,
		Channel:              joinChan,
		LocalIP:              localIP,
		Ports:                ports,
		CounterFilePath:      counterFile,
		RateLimit:            rateLimit,
		BlacklistedNicks:     blacklistedNicks,
		BlacklistedHostnames: blacklistedHostnames,
	}
}
