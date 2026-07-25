package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Source struct {
	URL     string `json:"url"`
	Enabled bool   `json:"enabled"`
}

type Config struct {
	UseGlobalMode    bool     `json:"USE_GLOBAL_MODE"`
	GlobalTopN       int      `json:"GLOBAL_TOP_N"`
	PerCountryTopN   int      `json:"PER_COUNTRY_TOP_N"`
	BandwidthCandidates int   `json:"BANDWIDTH_CANDIDATES"`
	TCPProbes        int      `json:"TCP_PROBES"`
	MinSuccessRate   float64  `json:"MIN_SUCCESS_RATE"`
	TCPLatencyWeight float64  `json:"TCP_LATENCY_WEIGHT"`
	Timeout          float64  `json:"TIMEOUT"`
	SocketDefaultTimeout int  `json:"SOCKET_DEFAULT_TIMEOUT"`
	ProgressPrintInterval int  `json:"PROGRESS_PRINT_INTERVAL"`
	FilterCountriesEnabled bool   `json:"FILTER_COUNTRIES_ENABLED"`
	AllowedCountries     []string `json:"ALLOWED_COUNTRIES"`
	PreFilterBlockedEnabled bool  `json:"PRE_FILTER_BLOCKED_ENABLED"`
	PreFilterBlockedCountries []string `json:"PRE_FILTER_BLOCKED_COUNTRIES"`
	PreFilterPortEnabled  bool   `json:"PRE_FILTER_PORT_ENABLED"`
	PreFilterPorts       []int   `json:"PRE_FILTER_PORTS"`
	EnableWxPusher      bool    `json:"ENABLE_WXPUSHER"`
	WxPusherAppToken    string  `json:"WXPUSHER_APP_TOKEN"`
	WxPusherUIDs        []string `json:"WXPUSHER_UIDS"`
	WxPusherAPIURL      string  `json:"WXPUSHER_API_URL"`
	NotifyTimeout       float64 `json:"NOTIFY_TIMEOUT"`
	NotifyConnectTimeout float64 `json:"NOTIFY_CONNECT_TIMEOUT"`
	CFEnabled           bool    `json:"CF_ENABLED"`
	CFAPIToken          string  `json:"CF_API_TOKEN"`
	CFZoneID            string  `json:"CF_ZONE_ID"`
	CFDNSRecordName     string  `json:"CF_DNS_RECORD_NAME"`
	CFTTL               int     `json:"CF_TTL"`
	CFProxied           bool    `json:"CF_PROXIED"`
	CFDNSConnectTimeout float64 `json:"CF_DNS_CONNECT_TIMEOUT"`
	CFDNSReadTimeout    float64 `json:"CF_DNS_READ_TIMEOUT"`
	DNSRecordType       string  `json:"DNS_RECORD_TYPE"`
	AdditionalSources   []Source `json:"ADDITIONAL_SOURCES"`
	UseURLSource        bool     `json:"USE_URL_SOURCE"`
	DirectNodes         []string `json:"DIRECT_NODES"`
	FetchMaxRetries     int     `json:"FETCH_MAX_RETRIES"`
	FetchRetryDelay     float64 `json:"FETCH_RETRY_DELAY"`
	FetchTimeout        float64 `json:"FETCH_TIMEOUT"`
	FetchConnectTimeout float64 `json:"FETCH_CONNECT_TIMEOUT"`
	IPCalibrationEnabled bool   `json:"IP_CALIBRATION_ENABLED"`
	IPCalibrationMinInterval float64 `json:"IP_CALIBRATION_MIN_INTERVAL"`
	IPCalibrationTokenFile   string `json:"IP_CALIBRATION_TOKEN_FILE"`
	IPCalibrationCacheFile   string `json:"IP_CALIBRATION_CACHE_FILE"`
	OutputFile         string  `json:"OUTPUT_FILE"`
	EnableLogging      bool    `json:"ENABLE_LOGGING"`
	LogFile            string  `json:"LOG_FILE"`
	ForceDirect        bool    `json:"FORCE_DIRECT"`
	TestAvailability   bool    `json:"TEST_AVAILABILITY"`
	AvailabilityCheckAPI      string  `json:"AVAILABILITY_CHECK_API"`
	AvailabilityTimeout       float64 `json:"AVAILABILITY_TIMEOUT"`
	AvailabilityConnectTimeout float64 `json:"AVAILABILITY_CONNECT_TIMEOUT"`
	AvailabilityRetryMax      int     `json:"AVAILABILITY_RETRY_MAX"`
	AvailabilityRetryDelay    float64 `json:"AVAILABILITY_RETRY_DELAY"`
	AvailabilityInnerRetryEnabled bool `json:"AVAILABILITY_INNER_RETRY_ENABLED"`
	AvailabilityInnerRetryMax    int  `json:"AVAILABILITY_INNER_RETRY_MAX"`
	AvailabilityInnerRetryDelay  float64 `json:"AVAILABILITY_INNER_RETRY_DELAY"`
	HTTPTestEnabled     bool    `json:"HTTP_TEST_ENABLED"`
	HTTPTestTimeout     float64 `json:"HTTP_TEST_TIMEOUT"`
	HTTPTestConnectTimeout float64 `json:"HTTP_TEST_CONNECT_TIMEOUT"`
	HTTPTestMaxRounds   int     `json:"HTTP_TEST_MAX_ROUNDS"`
	HTTPTestRoundDelay  float64 `json:"HTTP_TEST_ROUND_DELAY"`
	HTTPTestInnerRetryEnabled  bool  `json:"HTTP_TEST_INNER_RETRY_ENABLED"`
	HTTPTestMaxRetries  int     `json:"HTTP_TEST_MAX_RETRIES"`
	HTTPTestRetryDelay  float64 `json:"HTTP_TEST_RETRY_DELAY"`
	HTTPTestMethod      string  `json:"HTTP_TEST_METHOD"`
	HTTPLatencyWeight   float64 `json:"HTTP_LATENCY_WEIGHT"`
	JitterWeight        float64 `json:"JITTER_WEIGHT"`
	HTTPJitterSamples   int     `json:"HTTP_JITTER_SAMPLES"`
	FilterIPv6Availability   bool `json:"FILTER_IPV6_AVAILABILITY"`
	FilterBlockedCountriesEnabled bool `json:"FILTER_BLOCKED_COUNTRIES_ENABLED"`
	BlockedCountries    []string `json:"BLOCKED_COUNTRIES"`
	DNSIPRiskFilterEnabled   bool   `json:"DNS_IP_RISK_FILTER_ENABLED"`
	DNSIPRiskMaxLevel        string `json:"DNS_IP_RISK_MAX_LEVEL"`
	DNSUpdateTargetCount     int    `json:"DNS_UPDATE_TARGET_COUNT"`
	BandwidthSizeMB     float64 `json:"BANDWIDTH_SIZE_MB"`
	BandwidthTimeout    float64 `json:"BANDWIDTH_TIMEOUT"`
	BandwidthRetryMax   int     `json:"BANDWIDTH_RETRY_MAX"`
	BandwidthRetryDelay float64 `json:"BANDWIDTH_RETRY_DELAY"`
	BandwidthURLTemplate      string `json:"BANDWIDTH_URL_TEMPLATE"`
	BandwidthProcessBuffer    float64 `json:"BANDWIDTH_PROCESS_BUFFER"`
	BandwidthConnectTimeout   float64 `json:"BANDWIDTH_CONNECT_TIMEOUT"`
	SpeedWeight        float64 `json:"SPEED_WEIGHT"`
	IPCalibrationConcurrency int `json:"IP_CALIBRATION_CONCURRENCY"`
	MaxWorkers         int     `json:"MAX_WORKERS"`
	AvailabilityWorkers int    `json:"AVAILABILITY_WORKERS"`
	FallbackWorkers    int     `json:"FALLBACK_WORKERS"`
	BandwidthWorkers   int     `json:"BANDWIDTH_WORKERS"`
	HTTPTestWorkers    int     `json:"HTTP_TEST_WORKERS"`
	DNSUpdateMaxRetries int    `json:"DNS_UPDATE_MAX_RETRIES"`
	DNSUpdateRetryDelay float64 `json:"DNS_UPDATE_RETRY_DELAY"`
	GitHubSyncMaxRetries      int    `json:"GITHUB_SYNC_MAX_RETRIES"`
	GitHubSyncRetryDelay      float64 `json:"GITHUB_SYNC_RETRY_DELAY"`
	GitSyncProcessTimeout     int    `json:"GIT_SYNC_PROCESS_TIMEOUT"`
	GitHubSyncEnabled         bool   `json:"GIT_SYNC_ENABLED"`
	ADHeaderEnabled    bool     `json:"AD_HEADER_ENABLED"`
	ADHeaderLines      []string `json:"AD_HEADER_LINES"`
	ADFooterEnabled    bool     `json:"AD_FOOTER_ENABLED"`
	ADFooterLines      []string `json:"AD_FOOTER_LINES"`
	ADPerlineEnabled   bool     `json:"AD_PERLINE_ENABLED"`
	ADPerlineText      string   `json:"AD_PERLINE_TEXT"`
	IPTXTShowBandwidth bool     `json:"IP_TXT_SHOW_BANDWIDTH"`
	IPTXTShowHTTPLatency bool   `json:"IP_TXT_SHOW_HTTP_LATENCY"`
	IPTXTShowHTTPJitter  bool   `json:"IP_TXT_SHOW_HTTP_JITTER"`
	IPTXTShowLatency     bool   `json:"IP_TXT_SHOW_LATENCY"`
}

func DefaultConfig() *Config {
	return &Config{
		UseGlobalMode:           true,
		GlobalTopN:              15,
		PerCountryTopN:          1,
		BandwidthCandidates:     150,
		TCPProbes:               1,
		MinSuccessRate:          1.0,
		TCPLatencyWeight:        0.0,
		Timeout:                 2.0,
		SocketDefaultTimeout:    3,
		ProgressPrintInterval:   1,
		FilterCountriesEnabled:  false,
		AllowedCountries:        []string{"US"},
		PreFilterBlockedEnabled: true,
		PreFilterBlockedCountries: []string{"CN"},
		PreFilterPortEnabled:    true,
		PreFilterPorts:          []int{443},
		EnableWxPusher:          true,
		WxPusherAppToken:        "your_app_token_here",
		WxPusherUIDs:            []string{"your_uid_here"},
		WxPusherAPIURL:          "https://wxpusher.zjiecode.com/api/send/message",
		NotifyTimeout:           3,
		NotifyConnectTimeout:    3,
		CFEnabled:               true,
		CFAPIToken:              "your_CF_API_TOKEN",
		CFZoneID:                "your_CF_ZONE_ID",
		CFDNSRecordName:         "your_CF_DNS_RECORD_NAME",
		CFTTL:                   60,
		CFProxied:               false,
		CFDNSConnectTimeout:     3,
		CFDNSReadTimeout:        3,
		DNSRecordType:           "TXT",
		AdditionalSources:       []Source{},
		UseURLSource:            true,
		FetchMaxRetries:         3,
		FetchRetryDelay:         3,
		FetchTimeout:            3,
		FetchConnectTimeout:     3,
		IPCalibrationEnabled:    false,
		IPCalibrationMinInterval: 0.1,
		IPCalibrationTokenFile:  "valid_tokens.txt",
		IPCalibrationCacheFile:  "ipinfo_cache.txt",
		OutputFile:              "ip.txt",
		EnableLogging:           false,
		LogFile:                 "cfnb.log",
		ForceDirect:             false,
		TestAvailability:        true,
		AvailabilityCheckAPI:    "https://api.090227.xyz/check",
		AvailabilityTimeout:     3,
		AvailabilityConnectTimeout: 3,
		AvailabilityRetryMax:    2,
		AvailabilityRetryDelay:  3,
		AvailabilityInnerRetryEnabled: true,
		AvailabilityInnerRetryMax:    2,
		AvailabilityInnerRetryDelay:  3,
		HTTPTestEnabled:         true,
		HTTPTestTimeout:         3,
		HTTPTestConnectTimeout:  3,
		HTTPTestMaxRounds:       2,
		HTTPTestRoundDelay:      3,
		HTTPTestInnerRetryEnabled: true,
		HTTPTestMaxRetries:      2,
		HTTPTestRetryDelay:      3,
		HTTPTestMethod:          "HEAD",
		HTTPLatencyWeight:       3.0,
		JitterWeight:            3.0,
		HTTPJitterSamples:       3,
		FilterIPv6Availability:  true,
		FilterBlockedCountriesEnabled: true,
		BlockedCountries: []string{
			"BD", "BI", "BY", "CD", "CF", "CN", "CU", "DE", "ET", "HK",
			"IR", "KP", "LY", "MO", "NG", "NL", "PK", "RU", "SD", "SO",
			"SY", "TH", "TW", "UA", "VE", "VN", "YE", "ZW",
		},
		DNSIPRiskFilterEnabled:  false,
		DNSIPRiskMaxLevel:       "高风险",
		DNSUpdateTargetCount:    15,
		BandwidthSizeMB:         1.0,
		BandwidthTimeout:        3,
		BandwidthRetryMax:       2,
		BandwidthRetryDelay:     3,
		BandwidthURLTemplate:    "https://speed.cloudflare.com/__down?bytes={bytes}",
		BandwidthProcessBuffer:  2,
		BandwidthConnectTimeout: 3,
		SpeedWeight:             3.0,
		IPCalibrationConcurrency: 300,
		MaxWorkers:              300,
		AvailabilityWorkers:     32,
		FallbackWorkers:         32,
		BandwidthWorkers:        3,
		HTTPTestWorkers:         32,
		DNSUpdateMaxRetries:     3,
		DNSUpdateRetryDelay:     3,
		GitHubSyncMaxRetries:    3,
		GitHubSyncRetryDelay:    3,
		GitSyncProcessTimeout:   180,
		GitHubSyncEnabled:       false,
		ADHeaderEnabled:         false,
		ADHeaderLines:           []string{},
		ADFooterEnabled:         false,
		ADFooterLines:           []string{},
		ADPerlineEnabled:        false,
		ADPerlineText:           "",
		IPTXTShowBandwidth:      false,
		IPTXTShowHTTPLatency:    false,
		IPTXTShowHTTPJitter:     false,
		IPTXTShowLatency:        false,
	}
}

func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("未找到配置文件 %s，将使用内置默认配置运行。\n", path)
			fmt.Println("你可根据需要创建 config.json 文件（参考文档），程序会自动识别。")
			return cfg, nil
		}
		return nil, err
	}

	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("配置文件格式不正确: %w", err)
	}

	return cfg, nil
}
