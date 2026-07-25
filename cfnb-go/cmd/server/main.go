package main

import (
	"bufio"
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cfnb/pkg/config"
	"cfnb/pkg/pipeline"
)

//go:embed web/*
var embeddedWeb embed.FS
var webFS fs.FS

type PipelineStatus struct {
	Running  bool            `json:"running"`
	Progress string          `json:"progress"`
	Results  *PipelineResults `json:"results,omitempty"`
	Logs     []string        `json:"logs"`
}

type NodeInfo struct {
	Node        string  `json:"node"`
	Speed       float64 `json:"speed"`
	PeakSpeed   float64 `json:"peakSpeed"`
	Latency     float64 `json:"latency"`
	HTTPLatency float64 `json:"httpLatency"`
	HTTPJitter  float64 `json:"httpJitter"`
	CCTag       string  `json:"ccTag"`
	Country     string  `json:"country"`
	CountryCode string  `json:"countryCode"`
	ColoCode    string  `json:"coloCode"`
}

type PipelineResults struct {
	Nodes          []NodeInfo `json:"nodes"`
	TotalBandwidth float64    `json:"totalBandwidth"`
	AvgLatency     float64    `json:"avgLatency"`
	TotalTime      float64    `json:"totalTime"`
}

var (
	status             PipelineStatus
	mu                 sync.Mutex
	notifs             []chan bool
	notifMu            sync.Mutex
	lastPipelineResult *pipeline.Result
	cancelPipeline     func()
)

func RunServer(port string) {
	http.HandleFunc("/", middlewareCORS(serveIndex))
	http.HandleFunc("/api/run", middlewareCORS(handleRun))
	http.HandleFunc("/api/events", middlewareCORS(handleEvents))
	http.HandleFunc("/api/status", middlewareCORS(handleStatus))
	http.HandleFunc("/api/stop", middlewareCORS(handleStop))

	fmt.Printf("CFNB Web Dashboard starting on :%s\n", port)
	fmt.Fprintf(os.Stderr, "CFNB Web Dashboard starting on :%s\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}

func middlewareCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

func init() {
	log.SetFlags(0)
	var err error
	webFS, err = fs.Sub(embeddedWeb, "web")
	if err != nil {
		log.Fatalf("Failed to init embedded web FS: %v", err)
	}
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	if webFS != nil {
		// Try serving from embedded filesystem first
		f, err := webFS.Open(r.URL.Path)
		if err == nil {
			defer f.Close()
			stat, _ := f.Stat()
			if stat != nil && !stat.IsDir() {
				data, _ := io.ReadAll(f)
				ct := "text/plain"
				switch {
				case strings.HasSuffix(r.URL.Path, ".html"):
					ct = "text/html; charset=utf-8"
				case strings.HasSuffix(r.URL.Path, ".css"):
					ct = "text/css; charset=utf-8"
				case strings.HasSuffix(r.URL.Path, ".js"):
					ct = "application/javascript; charset=utf-8"
				}
				w.Header().Set("Content-Type", ct)
				w.Write(data)
				return
			}
		}
	}

	// Fallback to external WEB_DIR
	if webDir := os.Getenv("WEB_DIR"); webDir != "" {
		webPath := filepath.Join(webDir, "index.html")
		if _, err := os.Stat(webPath); err == nil {
			http.ServeFile(w, r, webPath)
			return
		}
	}

	exePath, _ := os.Executable()
	workDir := filepath.Dir(exePath)
	webPath := filepath.Join(workDir, "web", "index.html")
	if _, err := os.Stat(webPath); os.IsNotExist(err) {
		webPath = "web/index.html"
	}
	http.ServeFile(w, r, webPath)
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	b, err := json.Marshal(status)
	mu.Unlock()
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

func handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan bool, 10)
	notifMu.Lock()
	notifs = append(notifs, ch)
	notifMu.Unlock()

	defer func() {
		notifMu.Lock()
		for i, c := range notifs {
			if c == ch {
				notifs = append(notifs[:i], notifs[i+1:]...)
				break
			}
		}
		notifMu.Unlock()
	}()

	mu.Lock()
	b, _ := json.Marshal(status)
	mu.Unlock()
	fmt.Fprintf(w, "data: %s\n\n", b)
	w.(http.Flusher).Flush()

	for {
		select {
		case <-ch:
			mu.Lock()
			b, _ := json.Marshal(status)
			mu.Unlock()
			fmt.Fprintf(w, "data: %s\n\n", b)
			w.(http.Flusher).Flush()
		case <-r.Context().Done():
			return
		}
	}
}

type RunConfig struct {
	BandwidthSize          float64  `json:"bandwidthSize"`
	Candidates             int      `json:"candidates"`
	Mode                   string   `json:"mode"`
	GlobalTopN             int      `json:"globalTopN"`
	PerCountryTopN         int      `json:"perCountryTopN"`
	TCPProbes              int      `json:"tcpProbes"`
	TCPWorkers             int      `json:"tcpWorkers"`
	TCPTimeout             float64  `json:"tcpTimeout"`
	MinSuccessRate         float64  `json:"minSuccessRate"`
	BandwidthWorkers       int      `json:"bandwidthWorkers"`
	BandwidthTimeout       float64  `json:"bandwidthTimeout"`
	TestAvailability       *bool    `json:"testAvailability"`
	HTTPTestEnabled        *bool    `json:"httpTestEnabled"`
	SourceURLs             []string `json:"sourceURLs"`
	UseURLSource           *bool    `json:"useURLSource"`
	DirectIPs              []string `json:"directIPs"`
	PreFilterPortEnabled   *bool    `json:"preFilterPortEnabled"`
	PreFilterPorts         []int    `json:"preFilterPorts"`
	PreFilterBlockedEnabled *bool   `json:"preFilterBlockedEnabled"`
	PreFilterBlockedCountries []string `json:"preFilterBlockedCountries"`
}

func handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	mu.Lock()
	if status.Running {
		mu.Unlock()
		http.Error(w, "Pipeline already running", http.StatusConflict)
		return
	}
	status = PipelineStatus{Running: true, Logs: []string{}}
	mu.Unlock()
	broadcast()

	var rc RunConfig
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&rc); err != nil {
			addLog("警告: 解析请求配置失败: " + err.Error())
		}
	}

	addLog(fmt.Sprintf("接收到配置: 前置端口过滤=%v(%t), 端口=%v, 前置黑名单过滤=%v(%t), 黑名单国家=%v",
		rc.PreFilterPortEnabled, boolPtrVal(rc.PreFilterPortEnabled), rc.PreFilterPorts, rc.PreFilterBlockedEnabled, boolPtrVal(rc.PreFilterBlockedEnabled), rc.PreFilterBlockedCountries))

	ctx, cancel := context.WithCancel(context.Background())
	cancelPipeline = cancel

	go runPipeline(ctx, rc)

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

func handleStop(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	if !status.Running {
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":false,"msg":"没有正在运行的任务"}`))
		return
	}
	mu.Unlock()
	if cancelPipeline != nil {
		cancelPipeline()
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true,"msg":"正在终止任务..."}`))
}

func runPipeline(ctx context.Context, rc RunConfig) {
	defer func() {
		mu.Lock()
		status.Running = false
		mu.Unlock()
		broadcast()
	}()

	addLog("开始 CFNB 管道...")
	addLog("================================")
	updateProgress("Phase 1/6: 获取数据源")

	cfg, err := config.Load("config.json")
	if err != nil {
		addLog("错误: 无法加载配置文件: " + err.Error())
		return
	}

	if rc.BandwidthSize > 0 {
		cfg.BandwidthSizeMB = rc.BandwidthSize
	}
	if rc.Candidates > 0 {
		cfg.BandwidthCandidates = rc.Candidates
	}
	if rc.TCPProbes > 0 {
		cfg.TCPProbes = rc.TCPProbes
	}
	if rc.TCPWorkers > 0 {
		cfg.MaxWorkers = rc.TCPWorkers
	}
	if rc.TCPTimeout > 0 {
		cfg.Timeout = rc.TCPTimeout
	}
	if rc.MinSuccessRate > 0 {
		cfg.MinSuccessRate = rc.MinSuccessRate
	}
	if rc.BandwidthWorkers > 0 {
		cfg.BandwidthWorkers = rc.BandwidthWorkers
	}
	if rc.BandwidthTimeout > 0 {
		cfg.BandwidthTimeout = rc.BandwidthTimeout
	}
	if rc.Mode == "percountry" {
		cfg.UseGlobalMode = false
	} else {
		cfg.UseGlobalMode = true
	}
	if rc.GlobalTopN > 0 {
		cfg.GlobalTopN = rc.GlobalTopN
	}
	if rc.PerCountryTopN > 0 {
		cfg.PerCountryTopN = rc.PerCountryTopN
	}
	if rc.TestAvailability != nil {
		cfg.TestAvailability = *rc.TestAvailability
	}
	if rc.HTTPTestEnabled != nil {
		cfg.HTTPTestEnabled = *rc.HTTPTestEnabled
	}
	if len(rc.SourceURLs) > 0 {
		sources := make([]config.Source, 0, len(rc.SourceURLs))
		for _, u := range rc.SourceURLs {
			u = strings.TrimSpace(u)
			if u != "" {
				sources = append(sources, config.Source{URL: u, Enabled: true})
			}
		}
		if len(sources) > 0 {
			cfg.AdditionalSources = sources
		}
	}
	if len(rc.DirectIPs) > 0 {
		cfg.DirectNodes = rc.DirectIPs
	}
	if rc.UseURLSource != nil {
		cfg.UseURLSource = *rc.UseURLSource
	}
	if rc.PreFilterPortEnabled != nil {
		cfg.PreFilterPortEnabled = *rc.PreFilterPortEnabled
	}
	if len(rc.PreFilterPorts) > 0 {
		cfg.PreFilterPorts = rc.PreFilterPorts
	}
	if rc.PreFilterBlockedEnabled != nil {
		cfg.PreFilterBlockedEnabled = *rc.PreFilterBlockedEnabled
	}
	if len(rc.PreFilterBlockedCountries) > 0 {
		cfg.PreFilterBlockedCountries = rc.PreFilterBlockedCountries
	}

	pr, pw, err := os.Pipe()
	if err != nil {
		addLog("错误: 无法创建管道: " + err.Error())
		return
	}
	defer pr.Close()

	oldStdout := os.Stdout
	os.Stdout = pw

	restoreStdout := func() {
		pw.Close()
		os.Stdout = oldStdout
	}

	outBuf := &bytes.Buffer{}
	done := make(chan error, 1)

	var result *pipeline.Result
	go func() {
		defer restoreStdout()
		r, err := pipeline.Run(ctx, cfg, io.MultiWriter(pw, outBuf))
		result = r
		pw.Close()
		done <- err
	}()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastOutput string
	scannerDone := make(chan struct{}, 1)

	go func() {
		scanner := bufio.NewScanner(pr)
		for scanner.Scan() {
			line := scanner.Text()
			if line != "" {
				addLog(line)
				updateProgressFromLog(line)
			}
		}
		scannerDone <- struct{}{}
	}()

	for {
		select {
		case <-ctx.Done():
			pw.Close()
			restoreStdout()
			<-scannerDone
			addLog("================================")
			addLog("任务已被用户终止")
			loadResults()
			return
		case <-ticker.C:
			output := outBuf.String()
			if output != lastOutput {
				lastOutput = output
			}
		case <-scannerDone:
			output := outBuf.String()
			if output != lastOutput {
				lastOutput = output
			}
			addLog("================================")
			addLog("Pipeline completed successfully!")
			loadResults()
			return
		case err := <-done:
			<-scannerDone
			mu.Lock()
			lastPipelineResult = result
			mu.Unlock()
			output := outBuf.String()
			if output != lastOutput {
				lastOutput = output
			}
			if err != nil {
				addLog("Pipeline completed with errors: " + err.Error())
			} else {
				addLog("================================")
				addLog("Pipeline completed successfully!")
				loadResults()
			}
			return
		}
	}
}

func updateProgressFromLog(line string) {
	switch {
	case strings.Contains(line, "备用API查询"):
		updateProgress("Phase 2/6: Querying backup API...")
	case strings.Contains(line, "正在请求数据源"):
		updateProgress("Phase 2/6: Fetching data sources...")
	case strings.Contains(line, "合并后总计"):
		updateProgress("Phase 3/6: TCP latency testing...")
	case strings.Contains(line, "开始 TCP"):
		updateProgress("Phase 3/6: TCP latency testing...")
	case strings.Contains(line, "TCP 测试完成"):
		updateProgress("Phase 4/6: Availability & HTTP testing...")
	case strings.Contains(line, "可用性"):
		updateProgress("Phase 4/6: Availability testing...")
	case strings.Contains(line, "HTTP检测"):
		updateProgress("Phase 4/6: HTTP testing...")
	case strings.Contains(line, "带宽测速"):
		updateProgress("Phase 5/6: Bandwidth testing...")
	case strings.Contains(line, "结果已保存"):
		updateProgress("Phase 6/6: Finalizing results...")
	case strings.Contains(line, "已自动推送"):
		updateProgress("Completed!")
	}
}

func loadResults() {
	data, err := os.ReadFile("ip.txt")
	if err != nil {
		addLog("Could not read results file.")
		return
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	nodes := make([]NodeInfo, 0)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		node := NodeInfo{Node: line}
		parts := strings.Fields(line)
		if len(parts) > 0 {
			node.Node = parts[0]
		}
		if idx := strings.Index(node.Node, "#"); idx >= 0 {
			node.CCTag = node.Node[idx+1:]
			node.Country = countryName(node.CCTag)
		}

		for i := 0; i < len(parts); i++ {
			switch parts[i] {
			case "Mbps":
				if i > 0 {
					fmt.Sscanf(parts[i-1], "%f", &node.Speed)
				}
			case "ms":
				if i > 0 {
					fmt.Sscanf(parts[i-1], "%f", &node.Latency)
				}
			}
		}
		mu.Lock()
		if lastPipelineResult != nil {
			if ps, ok := lastPipelineResult.PeakSpeedMap[node.Node]; ok {
				node.PeakSpeed = ps
			}
			if hl, ok := lastPipelineResult.HTTPLatencyMap[node.Node]; ok {
				node.HTTPLatency = hl
			}
			if hj, ok := lastPipelineResult.HTTPJitterMap[node.Node]; ok {
				node.HTTPJitter = hj
			}
			if cc, ok := lastPipelineResult.CountryInfo[node.Node]; ok && cc != "" {
				node.CountryCode = cc
			}
			if co, ok := lastPipelineResult.ColoInfo[node.Node]; ok && co != "" {
				node.ColoCode = co
			}
		}
		if node.CountryCode == "" && node.CCTag != "" {
			if cc, ok := coloCountryMap[node.CCTag]; ok {
				node.CountryCode = cc
			}
		}
		mu.Unlock()
		nodes = append(nodes, node)
	}

	results := &PipelineResults{Nodes: nodes}
	if len(nodes) > 0 {
		var totalSpeed, totalLatency float64
		speedCount, latCount := 0, 0
		for _, n := range nodes {
			if n.Speed > 0 {
				totalSpeed += n.Speed
				speedCount++
			}
			if n.Latency > 0 {
				totalLatency += n.Latency
				latCount++
			}
		}
		if speedCount > 0 {
			results.TotalBandwidth = totalSpeed
		}
		if latCount > 0 {
			results.AvgLatency = totalLatency / float64(latCount)
		}
	}
	mu.Lock()
	if lastPipelineResult != nil {
		results.TotalTime = lastPipelineResult.TotalTime.Seconds()
	}
	mu.Unlock()

	mu.Lock()
	status.Results = results
	mu.Unlock()
	broadcast()
}

func addLog(msg string) {
	mu.Lock()
	status.Logs = append(status.Logs, msg)
	if len(status.Logs) > 200 {
		tail := make([]string, 200)
		copy(tail, status.Logs[len(status.Logs)-200:])
		status.Logs = tail
	}
	mu.Unlock()
	broadcast()
}

func updateProgress(p string) {
	mu.Lock()
	status.Progress = p
	mu.Unlock()
	broadcast()
}

func broadcast() {
	notifMu.Lock()
	defer notifMu.Unlock()
	for _, ch := range notifs {
		select {
		case ch <- true:
		default:
		}
	}
}

func countryName(code string) string {
	names := map[string]string{
		"US": "United States", "GB": "United Kingdom", "DE": "Germany",
		"JP": "Japan", "KR": "South Korea", "SG": "Singapore",
		"NL": "Netherlands", "FR": "France", "CA": "Canada",
		"AU": "Australia", "HK": "Hong Kong", "IN": "India",
		"BR": "Brazil", "RU": "Russia", "CN": "China",
		"TW": "Taiwan", "VN": "Vietnam", "TH": "Thailand",
		"MY": "Malaysia", "ID": "Indonesia", "PH": "Philippines",
		"CH": "Switzerland", "SE": "Sweden", "IT": "Italy",
		"ES": "Spain", "PL": "Poland", "UA": "Ukraine",
		"TR": "Turkey", "ZA": "South Africa", "AE": "UAE",
		"IE": "Ireland", "NO": "Norway", "FI": "Finland",
		"DK": "Denmark", "AT": "Austria", "BE": "Belgium",
		"PT": "Portugal", "GR": "Greece", "CZ": "Czech Republic",
		"HU": "Hungary", "RO": "Romania", "BG": "Bulgaria",
		"NZ": "New Zealand", "CL": "Chile", "AR": "Argentina",
		"MX": "Mexico", "CO": "Colombia", "PE": "Peru",
	}
	if name, ok := names[strings.ToUpper(code)]; ok {
		return name
	}
	return code
}

func boolPtrVal(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}


