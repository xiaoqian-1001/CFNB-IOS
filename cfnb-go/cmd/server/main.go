package main

import (
	"bufio"
	"bytes"
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

type PipelineResults struct {
	Nodes          []NodeInfo `json:"nodes"`
	TotalBandwidth float64    `json:"totalBandwidth"`
	AvgLatency     float64    `json:"avgLatency"`
}

type NodeInfo struct {
	Node    string  `json:"node"`
	Speed   float64 `json:"speed"`
	Latency float64 `json:"latency"`
	CCTag   string  `json:"ccTag"`
	Country string  `json:"country"`
}

var (
	status  PipelineStatus
	mu      sync.Mutex
	notifs  []chan bool
	notifMu sync.Mutex
)

func RunServer(port string) {
	http.HandleFunc("/", middlewareCORS(serveIndex))
	http.HandleFunc("/api/run", middlewareCORS(handleRun))
	http.HandleFunc("/api/events", middlewareCORS(handleEvents))
	http.HandleFunc("/api/status", middlewareCORS(handleStatus))

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
	BandwidthSize float64 `json:"bandwidthSize"`
	Candidates    int     `json:"candidates"`
	Mode          string  `json:"mode"`
	TCPProbes     int     `json:"tcpProbes"`
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
		json.NewDecoder(r.Body).Decode(&rc)
	}

	go runPipeline(rc)

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

func runPipeline(rc RunConfig) {
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
	if rc.Mode == "percountry" {
		cfg.UseGlobalMode = false
	} else {
		cfg.UseGlobalMode = true
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

	go func() {
		defer restoreStdout()
		_, err := pipeline.Run(cfg, io.MultiWriter(pw, outBuf))
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
	case strings.Contains(line, "HTTP"):
		updateProgress("Phase 4/6: HTTP testing...")
	case strings.Contains(line, "带宽测") || strings.Contains(line, "带宽"):
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


