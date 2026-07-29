package pipeline

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"cfnb/pkg/bandwidth"
	"cfnb/pkg/config"
	"cfnb/pkg/tcp"
)

type QuickNodeInfo struct {
	Node      string  `json:"node"`
	Speed     float64 `json:"speed"`
	PeakSpeed float64 `json:"peakSpeed"`
	Latency   float64 `json:"latency"`
	CCTag     string  `json:"ccTag"`
	Country   string  `json:"country"`
	Provider  string  `json:"provider"`
	CountryCode string `json:"countryCode"`
}

type QuickResult struct {
	Nodes     []QuickNodeInfo `json:"nodes"`
	TotalTime float64         `json:"totalTime"`
}

func QuickScan(ctx context.Context, cfg *config.Config, minBandwidth float64, desiredCount int, output io.Writer) (*QuickResult, error) {
	startTime := time.Now()
	log := func(format string, args ...interface{}) {
		fmt.Fprintln(output, fmt.Sprintf(format, args...))
	}

	oldStdout := os.Stdout
	pr, pw, _ := os.Pipe()
	os.Stdout = pw
	done := make(chan struct{})
	go func() {
		defer pr.Close()
		buf := make([]byte, 4096)
		for {
			n, err := pr.Read(buf)
			if n > 0 {
				output.Write(buf[:n])
			}
			if err != nil {
				close(done)
				return
			}
		}
	}()
	defer func() {
		pw.Close()
		os.Stdout = oldStdout
		<-done
	}()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if cfg.ForceDirect {
		for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"} {
			os.Unsetenv(key)
		}
		os.Setenv("NO_PROXY", "*")
	}

	log("开始快筛...")
	log("目标带宽: %.0f Mbps | 期望数量: %d 个", minBandwidth, desiredCount)

	nodes := fetchAllSources(ctx, cfg, log)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("没有获取到任何有效节点")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	nodes = prepFilter(nodes, cfg, log)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("过滤后无任何节点")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tcpResults := tcp.TestAll(ctx, nodes, cfg.Timeout, cfg.TCPProbes, cfg.MinSuccessRate, cfg.MaxWorkers)
	if len(tcpResults) == 0 {
		return nil, fmt.Errorf("没有通过 TCP 测试的节点")
	}
	sortTCPResults(tcpResults)

	latencyMap := make(map[string]float64)
	for _, r := range tcpResults {
		latencyMap[r.Node] = r.Latency
	}

	candidates, _ := selectCandidates(tcpResults, cfg, log)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("候选池为空")
	}

quickSizeMB := 10.0
	quickTimeout := 15.0
	bwURL := strings.Replace(cfg.BandwidthURLTemplate, "{bytes}", fmt.Sprintf("%d", int(quickSizeMB*1024*1024)), 1)
	quickWorkers := 3
	log("\n快速测速（文件大小 %.1fMB，并发 %d，超时 %.0fs）...", quickSizeMB, quickWorkers, quickTimeout)

	allResults := bandwidth.Filter(ctx, candidates, bwURL, cfg.BandwidthConnectTimeout, quickTimeout, cfg.BandwidthProcessBuffer, quickSizeMB, quickWorkers, cfg.ProgressPrintInterval)
	if len(allResults) == 0 {
		return nil, fmt.Errorf("测速无有效结果")
	}

	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Speed > allResults[j].Speed
	})

	var aboveThreshold []bandwidth.Result
	for _, r := range allResults {
		if r.Speed >= minBandwidth {
			aboveThreshold = append(aboveThreshold, r)
		}
	}

	log("测速完成: %d 个节点高于 %.0f Mbps", len(aboveThreshold), minBandwidth)

	if len(aboveThreshold) == 0 {
		aboveThreshold = allResults
	}

	keep := desiredCount
	if keep > len(aboveThreshold) {
		keep = len(aboveThreshold)
	}
	aboveThreshold = aboveThreshold[:keep]

	log("\n================ 快筛结果 ================")

	var quickNodes []QuickNodeInfo
	for i, r := range aboveThreshold {
		lat := latencyMap[r.Node] * 1000
		ccTag := ""
		country := ""
		countryCode := ""
		if idx := strings.IndexByte(r.Node, '#'); idx >= 0 {
			ccTag = r.Node[idx+1:]
			country = ccTag
		}

		log("%d. %s 速度 %.2f Mbps 延迟 %.2f ms", i+1, r.Node, r.Speed, lat)
		quickNodes = append(quickNodes, QuickNodeInfo{
			Node:        r.Node,
			Speed:       r.Speed,
			PeakSpeed:   r.PeakMbps,
			Latency:     lat,
			CCTag:       ccTag,
			Country:     country,
			CountryCode: countryCode,
		})
	}

	return &QuickResult{
		Nodes:     quickNodes,
		TotalTime: time.Since(startTime).Seconds(),
	}, nil
}