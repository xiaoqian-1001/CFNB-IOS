package httpcheck

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type HTTPResult struct {
	Node    string
	Valid   bool
	Server  string
	Latency float64
	Jitter  float64
}

func Check(nodeStr string, timeout, connectTimeout float64, method string, jitterSamples int) HTTPResult {
	ip, port := extractIPPort(nodeStr)
	if ip == "" {
		return HTTPResult{Node: nodeStr, Server: "parse_error"}
	}

	url := fmt.Sprintf("http://%s:%s/cdn-cgi/trace", ip, port)
	client := &http.Client{
		Timeout: time.Duration((connectTimeout + timeout) * float64(time.Second)),
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	testRounds := jitterSamples
	if testRounds < 3 {
		testRounds = 3
	}

	var latencies []float64
	headers := map[string]string{
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
	}

	for i := 0; i < testRounds; i++ {
		req, _ := http.NewRequest(method, url, nil)
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		start := time.Now()
		resp, err := client.Do(req)
		lat := float64(time.Since(start).Microseconds()) / 1000.0

		if err != nil {
			return HTTPResult{Node: nodeStr, Server: "connection_error"}
		}
		resp.Body.Close()

		if resp.StatusCode != 400 {
			return HTTPResult{Node: nodeStr, Server: fmt.Sprintf("status_%d", resp.StatusCode)}
		}

		server := resp.Header.Get("server")
		if !strings.HasPrefix(strings.ToLower(server), "cloudflare") {
			return HTTPResult{Node: nodeStr, Server: server}
		}

		latencies = append(latencies, lat)
	}

	if len(latencies) < testRounds {
		return HTTPResult{Node: nodeStr, Server: "not_enough_samples"}
	}

	var sum float64
	for _, l := range latencies {
		sum += l
	}
	avg := sum / float64(len(latencies))

	var variance float64
	for _, l := range latencies {
		variance += (l - avg) * (l - avg)
	}
	variance /= float64(len(latencies))
	jitter := math.Sqrt(variance)

	return HTTPResult{
		Node:    nodeStr,
		Valid:   true,
		Server:  "cloudflare",
		Latency: avg,
		Jitter:  jitter,
	}
}

func extractIPPort(node string) (string, string) {
	if idx := strings.Index(node, "#"); idx >= 0 {
		node = node[:idx]
	}
	host, port, err := net.SplitHostPort(node)
	if err != nil {
		parts := strings.SplitN(node, ":", 2)
		if len(parts) == 2 {
			return parts[0], parts[1]
		}
		return "", ""
	}
	return host, port
}

func FilterCandidates(ctx context.Context, candidates []string, timeout, connectTimeout float64, method string, jitterSamples int, workers int, progressInterval int) (passed []string, latencyMap map[string]float64, jitterMap map[string]float64) {
	latencyMap = make(map[string]float64)
	jitterMap = make(map[string]float64)

	if len(candidates) == 0 {
		return
	}

	total := len(candidates)

	tasks := make(chan string, total)
	results := make(chan HTTPResult, total)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case node, ok := <-tasks:
					if !ok {
						return
					}
					if ctx.Err() != nil {
						return
					}
					r := Check(node, timeout, connectTimeout, method, jitterSamples)
					select {
					case results <- r:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	for _, node := range candidates {
		tasks <- node
	}
	close(tasks)

	go func() {
		wg.Wait()
		close(results)
	}()

	completed := 0
	lastPrintPct := -1
	for r := range results {
		if ctx.Err() != nil {
			break
		}
		completed++
		if r.Valid {
			passed = append(passed, r.Node)
			latencyMap[r.Node] = r.Latency
			jitterMap[r.Node] = r.Jitter
		}
		printPct := completed * 100 / total
		if printPct/10 != lastPrintPct/10 || completed == total {
			lastPrintPct = printPct
			pctF := float64(completed) / float64(total) * 100
			fmt.Printf("进度：%d/%d (%.1f%%) 通过数量：%d\n", completed, total, pctF, len(passed))
		}
	}
	return
}

func FilterWithRetry(ctx context.Context, candidates []string, timeout, connectTimeout float64, method string, jitterSamples int, workers int, progressInterval int, maxRounds int, roundDelay float64, notify func(string, string)) ([]string, map[string]float64, map[string]float64) {
	if len(candidates) == 0 {
		return candidates, make(map[string]float64), make(map[string]float64)
	}

	for round := 1; round <= maxRounds; round++ {
		if ctx.Err() != nil {
			return candidates, make(map[string]float64), make(map[string]float64)
		}
		fmt.Printf("第 %d 轮检测\n", round)
		passed, latencyMap, jitterMap := FilterCandidates(ctx, candidates, timeout, connectTimeout, method, jitterSamples, workers, progressInterval)
		if len(passed) > 0 {
			return passed, latencyMap, jitterMap
		}
		if round < maxRounds {
			fmt.Printf("本轮 HTTP 检测通过率为 0%%，等待 %.0f 秒后重试...\n", roundDelay)
			select {
			case <-ctx.Done():
				return candidates, make(map[string]float64), make(map[string]float64)
			case <-time.After(time.Duration(roundDelay * float64(time.Second))):
			}
		}
	}

	msg := fmt.Sprintf("HTTP检测经 %d 轮重试后仍无节点通过，已降级使用过滤前列表。", maxRounds)
	fmt.Println(msg)
	if notify != nil {
		notify(msg, "HTTP检测全部失败")
	}
	return candidates, make(map[string]float64), make(map[string]float64)
}
