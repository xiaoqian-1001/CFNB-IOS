package bandwidth

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const peakWindow = 50 * time.Millisecond
const minPeakWindow = 20 * time.Millisecond
const minFinalWindow = 5 * time.Millisecond

type Result struct {
	Node     string
	Speed    float64
	PeakMbps float64
}

func Measure(ctx context.Context, nodeStr string, bwURL string, connectTimeout, timeout, processBuffer, expectedSizeMB float64) Result {
	ip, port := extractIPPort(nodeStr)
	if ip == "" {
		return Result{Node: nodeStr}
	}

	expectedSize := expectedSizeMB * 1024 * 1024

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := &net.Dialer{
				Timeout:   time.Duration(connectTimeout * float64(time.Second)),
				KeepAlive: -1,
			}
			return dialer.DialContext(ctx, "tcp", ip+":"+port)
		},
		DisableKeepAlives: true,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration((timeout + processBuffer) * float64(time.Second)),
	}

	req, err := http.NewRequest("GET", bwURL, nil)
	if err != nil {
		return Result{Node: nodeStr}
	}
	req.Host = "speed.cloudflare.com"
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "*/*")

	totalTimeout := time.Duration((timeout + processBuffer) * float64(time.Second))

	done := make(chan Result, 1)

	go func() {
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			done <- Result{Node: nodeStr}
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			done <- Result{Node: nodeStr}
			return
		}

		timeStartTransfer := time.Since(start).Seconds()

		var totalBytes int64
		peakMbps := 0.0
		windowStart := time.Now()
		windowBytes := int64(0)
		buf := make([]byte, 256*1024)

		for {
			n, err := resp.Body.Read(buf)
		if n > 0 {
			totalBytes += int64(n)
			windowBytes += int64(n)
		}
		elapsed := time.Since(windowStart)
		if elapsed >= peakWindow || err != nil {
			if elapsed >= minPeakWindow {
				windowSpeed := float64(windowBytes) * 8 / (elapsed.Seconds() * 1000 * 1000)
				if windowSpeed > peakMbps {
					peakMbps = windowSpeed
				}
			} else if err == io.EOF && windowBytes > 0 && elapsed >= minFinalWindow {
				windowSpeed := float64(windowBytes) * 8 / (elapsed.Seconds() * 1000 * 1000)
				if windowSpeed > peakMbps {
					peakMbps = windowSpeed
				}
			}
			windowStart = time.Now()
			windowBytes = 0
		}
			if err != nil {
				if err == io.EOF {
					break
				}
				done <- Result{Node: nodeStr}
				return
			}
		}

		timeTotal := time.Since(start).Seconds()

		if totalBytes < int64(expectedSize) {
			done <- Result{Node: nodeStr}
			return
		}

		transferTime := timeTotal - timeStartTransfer
		if transferTime <= 0 {
			done <- Result{Node: nodeStr}
			return
		}
		speedMbps := (float64(totalBytes) * 8) / (transferTime * 1000 * 1000)
		done <- Result{Node: nodeStr, Speed: speedMbps, PeakMbps: peakMbps}
	}()

	select {
	case <-time.After(totalTimeout):
		transport.CloseIdleConnections()
		return Result{Node: nodeStr}
	case result := <-done:
		return result
	case <-ctx.Done():
		transport.CloseIdleConnections()
		return Result{Node: nodeStr}
	}
}

func extractIPPort(node string) (string, string) {
	if idx := strings.Index(node, "#"); idx >= 0 {
		node = node[:idx]
	}
	parts := strings.SplitN(node, ":", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func Filter(ctx context.Context, candidates []string, bwURL string, connectTimeout, timeout, processBuffer, expectedSizeMB float64, workers int, progressInterval int) []Result {
	if len(candidates) == 0 {
		return nil
	}

	total := len(candidates)

	tasks := make(chan string, total)
	results := make(chan Result, total)

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
					r := Measure(ctx, node, bwURL, connectTimeout, timeout, processBuffer, expectedSizeMB)
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

	var allResults []Result
	completed := 0
	progressThreshold := 10
	for r := range results {
		if ctx.Err() != nil {
			break
		}
		completed++
		if r.Speed > 0 {
			allResults = append(allResults, r)
		}
		pct := completed * 100 / total
		if pct >= progressThreshold || completed == total {
			fmt.Printf("进度：%d/%d (%d%%)\n", completed, total, pct)
			progressThreshold += 10
		}
	}

	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Speed > allResults[j].Speed
	})
	return allResults
}

func FilterWithRetry(ctx context.Context, candidates []string, bwURL string, connectTimeout, timeout, processBuffer, expectedSizeMB float64, workers int, progressInterval int, retryMax int, retryDelay float64, notify func(string, string)) []Result {
	for attempt := 1; attempt <= retryMax; attempt++ {
		if ctx.Err() != nil {
			return nil
		}
		fmt.Printf("第 %d 轮检测\n", attempt)
		results := Filter(ctx, candidates, bwURL, connectTimeout, timeout, processBuffer, expectedSizeMB, workers, progressInterval)
		if len(results) > 0 {
			return results
		}
		if attempt < retryMax {
			fmt.Printf("本轮测速无有效结果，等待 %.0f 秒后重试...\n", retryDelay)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Duration(retryDelay * float64(time.Second))):
			}
		}
	}

	if notify != nil {
		notify(fmt.Sprintf("带宽测速经 %d 轮尝试后仍无有效结果。", retryMax), "带宽测速全部失败")
	}
	return nil
}