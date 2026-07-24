package bandwidth

import (
	"fmt"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Result struct {
	Node  string
	Speed float64
}

func Measure(nodeStr string, bwURL string, connectTimeout, timeout, processBuffer, expectedSizeMB float64) Result {
	ip, port := extractIPPort(nodeStr)
	if ip == "" {
		return Result{Node: nodeStr}
	}

	nullDevice := "/dev/null"
	if runtime.GOOS == "windows" {
		nullDevice = "NUL"
	}

	expectedSize := expectedSizeMB * 1024 * 1024

	args := []string{
		"-s", "-o", nullDevice,
		"-w", "%{size_download} %{time_starttransfer} %{time_total}",
		"-L",
		"-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
		"--http2",
		"--noproxy", "*",
		"--resolve", fmt.Sprintf("speed.cloudflare.com:%s:%s", port, ip),
		"--connect-timeout", fmt.Sprintf("%.0f", connectTimeout),
		"--max-time", fmt.Sprintf("%.0f", timeout),
		"--insecure",
		bwURL,
	}

	cmd := exec.Command("curl", args...)
	totalTimeout := time.Duration((timeout + processBuffer) * float64(time.Second))

	done := make(chan struct {
		stdout string
		err    error
	}, 1)

	go func() {
		out, err := cmd.Output()
		done <- struct {
			stdout string
			err    error
		}{string(out), err}
	}()

	select {
	case <-time.After(totalTimeout):
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		return Result{Node: nodeStr}
	case result := <-done:
		if result.err != nil {
			return Result{Node: nodeStr}
		}
		stdout := strings.TrimSpace(result.stdout)
		if stdout == "" {
			return Result{Node: nodeStr}
		}
		parts := strings.Fields(stdout)
		if len(parts) < 3 {
			return Result{Node: nodeStr}
		}
		sizeBytes, err := strconv.ParseFloat(parts[0], 64)
		if err != nil || sizeBytes < expectedSize {
			return Result{Node: nodeStr}
		}
		timeStartTransfer, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return Result{Node: nodeStr}
		}
		timeTotal, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			return Result{Node: nodeStr}
		}
		transferTime := timeTotal - timeStartTransfer
		if transferTime <= 0 {
			return Result{Node: nodeStr}
		}
		speedMbps := (sizeBytes * 8) / (transferTime * 1000 * 1000)
		return Result{Node: nodeStr, Speed: speedMbps}
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

func Filter(candidates []string, bwURL string, connectTimeout, timeout, processBuffer, expectedSizeMB float64, workers int, progressInterval int) []Result {
	if len(candidates) == 0 {
		return nil
	}

	if _, err := exec.LookPath("curl"); err != nil {
		fmt.Println("未检测到 curl 命令，带宽测速将跳过。")
		return nil
	}

	fmt.Printf("\n开始带宽测速（对前 %d 个节点，并发 %d，超时 %.0fs）...\n", len(candidates), workers, timeout)
	total := len(candidates)

	tasks := make(chan string, total)
	results := make(chan Result, total)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for node := range tasks {
				results <- Measure(node, bwURL, connectTimeout, timeout, processBuffer, expectedSizeMB)
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
	lastPrint := time.Now()
	for r := range results {
		completed++
		if r.Speed > 0 {
			allResults = append(allResults, r)
		}
		now := time.Now()
		if now.Sub(lastPrint) >= time.Duration(progressInterval)*time.Second || completed == total {
			fmt.Printf("\r[带宽测速] 进度：%d/%d (%.1f%%)", completed, total, float64(completed)/float64(total)*100)
			lastPrint = now
		}
	}

	fmt.Println()

	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Speed > allResults[j].Speed
	})
	return allResults
}

func FilterWithRetry(candidates []string, bwURL string, connectTimeout, timeout, processBuffer, expectedSizeMB float64, workers int, progressInterval int, retryMax int, retryDelay float64, notify func(string, string)) []Result {
	for attempt := 1; attempt <= retryMax; attempt++ {
		fmt.Printf("\n[带宽测速] 第 %d 轮测试...\n", attempt)
		results := Filter(candidates, bwURL, connectTimeout, timeout, processBuffer, expectedSizeMB, workers, progressInterval)
		if len(results) > 0 {
			return results
		}
		if attempt < retryMax {
			fmt.Printf("本轮测速无有效结果，等待 %.0f 秒后重试...\n", retryDelay)
			time.Sleep(time.Duration(retryDelay * float64(time.Second)))
		}
	}

	fmt.Println("\n带宽测速多次重试仍无有效结果。")
	if notify != nil {
		notify(fmt.Sprintf("带宽测速经 %d 轮尝试后仍无有效结果。", retryMax), "带宽测速全部失败")
	}
	return nil
}
