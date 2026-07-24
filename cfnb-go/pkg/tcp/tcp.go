package tcp

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"cfnb/pkg/parser"
)

type TCPResult struct {
	Node  string
	Latency float64
	Country string
	Success int
}

func testLatency(ip, port string, timeout float64, probes int) (minLatency float64, success int) {
	minLatency = 1e9
	for i := 0; i < probes; i++ {
		start := time.Now()
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(ip, port), time.Duration(timeout*float64(time.Second)))
		if err != nil {
			continue
		}
		latency := time.Since(start).Seconds()
		conn.Close()

		if latency < minLatency {
			minLatency = latency
		}
		success++
	}
	return
}

func TestNode(nodeStr string, timeout float64, probes int, minSuccessRate float64) *TCPResult {
	m := parser.IpportPattern.FindStringSubmatch(nodeStr)
	if m == nil {
		return nil
	}
	ip, port := m[1], m[2]

	country := ""
	if idx := strings.Index(nodeStr, "#"); idx >= 0 {
		country = strings.SplitN(nodeStr[idx+1:], " ", 2)[0]
	}

	minLat, success := testLatency(ip, port, timeout, probes)
	if success == 0 || float64(success)/float64(probes) < minSuccessRate {
		return nil
	}
	return &TCPResult{
		Node:    nodeStr,
		Latency: minLat,
		Country: country,
		Success: success,
	}
}

func TestAll(nodes []string, timeout float64, probes int, minSuccessRate float64, workers int, progressInterval int) []TCPResult {
	total := len(nodes)
	if total == 0 {
		return nil
	}

	fmt.Printf("开始 TCP 连接测试（超时 %.0fs，并发 %d）...\n", timeout, workers)

	tasks := make(chan string, total)
	results := make(chan *TCPResult, total)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for node := range tasks {
				res := TestNode(node, timeout, probes, minSuccessRate)
				results <- res
			}
		}()
	}

	for _, node := range nodes {
		tasks <- node
	}
	close(tasks)

	go func() {
		wg.Wait()
		close(results)
	}()

	var allResults []TCPResult
	completed := 0
	lastPrint := time.Now()
	for r := range results {
		completed++
		if r != nil {
			allResults = append(allResults, *r)
		}
		now := time.Now()
		if now.Sub(lastPrint) >= time.Duration(progressInterval)*time.Second || completed == total {
			fmt.Printf("\r进度：%d/%d (%.1f%%)", completed, total, float64(completed)/float64(total)*100)
			lastPrint = now
		}
	}
	fmt.Println("\nTCP 测试完成！")
	return allResults
}
