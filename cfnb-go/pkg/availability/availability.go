package availability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type CheckResult struct {
	Node     string
	OK       bool
	Stack    string
	Country  string
	ExitInfo map[string]string
}

func Check(nodeStr string, apiURL string, connectTimeout, readTimeout float64, innerRetryEnabled bool, innerRetryMax int, innerRetryDelay float64) CheckResult {
	ip, port := extractIPPort(nodeStr)
	if ip == "" {
		return CheckResult{Node: nodeStr, Stack: "unknown"}
	}

	proxyIP := ip + ":" + port
	maxAttempts := 1
	if innerRetryEnabled {
		maxAttempts = innerRetryMax + 1
	}
	delay := time.Duration(0)
	if innerRetryEnabled {
		delay = time.Duration(innerRetryDelay * float64(time.Second))
	}

	client := &http.Client{
		Timeout: time.Duration((connectTimeout + readTimeout) * float64(time.Second)),
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		reqURL := fmt.Sprintf("%s?proxyip=%s", apiURL, url.QueryEscape(proxyIP))
		resp, err := client.Get(reqURL)
		if err != nil {
			if attempt < maxAttempts-1 && delay > 0 {
				time.Sleep(delay)
			}
			continue
		}

		if resp.StatusCode == http.StatusOK {
			var data map[string]interface{}
			decodeErr := json.NewDecoder(resp.Body).Decode(&data)
			resp.Body.Close()
			if decodeErr == nil {
				if success, _ := data["success"].(bool); success {
					stack, _ := data["inferred_stack"].(string)
					exitInfo := make(map[string]string)
					probe, _ := data["probe_results"].(map[string]interface{})
					var exit map[string]interface{}
					if probe != nil {
						if ipv6, ok := probe["ipv6"].(map[string]interface{}); ok {
							if e, ok := ipv6["exit"].(map[string]interface{}); ok {
								exit = e
							}
						}
						if exit == nil {
							if ipv4, ok := probe["ipv4"].(map[string]interface{}); ok {
								if e, ok := ipv4["exit"].(map[string]interface{}); ok {
									exit = e
								}
							}
						}
					}
					if exit != nil {
						for k, v := range exit {
							if s, ok := v.(string); ok {
								exitInfo[k] = s
							}
						}
					}
					country, _ := exitInfo["country"]
					return CheckResult{Node: nodeStr, OK: true, Stack: stack, Country: country, ExitInfo: exitInfo}
				}
			}
		} else {
			resp.Body.Close()
		}

		if attempt < maxAttempts-1 && delay > 0 {
			time.Sleep(delay)
		}
	}

	return CheckResult{Node: nodeStr, Stack: "unknown"}
}

func extractIPPort(node string) (string, string) {
	if idx := strings.Index(node, "#"); idx >= 0 {
		node = node[:idx]
	}
	parts := strings.SplitN(node, ":", 2)
	if len(parts) != 2 {
		return "", ""
	}
	if parts[1] != "" {
		parts[1] = strings.SplitN(parts[1], "#", 2)[0]
	}
	return parts[0], parts[1]
}

func FilterCandidates(ctx context.Context, candidates []string, apiURL string, connectTimeout, readTimeout float64, innerRetryEnabled bool, innerRetryMax int, innerRetryDelay float64, workers int, progressInterval int) (passed []string, ipInfo map[string]string, countryInfo map[string]string, exitDetails map[string]map[string]string) {
	ipInfo = make(map[string]string)
	countryInfo = make(map[string]string)
	exitDetails = make(map[string]map[string]string)

	if len(candidates) == 0 {
		return
	}

	total := len(candidates)

	tasks := make(chan string, total)
	results := make(chan CheckResult, total)

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
					r := Check(node, apiURL, connectTimeout, readTimeout, innerRetryEnabled, innerRetryMax, innerRetryDelay)
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
		if r.OK {
			passed = append(passed, r.Node)
			ipInfo[r.Node] = r.Stack
			countryInfo[r.Node] = r.Country
			exitDetails[r.Node] = r.ExitInfo
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

func FilterWithRetry(ctx context.Context, candidates []string, apiURL string, connectTimeout, readTimeout float64, innerRetryEnabled bool, innerRetryMax int, innerRetryDelay float64, retryMax int, retryDelay float64, workers int, progressInterval int, notify func(string, string)) ([]string, map[string]string, map[string]string, map[string]map[string]string) {
	if len(candidates) == 0 {
		return candidates, make(map[string]string), make(map[string]string), make(map[string]map[string]string)
	}

	for attempt := 1; attempt <= retryMax; attempt++ {
		if ctx.Err() != nil {
			return candidates, make(map[string]string), make(map[string]string), make(map[string]map[string]string)
		}
		fmt.Printf("第 %d 轮检测\n", attempt)
		passed, ipInfo, countryInfo, exitDetails := FilterCandidates(ctx, candidates, apiURL, connectTimeout, readTimeout, innerRetryEnabled, innerRetryMax, innerRetryDelay, workers, progressInterval)
		if len(passed) > 0 {
			return passed, ipInfo, countryInfo, exitDetails
		}
		if attempt < retryMax {
			fmt.Printf("本轮可用性检测通过率为 0%%，等待 %.0f 秒后重试...\n", retryDelay)
			select {
			case <-ctx.Done():
				return candidates, make(map[string]string), make(map[string]string), make(map[string]map[string]string)
			case <-time.After(time.Duration(retryDelay * float64(time.Second))):
			}
		}
	}

	msg := fmt.Sprintf("IP 可用性检测经 %d 轮重试后仍无节点通过，已跳过过滤，使用原候选列表继续。", retryMax)
	fmt.Println(msg)
	if notify != nil {
		notify(msg, "可用性检测全部失败")
	}
	return candidates, make(map[string]string), make(map[string]string), make(map[string]map[string]string)
}
