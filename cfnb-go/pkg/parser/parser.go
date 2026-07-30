package parser

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

var (
	IpportPattern   = regexp.MustCompile(`^(\d+\.\d+\.\d+\.\d+):(\d+)#?`)
	pureIPPort       = regexp.MustCompile(`^(\d+\.\d+\.\d+\.\d+:\d+)$`)
	tokenSplitter    = regexp.MustCompile(`[\s,;|/]+`)
	leadingNonAlpha  = regexp.MustCompile(`^[\d\s\-_.|#]+`)
	cnNamePattern    = regexp.MustCompile(`^([\p{Han}（）()]+)\d*$`)
	alphaCodePattern = regexp.MustCompile(`^([A-Z]{2,3})$`)
)

func ExtractCountryCode(label string) string {
	label = strings.TrimSpace(label)
	if label == "" {
		return ""
	}

	tokens := tokenSplitter.Split(label, -1)

	for _, token := range tokens {
		token = leadingNonAlpha.ReplaceAllString(strings.TrimSpace(token), "")
		match := alphaCodePattern.FindStringSubmatch(token)
		if match != nil {
			code := match[1]
			if len(code) == 3 {
				if alpha2, ok := Alpha3ToAlpha2[code]; ok {
					return alpha2
				}
			} else if len(code) == 2 {
				if CodeSet[code] {
					return code
				}
			}
		}
	}

	for _, token := range tokens {
		token = leadingNonAlpha.ReplaceAllString(strings.TrimSpace(token), "")
		token = stripEmojiFlags(token)
		if cnMatch := cnNamePattern.FindStringSubmatch(token); cnMatch != nil {
			if code, ok := CNToCode[cnMatch[1]]; ok {
				return code
			}
		}
	}

	runes := []rune(label)
	var emojiChars []rune
	for _, r := range runes {
		if r >= 0x1F1E6 && r <= 0x1F1FF {
			emojiChars = append(emojiChars, r)
		}
	}
	if len(emojiChars) >= 2 && len(emojiChars)%2 == 0 {
		first := int(emojiChars[0]) - 0x1F1E6
		second := int(emojiChars[1]) - 0x1F1E6
		if first >= 0 && first <= 25 && second >= 0 && second <= 25 {
			return string(rune(first+'A')) + string(rune(second+'A'))
		}
	}

	return ""
}

func stripEmojiFlags(s string) string {
	var result []rune
	for _, r := range s {
		if r >= 0x1F1E6 && r <= 0x1F1FF {
			continue
		}
		result = append(result, r)
	}
	return string(result)
}

func parseJSONNodes(data interface{}) []string {
	nodes := make([]string, 0)
	switch v := data.(type) {
	case []interface{}:
		for _, item := range v {
			nodes = append(nodes, parseJSONNodes(item)...)
		}
	case map[string]interface{}:
		hasArrayKey := false
		for _, key := range []string{"nodes", "data", "result", "list"} {
			if arr, ok := v[key].([]interface{}); ok {
				nodes = append(nodes, parseJSONNodes(arr)...)
				hasArrayKey = true
				break
			}
		}
		if !hasArrayKey {
			ip := stringValue(v, "ip")
			if ip == "" {
				ip = stringValue(v, "host")
			}
			port := stringValue(v, "port")
			code := stringValue(v, "country")
			if code == "" {
				code = stringValue(v, "cc")
			}
			if ip != "" && port != "" && code != "" {
				nodes = append(nodes, fmt.Sprintf("%s:%s#%s", ip, port, strings.ToUpper(code)))
			}
		}
	case string:
		parsed, _ := parseTextNodes(v)
		nodes = append(nodes, parsed...)
	}
	return nodes
}

func stringValue(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == float64(int(val)) {
			return fmt.Sprintf("%d", int(val))
		}
		return fmt.Sprintf("%v", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func parseTextNodes(text string) (parsed []string, pending []string) {
	nodes := make([]string, 0)
	unresolved := make([]string, 0)

	tokens := strings.Fields(text)
	for _, token := range tokens {
		if pureIPPort.MatchString(token) {
			unresolved = append(unresolved, token)
			continue
		}

		if !strings.Contains(token, "#") {
			continue
		}

		parts := strings.SplitN(token, "#", 2)
		if len(parts) != 2 {
			continue
		}
		ipport := strings.TrimSpace(parts[0])
		label := strings.TrimSpace(parts[1])

		if strings.HasPrefix(ipport, "[") {
			continue
		}
		if !IpportPattern.MatchString(ipport) {
			continue
		}

		code := ExtractCountryCode(label)
		if code != "" {
			nodes = append(nodes, fmt.Sprintf("%s#%s", ipport, code))
		} else {
			unresolved = append(unresolved, ipport)
		}
	}

	return nodes, unresolved
}

func ParseAdaptive(text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}

	if strings.HasPrefix(text, "{") || strings.HasPrefix(text, "[") {
		var data interface{}
		if err := json.Unmarshal([]byte(text), &data); err == nil {
			return parseJSONNodes(data)
		}
	}

	parsed, _ := parseTextNodes(text)
	return parsed
}

func ParseAdaptiveWithFallback(ctx context.Context, text string, availabilityAPI string, connectTimeout, readTimeout float64, workers int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}

	if strings.HasPrefix(text, "{") || strings.HasPrefix(text, "[") {
		var data interface{}
		if err := json.Unmarshal([]byte(text), &data); err == nil {
			return parseJSONNodes(data)
		}
	}

	parsed, pending := parseTextNodes(text)
	if len(pending) == 0 {
		return parsed
	}

	fmt.Printf("%d 个节点未能识别或缺少国家，通过可用性检测 API 查询国家...\n", len(pending))
	os.Stdout.Sync()
	resolved := ResolveCountriesBatch(ctx, pending, availabilityAPI, connectTimeout, readTimeout, workers, 1)
	for ipport, code := range resolved {
		if code != "" {
			parsed = append(parsed, fmt.Sprintf("%s#%s", ipport, code))
		} else {
			parsed = append(parsed, ipport)
		}
	}

	return parsed
}

type FetchResult struct {
	Nodes []string
	Error error
}

func FetchSource(ctx context.Context, urlStr string, maxRetries int, retryDelay float64, connectTimeout float64, readTimeout float64) ([]string, error) {
	return FetchSourceWithFallback(ctx, urlStr, maxRetries, retryDelay, connectTimeout, readTimeout, "", 0, 0, 0)
}

func FetchSourceWithFallback(ctx context.Context, urlStr string, maxRetries int, retryDelay float64, connectTimeout float64, readTimeout float64, availabilityAPI string, availConnectTimeout, availReadTimeout float64, fallbackWorkers int) ([]string, error) {
	if urlStr == "" {
		return nil, nil
	}

	client := &http.Client{
		Timeout: time.Duration((connectTimeout + readTimeout + 20) * float64(time.Second)),
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		msg := fmt.Sprintf("正在拉取数据源: %s (尝试 %d/%d)", urlStr, attempt, maxRetries)
		fmt.Println(msg)
		os.Stdout.Sync()

		req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			msg := fmt.Sprintf("请求超时")
			fmt.Println(msg)
			os.Stdout.Sync()
			if attempt < maxRetries {
				retryMsg := fmt.Sprintf("等待%.0fs后重试", retryDelay)
				fmt.Println(retryMsg)
				os.Stdout.Sync()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(time.Duration(retryDelay * float64(time.Second))):
				}
			}
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			readErrMsg := fmt.Sprintf("读取响应失败 (%s): %v", urlStr, err)
			fmt.Println(readErrMsg)
			os.Stdout.Sync()
			if attempt < maxRetries {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(time.Duration(retryDelay * float64(time.Second))):
				}
			}
			continue
		}

		var nodes []string
		if availabilityAPI != "" {
			nodes = ParseAdaptiveWithFallback(ctx, string(body), availabilityAPI, availConnectTimeout, availReadTimeout, fallbackWorkers)
		} else {
			nodes = ParseAdaptive(string(body))
		}
		fmt.Printf("解析完毕 | 共获取 IP ：%d 个\n", len(nodes))
		os.Stdout.Sync()
		return nodes, nil
	}

	return nil, fmt.Errorf("已尝试 %d 次，放弃该数据源", maxRetries)
}

func QueryCountry(ipport, apiURL string, connectTimeout, readTimeout float64) string {
	return queryCountryAPI(ipport, apiURL, connectTimeout, readTimeout)
}

func queryCountryAPI(ipport, apiURL string, connectTimeout, readTimeout float64) string {
	parts := strings.Split(ipport, ":")
	if len(parts) != 2 {
		return ""
	}

	client := &http.Client{
		Timeout: time.Duration((connectTimeout + readTimeout) * float64(time.Second)),
	}

	reqURL := fmt.Sprintf("%s?proxyip=%s", apiURL, url.QueryEscape(ipport))
	resp, err := client.Get(reqURL)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ""
	}

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return ""
	}

	probeResults, ok := data["probe_results"].(map[string]interface{})
	if !ok {
		return ""
	}
	ipv4, ok := probeResults["ipv4"].(map[string]interface{})
	if !ok {
		return ""
	}
	exit, ok := ipv4["exit"].(map[string]interface{})
	if !ok {
		return ""
	}
	country, ok := exit["country"].(string)
	if !ok || len(country) != 2 {
		return ""
	}
	return strings.ToUpper(country)
}

func ResolveCountriesBatch(ctx context.Context, ipports []string, apiURL string, connectTimeout, readTimeout float64, workers int, progressInterval int) map[string]string {
	results := make(map[string]string)
	total := len(ipports)
	if total == 0 {
		return results
	}

	sem := make(chan struct{}, workers)
	resultCh := make(chan struct {
		ipport string
		code   string
	}, total)

	for _, ipp := range ipports {
		sem <- struct{}{}
		go func(ip string) {
			defer func() { <-sem }()
			code := QueryCountry(ip, apiURL, connectTimeout, readTimeout)
			resultCh <- struct {
				ipport string
				code   string
			}{ip, code}
		}(ipp)
	}

	completed := 0
	lastPrint := time.Now()
	for completed < total {
		select {
		case <-ctx.Done():
			return results
		case r := <-resultCh:
			results[r.ipport] = r.code
			completed++

			now := time.Now()
			if now.Sub(lastPrint) >= time.Duration(progressInterval)*time.Second || completed == total {
				msg := fmt.Sprintf("[备用API查询] 进度：%d/%d (%.1f%%)", completed, total, float64(completed)/float64(total)*100)
				fmt.Println(msg)
				os.Stdout.Sync()
				lastPrint = now
			}
		}
	}
	fmt.Println()
	return results
}

func ParseWithFallback(ctx context.Context, text string, availabilityAPI string, connectTimeout, readTimeout float64, workers int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}

	if strings.HasPrefix(text, "{") || strings.HasPrefix(text, "[") {
		var data interface{}
		if err := json.Unmarshal([]byte(text), &data); err == nil {
			return parseJSONNodes(data)
		}
	}

	parsed, pending := parseTextNodes(text)
	if len(pending) == 0 {
		return parsed
	}

	fmt.Printf("%d 个节点未能识别或缺少国家，通过可用性检测 API 查询国家...\n", len(pending))
	os.Stdout.Sync()
	resolved := ResolveCountriesBatch(ctx, pending, availabilityAPI, connectTimeout, readTimeout, workers, 1)
	for ipport, code := range resolved {
		if code != "" {
			parsed = append(parsed, fmt.Sprintf("%s#%s", ipport, code))
		} else {
			parsed = append(parsed, ipport)
		}
	}

	return parsed
}
