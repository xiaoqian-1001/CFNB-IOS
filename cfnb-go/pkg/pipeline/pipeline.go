package pipeline

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"cfnb/pkg/availability"
	"cfnb/pkg/bandwidth"
	"cfnb/pkg/cloudflare"
	"cfnb/pkg/config"
	"cfnb/pkg/httpcheck"
	"cfnb/pkg/notify"
	"cfnb/pkg/parser"
	"cfnb/pkg/ranking"
	"cfnb/pkg/sync"
	"cfnb/pkg/tcp"
	"cfnb/pkg/whois"
)

type Result struct {
	SelectedNodes  []string
	SpeedMap       map[string]float64
	PeakSpeedMap   map[string]float64
	LatencyMap     map[string]float64
	HTTPLatencyMap map[string]float64
	HTTPJitterMap  map[string]float64
	CountryInfo    map[string]string
	ColoInfo       map[string]string
	ProviderMap    map[string]string
	TotalTime      time.Duration
}

func Run(ctx context.Context, cfg *config.Config, output io.Writer) (*Result, error) {
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

	notifier := func(content, summary string) {
		notify.SendWxPusher(cfg.EnableWxPusher, cfg.WxPusherAppToken, cfg.WxPusherUIDs, cfg.WxPusherAPIURL, cfg.NotifyConnectTimeout, cfg.NotifyTimeout, content, summary)
	}

	log("【 扫描配置 】")
	if cfg.UseGlobalMode {
		log("模式：全局最优TOP%d | TCP探测：%d次 | 最低成功率：%.0f%%", cfg.GlobalTopN, cfg.TCPProbes, cfg.MinSuccessRate*100)
	} else {
		log("模式：每个国家最优TOP%d | TCP探测：%d次 | 最低成功率：%.0f%%", cfg.PerCountryTopN, cfg.TCPProbes, cfg.MinSuccessRate*100)
	}
	log("可用性检测：%s | HTTP检测：%s | 国家黑名单：%s ",
		boolStr(cfg.TestAvailability), boolStr(cfg.HTTPTestEnabled),
		boolStr(cfg.PreFilterBlockedEnabled))
	log("DNS黑名单过滤：%s | 风险等级过滤：%s | IPV6落地过滤：%s",
		boolStr(cfg.DNSIPRiskFilterEnabled),
		map[bool]string{true: cfg.DNSIPRiskMaxLevel, false: "禁用"}[cfg.DNSIPRiskFilterEnabled],
		boolStr(cfg.FilterIPv6Availability))
	log("候选上限：%d | 测速文件：%.1fMB | 测速超时：%.0fs", cfg.BandwidthCandidates, cfg.BandwidthSizeMB, cfg.BandwidthTimeout)
	log("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

	if cfg.FilterCountriesEnabled {
		log("前置白名单过滤：启用，仅保留：%s", strings.Join(cfg.AllowedCountries, ", "))
	}

	bwURL := strings.Replace(cfg.BandwidthURLTemplate, "{bytes}", fmt.Sprintf("%d", int(cfg.BandwidthSizeMB*1024*1024)), 1)

	nodes := fetchAllSources(ctx, cfg, log)
	if len(nodes) == 0 {
		log("没有获取到任何有效节点，退出。")
		return nil, fmt.Errorf("没有获取到任何有效节点")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	nodes = prepFilter(nodes, cfg, log)
	if len(nodes) == 0 {
		log("过滤后无任何节点，退出。")
		return nil, fmt.Errorf("过滤后无任何节点")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tcpResults := tcp.TestAll(ctx, nodes, cfg.Timeout, cfg.TCPProbes, cfg.MinSuccessRate, cfg.MaxWorkers)
	if len(tcpResults) == 0 {
		log("没有通过成功率筛选的节点，请检查网络或降低 MIN_SUCCESS_RATE。")
		return nil, fmt.Errorf("没有通过成功率筛选的节点")
	}

	sortTCPResults(tcpResults)
	latencyMap := make(map[string]float64)
	for _, r := range tcpResults {
		latencyMap[r.Node] = r.Latency
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	candidates, countryNodes := selectCandidates(tcpResults, cfg, log)

	availIPInfo := make(map[string]string)
	availCountryInfo := make(map[string]string)
	availColoInfo := make(map[string]string)
	if cfg.TestAvailability {
		var availExtra map[string]map[string]string
		candidates, availIPInfo, availCountryInfo, availExtra = availability.FilterWithRetry(
			ctx,
			candidates,
			cfg.AvailabilityCheckAPI,
			cfg.AvailabilityConnectTimeout,
			cfg.AvailabilityTimeout,
			cfg.AvailabilityInnerRetryEnabled,
			cfg.AvailabilityInnerRetryMax,
			cfg.AvailabilityInnerRetryDelay,
			cfg.AvailabilityRetryMax,
			cfg.AvailabilityRetryDelay,
			cfg.AvailabilityWorkers,
			cfg.ProgressPrintInterval,
			notifier,
		)
		for node, info := range availExtra {
			if colo, ok := info["colo"]; ok && colo != "" {
				availColoInfo[node] = colo
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if cfg.FilterIPv6Availability {
		before := len(candidates)
		filtered := make([]string, 0)
		for _, node := range candidates {
			stack, ok := availIPInfo[node]
			if !ok || stack != "ipv6_only" {
				filtered = append(filtered, node)
			}
		}
		candidates = filtered
		log("IPv6落地过滤（ipv6_only）：%d -> %d 个节点", before, len(candidates))
		if len(candidates) == 0 {
			return nil, fmt.Errorf("IPv6过滤后无候选节点")
		}
	}

	httpLatencyMap := make(map[string]float64)
	httpJitterMap := make(map[string]float64)
	if cfg.HTTPTestEnabled {
		candidates, httpLatencyMap, httpJitterMap = httpcheck.FilterWithRetry(
			ctx,
			candidates,
			cfg.HTTPTestTimeout,
			cfg.HTTPTestConnectTimeout,
			cfg.HTTPTestMethod,
			cfg.HTTPJitterSamples,
			cfg.HTTPTestWorkers,
			cfg.ProgressPrintInterval,
			cfg.HTTPTestMaxRounds,
			cfg.HTTPTestRoundDelay,
			notifier,
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	bwResults := bandwidth.FilterWithRetry(
		ctx,
		candidates,
		bwURL,
		cfg.BandwidthConnectTimeout,
		cfg.BandwidthTimeout,
		cfg.BandwidthProcessBuffer,
		cfg.BandwidthSizeMB,
		cfg.BandwidthWorkers,
		cfg.ProgressPrintInterval,
		cfg.BandwidthRetryMax,
		cfg.BandwidthRetryDelay,
		notifier,
	)

	speedMap := make(map[string]float64)
	peakSpeedMap := make(map[string]float64)
	var finalSelected []string

	if len(bwResults) > 0 && cfg.BandwidthMinMbps > 0 {
		log("带宽最低阈值已启用: %.1f Mbps，共 %d 个节点待评估", cfg.BandwidthMinMbps, len(bwResults))
		filtered := make([]bandwidth.Result, 0, len(bwResults))
		for _, r := range bwResults {
			if r.Speed >= cfg.BandwidthMinMbps {
				filtered = append(filtered, r)
			}
		}
		excluded := len(bwResults) - len(filtered)
		if excluded > 0 {
			log("带宽最低阈值过滤: %d 个节点低于 %.1f Mbps 已排除，剩余 %d 个节点", excluded, cfg.BandwidthMinMbps, len(filtered))
		} else {
			log("带宽最低阈值过滤: 所有 %d 个节点均满足要求，无节点被排除", len(bwResults))
		}
		bwResults = filtered
	}

	if len(bwResults) == 0 {
		log("\n带宽测速多次重试仍无有效结果，将使用 TCP 筛选结果作为最终节点。")
		notifier(fmt.Sprintf("带宽测速经 %d 轮尝试后仍无有效结果，已降级使用 TCP 排序节点。", cfg.BandwidthRetryMax), "带宽测速全部失败")

		if cfg.UseGlobalMode {
			for i, r := range tcpResults {
				if i >= cfg.GlobalTopN {
					break
				}
				finalSelected = append(finalSelected, r.Node)
			}
		} else {
			for _, nodes := range countryNodes {
				sort.Slice(nodes, func(i, j int) bool {
					if nodes[i].Success != nodes[j].Success {
						return nodes[i].Success > nodes[j].Success
					}
					return nodes[i].Latency < nodes[j].Latency
				})
				for i, n := range nodes {
					if i >= cfg.PerCountryTopN {
						break
					}
					finalSelected = append(finalSelected, n.Node)
				}
			}
		}
	} else {
		for _, r := range bwResults {
			speedMap[r.Node] = r.Speed
			peakSpeedMap[r.Node] = r.PeakMbps
		}

		inputs := make([]ranking.ScoredNodeInput, len(bwResults))
		for i, r := range bwResults {
			inputs[i] = ranking.ScoredNodeInput{Node: r.Node, Speed: r.Speed}
		}

		scored := ranking.ScoreAndRank(inputs, latencyMap, httpLatencyMap, httpJitterMap,
			cfg.SpeedWeight, cfg.TCPLatencyWeight, cfg.HTTPLatencyWeight, cfg.JitterWeight)

		if cfg.UseGlobalMode {
			finalSelected = ranking.SelectGlobal(scored, cfg.GlobalTopN)
		} else {
			finalSelected = ranking.SelectPerCountry(scored, cfg.PerCountryTopN)
		}

		printFinalNodes(finalSelected, speedMap, latencyMap, httpLatencyMap, httpJitterMap, log)
	}

	writeIPTxt(finalSelected, cfg, speedMap, latencyMap, httpLatencyMap, httpJitterMap, log)
	log("\n结果已保存到 %s（共 %d 个节点）", cfg.OutputFile, len(finalSelected))

	runDNSUpdate(finalSelected, cfg, availIPInfo, bwResults, latencyMap, httpLatencyMap, httpJitterMap, notifier, log)

	if cfg.GitHubSyncEnabled {
		sync.SyncToGitHub(cfg.GitHubSyncMaxRetries, cfg.GitHubSyncRetryDelay, cfg.GitSyncProcessTimeout)
	}

	providerMap := make(map[string]string)
	if len(finalSelected) > 0 {
		log("正在查询节点厂商信息...")
		type provResult struct {
			node string
			prov string
		}
		ch := make(chan provResult, len(finalSelected))
		workers := 8
		if workers > len(finalSelected) {
			workers = len(finalSelected)
		}
		sem := make(chan struct{}, workers)
		for _, node := range finalSelected {
			sem <- struct{}{}
			go func(n string) {
				defer func() { <-sem }()
				ip := n
				if idx := strings.IndexByte(n, ':'); idx >= 0 {
					ip = n[:idx]
				}
				prov := whois.Lookup(ip)
				ch <- provResult{n, prov}
			}(node)
		}
		for i := 0; i < len(finalSelected); i++ {
			r := <-ch
			if r.prov != "" {
				providerMap[r.node] = r.prov
			}
		}
		provCount := len(providerMap)
		if provCount > 0 {
			log("厂商信息查询完成，共识别 %d 个节点。", provCount)
		} else {
			log("厂商信息查询完成，未识别到厂商信息。")
		}
	}

	return &Result{
		SelectedNodes:  finalSelected,
		SpeedMap:       speedMap,
		PeakSpeedMap:   peakSpeedMap,
		LatencyMap:     latencyMap,
		HTTPLatencyMap: httpLatencyMap,
		HTTPJitterMap:  httpJitterMap,
		CountryInfo:    availCountryInfo,
		ColoInfo:       availColoInfo,
		ProviderMap:    providerMap,
		TotalTime:      time.Since(startTime),
	}, nil
}

func fetchAllSources(ctx context.Context, cfg *config.Config, log func(string, ...interface{})) []string {
	nodes := make([]string, 0)
	seen := make(map[string]bool)

	if cfg.UseURLSource {
		for _, source := range cfg.AdditionalSources {
			if ctx.Err() != nil {
				return nodes
			}
			if !source.Enabled || source.URL == "" {
				continue
			}
			sourceNodes, err := parser.FetchSourceWithFallback(ctx, source.URL, cfg.FetchMaxRetries, cfg.FetchRetryDelay, cfg.FetchConnectTimeout, cfg.FetchTimeout, cfg.AvailabilityCheckAPI, cfg.AvailabilityConnectTimeout, cfg.AvailabilityTimeout, cfg.FallbackWorkers)
			if err != nil {
				log("获取数据源失败: %v", err)
				continue
			}
			for _, n := range sourceNodes {
				key := strings.SplitN(n, "#", 2)[0]
				if !seen[key] {
					seen[key] = true
					nodes = append(nodes, n)
				}
			}
		}
	}

	if len(cfg.DirectNodes) > 0 {
		raw := strings.Join(cfg.DirectNodes, "\n")
		directNodes := parser.ParseAdaptiveWithFallback(ctx, raw, cfg.AvailabilityCheckAPI, cfg.AvailabilityConnectTimeout, cfg.AvailabilityTimeout, cfg.FallbackWorkers)
		log("直接输入 IP 解析到 %d 个节点", len(directNodes))
		for _, n := range directNodes {
			key := strings.SplitN(n, "#", 2)[0]
			if !seen[key] {
				seen[key] = true
				nodes = append(nodes, n)
			}
		}
	}

	log("合并后总计 %d 个节点。", len(nodes))
	return nodes
}

func prepFilter(nodes []string, cfg *config.Config, log func(string, ...interface{})) []string {
	if cfg.PreFilterPortEnabled && len(cfg.PreFilterPorts) > 0 {
		before := len(nodes)
		portSet := make(map[string]bool)
		for _, p := range cfg.PreFilterPorts {
			portSet[fmt.Sprintf("%d", p)] = true
		}
		filtered := make([]string, 0)
		for _, n := range nodes {
			ipport := strings.SplitN(n, "#", 2)[0]
			host, port, err := net.SplitHostPort(ipport)
			if err != nil {
				parts := strings.SplitN(ipport, ":", 2)
				if len(parts) == 2 && portSet[parts[1]] {
					filtered = append(filtered, n)
				}
				continue
			}
			_ = host
			if portSet[port] {
				filtered = append(filtered, n)
			}
		}
		nodes = filtered
		ports := make([]string, 0, len(cfg.PreFilterPorts))
		for _, p := range cfg.PreFilterPorts {
			ports = append(ports, fmt.Sprintf("%d", p))
		}
		log("前置端口过滤（仅保留端口 %s）：%d -> %d 个节点", strings.Join(ports, ", "), before, len(nodes))
		if len(nodes) == 0 {
			return nil
		}
	}

	if cfg.PreFilterBlockedEnabled && len(cfg.PreFilterBlockedCountries) > 0 {
		before := len(nodes)
		blockedSet := make(map[string]bool)
		for _, c := range cfg.PreFilterBlockedCountries {
			blockedSet[strings.ToUpper(c)] = true
		}
		filtered := make([]string, 0)
		for _, n := range nodes {
			if idx := strings.LastIndexByte(n, '#'); idx >= 0 {
				country := strings.ToUpper(strings.SplitN(n[idx+1:], " ", 2)[0])
				if !blockedSet[country] {
					filtered = append(filtered, n)
				}
			}
		}
		nodes = filtered
		blockedList := make([]string, 0, len(blockedSet))
		for c := range blockedSet {
			blockedList = append(blockedList, c)
		}
		sort.Strings(blockedList)
		log("前置黑名单过滤：%d -> %d 个节点（已屏蔽：%s）", before, len(nodes), strings.Join(blockedList, ", "))
		if len(nodes) == 0 {
			return nil
		}
	}

	if cfg.FilterCountriesEnabled && len(cfg.AllowedCountries) > 0 {
		before := len(nodes)
		allowedSet := make(map[string]bool)
		for _, c := range cfg.AllowedCountries {
			allowedSet[strings.ToUpper(c)] = true
		}
		filtered := make([]string, 0)
		for _, n := range nodes {
			parts := strings.SplitN(n, "#", 2)
			if len(parts) == 2 {
				country := strings.ToUpper(strings.SplitN(parts[1], " ", 2)[0])
				if allowedSet[country] {
					filtered = append(filtered, n)
				}
			}
		}
		nodes = filtered
		allowedList := make([]string, 0, len(allowedSet))
		for c := range allowedSet {
			allowedList = append(allowedList, c)
		}
		log("\n国家过滤（测试前）：%d -> %d 个节点（允许国家：%s）", before, len(nodes), strings.Join(allowedList, ", "))
	}

	return nodes
}

func sortTCPResults(results []tcp.TCPResult) {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Success != results[j].Success {
			return results[i].Success > results[j].Success
		}
		return results[i].Latency < results[j].Latency
	})
}

func selectCandidates(results []tcp.TCPResult, cfg *config.Config, log func(string, ...interface{})) (candidates []string, countryNodes map[string][]tcp.TCPResult) {
	countryNodes = make(map[string][]tcp.TCPResult)

	if cfg.UseGlobalMode {
		for i, r := range results {
			if i >= cfg.BandwidthCandidates {
				break
			}
			candidates = append(candidates, r.Node)
		}
		log("\nTCP 最优前 %d 个节点进入候选池。", len(candidates))
		return
	}

	for _, r := range results {
		countryNodes[r.Country] = append(countryNodes[r.Country], r)
	}

	totalCountries := len(countryNodes)
	if totalCountries == 0 {
		return
	}
	baseLimit := cfg.BandwidthCandidates / totalCountries
	if baseLimit < 1 {
		baseLimit = 1
	}

	for _, nodes := range countryNodes {
		sort.Slice(nodes, func(i, j int) bool {
			if nodes[i].Success != nodes[j].Success {
				return nodes[i].Success > nodes[j].Success
			}
			return nodes[i].Latency < nodes[j].Latency
		})
		limit := len(nodes)
		if limit > baseLimit {
			limit = baseLimit
		}
		for i := 0; i < limit; i++ {
			candidates = append(candidates, nodes[i].Node)
		}
	}
	log("\n各国家候选池分配：共 %d 个国家，每国最多 %d 个候选，总计 %d 个节点进入候选池。", totalCountries, baseLimit, len(candidates))
	return
}

func printFinalNodes(finalSelected []string, speedMap map[string]float64, latencyMap map[string]float64, httpLatencyMap map[string]float64, httpJitterMap map[string]float64, log func(string, ...interface{})) {
	log("\n================ 最终优选节点 ================")
	for i, node := range finalSelected {
		speed := speedMap[node]
		tcpLat := latencyMap[node]
		httpLat, hasHTTP := httpLatencyMap[node]
		httpJitter, hasJitter := httpJitterMap[node]

		line := fmt.Sprintf("%d. %s 速度 %.2f Mbps", i+1, node, speed)
		if hasHTTP {
			line += fmt.Sprintf(" 延迟 %.2f ms", httpLat)
		} else if tcpLat > 0 {
			line += fmt.Sprintf(" 延迟 %.2f ms", tcpLat*1000)
		}
		if hasJitter {
			line += fmt.Sprintf(" 抖动 %.2f ms", httpJitter)
		}
		log(line)
	}
}

func writeIPTxt(finalSelected []string, cfg *config.Config, speedMap, latencyMap, httpLatencyMap, httpJitterMap map[string]float64, log func(string, ...interface{})) {
	f, err := os.Create(cfg.OutputFile)
	if err != nil {
		log("无法写入 %s: %v", cfg.OutputFile, err)
		return
	}
	defer f.Close()

	if cfg.ADHeaderEnabled {
		for _, line := range cfg.ADHeaderLines {
			fmt.Fprintln(f, line)
		}
	}

	for _, node := range finalSelected {
		line := node
		if speed, ok := speedMap[node]; ok {
			line += fmt.Sprintf(" %.2f Mbps", speed)
		}
		if lat, ok := latencyMap[node]; ok {
			line += fmt.Sprintf(" %.2f ms", lat*1000)
		}
		if cfg.ADPerlineEnabled && cfg.ADPerlineText != "" {
			line += cfg.ADPerlineText
		}
		fmt.Fprintln(f, line)
	}

	if cfg.ADFooterEnabled {
		for _, line := range cfg.ADFooterLines {
			fmt.Fprintln(f, line)
		}
	}
}

func runDNSUpdate(finalSelected []string, cfg *config.Config, availIPInfo map[string]string, bwResults []bandwidth.Result, latencyMap, httpLatencyMap, httpJitterMap map[string]float64, notifier func(string, string), log func(string, ...interface{})) {
	if !cfg.CFEnabled {
		return
	}

	recordType := strings.ToUpper(cfg.DNSRecordType)
	dnsContent := make([]string, 0)
	dnsNodes := make([]string, 0)

	filteredByPort := 0
	filteredByIPv6 := 0
	filteredByCountry := 0

	blockedSet := make(map[string]bool)
	if cfg.FilterBlockedCountriesEnabled {
		for _, c := range cfg.BlockedCountries {
			blockedSet[strings.ToUpper(c)] = true
		}
	}

	riskMap := make(map[string]string)
	if cfg.DNSIPRiskFilterEnabled && len(bwResults) > 0 {
		ipSet := make(map[string]bool)
		for _, r := range bwResults {
			if idx := strings.IndexByte(r.Node, ':'); idx >= 0 {
				ipSet[r.Node[:idx]] = true
			}
		}
		if len(ipSet) > 0 {
			workers := cfg.FallbackWorkers
			if workers > len(ipSet) {
				workers = len(ipSet)
			}
			log("正在并发查询 %d 个 IP 的风险等级（并发 %d）...", len(ipSet), workers)
			type riskResult struct {
				ip   string
				risk string
			}
			sem := make(chan struct{}, workers)
			ch := make(chan riskResult, len(ipSet))
			for ip := range ipSet {
				sem <- struct{}{}
				go func(ip string) {
					defer func() { <-sem }()
					ch <- riskResult{ip, cloudflare.GetIPRiskLevel(ip)}
				}(ip)
			}
			for i := 0; i < len(ipSet); i++ {
				r := <-ch
				riskMap[r.ip] = r.risk
			}
			log("风险等级查询完成。")
		}
	}

	riskFallbackIPs := make([]string, 0)
	riskFallbackNodes := make([]string, 0)
	filteredByRisk := 0

	if len(bwResults) > 0 && len(availIPInfo) > 0 {
		for _, r := range bwResults {
			node := r.Node
			if !strings.Contains(node, ":") {
				continue
			}

			parts := strings.SplitN(node, ":", 2)
			pureIP := parts[0]
			port := strings.SplitN(strings.SplitN(parts[1], "#", 2)[0], " ", 2)[0]

			if port != "443" {
				filteredByPort++
				continue
			}

			if len(cfg.DNSBlacklistCountries) > 0 {
				blocked := false
				for _, bc := range cfg.DNSBlacklistCountries {
					country := strings.ToUpper(strings.SplitN(strings.SplitN(node, "#", 2)[1], " ", 2)[0])
					if bc == country {
						blocked = true
						break
					}
				}
				if blocked {
					filteredByPort++
					continue
				}
			}

			if cfg.FilterIPv6Availability {
				stack, ok := availIPInfo[node]
				if ok && stack == "ipv6_only" {
					filteredByIPv6++
					continue
				}
			}

			if len(blockedSet) > 0 && strings.Contains(node, "#") {
				country := strings.ToUpper(strings.SplitN(strings.SplitN(node, "#", 2)[1], " ", 2)[0])
				if blockedSet[country] {
					filteredByCountry++
					continue
				}
			}

			if cfg.DNSIPRiskFilterEnabled {
				riskFallbackIPs = append(riskFallbackIPs, pureIP)
				riskFallbackNodes = append(riskFallbackNodes, node)

				riskLevel := riskMap[pureIP]
				if riskLevel == "" {
					riskLevel = "未知"
				}
				maxLevel := cfg.DNSIPRiskMaxLevel
				maxVal, ok := cloudflare.RiskLevelOrder[maxLevel]
				if !ok {
					maxVal = 2
				}
				riskVal, ok := cloudflare.RiskLevelOrder[riskLevel]
				if !ok {
					riskVal = 99
				}
				if riskLevel == "未知" || riskVal > maxVal {
					filteredByRisk++
					continue
				}
			}

			if recordType == "A" {
				dnsContent = append(dnsContent, pureIP)
			} else {
				ipport := strings.SplitN(node, "#", 2)[0]
				dnsContent = append(dnsContent, ipport)
			}
			dnsNodes = append(dnsNodes, node)

			if len(dnsContent) >= cfg.DNSUpdateTargetCount {
				break
			}
		}

		if cfg.DNSIPRiskFilterEnabled && len(dnsContent) == 0 && filteredByRisk > 0 {
			notifier("风险等级检测全部失败：所有候选节点均因风险等级过高或 API 查询失败被过滤，已回退到无风险等级过滤的候选列表。", "风险等级检测全部失败")
			for i := 0; i < len(riskFallbackIPs) && len(dnsContent) < cfg.DNSUpdateTargetCount; i++ {
				if recordType == "A" {
					dnsContent = append(dnsContent, riskFallbackIPs[i])
				} else {
					ipport := strings.SplitN(riskFallbackNodes[i], "#", 2)[0]
					dnsContent = append(dnsContent, ipport)
				}
				dnsNodes = append(dnsNodes, riskFallbackNodes[i])
			}
		}

		filterParts := make([]string, 0)
		if filteredByPort > 0 {
			filterParts = append(filterParts, fmt.Sprintf("非443端口过滤(%d个)", filteredByPort))
		}
		if cfg.FilterIPv6Availability {
			filterParts = append(filterParts, fmt.Sprintf("IPv6落地过滤(%d个)", filteredByIPv6))
		}
		if cfg.FilterBlockedCountriesEnabled {
			filterParts = append(filterParts, fmt.Sprintf("DNS黑名单过滤(%d个)", filteredByCountry))
		}
		if cfg.DNSIPRiskFilterEnabled && filteredByRisk > 0 {
			filterParts = append(filterParts, fmt.Sprintf("风险等级过滤(%d个)", filteredByRisk))
		}
		filterStr := "无过滤"
		if len(filterParts) > 0 {
			filterStr = strings.Join(filterParts, " + ")
		}
		log("从 %d 个测速节点中筛选出 %d 个%s 用于 DNS 更新（%s）。", len(bwResults), len(dnsContent), map[bool]string{true: "IP", false: "IP:端口"}[recordType == "A"], filterStr)
	}

	if len(dnsContent) == 0 {
		if len(finalSelected) > 0 {
			log("未能从完整测速结果构建 DNS 列表，降级使用 ip.txt 中的 IP。")
			if recordType == "A" {
				for _, node := range finalSelected {
					idx := strings.IndexByte(node, ':')
					if idx > 0 {
						dnsContent = append(dnsContent, node[:idx])
					} else {
						dnsContent = append(dnsContent, node)
					}
				}
			} else {
				log("TXT 模式需要端口信息，但降级数据中无端口，DNS 更新跳过。")
				return
			}
		} else {
			msg := "没有可用的 IP 用于 DNS 更新，跳过。"
			log(msg)
			notifier(msg, "DNS 更新跳过")
			return
		}
	}

	seen := make(map[string]bool)
	uniqueContent := make([]string, 0)
	uniqueNodes := make([]string, 0)
	for i, content := range dnsContent {
		if !seen[content] {
			seen[content] = true
			uniqueContent = append(uniqueContent, content)
			if i < len(dnsNodes) {
				uniqueNodes = append(uniqueNodes, dnsNodes[i])
			}
		}
	}
	dnsContent = uniqueContent
	dnsNodes = uniqueNodes

	log("\n准备将以下 %d 个%s 更新到 Cloudflare DNS（记录类型 %s）:", len(dnsContent), map[bool]string{true: "IP", false: "IP:端口"}[recordType == "A"], recordType)
	printDNSNodes(dnsContent, dnsNodes, bwResults, latencyMap, httpLatencyMap, httpJitterMap, log)

	err := cloudflare.BatchUpdateDNS(
		cfg.CFAPIToken, cfg.CFZoneID, cfg.CFDNSRecordName,
		cfg.CFTTL, cfg.CFProxied, cfg.DNSRecordType,
		dnsContent, dnsNodes,
		cfg.CFDNSConnectTimeout, cfg.CFDNSReadTimeout,
		cfg.DNSUpdateMaxRetries, cfg.DNSUpdateRetryDelay,
	)
	if err != nil {
		log("DNS update error: %v", err)
		notifier(err.Error(), "DNS 更新失败")
	}
}

func printDNSNodes(content, nodes []string, bwResults []bandwidth.Result, latencyMap, httpLatencyMap, httpJitterMap map[string]float64, log func(string, ...interface{})) {
	speedMap := make(map[string]float64)
	for _, r := range bwResults {
		speedMap[r.Node] = r.Speed
	}

	for i := 0; i < len(content) && i < len(nodes); i++ {
		speed := speedMap[nodes[i]]
		line := fmt.Sprintf("%d. %s 速度 %.2f Mbps", i+1, nodes[i], speed)

		if lat, ok := httpLatencyMap[nodes[i]]; ok {
			line += fmt.Sprintf(" 延迟 %.2f ms", lat)
		} else if lat, ok := latencyMap[nodes[i]]; ok {
			line += fmt.Sprintf(" 延迟 %.2f ms", lat*1000)
		}
		if jitter, ok := httpJitterMap[nodes[i]]; ok {
			line += fmt.Sprintf(" 抖动 %.2f ms", jitter)
		}
		log(line)
	}
}

func boolStr(b bool) string {
	if b {
		return "启用"
	}
	return "禁用"
}