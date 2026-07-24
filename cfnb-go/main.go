package main

import (
	"fmt"
	"net"
	"os"
	"sort"
	"strings"

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
)

func main() {
	cfg, err := config.Load("config.json")
	if err != nil {
		fmt.Printf("错误：%v\n", err)
		os.Exit(1)
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

	modeStr := fmt.Sprintf("全局最优%d个", cfg.GlobalTopN)
	if !cfg.UseGlobalMode {
		modeStr = fmt.Sprintf("每个国家最优%d个", cfg.PerCountryTopN)
	}
	fmt.Printf("当前模式：%s，每个节点测试 %d 次 TCP 连接\n", modeStr, cfg.TCPProbes)
	fmt.Printf("最低成功率要求：%.0f%%\n", cfg.MinSuccessRate*100)
	fmt.Printf("IP 可用性二次筛选：%s\n", boolStr(cfg.TestAvailability))
	fmt.Printf("HTTP检测：%s\n", boolStr(cfg.HTTPTestEnabled))
	fmt.Printf("IPv6 客户端 IP 过滤（仅作用于DNS更新环节）：%s\n", boolStr(cfg.FilterIPv6Availability))
	fmt.Printf("DNS黑名单过滤：%s，黑名单国家：%s\n", boolStr(cfg.FilterBlockedCountriesEnabled), strings.Join(cfg.BlockedCountries, ", "))
	fmt.Printf("IP 风险等级过滤：%s（最高允许：%s）\n", boolStr(cfg.DNSIPRiskFilterEnabled), cfg.DNSIPRiskMaxLevel)
	fmt.Printf("带宽测速候选数：%d，测速文件大小：%.1f MB，超时：%.0fs\n", cfg.BandwidthCandidates, cfg.BandwidthSizeMB, cfg.BandwidthTimeout)

	if cfg.FilterCountriesEnabled {
		fmt.Printf("前置白名单过滤：启用，仅保留：%s\n", strings.Join(cfg.AllowedCountries, ", "))
	}

	bwURL := strings.Replace(cfg.BandwidthURLTemplate, "{bytes}", fmt.Sprintf("%d", int(cfg.BandwidthSizeMB*1024*1024)), 1)

	nodes := fetchAllSources(cfg)
	if len(nodes) == 0 {
		fmt.Println("没有获取到任何有效节点，退出。")
		os.Exit(1)
	}

	nodes = prepFilter(nodes, cfg)
	if len(nodes) == 0 {
		fmt.Println("过滤后无任何节点，退出。")
		os.Exit(0)
	}

	tcpResults := tcp.TestAll(nodes, cfg.Timeout, cfg.TCPProbes, cfg.MinSuccessRate, cfg.MaxWorkers, cfg.ProgressPrintInterval)
	if len(tcpResults) == 0 {
		fmt.Println("没有通过成功率筛选的节点，请检查网络或降低 MIN_SUCCESS_RATE。")
		os.Exit(0)
	}

	sortTCPResults(tcpResults)
	latencyMap := make(map[string]float64)
	for _, r := range tcpResults {
		latencyMap[r.Node] = r.Latency
	}

	candidates, countryNodes := selectCandidates(tcpResults, cfg)

	availIPInfo := make(map[string]string)
	if cfg.TestAvailability {
		candidates, availIPInfo, _ = availability.FilterWithRetry(
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
	}

	httpLatencyMap := make(map[string]float64)
	httpJitterMap := make(map[string]float64)
	if cfg.HTTPTestEnabled {
		candidates, httpLatencyMap, httpJitterMap = httpcheck.FilterWithRetry(
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

	bwResults := bandwidth.FilterWithRetry(
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
	var finalSelected []string

	if len(bwResults) == 0 {
		fmt.Println("\n带宽测速多次重试仍无有效结果，将使用 TCP 筛选结果作为最终节点。")
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

		printFinalNodes(finalSelected, speedMap, latencyMap, httpLatencyMap, httpJitterMap)
	}

	writeIPTxt(finalSelected, cfg, speedMap, latencyMap, httpLatencyMap, httpJitterMap)
	fmt.Printf("\n结果已保存到 %s（共 %d 个节点）\n", cfg.OutputFile, len(finalSelected))

	runDNSUpdate(finalSelected, cfg, availIPInfo, bwResults, latencyMap, httpLatencyMap, httpJitterMap, notifier)

	if cfg.GitHubSyncEnabled {
		sync.SyncToGitHub(cfg.GitHubSyncMaxRetries, cfg.GitHubSyncRetryDelay, cfg.GitSyncProcessTimeout)
	} else {
		fmt.Println("Git 同步未启用。")
	}
}

func fetchAllSources(cfg *config.Config) []string {
	nodes := make([]string, 0)
	seen := make(map[string]bool)

	for _, source := range cfg.AdditionalSources {
		if !source.Enabled || source.URL == "" {
			continue
		}
		sourceNodes, err := parser.FetchSourceWithFallback(source.URL, cfg.FetchMaxRetries, cfg.FetchRetryDelay, cfg.FetchConnectTimeout, cfg.FetchTimeout, cfg.AvailabilityCheckAPI, cfg.AvailabilityConnectTimeout, cfg.AvailabilityTimeout, cfg.FallbackWorkers)
		if err != nil {
			fmt.Printf("获取数据源失败: %v\n", err)
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

	fmt.Printf("合并后总计 %d 个节点。\n", len(nodes))
	return nodes
}

func prepFilter(nodes []string, cfg *config.Config) []string {
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
		fmt.Printf("前置端口过滤（仅保留端口 %s）：%d -> %d 个节点\n", strings.Join(ports, ", "), before, len(nodes))
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
			if idx := findLastByte(n, '#'); idx >= 0 {
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
		fmt.Printf("前置黑名单过滤：%d -> %d 个节点（已屏蔽：%s）\n", before, len(nodes), strings.Join(blockedList, ", "))
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
		fmt.Printf("\n国家过滤（测试前）：%d -> %d 个节点（允许国家：%s）\n", before, len(nodes), strings.Join(allowedList, ", "))
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

func selectCandidates(results []tcp.TCPResult, cfg *config.Config) (candidates []string, countryNodes map[string][]tcp.TCPResult) {
	countryNodes = make(map[string][]tcp.TCPResult)

	if cfg.UseGlobalMode {
		for i, r := range results {
			if i >= cfg.BandwidthCandidates {
				break
			}
			candidates = append(candidates, r.Node)
		}
		fmt.Printf("\nTCP 最优前 %d 个节点进入候选池。\n", len(candidates))
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
	fmt.Printf("\n各国家候选池分配：共 %d 个国家，每国最多 %d 个候选，总计 %d 个节点进入候选池。\n", totalCountries, baseLimit, len(candidates))
	return
}

func printFinalNodes(finalSelected []string, speedMap map[string]float64, latencyMap map[string]float64, httpLatencyMap map[string]float64, httpJitterMap map[string]float64) {
	fmt.Println("\n================ 最终优选节点 ================")
	for i, node := range finalSelected {
		speed := speedMap[node]
		tcpLat := latencyMap[node]
		httpLat, hasHTTP := httpLatencyMap[node]
		httpJitter, hasJitter := httpJitterMap[node]

		line := fmt.Sprintf("%d. %s 速度 %.2f Mbps", i+1, node, speed)
		if hasHTTP {
			line += fmt.Sprintf(" 延迟 %.2f ms", httpLat)
		}
		if hasJitter {
			line += fmt.Sprintf(" 抖动 %.2f ms", httpJitter)
		}
		if tcpLat > 0 {
			line += fmt.Sprintf(" 延迟 %.2f ms", tcpLat*1000)
		}
		fmt.Println(line)
	}
}

func writeIPTxt(finalSelected []string, cfg *config.Config, speedMap, latencyMap, httpLatencyMap, httpJitterMap map[string]float64) {
	f, err := os.Create(cfg.OutputFile)
	if err != nil {
		fmt.Printf("无法写入 %s: %v\n", cfg.OutputFile, err)
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

func runDNSUpdate(finalSelected []string, cfg *config.Config, availIPInfo map[string]string, bwResults []bandwidth.Result, latencyMap, httpLatencyMap, httpJitterMap map[string]float64, notifier func(string, string)) {
	if !cfg.CFEnabled {
		fmt.Println("Cloudflare DNS 批量更新未启用。")
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
			fmt.Printf("正在并发查询 %d 个 IP 的风险等级（并发 %d）...\n", len(ipSet), workers)
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
			fmt.Println("风险等级查询完成。")
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
		fmt.Printf("从 %d 个测速节点中筛选出 %d 个%s 用于 DNS 更新（%s）。\n", len(bwResults), len(dnsContent), map[bool]string{true: "IP", false: "IP:端口"}[recordType == "A"], filterStr)
	}

	if len(dnsContent) == 0 {
		if len(finalSelected) > 0 {
			fmt.Println("未能从完整测速结果构建 DNS 列表，降级使用 ip.txt 中的 IP。")
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
				fmt.Println("TXT 模式需要端口信息，但降级数据中无端口，DNS 更新跳过。")
				return
			}
		} else {
			msg := "没有可用的 IP 用于 DNS 更新，跳过。"
			fmt.Println(msg)
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

	fmt.Printf("\n准备将以下 %d 个%s 更新到 Cloudflare DNS（记录类型 %s）:\n", len(dnsContent), map[bool]string{true: "IP", false: "IP:端口"}[recordType == "A"], recordType)
	printDNSNodes(dnsContent, dnsNodes, bwResults, latencyMap, httpLatencyMap, httpJitterMap)

	err := cloudflare.BatchUpdateDNS(
		cfg.CFAPIToken, cfg.CFZoneID, cfg.CFDNSRecordName,
		cfg.CFTTL, cfg.CFProxied, cfg.DNSRecordType,
		dnsContent, dnsNodes,
		cfg.CFDNSConnectTimeout, cfg.CFDNSReadTimeout,
		cfg.DNSUpdateMaxRetries, cfg.DNSUpdateRetryDelay,
	)
	if err != nil {
		fmt.Println(err)
		notifier(err.Error(), "DNS 更新失败")
	}
}

func printDNSNodes(content, nodes []string, bwResults []bandwidth.Result, latencyMap, httpLatencyMap, httpJitterMap map[string]float64) {
	speedMap := make(map[string]float64)
	for _, r := range bwResults {
		speedMap[r.Node] = r.Speed
	}

	for i := 0; i < len(content) && i < len(nodes); i++ {
		speed := speedMap[nodes[i]]
		line := fmt.Sprintf("%d. %s 速度 %.2f Mbps", i+1, nodes[i], speed)

		if lat, ok := httpLatencyMap[nodes[i]]; ok {
			line += fmt.Sprintf(" 延迟 %.2f ms", lat)
		}
		if jitter, ok := httpJitterMap[nodes[i]]; ok {
			line += fmt.Sprintf(" 抖动 %.2f ms", jitter)
		}
		if lat, ok := latencyMap[nodes[i]]; ok {
			line += fmt.Sprintf(" 延迟 %.2f ms", lat*1000)
		}
		fmt.Println(line)
	}
}

func findLastByte(s string, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func boolStr(b bool) string {
	if b {
		return "启用"
	}
	return "禁用"
}
