package cloudflare

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var scoreRegex = regexp.MustCompile(`([\d.]+)\s*\(`)

type DNSRecord struct {
	ID      string `json:"id,omitempty"`
	Name    string `json:"name"`
	Type    string `json:"type"`
	Content string `json:"content"`
	TTL     int    `json:"ttl"`
	Proxied bool   `json:"proxied,omitempty"`
}

type BatchPayload struct {
	Deletes []map[string]string `json:"deletes"`
	Posts   []DNSRecord         `json:"posts"`
}

type CFResponse struct {
	Success  bool          `json:"success"`
	Errors   []interface{} `json:"errors"`
	Result   []DNSRecord   `json:"result"`
}

var RiskLevelOrder = map[string]int{
	"极度纯净": 0,
	"纯净":   1,
	"轻微风险": 2,
	"高风险":  3,
	"极度危险": 4,
}

func GetIPRiskLevel(ip string) string {
	url := fmt.Sprintf("https://api.ipapi.is/?q=%s", ip)
	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Get(url)
	if err != nil {
		return "未知"
	}
	defer resp.Body.Close()

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "未知"
	}

	return calculateRisk(data)
}

func calculateRisk(data map[string]interface{}) string {
	type scoreInfo struct {
		company float64
		asn     float64
	}
	scores := scoreInfo{}

	if company, ok := data["company"].(map[string]interface{}); ok {
		if ab, ok := company["abuser_score"]; ok {
			scores.company = extractScore(ab)
		}
	}
	if asn, ok := data["asn"].(map[string]interface{}); ok {
		if ab, ok := asn["abuser_score"]; ok {
			scores.asn = extractScore(ab)
		}
	}

	baseScore := ((scores.company + scores.asn) / 2) * 5

	flags := []string{"is_crawler", "is_proxy", "is_vpn", "is_tor", "is_abuser"}
	riskCount := 0
	for _, flag := range flags {
		if v, ok := data[flag].(bool); ok && v {
			riskCount++
		}
	}

	finalScore := baseScore + float64(riskCount)*0.15
	if v, ok := data["is_bogon"].(bool); ok && v {
		finalScore += 1.0
	}

	percentage := finalScore * 100
	switch {
	case percentage >= 100:
		return "极度危险"
	case percentage >= 20:
		return "高风险"
	case percentage >= 5:
		return "轻微风险"
	case percentage >= 0.25:
		return "纯净"
	default:
		return "极度纯净"
	}
}

func extractScore(v interface{}) float64 {
	s, ok := v.(string)
	if !ok {
		return 0.0
	}
	s = strings.TrimSpace(s)
	if m := scoreRegex.FindStringSubmatch(s); m != nil {
		if val, err := fmt.Sscanf(m[1], "%f", new(float64)); err == nil && val == 1 {
			var f float64
			fmt.Sscanf(m[1], "%f", &f)
			return f
		}
	}
	var val float64
	if _, err := fmt.Sscanf(s, "%f", &val); err == nil {
		return val
	}
	return 0.0
}

func BatchUpdateDNS(cfgToken, cfgZoneID, cfgRecordName string, cfgTTL int, cfgProxied bool, cfgRecordType string, dnsContent []string, dnsNodes []string, connectTimeout, readTimeout float64, maxRetries int, retryDelay float64) error {
	recordType := strings.ToUpper(cfgRecordType)
	if recordType != "A" && recordType != "TXT" {
		return fmt.Errorf("不支持的 DNS_RECORD_TYPE: %s", recordType)
	}

	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", cfgToken),
		"Content-Type":  "application/json",
	}

	timeout := time.Duration((connectTimeout + readTimeout) * float64(time.Second))

	if recordType == "A" {
		return updateARecords(cfgZoneID, cfgRecordName, cfgTTL, cfgProxied, dnsContent, headers, timeout, maxRetries, retryDelay)
	}
	return updateTXTRecords(cfgZoneID, cfgRecordName, cfgTTL, dnsContent, headers, timeout, maxRetries, retryDelay)
}

func updateARecords(zoneID, recordName string, ttl int, proxied bool, ipList []string, headers map[string]string, timeout time.Duration, maxRetries int, retryDelay float64) error {
	client := &http.Client{Timeout: timeout}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		fmt.Printf("\n[DNS 更新] 尝试 %d/%d...\n", attempt, maxRetries)

		listURL := fmt.Sprintf("https://api.cloudflare.com/client/v4/zones/%s/dns_records?type=A&name=%s", zoneID, recordName)
		resp, err := cfRequest(client, "GET", listURL, nil, headers)
		if err != nil {
			if attempt < maxRetries {
				time.Sleep(time.Duration(retryDelay * float64(time.Second)))
			}
			continue
		}

		var listResp CFResponse
		json.Unmarshal(resp, &listResp)
		if !listResp.Success {
			if attempt < maxRetries {
				time.Sleep(time.Duration(retryDelay * float64(time.Second)))
			}
			continue
		}

		deletes := make([]map[string]string, 0)
		for _, rec := range listResp.Result {
			deletes = append(deletes, map[string]string{"id": rec.ID})
		}

		posts := make([]DNSRecord, 0)
		for _, ip := range ipList {
			posts = append(posts, DNSRecord{
				Name:    recordName,
				Type:    "A",
				Content: ip,
				TTL:     ttl,
				Proxied: proxied,
			})
		}

		payload := BatchPayload{Deletes: deletes, Posts: posts}
		body, _ := json.Marshal(payload)

		batchURL := fmt.Sprintf("https://api.cloudflare.com/client/v4/zones/%s/dns_records/batch", zoneID)
		resp, err = cfRequest(client, "POST", batchURL, body, headers)
		if err != nil {
			fmt.Printf("[尝试 %d/%d] DNS 更新出错: %v\n", attempt, maxRetries, err)
			if attempt < maxRetries {
				time.Sleep(time.Duration(retryDelay * float64(time.Second)))
			}
			continue
		}

		var batchResp CFResponse
		json.Unmarshal(resp, &batchResp)
		if !batchResp.Success {
			fmt.Printf("[尝试 %d/%d] 批量更新失败: %v\n", attempt, maxRetries, batchResp.Errors)
			if attempt < maxRetries {
				time.Sleep(time.Duration(retryDelay * float64(time.Second)))
			}
			continue
		}

		fmt.Printf("Cloudflare DNS 批量更新成功！已将 %s 指向 %d 个 IP。\n", recordName, len(ipList))
		return nil
	}

	return fmt.Errorf("DNS 更新失败，已重试 %d 次", maxRetries)
}

func updateTXTRecords(zoneID, recordName string, ttl int, contentList []string, headers map[string]string, timeout time.Duration, maxRetries int, retryDelay float64) error {
	client := &http.Client{Timeout: timeout}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		fmt.Printf("\n[TXT 记录更新] 尝试 %d/%d...\n", attempt, maxRetries)

		listURL := fmt.Sprintf("https://api.cloudflare.com/client/v4/zones/%s/dns_records?type=TXT&name=%s", zoneID, recordName)
		resp, err := cfRequest(client, "GET", listURL, nil, headers)
		if err != nil {
			if attempt < maxRetries {
				time.Sleep(time.Duration(retryDelay * float64(time.Second)))
			}
			continue
		}

		var listResp CFResponse
		json.Unmarshal(resp, &listResp)
		deletes := make([]map[string]string, 0)
		for _, rec := range listResp.Result {
			deletes = append(deletes, map[string]string{"id": rec.ID})
		}

		posts := make([]DNSRecord, 0)
		for _, content := range contentList {
			posts = append(posts, DNSRecord{
				Name:    recordName,
				Type:    "TXT",
				Content: content,
				TTL:     ttl,
			})
		}

		payload := BatchPayload{Deletes: deletes, Posts: posts}
		body, _ := json.Marshal(payload)

		batchURL := fmt.Sprintf("https://api.cloudflare.com/client/v4/zones/%s/dns_records/batch", zoneID)
		resp, err = cfRequest(client, "POST", batchURL, body, headers)
		if err != nil {
			fmt.Printf("[尝试 %d/%d] TXT 更新出错: %v\n", attempt, maxRetries, err)
			if attempt < maxRetries {
				time.Sleep(time.Duration(retryDelay * float64(time.Second)))
			}
			continue
		}

		var batchResp CFResponse
		json.Unmarshal(resp, &batchResp)
		if !batchResp.Success {
			fmt.Printf("[尝试 %d/%d] 批量更新失败: %v\n", attempt, maxRetries, batchResp.Errors)
			if attempt < maxRetries {
				time.Sleep(time.Duration(retryDelay * float64(time.Second)))
			}
			continue
		}

		fmt.Printf("Cloudflare TXT 记录批量更新成功！共 %d 条记录。\n", len(contentList))
		return nil
	}

	return fmt.Errorf("TXT 记录更新失败，已重试 %d 次", maxRetries)
}

func cfRequest(client *http.Client, method, url string, body []byte, headers map[string]string) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}
