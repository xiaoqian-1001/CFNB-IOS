package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func SendWxPusher(enabled bool, appToken string, uids []string, apiURL string, connectTimeout, readTimeout float64, content, summary string) {
	if !enabled {
		return
	}

	payload := map[string]interface{}{
		"appToken": appToken,
		"content":  content,
		"summary":  summary,
		"uids":     uids,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("微信通知 JSON 序列化失败: %v\n", err)
		return
	}

	client := &http.Client{
		Timeout: time.Duration((connectTimeout + readTimeout) * float64(time.Second)),
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewReader(body))
	if err != nil {
		fmt.Printf("微信通知创建请求失败: %v\n", err)
		return
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("微信通知异常: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Println("微信通知已发送")
	} else {
		fmt.Printf("微信通知发送失败: %d\n", resp.StatusCode)
	}
}
