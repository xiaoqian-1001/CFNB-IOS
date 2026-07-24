package sync

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"
)

func SyncToGitHub(maxRetries int, retryDelay float64, processTimeout int) {
	scriptDir, err := os.Getwd()
	if err != nil {
		fmt.Println("无法获取当前目录")
		return
	}

	var scriptName string
	var interpreter []string

	if runtime.GOOS == "windows" {
		scriptName = "git_sync.ps1"
		interpreter = []string{"powershell", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File"}
	} else {
		scriptName = "git_sync.sh"
		interpreter = []string{"bash"}
	}

	scriptPath := filepath.Join(scriptDir, scriptName)
	if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
		fmt.Printf("未找到 %s，跳过 GitHub 同步。\n", scriptName)
		return
	}

	if runtime.GOOS != "windows" {
		os.Chmod(scriptPath, 0755)
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		fmt.Printf("\n正在同步到 GitHub (尝试 %d/%d)...\n", attempt, maxRetries)

		args := append(interpreter, scriptPath)
		cmd := exec.Command(args[0], args[1:]...)

		done := make(chan error, 1)
		go func() {
			output, err := cmd.CombinedOutput()
			if err == nil {
				fmt.Println("已自动推送到 GitHub。")
			} else {
				fmt.Printf("推送失败 (退出码 %v)\n", err)
				if len(output) > 0 {
					fmt.Printf("输出: %s\n", string(output))
				}
			}
			done <- err
		}()

		select {
		case err := <-done:
			if err == nil {
				return
			}
		case <-time.After(time.Duration(processTimeout) * time.Second):
			if cmd.Process != nil {
				cmd.Process.Kill()
			}
			fmt.Printf("推送超时（超过 %d 秒）\n", processTimeout)
		}

		if attempt < maxRetries {
			time.Sleep(time.Duration(retryDelay * float64(time.Second)))
		}
	}

	fmt.Printf("已尝试 %d 次推送，均失败。\n", maxRetries)
}
