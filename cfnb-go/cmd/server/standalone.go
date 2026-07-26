//go:build !ios

package main

import "os"

func main() {
	port := "9999"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}
	RunServer(port)
}
