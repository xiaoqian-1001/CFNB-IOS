//go:build !carchive

package main

import "os"

func main() {
	port := "8080"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}
	RunServer(port)
}
