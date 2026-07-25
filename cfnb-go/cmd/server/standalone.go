//go:build !cfios

package main

import "os"

func main() {
	port := "8080"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}
	RunServer(port)
}
