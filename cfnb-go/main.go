package main

import (
	"fmt"
	"os"

	"cfnb/pkg/config"
	"cfnb/pkg/pipeline"
)

func main() {
	cfg, err := config.Load("config.json")
	if err != nil {
		fmt.Printf("错误：%v\n", err)
		os.Exit(1)
	}

	_, err = pipeline.Run(cfg, os.Stdout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pipeline error: %v\n", err)
		os.Exit(1)
	}
}