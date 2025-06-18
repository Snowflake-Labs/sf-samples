package main

import (
	"log"

	"github.com/sfc-gh-wahuang/cortexanalystpublicdemo/cortexanalystapp"
)

func main() {
	cmd := cortexanalystapp.Cmd()
	if err := cmd.Execute(); err != nil {
		log.Fatalf("Error executing command: %v", err)
	}
	log.Println("Server stopped")
}
