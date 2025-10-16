package main

import (
	"log"

	"github.com/Snowflake-Labs/sf-samples/samples/cortex-analyst/cortex-analyst-public-demo/cortexanalystapp"
)

func main() {
	cmd := cortexanalystapp.Cmd()
	if err := cmd.Execute(); err != nil {
		log.Fatalf("Error executing command: %v", err)
	}
	log.Println("Server stopped")
}
