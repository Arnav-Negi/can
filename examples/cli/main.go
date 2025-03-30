package main

import (
	"fmt"
	"github.com/Arnav-Negi/can"
)

func main() {
	node := can.Node[string, string]{ // Key type is string, val type is string
		NodeID:    1,
		IPAddress: "localhost:8080",
	}
	fmt.Println(node)
}
