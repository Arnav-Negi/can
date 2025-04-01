package main

import (
	"flag"
	"fmt"
	"github.com/Arnav-Negi/can"
)

const (
	// bootstrap
	bootstrapPort = 5000
)

var (
	port = flag.Int("port", 8080, "Port to listen on")
)

func main() {
	flag.Parse()
	fmt.Println("Starting CAN DHT...")
	dht, err := can.NewDHT(*port)
	if err != nil {
		fmt.Println("Error initializing DHT:", err)
		return
	}
	fmt.Println("DHT initialized with IP address:", dht.IPAddress)
}
