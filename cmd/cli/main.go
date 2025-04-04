package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/Arnav-Negi/can"
	"os"
	"strings"
)

var (
	port          = flag.Int("port", 0, "Port to listen on")
	bootstrapPort = flag.Int("bootstrapPort", 5000, "Port to listen on")
)

func main() {
	flag.Parse()
	fmt.Println("Starting CAN DHT...")
	dht, err := can.NewDHT(*port)
	if err != nil {
		fmt.Println("Error initializing DHT:", err)
		return
	}
	err = dht.Join(fmt.Sprintf("localhost:%d", *bootstrapPort))

	if err != nil {
		fmt.Println("Error joining DHT:", err)
		return
	}

	go dht.StartNode()

	fmt.Println("DHT started and listening on:", dht.Node.IPAddress)

	// CLI for interacting with the DHT
	var command string
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command (put <k> <v>/get <k>/exit): ")
		command, err = reader.ReadString('\n')
		command = strings.TrimSpace(command)

		args := strings.Split(command, " ")
		switch args[0] {
		case "put":
			if len(args) != 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			key, value := args[1], args[2]
			err := dht.Put(key, []byte(value))
			if err != nil {
				fmt.Println("Error putting value:", err)
			}
		case "get":
			if len(args) < 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := args[1]
			value, err := dht.Get(key)
			if err != nil {
				fmt.Println("Error getting value:", err)
			} else {
				fmt.Println("Value retrieved:", string(value))
			}
		case "exit":
			return
		default:
			fmt.Println("Unknown command: ", command)
		}
	}
}
