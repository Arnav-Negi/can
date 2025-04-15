package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Arnav-Negi/can"
	"github.com/chzyer/readline"
)

var (
	port          = flag.Int("port", 0, "Port to listen on")
	bootstrapPort = flag.Int("bootstrapPort", 5000, "Port to listen on")
)

func main() {
	flag.Parse()
	fmt.Println("Starting CAN DHT...")

	// DHT must be started in a goroutine before making any calls to it
	dht := can.NewDHT()
	go dht.StartNode(*port)

	// TODO: Replace with synchronization structure like ctx
	time.Sleep(1 * time.Second)
	fmt.Println("Listening on :", dht.Node.IPAddress)

	err := dht.Join(fmt.Sprintf("localhost:%d", *bootstrapPort))
	if err != nil {
		fmt.Println("Error joining DHT:", err)
		return
	}
	
	fmt.Println("DHT started and listening on:", dht.Node.IPAddress)

	// Start Heartbeat routine
	go dht.Node.HeartbeatRoutine()

	// Initialize readline
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "\033[1;32mCAN-DHT > \033[0m",
		HistoryFile:     "/tmp/can_dht_history.tmp",
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		log.Fatalf("Failed to initialize CLI: %v", err)
	}
	defer rl.Close()

	fmt.Println("Welcome to the CAN-DHT CLI")
	fmt.Println("Type 'help' for a list of available commands.")
	fmt.Println("Type 'exit' to quit the CLI.")
	for {
		line, err := rl.Readline()
		if err != nil { // Ctrl+D or Ctrl+C
			break
		}

		command := strings.TrimSpace(line)
		if command == "" {
			continue
		}

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
			} else {
				fmt.Println("Put successful.")
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
				fmt.Printf("Value retrieved: %s\n", string(value))
			}
		case "exit":
			fmt.Println("Exiting...")
			return
		case "help":
			fmt.Println("Available commands:")
			fmt.Println("  put <key> <value> - Store a value in the DHT")
			fmt.Println("  get <key>         - Retrieve a value from the DHT")
			fmt.Println("  exit              - Exit the CLI")
			fmt.Println("  help              - Show this help message")
		default:
			fmt.Println("Unknown command:", command)
			fmt.Println("Type 'help' for a list of available commands.")
		}
	}
}
