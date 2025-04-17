package main

import (
	"flag"
	"fmt"
	"github.com/Arnav-Negi/can/internal/utils"
	"log"
	"strings"
	"time"

	"github.com/Arnav-Negi/can"
	"github.com/chzyer/readline"
)

var (
	selfIP = flag.String("ip", "127.0.0.1", "Node's IP address")
	port   = flag.Int("port", 0, "Node's port")

	bootstrapIP   = flag.String("bstrap-ip", "127.0.0.1", "Bootstrap IP address")
	bootstrapPort = flag.Int("bstrap-port", 5000, "Bootstrap port")
)

func main() {
	flag.Parse()
	fmt.Println("Starting CAN DHT...")

	// validate IP
	if !utils.ValidateIP(*selfIP) {
		log.Fatalf("Invalid IP: %s", *selfIP)
	}

	// DHT must be started in a goroutine before making any calls to it
	dht := can.NewDHT()
	go dht.StartNode(*selfIP, *port , fmt.Sprintf("%s:%d", *bootstrapIP, *bootstrapPort))

	// TODO: Replace with synchronization structure like ctx
	time.Sleep(1 * time.Second)
	fmt.Println("Listening on :", dht.Node.IPAddress)

	err := dht.Join(fmt.Sprintf("%s:%d", *bootstrapIP, *bootstrapPort))
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
		case "delete":
			if len(args) != 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			key := args[1]
			err := dht.Delete(key)
			if err != nil {
				fmt.Println("Error deleting value:", err)
			} else {
				fmt.Println("Delete successful.")
			}
		case "exit":
			fmt.Println("Exiting...")
			return
		case "help":
			fmt.Println("Available commands:")
			fmt.Println("  put <key> <value> - Store a value in the DHT")
			fmt.Println("  get <key>         - Retrieve a value from the DHT")
			fmt.Println("  delete <key>      - Delete a value from the DHT")
			fmt.Println("  exit              - Exit the CLI")
			fmt.Println("  help              - Show this help message")
		default:
			fmt.Println("Unknown command: ", command)
			fmt.Println("Type 'help' for a list of available commands.")
		}
	}
}
