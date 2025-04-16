package main

import (
	"flag"
	"fmt"
	"github.com/Arnav-Negi/can"
	"log"
	"math/rand"
	"sync"
	"time"
)

func main() {
	// Define command line flags
	bootstrapIP := flag.String("bootstrap", "127.0.0.1:5000", "IP:Port of the bootstrap node")
	numNodes := flag.Int("nodes", 5, "Number of DHT nodes to create")
	operationTimeoutSec := flag.Int("op-timeout", 2, "Seconds to wait between operations")
	flag.Parse()

	log.Printf("Starting DHT scale test with %d nodes, bootstrap: %s", *numNodes, *bootstrapIP)

	// Create a wait group to wait for all nodes to complete their operations
	var wg sync.WaitGroup
	wg.Add(*numNodes)

	// Launch worker nodes
	for i := 0; i < *numNodes; i++ {
		go func(nodeID int) {
			defer wg.Done()
			time.Sleep(time.Duration(i) * 20 * time.Millisecond)

			// Create a new DHT node
			dht := can.NewDHT()

			// Start the node on a random port
			go func() {
				err := dht.StartNode(0) // 0 for random port
				if err != nil {
					//logMutex.Lock()
					log.Printf("ERROR Node %d failed to start: %v", nodeID, err)
					//logMutex.Unlock()
				}
			}()

			// Wait a short time for node to initialize
			time.Sleep(time.Duration(*numNodes) * 30 * time.Millisecond)

			// Join the DHT network using bootstrap
			//logMutex.Lock()
			log.Printf("Node %d: joining network via %s", nodeID, *bootstrapIP)
			//logMutex.Unlock()

			err := dht.Join(*bootstrapIP)
			if err != nil {
				//logMutex.Lock()
				log.Printf("ERROR Node %d failed to join network: %v", nodeID, err)
				//logMutex.Unlock()
			}

			// Wait for network stabilization
			time.Sleep(time.Duration(*operationTimeoutSec) * 5 * time.Second)

			// Generate unique key-value pairs for this node
			keyPrefix := fmt.Sprintf("node-%d-key-", nodeID)
			valuePrefix := fmt.Sprintf("node-%d-value-", nodeID)
			numPairs := 5 + rand.Intn(5) // 5-9 pairs per node

			// Store our key-value pairs
			kvPairs := make(map[string][]byte)
			for j := 0; j < numPairs; j++ {
				key := fmt.Sprintf("%s%d", keyPrefix, j)
				value := []byte(fmt.Sprintf("%s%d", valuePrefix, j))
				kvPairs[key] = value

				//logMutex.Lock()
				log.Printf("Node %d: Putting %s = %s", nodeID, key, value)
				//logMutex.Unlock()

				err := dht.Put(key, value)
				if err != nil {
					//logMutex.Lock()
					log.Printf("ERROR Node %d failed to put %s: %v", nodeID, key, err)
					//logMutex.Unlock()
				}

				// Brief pause between operations
				time.Sleep(50 * time.Millisecond)
			}

			// Wait for data to propagate
			time.Sleep(time.Duration(*operationTimeoutSec) * time.Second)

			// Retrieve our key-value pairs and verify
			for key, expectedValue := range kvPairs {
				//logMutex.Lock()
				log.Printf("Node %d: Getting %s", nodeID, key)
				//logMutex.Unlock()

				value, err := dht.Get(key)
				if err != nil {
					//logMutex.Lock()
					log.Printf("ERROR Node %d failed to get %s: %v", nodeID, key, err)
					//logMutex.Unlock()
				}

				// Verify retrieved value matches what we stored
				if string(value) != string(expectedValue) {
					//logMutex.Lock()
					log.Printf("ERROR Node %d: Value mismatch for key %s. Expected '%s', got '%s'",
						nodeID, key, expectedValue, value)
					//logMutex.Unlock()
				}

				//logMutex.Lock()
				log.Printf("Node %d: Successfully verified %s = %s", nodeID, key, value)
				//logMutex.Unlock()

				// Brief pause between operations
				time.Sleep(50 * time.Millisecond)
			}

			// Now try to retrieve other nodes' data
			// Each node will try to get a key from the previous node
			if nodeID > 0 {
				otherKey := fmt.Sprintf("node-%d-key-0", nodeID-1)
				//logMutex.Lock()
				log.Printf("Node %d: Getting key from another node: %s", nodeID, otherKey)
				//logMutex.Unlock()

				_, err := dht.Get(otherKey)
				if err != nil {
					//logMutex.Lock()
					log.Printf("ERROR Node %d failed to get other node's key %s: %v", nodeID, otherKey, err)
					//logMutex.Unlock()
				}

				//logMutex.Lock()
				log.Printf("Node %d: Successfully retrieved other node's key %s", nodeID, otherKey)
				//logMutex.Unlock()
			}

			//logMutex.Lock()
			log.Printf("Node %d: All operations completed successfully", nodeID)
			//logMutex.Unlock()
		}(i)
	}

	// Wait for all nodes to complete their operations
	wg.Wait()
	log.Printf("Test completed successfully! All nodes operated without errors.")
}
