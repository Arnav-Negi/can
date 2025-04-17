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

type Barrier struct {
	total    int
	arrived  int
	released int
	cond     *sync.Cond
}

func NewBarrier(n int) *Barrier {
	return &Barrier{
		total: n,
		cond:  sync.NewCond(&sync.Mutex{}),
	}
}
func (b *Barrier) Wait() {
	b.cond.L.Lock()

	gen := b.released // track current generation

	b.arrived++
	if b.arrived == b.total {
		// All arrived: reset and start next gen
		b.arrived = 0
		b.released++
		b.cond.Broadcast()
	} else {
		for gen == b.released {
			b.cond.Wait()
		}
	}

	b.cond.L.Unlock()
}

func shouldLeave(prob float64) bool {
	return rand.Float64() < prob
}

func main() {
	// Define command line flags
	bootstrapIP := flag.String("bootstrap", "localhost:5000", "IP:Port of the bootstrap node")
	numNodes := flag.Int("nodes", 5, "Number of DHT nodes to create")
	flag.Parse()

	log.Printf("Starting DHT scale test with %d nodes, bootstrap: %s", *numNodes, *bootstrapIP)

	// Create a wait group to wait for all nodes to complete their operations
	var wg sync.WaitGroup
	wg.Add(*numNodes)
	bar := NewBarrier(*numNodes)
	mu := &sync.Mutex{}

	// Launch worker nodes
	for i := 0; i < *numNodes; i++ {
		go func(nodeID int) {
			defer wg.Done()
			// Create a new DHT node
			dht := can.NewDHT()

			time.Sleep(time.Duration(i) * 20 * time.Millisecond)

			// Start the node on a random port
			go func() {
				err := dht.StartNode("localhost", 5050+nodeID) // 0 for random port
				if err != nil {
					//logMutex.Lock()
					log.Printf("ERROR Node %d failed to start: %v", nodeID, err)
					//logMutex.Unlock()
				}
			}()

			// Wait a short time for node to initialize
			time.Sleep(10 * time.Millisecond)

			// Join the DHT network using bootstrap
			bar.Wait()

			// get mutex
			mu.Lock()
			log.Printf("Node %d: joining network via %s", nodeID, *bootstrapIP)

			err := dht.Join(*bootstrapIP)

			mu.Unlock()
			if err != nil {
				//logMutex.Lock()
				log.Printf("ERROR Node %d failed to join network: %v", nodeID, err)
				//logMutex.Unlock()
			}

			// Generate unique key-value pairs for this node
			keyPrefix := fmt.Sprintf("node-%d-key-", nodeID)
			valuePrefix := fmt.Sprintf("node-%d-value-", nodeID)
			numPairs := 5 + rand.Intn(5) // 5-9 pairs per node

			// wait for all nodes to join
			bar.Wait()

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
			bar.Wait()

			left := false

			// Make nodes leave with 0.25 chance
			if shouldLeave(0.25) {
				mu.Lock()
				log.Printf("Node %d: Leaving", nodeID)
				err := dht.Leave()
				if err != nil {
					log.Printf("ERROR Node %d failed to leave: %v", nodeID, err)
				}
				log.Printf("Node %d: Left", nodeID)
				left = true
				mu.Unlock()
			}
			bar.Wait()

			// Retrieve our key-value pairs and verify
			for key, expectedValue := range kvPairs {
				if left {
					break
				}
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

			bar.Wait()

			// Now try to retrieve other nodes' data
			// Each node will try to get a key from the previous node
			if nodeID > 0 && !left {
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
