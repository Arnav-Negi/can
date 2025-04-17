package main

import (
	"crypto/rand"
	"encoding/base64"
	"flag"
	"github.com/Arnav-Negi/can"
	"log"
	"math"
	"sort"
	"sync"
	"time"
)

// LatencyStats holds various statistics about operation latencies
type LatencyStats struct {
	OperationType string
	Count         int
	Min           time.Duration
	Max           time.Duration
	Average       time.Duration
	Median        time.Duration
	Percentile95  time.Duration
	Percentile99  time.Duration
	StdDev        time.Duration
}

func main() {
	// Define command line flags
	bootstrapIP := flag.String("bootstrap", "127.0.0.1:5000", "IP:Port of the already running bootstrap node")
	numNodes := flag.Int("nodes", 5, "Number of DHT nodes to create")
	numOps := flag.Int("ops", 100, "Number of operations (put/get pairs) to perform")
	valueSize := flag.Int("value-size", 1024, "Size of random values in bytes")
	setupTimeoutSec := flag.Int("setup-timeout", 5, "Seconds to wait for node setup")
	flag.Parse()

	log.Printf("Starting DHT latency test with %d nodes and %d operations", *numNodes, *numOps)
	log.Printf("Using bootstrap node at: %s", *bootstrapIP)

	// Create nodes and connect them to the network
	nodes := make([]*can.DHT, *numNodes)

	// Use a wait group to ensure all nodes are ready
	var wg sync.WaitGroup
	wg.Add(*numNodes)

	mu := &sync.Mutex{}

	for i := 0; i < *numNodes; i++ {
		go func(nodeID int) {
			defer wg.Done()

			// Create and start a new DHT node
			dht := can.NewDHT()

			go func() {
				err := dht.StartNode("127.0.0.1", 5050+nodeID, *bootstrapIP) // Use random port
				if err != nil {
					log.Fatalf("Node %d failed to start: %v", nodeID, err)
				}
			}()

			// Brief pause to allow node to initialize
			time.Sleep(1 * time.Second)

			mu.Lock()
			log.Printf("Node %d: Joining network via %s", nodeID, *bootstrapIP)
			err := dht.Join(*bootstrapIP)
			if err != nil {
				log.Fatalf("Node %d failed to join network: %v", nodeID, err)
			}

			nodes[nodeID] = dht
			mu.Unlock()

			log.Printf("Node %d: Successfully joined the DHT network", nodeID)
		}(i)
	}

	// Wait for all nodes to join the network
	wg.Wait()
	log.Printf("All %d nodes have joined the network", *numNodes)

	// Allow time for network stabilization
	log.Printf("Waiting %d seconds for network stabilization...", *setupTimeoutSec)
	time.Sleep(time.Duration(*setupTimeoutSec) * time.Second)

	// Choose one node to perform all the operations
	testNode := nodes[0]
	log.Printf("Using Node 0 as the test node for performing operations")

	// Prepare storage for latency measurements
	putLatencies := make([]time.Duration, *numOps)
	getLatencies := make([]time.Duration, *numOps)

	// Store keys for later retrieval
	keys := make([]string, *numOps)

	// Perform put operations and measure latency
	log.Printf("Starting %d PUT operations with %d byte values...", *numOps, *valueSize)
	for i := 0; i < *numOps; i++ {
		// Generate random key and value
		key := generateRandomString(16)
		value := generateRandomBytes(*valueSize)
		keys[i] = key

		// Measure put latency
		start := time.Now()
		err := testNode.Put(key, value)
		elapsed := time.Since(start)

		if err != nil {
			log.Fatalf("PUT no. %d operation failed for key %s: %v", i, key, err)
		}

		putLatencies[i] = elapsed

		if i > 0 && i%(*numOps/10) == 0 {
			log.Printf("Completed %d PUT operations (%d%%)", i, i*100/(*numOps))
		}
	}

	// Allow time for data to propagate
	log.Printf("All PUT operations completed. Waiting for data propagation...")
	time.Sleep(2 * time.Second)

	// Perform get operations and measure latency
	log.Printf("Starting %d GET operations...", *numOps)
	for i := 0; i < *numOps; i++ {
		key := keys[i]

		// Measure get latency
		start := time.Now()
		_, err := testNode.Get(key)
		elapsed := time.Since(start)

		if err != nil {
			log.Fatalf("GET operation failed for key %s: %v", key, err)
		}

		getLatencies[i] = elapsed

		if i > 0 && i%(*numOps/10) == 0 {
			log.Printf("Completed %d GET operations (%d%%)", i, i*100/(*numOps))
		}
	}

	// Calculate and display statistics
	putStats := calculateLatencyStats("PUT", putLatencies)
	getStats := calculateLatencyStats("GET", getLatencies)

	// Display statistics
	displayStats(putStats)
	displayStats(getStats)

	log.Printf("Latency test completed successfully!")
}

// Calculate various statistics from a list of latencies
func calculateLatencyStats(opType string, latencies []time.Duration) LatencyStats {
	count := len(latencies)
	if count == 0 {
		return LatencyStats{OperationType: opType}
	}

	// Sort latencies for percentile calculations
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// Calculate min and max
	min := latencies[0]
	max := latencies[count-1]

	// Calculate average
	var sum time.Duration
	for _, latency := range latencies {
		sum += latency
	}
	avg := sum / time.Duration(count)

	// Calculate median
	median := latencies[count/2]
	if count%2 == 0 && count > 1 {
		median = (latencies[count/2-1] + latencies[count/2]) / 2
	}

	// Calculate percentiles
	p95Index := int(math.Ceil(float64(count)*0.95)) - 1
	p99Index := int(math.Ceil(float64(count)*0.99)) - 1
	if p95Index >= count {
		p95Index = count - 1
	}
	if p99Index >= count {
		p99Index = count - 1
	}
	p95 := latencies[p95Index]
	p99 := latencies[p99Index]

	// Calculate standard deviation
	var varianceSum float64
	avgNs := avg.Nanoseconds()
	for _, latency := range latencies {
		diff := float64(latency.Nanoseconds() - avgNs)
		varianceSum += diff * diff
	}
	variance := varianceSum / float64(count)
	stdDev := time.Duration(math.Sqrt(variance))

	return LatencyStats{
		OperationType: opType,
		Count:         count,
		Min:           min,
		Max:           max,
		Average:       avg,
		Median:        median,
		Percentile95:  p95,
		Percentile99:  p99,
		StdDev:        stdDev,
	}
}

// Display statistics in a formatted way
func displayStats(stats LatencyStats) {
	log.Printf("\n--- %s Operation Latency Statistics ---", stats.OperationType)
	log.Printf("Count:            %d operations", stats.Count)
	log.Printf("Minimum:          %v", stats.Min)
	log.Printf("Maximum:          %v", stats.Max)
	log.Printf("Average:          %v", stats.Average)
	log.Printf("Median:           %v", stats.Median)
	log.Printf("95th Percentile:  %v", stats.Percentile95)
	log.Printf("99th Percentile:  %v", stats.Percentile99)
	log.Printf("Standard Dev:     %v", stats.StdDev)
	log.Printf("-------------------------------------\n")
}

// Generate a random string of specified length
func generateRandomString(length int) string {
	bytes := generateRandomBytes(length)
	return base64.URLEncoding.EncodeToString(bytes)[:length]
}

// Generate random bytes of specified length
func generateRandomBytes(length int) []byte {
	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	if err != nil {
		log.Fatalf("Failed to generate random bytes: %v", err)
	}
	return bytes
}
