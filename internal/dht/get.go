package dht

import (
	"context"
	"log"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Arnav-Negi/can/protofiles"
)

// GetImplementation This function is used to retrieve a value from the DHT.
// Error if the key does not exist or unable to retrieve the value.
func (node *Node) GetImplementation(key string, hashToUse int) ([]byte, error) {
	// Before anything, check if the key is in the cache
	// If found in cache, return the cached value
	if cachedValue, found := node.QueryCache.Cache.Get(key); found {
		log.Println("Cache hit for key:", key)
		return cachedValue, nil
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultChan := make(chan []byte, 1)
	
	// If the hashToUse is nil, then send on all
	// Else send on that given hash to use only
	node.mu.RLock() // -> To store the error (each can send 1)
	errorChan := make(chan error, node.RoutingTable.NumHashes)
	tryList := make([]int, 0)
	if hashToUse == -1 { // Send on all
		tryList = make([]int, node.RoutingTable.NumHashes)
		for i := 0; i < int(node.RoutingTable.NumHashes); i++ {
			tryList[i] = i
		}
	} else { // Forward on the given hash ID
		tryList = append(tryList, hashToUse)
	}
	node.mu.RUnlock()

	// Try on the hash functions
	for _, i := range tryList {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// Find coordinates for the key
			coords := node.RoutingTable.HashFunctions[i].GetCoordinates(key)
		
			// If in zone, retrieve it locally -> NO ROUTING NEEDED
			if node.Info.Zone.Contains(coords) {
				value, err := node.KVStore.Retrieve(key)
				if err != nil {
					// If not found, return an error
					errorChan <- status.Errorf(codes.NotFound, "Key not found")
					return
				} else {
					select {
					case resultChan <- value:
						// Successfully sent the value to the channel
						// Cancel the context to stop other goroutines
						cancel()
					case <-ctx.Done():
						// Context is done, exit the goroutine
					}
					return
				}
			}

			// ROUTING NEEDED
			// Get IPs sorted by distance
			closestNodes := node.RoutingTable.GetNodesSorted(coords, 3)
			if len(closestNodes) == 0 {
				errorChan <- status.Errorf(codes.NotFound, "No nodes found in the routing table")
				return
			}
			
			// Send the get request to the closest node
			for _, closestNode := range closestNodes {
				conn, err := node.getClientConn(closestNode.IpAddress)
				if err != nil { continue }
				canServiceClient := pb.NewCANNodeClient(conn)
			
				resp, err := canServiceClient.Get(ctx, &pb.GetRequest{ 
					Key: key,
					HashToUse: int32(i),
				})
				if err == nil && resp.Value != nil {
					select {
					case resultChan <- resp.Value:
						// Successfully sent the value to the channel
						// Cancel the context to stop other goroutines
						cancel()
					case <-ctx.Done():
						// Context is done, exit the goroutine
					}
					return
				}
			}
		}(i)
	}

	var result []byte
	select {
	case result = <-resultChan:
		// Successfully retrieved the value
		// Store the value in the cache
		node.QueryCache.Cache.Add(key, result)
	case <- ctx.Done():
		// Everyone failed
	}
	wg.Wait() // Wait for all goroutines to finish

	if result != nil {
		return result, nil
	}
	return nil, status.Errorf(codes.NotFound, "Key not found in the DHT")
}
