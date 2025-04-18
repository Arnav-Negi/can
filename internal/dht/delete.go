package dht

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Arnav-Negi/can/protofiles"
)

// DeleteImplementation This function is used to delete a key in the DHT.
func (node *Node) DeleteImplementation(key string, hashToUse int) error {
	// We must send on every hash function
	// Since the DELETE needs to succeed everywhere

	// Let's query on every hash function
	var wg sync.WaitGroup

	// If the hashToUse is nil, then send on all
	// Else send on that given hash to use only
	node.mu.RLock() // -> To store the error (each can send 1)
	tryList := make([]int, 0)
	if hashToUse == -1 {
		// Send on all
		tryList = make([]int, node.RoutingTable.NumHashes)
		for i := range node.RoutingTable.HashFunctions {
			tryList[i] = i
		}
	} else {
		// Send on that only
		tryList = append(tryList, hashToUse)
	}
	node.mu.RUnlock()

	// Try on the hash functions
	successChan := make(chan struct{}, len(tryList))
	for _, i := range tryList {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// Find coordinates for the key
			// If in zone, delete it locally -> NO ROUTING NEEDED
			coords := node.RoutingTable.HashFunctions[i].GetCoordinates(key)
			if node.Info.Zone.Contains(coords) {
				node.KVStore.Delete(key)
				successChan <- struct{}{} // Signal success
				return
			}

			// ROUTING NEEDED
			// Get IPs sorted by distance
			closestNodes := node.RoutingTable.GetNodesSorted(coords, node.Info.Zone, 3)
			if len(closestNodes) == 0 {
				return
			} // No nodes found in the routing table

			// Send the store request to the closest node
			for _, closestNode := range closestNodes {
				conn, err := node.getClientConn(closestNode.IpAddress)
				if err != nil {
					continue
				}
				canServiceClient := pb.NewCANNodeClient(conn)

				deleteResponse, err := canServiceClient.Delete(
					context.Background(),
					&pb.DeleteRequest{
						Key:       key,
						HashToUse: int32(i),
					},
				)
				if err != nil || !deleteResponse.Success {
					continue
				}
				successChan <- struct{}{} // Signal success

				return
			}
		}(i)
	}
	wg.Wait()

	close(successChan)
	if len(successChan) > 0 {
		// Delete the key from cache if it exists
		node.QueryCache.Cache.Remove(key)
		return nil
	}
	return status.Errorf(codes.Unavailable, "DHT is unavailable")
}
