package dht

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Arnav-Negi/can/protofiles"
)

// PutImplementation This function is used to store a value in the DHT.
// Overwrites the value if the key already exists.
func (node *Node) PutImplementation(key string, value []byte, hashToUse int) error {
	// We must send on every hash function
	// Since the PUT needs to succeed everywhere
	// TODO : 2PC + Timeout needed later

	// Let's query on every hash function
	var wg sync.WaitGroup

	// If the hashToUse is nil, then send on all
	// Else send on that given hash to use only
	node.mu.RLock() // -> To store the error (each can send 1)
	tryList := make([]int, 0)
	if hashToUse == -1 {
		// Send on all
		tryList = make([]int, node.RoutingTable.NumHashes)
		for i := 0; i < int(node.RoutingTable.NumHashes); i++ {
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
			// If in zone, store it locally -> NO ROUTING NEEDED
			coords := node.RoutingTable.HashFunctions[i].GetCoordinates(key)
			if node.Info.Zone.Contains(coords) {
				node.KVStore.Insert(key, value)
				successChan <- struct{}{} // Signal success
				return
			}

			// ROUTING NEEDED
			// Get IPs sorted by distance
			closestNodes := node.RoutingTable.GetNodesSorted(coords, node.Info.Zone, 3)
			if len(closestNodes) == 0 {
				return //status.Errorf(codes.NotFound, "No nodes found in the routing table")
			}

			// Send the store request to the closest node
			for _, closestNode := range closestNodes {
				conn, err := node.getClientConn(closestNode.IpAddress)
				if err != nil {
					continue
				}
				canServiceClient := pb.NewCANNodeClient(conn)

				putResponse, err := canServiceClient.Put(context.Background(), &pb.PutRequest{
					Key:       key,
					Value:     value,
					HashToUse: int32(i),
				})
				if err != nil || !putResponse.Success {
					continue
				}
				successChan <- struct{}{} // Signal success
				return
			}

		}(i)
	}
	wg.Wait()

	close(successChan)
	if len(successChan) > 0 { // TODO: ONLY 1 successful PUT NEEDED rn, change to 2PC/quorum later
		node.QueryCache.Cache.Add(key, value) // Cache the value
		return nil
	}
	return status.Errorf(codes.Unavailable, "DHT is unavailable")
}
