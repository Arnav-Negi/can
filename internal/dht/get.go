package dht

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Arnav-Negi/can/protofiles"
)

func (node *Node) GetHelper(helperCtx context.Context, key string, hashIdx int) ([]byte, error) {
	// Find coordinates for the key
	coords := node.RoutingTable.HashFunctions[hashIdx].GetCoordinates(key)

	// If in zone, retrieve it locally -> NO ROUTING NEEDED
	if node.Info.Zone.Contains(coords) {
		value, err := node.KVStore.Retrieve(key)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "Key not found in the DHT")
		} else {
			return value, nil
		}
	}

	// ROUTING NEEDED
	// Get IPs sorted by distance
	closestNodes := node.RoutingTable.GetNodesSorted(coords, 3)
	if len(closestNodes) == 0 {
		return nil, status.Errorf(codes.Unavailable, "No nodes found in the routing table")
	}

	// Send the get request to the closest node
	for _, closestNode := range closestNodes {
		// before querying, check if ctx is done, if so return
		if helperCtx.Err() != nil {
			return nil, helperCtx.Err()
		}

		conn, err := node.getClientConn(closestNode.IpAddress)
		if err != nil {
			continue
		}
		canServiceClient := pb.NewCANNodeClient(conn)

		resp, err := canServiceClient.Get(context.Background(), &pb.GetRequest{
			Key:       key,
			HashToUse: int32(hashIdx),
		})
		if err == nil && resp.Value != nil {
			return resp.Value, nil
		}

		// notFound then return notFound
		if status.Code(err) == codes.NotFound {
			return nil, err
		}

		// else keep trying
	}

	return nil, status.Errorf(codes.Unavailable, "DHT is unavailable")
}

// GetImplementation This function is used to retrieve a value from the DHT.
// Error if the key does not exist or unable to retrieve the value.
func (node *Node) GetImplementation(key string, hashToUse int) ([]byte, error) {
	// Before anything, check if the key is in the cache
	// If found in cache, return the cached value
	if cachedValue, found := node.QueryCache.Cache.Get(key); found {
		node.logger.Println("Cache hit for key:", key)
		return cachedValue, nil
	}

	var wg sync.WaitGroup

	// ctx is cancelled when notFound or value is returned from some hash
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Different behaviors for each case
	resultChan := make(chan []byte, 1)

	// If the hashToUse is nil, then send on all
	// Else send on that given hash to use only
	node.mu.RLock()
	var tryList []int
	if hashToUse == -1 { // Send on all
		tryList = make([]int, node.RoutingTable.NumHashes)
		for i := 0; i < int(node.RoutingTable.NumHashes); i++ {
			tryList[i] = i
		}
	} else { // Forward on the given hash ID
		tryList = []int{hashToUse}
	}
	node.mu.RUnlock()

	errChan := make(chan error, len(tryList))

	// Try on the hash functions
	for _, i := range tryList {
		wg.Add(1)
		go func(hashIdx int) {
			defer wg.Done()
			val, err := node.GetHelper(ctx, key, hashIdx)
			if err == nil {
				select {
				case resultChan <- val:
					cancel()
					return
				default:
					return
				}
			}
			errChan <- err
		}(i)
	}
	wg.Wait() // Wait for all goroutines to finish

	retrieved := false
	unavailable := false
	for i := 0; i < len(tryList); i++ {
		err := <-errChan
		if err == nil {
			retrieved = true
		} else if status.Code(err) != codes.NotFound {
			unavailable = true
		}
	}

	var result []byte
	if retrieved {
		result = <-resultChan
	}

	if result != nil {
		return result, nil
	} else if unavailable {
		return nil, status.Errorf(codes.Unavailable, "DHT is unavailable")
	} else {
		return nil, status.Errorf(codes.NotFound, "Key not found in the DHT")
	}
}
