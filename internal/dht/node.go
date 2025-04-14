package dht

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/Arnav-Negi/can/internal/cache"
	"github.com/Arnav-Negi/can/internal/routing"
	"github.com/Arnav-Negi/can/internal/store"
	"github.com/Arnav-Negi/can/internal/topology"
	pb "github.com/Arnav-Negi/can/protofiles"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Node struct {
	pb.UnimplementedCANNodeServer
	IPAddress    string
	Info         *topology.NodeInfo
	KVStore      *store.MemoryStore
	RoutingTable *routing.RoutingTable
	QueryCache   *cache.Cache
	mu           sync.RWMutex
}

// NewNode This function initializes a new Node instance.
func NewNode() *Node {
	ipAddress := "localhost:0"

	return &Node{
		IPAddress:    ipAddress,
		KVStore:      store.NewMemoryStore(),
		QueryCache:   cache.NewCache(128, 10 * time.Second), // TODO: Make this configurable
		RoutingTable: nil, 
	}
}

func GetRandomCoordinates(dims uint) []float32 {
	coords := make([]float32, dims)
	for i := 0; i < int(dims); i++ {
		coords[i] = rand.Float32()
	}
	return coords
}

// splitZone splits the current zone and returns the new zone and transferred data
func (node *Node) splitZone(coords []float32) (topology.Zone, map[string][]byte, error) {
	dims := len(node.Info.Zone.GetCoordMins())

	// Determine the dimension with the largest span
	maxSpan := float32(0)
	splitDim := 0
	currentMin := node.Info.Zone.GetCoordMins()
	currentMax := node.Info.Zone.GetCoordMaxs()

	for i := 0; i < dims; i++ {
		span := currentMax[i] - currentMin[i]
		if span > maxSpan {
			maxSpan = span
			splitDim = i
		}
	}

	// Split the zone along the dimension with the largest span
	midpoint := (currentMin[splitDim] + currentMax[splitDim]) / 2

	// Create the new zone
	newMin := make([]float32, dims)
	newMax := make([]float32, dims)
	copy(newMin, currentMin)
	copy(newMax, currentMax)

	// Adjust the zones based on where the coordinates are
	if coords[splitDim] < midpoint {
		// Joining point is in the lower half
		// New node gets lower half
		newMax[splitDim] = midpoint
		currentMin[splitDim] = midpoint
	} else {
		// Joining point is in the upper half
		// New node gets upper half
		newMin[splitDim] = midpoint
		currentMax[splitDim] = midpoint
	}

	// Update our own zone
	node.Info.Zone.SetCoordMins(currentMin)
	node.Info.Zone.SetCoordMaxs(currentMax)

	// Create the new zone object
	newZone := topology.NewZone(uint(dims))
	newZone.SetCoordMins(newMin)
	newZone.SetCoordMaxs(newMax)

	// Transfer data that falls in the new zone
	transferredData := make(map[string][]byte)
	keysToRemove := []string{}

	node.KVStore.ForEach(func(key string, value []byte) {
		toRemove := true 
		for i := 0; i < int(node.RoutingTable.NumHashes); i++ {
			// Calculate coordinates for the key
			keyCoords := node.RoutingTable.HashFunctions[i].GetCoordinates(key)
	
			// Check if the key belongs to the new zone
			if newZone.Contains(keyCoords) {
				transferredData[key] = value
			} else {
				toRemove = false
			}
		}

		// If the key belongs to current node in ANY hash 
		// function - Keep it, otherwise remove it
		if toRemove {
			keysToRemove = append(keysToRemove, key)
		}
	})

	// Remove transferred keys from our store
	for _, key := range keysToRemove {
		node.KVStore.Delete(key)
	}

	return newZone, transferredData, nil
}

// updateNeighbors returns the list of neighbors for a given zone
func (node *Node) updateNeighbors(newZone topology.Zone) []topology.NodeInfo {
	// First, collect all current neighbors
	neighbors := make([]topology.NodeInfo, 0)

	// Add ourselves as a neighbor to the new node
	neighbors = append(neighbors, *node.Info)

	// Find neighbors that are no longer adjacent after zone split
	nodesToRemove := []string{}
	for _, neighbor := range node.RoutingTable.Neighbours {
		// If the neighbor is adjacent to the new zone, add it
		if newZone.IsAdjacent(neighbor.Zone) {
			neighbors = append(neighbors, neighbor)
		}

		// If the neighbor is no longer adjacent to our zone, mark for removal
		if !node.Info.Zone.IsAdjacent(neighbor.Zone) {
			nodesToRemove = append(nodesToRemove, neighbor.NodeId)
		}
	}

	// Remove neighbors that are no longer adjacent
	for _, nodeIdToRemove := range nodesToRemove {
		for i, n := range node.RoutingTable.Neighbours {
			if n.NodeId == nodeIdToRemove {
				node.RoutingTable.Neighbours = append(
					node.RoutingTable.Neighbours[:i],
					node.RoutingTable.Neighbours[i+1:]...,
				)
				break
			}
		}
	}

	return neighbors
}

// NotifyNeighbors notifies all neighbors about this node
func (node *Node) NotifyNeighbors() error {
	for _, neighbor := range node.RoutingTable.Neighbours {
		// Skip notification if the neighbor is ourselves
		if neighbor.NodeId == node.Info.NodeId {
			continue
		}

		// Create connection to neighbor
		canConn, err := grpc.NewClient(neighbor.IpAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			// Log error but continue with other neighbors
			log.Printf("Failed to connect to neighbor %s: %v", neighbor.NodeId, err)
			continue
		}

		// Create the client and request
		canServiceClient := pb.NewCANNodeClient(canConn)
		nodeProto := &pb.Node{
			NodeId:  node.Info.NodeId,
			Address: node.Info.IpAddress,
			Zone:    zoneToProto(node.Info.Zone),
		}

		addNeighborRequest := &pb.AddNeighborRequest{
			Neighbor: nodeProto,
		}

		// Send the request
		_, err = canServiceClient.AddNeighbor(context.Background(), addNeighborRequest)
		if err != nil {
			log.Printf("Failed to notify neighbor %s: %v", neighbor.NodeId, err)
			// Continue with other neighbors
		}
	}
	return nil
}

// JoinImplementation queries bootstrap node and sends a join query
func (node *Node) JoinImplementation(bootstrapAddr string) error {
	node.mu.Lock()
	defer node.mu.Unlock()
	// Query the bootstrap node for info
	bootstrapConn, err := grpc.NewClient(bootstrapAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	bootstrapServiceClient := pb.NewBootstrapServiceClient(bootstrapConn)
	JoinInfoRequest := &pb.JoinInfoRequest{
		Address: node.IPAddress,
	}
	joinInfo, err := bootstrapServiceClient.JoinInfo(context.Background(), JoinInfoRequest)
	if err != nil {
		return err
	}

	dims := uint(joinInfo.Dimensions)
	numHashes := uint(joinInfo.NumHashes)

	// Initialise node info
	node.RoutingTable = routing.NewRoutingTable(dims, numHashes)
	node.Info = &topology.NodeInfo{
		NodeId:    joinInfo.NodeId,
		IpAddress: node.IPAddress,
		Zone:      topology.NewZone(dims),
	}

	// If no CAN nodes in network, take whole zone
	if len(joinInfo.ActiveNodes) == 0 {
		return nil
	}

	// Send Bootstrap request to one of the CAN nodes
	randIndex := rand.Intn(len(joinInfo.ActiveNodes))
	randCoords := GetRandomCoordinates(dims) // Joining point P
	joinRequest := &pb.JoinRequest{
		Coordinates: randCoords,
		NodeId:      joinInfo.NodeId,
		Address:     node.IPAddress,
	}
	canConn, err := grpc.NewClient(joinInfo.ActiveNodes[randIndex], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	canServiceClient := pb.NewCANNodeClient(canConn)
	joinResponse, err := canServiceClient.Join(context.Background(), joinRequest)
	if err != nil {
		return err
	}

	// use join response to update node info
	node.Info.Zone = topology.NewZoneFromProto(joinResponse.AssignedZone)

	// assigning neighbours
	for _, nodeInfo := range joinResponse.Neighbors {
		node.RoutingTable.AddNode(topology.NodeInfo{
			NodeId:    nodeInfo.NodeId,
			IpAddress: nodeInfo.Address,
			Zone:      topology.NewZoneFromProto(nodeInfo.Zone),
		})
	}

	// Store the transferred data
	for _, kv := range joinResponse.TransferredData {
		node.KVStore.Insert(kv.Key, kv.Value)
	}

	// Notify all neighbors about our existence
	err = node.NotifyNeighbors()
	if err != nil {
		log.Printf("Warning: Failed to notify some neighbors: %v", err)
	}

	// Log the join event
	log.Printf("Node %s joined the network with zone %v", node.Info.NodeId, node.Info.Zone)

	return nil
}

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
	successChan := make(chan struct{}, len(tryList)) // Buffered so all goroutines can write without blocking
	for _, i := range tryList {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// Find coordinates for the key
			coords := node.RoutingTable.HashFunctions[i].GetCoordinates(key)

			// If in zone, store it locally -> NO ROUTING NEEDED
			if node.Info.Zone.Contains(coords) {
				node.KVStore.Insert(key, value)
				successChan <- struct{}{} // Signal success
				return
			}

			// ROUTING NEEDED
			// Get IPs sorted by distance
			closestNodes := node.RoutingTable.GetNodesSorted(coords, 3)
			if len(closestNodes) == 0 {
				return //status.Errorf(codes.NotFound, "No nodes found in the routing table")
			}

			// Send the store request to the closest node
			for _, closestNode := range closestNodes {
				canConn, err := grpc.NewClient(closestNode.IpAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					continue
				}
				canServiceClient := pb.NewCANNodeClient(canConn)
				putResponse, err := canServiceClient.Put(context.Background(), &pb.PutRequest{
					Key:   key,
					Value: value,
					HashToUse: int32(i),
				})
				if err != nil  || !putResponse.Success {
					continue
				}
				successChan <- struct{}{} // Signal success
				return
			}
		
		}(i)
	}
	wg.Wait()

	close(successChan)
	if len(successChan) > 0 { // TODO: ONLY 1 PUT NEEDED, change to 2PC/quorum later
		node.QueryCache.Cache.Add(key, value) // Cache the value
		return nil
	}
	return status.Errorf(codes.Unavailable, "DHT is unavailable")
}

// GetImplementation This function is used to retrieve a value from the DHT.
// Error if the key does not exist or unable to retrieve the value.
func (node *Node) GetImplementation(key string, hashToUse int) ([]byte, error) {
	// Before anything, check if the key is in the cache
	// If found in cache, return the cached value
	if cachedValue, found := node.QueryCache.Cache.Get(key); found {
		log.Println("Cache hit for key:", key)
		return cachedValue, nil
	}

	var wg sync.WaitGroup // -> To clean up goroutines
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // -> Cancel the context when done

	// Let's query on every hash function
	resultChan := make(chan []byte, 1) // -> To store the result (only 1 needed)
	
	// If the hashToUse is nil, then send on all
	// Else send on that given hash to use only
	node.mu.RLock() // -> To store the error (each can send 1)
	errorChan := make(chan error, node.RoutingTable.NumHashes)
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
				conn, err := grpc.NewClient(closestNode.IpAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					continue
				}
			
				canServiceClient := pb.NewCANNodeClient(conn)
				resp, err := canServiceClient.Get(ctx, &pb.GetRequest{ 
					Key: key,
					HashToUse: int32(i),
				})
				if err == nil && resp.Value != nil {
					// See off the value
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
