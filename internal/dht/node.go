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
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	pb.UnimplementedCANNodeServer
	conns        	map[string]*grpc.ClientConn
	
	IPAddress     	string
	
	Info         	*topology.NodeInfo
	RoutingTable 	*routing.RoutingTable
	NeighInfo   	[]topology.NodeInfo
	lastHeartbeat	map[string]time.Time
	
	KVStore      	*store.MemoryStore
	QueryCache   	*cache.Cache
	
	mu           	sync.RWMutex
}

// NewNode This function initializes a new Node instance.
func NewNode() *Node {
	ipAddress := "localhost:0"

	return &Node{
		conns:        	make(map[string]*grpc.ClientConn),
		IPAddress:   	ipAddress,
		RoutingTable: 	nil, 
		NeighInfo:    	make([]topology.NodeInfo, 0),
		lastHeartbeat: 	make(map[string]time.Time),
		KVStore:      	store.NewMemoryStore(),
		QueryCache:   	cache.NewCache(128, 10 * time.Second), // TODO: Make this configurable
		mu: 		  	sync.RWMutex{},
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
		conn, err := grpc.NewClient(neighbor.IpAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			// Log error but continue with other neighbors
			log.Printf("Failed to connect to neighbor %s: %v", neighbor.NodeId, err)
			continue
		}

		// Create the client and request
		canServiceClient := pb.NewCANNodeClient(conn)
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

// NotifyAllNeighboursOfTwoHopInfo notifies all neighbours about the 2-hop information
func (node *Node) NotifyAllNeighboursOfTwoHopInfo() {
	node.mu.RLock()
	neighbours := node.RoutingTable.Neighbours
	node.mu.RUnlock()

	var wg sync.WaitGroup

	for _, neighbour := range neighbours {
		wg.Add(1)
		go func(nbr topology.NodeInfo) {
			defer wg.Done()

			conn, err := node.getClientConn(nbr.IpAddress)
			if err != nil {
				log.Printf("Failed to connect to %s: %v", nbr.IpAddress, err)
				return
			}
			client := pb.NewCANNodeClient(conn)

			node.mu.RLock()
			// Build zones and neighbour protos while holding read lock
			zones := make([]*pb.Zone, len(neighbours))
			protoNeighbours := make([]*pb.Node, len(neighbours))
			for i, n := range neighbours {
				zones[i] = zoneToProto(n.Zone)
				protoNeighbours[i] = NodeInfoToProto(n)
			}
			zone := zoneToProto(node.Info.Zone)
			nodeId := node.Info.NodeId
			node.mu.RUnlock()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			_, err = client.SendNeighbourInfo(ctx, &pb.NeighbourInfoRequest{
				NodeId:     nodeId,
				Zone:       zone,
				Neighbours: protoNeighbours,
				Zones:      zones,
			})
			if err != nil {
				log.Printf("Failed to send neighbour info to %s: %v", nbr.IpAddress, err)
			}
		}(neighbour)
	}

	wg.Wait()
	log.Printf("Updated 2-hop neighbours for all nodes")
}

// JoinImplementation queries bootstrap node and sends a join query
func (node *Node) JoinImplementation(bootstrapAddr string) error {
	// Query the bootstrap node for info
	bootstrapConn, err := grpc.NewClient(bootstrapAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { return err }
	defer bootstrapConn.Close()
	bootstrapServiceClient := pb.NewBootstrapServiceClient(bootstrapConn)

	joinInfo, err := bootstrapServiceClient.JoinInfo(
		context.Background(),
		&pb.JoinInfoRequest{
			Address: node.IPAddress,
		},
	)
	if err != nil { return err }

	// Initialise node info
	node.mu.Lock()
	dims := uint(joinInfo.Dimensions)
	numHashes := uint(joinInfo.NumHashes)
	node.RoutingTable = routing.NewRoutingTable(dims, numHashes)
	node.Info = &topology.NodeInfo{
		NodeId:    joinInfo.NodeId,
		IpAddress: node.IPAddress,
		Zone:      topology.NewZone(dims),
	}
	node.mu.Unlock()

	// If no CAN nodes in network, take whole zone
	if len(joinInfo.ActiveNodes) == 0 { return nil }

	// Send Bootstrap request to one of the CAN nodes
	randIndex := rand.Intn(len(joinInfo.ActiveNodes))
	randCoords := GetRandomCoordinates(dims) // Joining point P
	conn, err := node.getClientConn(joinInfo.ActiveNodes[randIndex])
	if err != nil { return err }

	canServiceClient := pb.NewCANNodeClient(conn)
	joinResponse, err := canServiceClient.Join(
		context.Background(),
		&pb.JoinRequest{
			Coordinates: randCoords,
			NodeId:      joinInfo.NodeId,
			Address:     node.IPAddress,
		},
	)
	if err != nil { return err }

	// use join response to update node info
	node.mu.Lock()
	defer node.mu.Unlock()
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
	if err = node.NotifyNeighbors(); err != nil {
		log.Printf("Warning: Failed to notify some neighbors: %v", err)
	}

	// Log the join event
	log.Printf("Node %s joined the network with zone %v", node.Info.NodeId, node.Info.Zone)

	return nil
}
