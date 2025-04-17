package dht

import (
	"context"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/Arnav-Negi/can/internal/routing"
	"github.com/Arnav-Negi/can/internal/store"
	"github.com/Arnav-Negi/can/internal/topology"
	pb "github.com/Arnav-Negi/can/protofiles"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sirupsen/logrus"
)

type Node struct {
	pb.UnimplementedCANNodeServer

	conns map[string]*grpc.ClientConn

	IPAddress string

	Info          *topology.NodeInfo
	RoutingTable  *routing.RoutingTable
	NeighInfo     []topology.NodeInfo
	lastHeartbeat map[string]time.Time

	KVStore    *store.MemoryStore
	QueryCache *Cache

	mu sync.RWMutex

	logger *logrus.Logger

	// server to stop
	grpcServer  *grpc.Server
	bootstrapIP string
}

// NewNode This function initializes a new Node instance.
func NewNode() *Node {
	ipAddress := "127.0.0.1:0"

	retNode := &Node{
		conns:         make(map[string]*grpc.ClientConn),
		IPAddress:     ipAddress,
		RoutingTable:  nil,
		NeighInfo:     make([]topology.NodeInfo, 0),
		lastHeartbeat: make(map[string]time.Time),
		KVStore:       store.NewMemoryStore(),
		QueryCache:    nil,
		mu:            sync.RWMutex{},
		logger:        logrus.New(),
	}
	retNode.QueryCache = retNode.GetNewCache(128, 10*time.Second) // TODO: Make this configurable
	return retNode
}

func (node *Node) GetInfo() (string, []float32, []float32) {
	return node.Info.IpAddress, node.Info.Zone.GetCoordMins(), node.Info.Zone.GetCoordMaxs()
}

func GetRandomCoordinates(dims uint) []float32 {
	coords := make([]float32, dims)
	for i := 0; i < int(dims); i++ {
		coords[i] = rand.Float32()
	}
	return coords
}

func (node *Node) getGRPCConn(addr string) (*grpc.ClientConn, error) {
	// Load TLS credentials
	tlsCreds, err := LoadTLSCredentials(node.IPAddress)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(tlsCreds),
		grpc.WithUnaryInterceptor(LoggingUnaryClientInterceptor(node.logger)),
	)
	return conn, err
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
	var keysToRemove []string

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

	// Notify and update the neighbors about our updated zone
	if err := node.NotifyNeighbors(); err != nil {
		node.logger.Printf("Warning: Failed to update some neighbors: %v", err)
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
		conn, err := node.getGRPCConn(neighbor.IpAddress)
		if err != nil {
			// Log error but continue with other neighbors
			node.logger.Printf("Failed to connect to neighbor %s: %v", neighbor.NodeId, err)
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
			node.logger.Printf("Failed to notify neighbor %s: %v", neighbor.NodeId, err)
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
				node.logger.Printf("Failed to connect to %s: %v", nbr.IpAddress, err)
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
				node.logger.Printf("Failed to send neighbour info to %s: %v", nbr.IpAddress, err)
			}
		}(neighbour)
	}

	wg.Wait()
	node.logger.Printf("Updated 2-hop neighbours for all nodes")
}

// JoinImplementation queries bootstrap node and sends a join query
func (node *Node) JoinImplementation(bootstrapAddr string) error {
	// Query the bootstrap node for info
	node.bootstrapIP = bootstrapAddr

	bootstrapConn, err := grpc.NewClient(
		bootstrapAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(LoggingUnaryClientInterceptor(node.logger)),
	)
	if err != nil {
		return err
	}

	defer bootstrapConn.Close()
	bootstrapServiceClient := pb.NewBootstrapServiceClient(bootstrapConn)

	joinInfo, err := bootstrapServiceClient.JoinInfo(
		context.Background(),
		&pb.JoinInfoRequest{
			Address: node.IPAddress,
		},
	)
	if err != nil {
		return err
	}

	// Initialise node info
	dims := uint(joinInfo.Dimensions)
	numHashes := uint(joinInfo.NumHashes)
	node.mu.Lock()
	node.RoutingTable = routing.NewRoutingTable(dims, numHashes)
	node.Info = &topology.NodeInfo{
		NodeId:    joinInfo.NodeId,
		IpAddress: node.IPAddress,
		Zone:      topology.NewZone(dims),
	}
	node.mu.Unlock()

	// Set up logger file and open file with node.logger
	logDir := "./logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	logFilePath := logDir + "/" + node.Info.NodeId + ".log"
	file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	node.logger.SetOutput(file)
	node.logger.SetFormatter(&logrus.TextFormatter{
		DisableColors:   true,
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05.000000",
	})

	// If no CAN nodes in network, take whole zone
	if len(joinInfo.ActiveNodes) == 0 {
		return nil
	}

	// Send Bootstrap request to one of the CAN nodes
	randIndex := rand.Intn(len(joinInfo.ActiveNodes))
	randCoords := GetRandomCoordinates(dims) // Joining point P
	conn, err := node.getClientConn(joinInfo.ActiveNodes[randIndex])
	if err != nil {
		return err
	}

	canServiceClient := pb.NewCANNodeClient(conn)
	log.Printf("HALLO0")
	joinResponse, err := canServiceClient.Join(
		context.Background(),
		&pb.JoinRequest{
			Coordinates: randCoords,
			NodeId:      joinInfo.NodeId,
			Address:     node.IPAddress,
		},
	)
	log.Printf("HALLO1")
	if err != nil {
		return err
	}

	log.Printf("HALLO2")

	// use join response to update node info
	node.mu.Lock()
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
	node.mu.Unlock()

	// Notify all neighbors about our existence
	if err = node.NotifyNeighbors(); err != nil {
		node.logger.Printf("Warning: Failed to notify some neighbors: %v", err)
	}

	// Log the join event
	node.logger.Printf("Node %s joined the network with zone %v", node.Info.NodeId, node.Info.Zone)

	return nil
}
