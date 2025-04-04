package dht

import (
	"context"
	"github.com/Arnav-Negi/can/internal/routing"
	"github.com/Arnav-Negi/can/internal/store"
	"github.com/Arnav-Negi/can/internal/topology"
	pb "github.com/Arnav-Negi/can/protofiles"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"math/rand"
	"sync"
)

type Node struct {
	pb.UnimplementedCANNodeServer
	IPAddress    string
	Info         *topology.NodeInfo
	KVStore      *store.MemoryStore
	RoutingTable *routing.RoutingTable
	mu           sync.RWMutex
}

// NewNode This function initializes a new Node instance.
func NewNode() *Node {
	ipAddress := "localhost:0"

	return &Node{
		IPAddress:    ipAddress,
		KVStore:      store.NewMemoryStore(),
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

	// Initialise node info
	node.RoutingTable = routing.NewRoutingTable(dims)
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
	randCoords := GetRandomCoordinates(dims)
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

	return nil
}

// PutImplementation This function is used to store a value in the DHT.
// Overwrites the value if the key already exists.
func (node *Node) PutImplementation(key string, value []byte) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	// Find coordinates for the key
	coords := node.RoutingTable.HashFunction.GetCoordinates(key)

	// If in zone, store it locally
	if node.Info.Zone.Contains(coords) {
		// Store the key-value pair in the local store
		node.KVStore.Insert(key, value)
		return nil
	}

	// Need to route
	// Get IPs sorted by distance
	closestNodes := node.RoutingTable.GetNodesSorted(coords, 3)
	if len(closestNodes) == 0 {
		return status.Errorf(codes.NotFound, "No nodes found in the routing table")
	}

	// Send the store request to the closest node
	storeRequest := &pb.PutRequest{
		Key:   key,
		Value: value,
	}
	for _, closestNode := range closestNodes {
		canConn, err := grpc.NewClient(closestNode.IpAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		canServiceClient := pb.NewCANNodeClient(canConn)
		_, err = canServiceClient.Put(context.Background(), storeRequest)
		if err != nil {
			continue
		}
		return nil
	}
	return status.Errorf(codes.Unavailable, "DHT is unavailable")
}

// GetImplementation This function is used to retrieve a value from the DHT.
// Error if the key does not exist or unable to retrieve the value.
func (node *Node) GetImplementation(key string) ([]byte, error) {
	node.mu.RLock()
	defer node.mu.RUnlock()

	// Find coordinates for the key
	coords := node.RoutingTable.HashFunction.GetCoordinates(key)

	// If in zone, retrieve it locally
	if node.Info.Zone.Contains(coords) {
		// Retrieve the key-value pair from the local store
		value, err := node.KVStore.Retrieve(key)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "Key not found")
		}
		return value, nil
	}

	// Need to route
	// Get IPs sorted by distance
	closestNodes := node.RoutingTable.GetNodesSorted(coords, 3)
	if len(closestNodes) == 0 {
		return nil, status.Errorf(codes.NotFound, "No nodes found in the routing table")
	}

	// Send the get request to the closest node
	getRequest := &pb.GetRequest{
		Key: key,
	}
	for _, closestNode := range closestNodes {
		canConn, err := grpc.NewClient(closestNode.IpAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		canServiceClient := pb.NewCANNodeClient(canConn)
		getResponse, err := canServiceClient.Get(context.Background(), getRequest)
		if err == nil {
			return getResponse.Value, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "Key not found in DHT")
}
