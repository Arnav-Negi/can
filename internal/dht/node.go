package dht

import (
	"context"
	"github.com/Arnav-Negi/can/internal/routing"
	"github.com/Arnav-Negi/can/internal/store"
	"github.com/Arnav-Negi/can/internal/topology"
	"github.com/Arnav-Negi/can/internal/utils"
	pb "github.com/Arnav-Negi/can/protofiles"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"math/rand"
	"strconv"
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
func NewNode(port int) (*Node, error) {
	ip, err := utils.GetIPAddress()
	if err != nil {
		return nil, err
	}

	ipAddress := ip + ":" + strconv.Itoa(port)

	return &Node{
		IPAddress:    ipAddress,
		KVStore:      store.NewMemoryStore(),
		RoutingTable: nil,
	}, nil
}

func GetRandomCoordinates(dims uint) []float32 {
	coords := make([]float32, dims)
	for i := 0; i < int(dims); i++ {
		coords[i] = rand.Float32()
	}
	return coords
}

// Join queries bootstrap node and sends a join query
func (node *Node) Join(bootstrapAddr string) error {
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

	// Send Join request to one of the CAN nodes
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
