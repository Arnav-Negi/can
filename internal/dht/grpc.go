package dht

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Arnav-Negi/can/internal/topology"
	"github.com/Arnav-Negi/can/internal/utils"
	pb "github.com/Arnav-Negi/can/protofiles"
)

func (node *Node) StartGRPCServer(port int) error {
	ip, err := utils.GetIPAddress()
	if err != nil {
		node.logger.Fatalf("failed to get IP address: %v", err)
	}

	// Start the gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))

	// extract IP address from the listener
	node.IPAddress = lis.Addr().String()

	if err != nil {
		node.logger.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer(grpc.UnaryInterceptor(LoggingUnaryServerInterceptor(node.logger)))
	pb.RegisterCANNodeServer(s, node)
	if err := s.Serve(lis); err != nil {
		node.logger.Fatalf("failed to serve: %v", err)
	}
	return nil
}

func (node *Node) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	node.mu.Lock()
	defer node.mu.Unlock()

	// Check if coordinates are in this node's zone
	coords := req.Coordinates
	if !node.Info.Zone.Contains(coords) {
		// Forward to the closest neighbor
		closestNodes := node.RoutingTable.GetNodesSorted(coords, node.Info.Zone, 3)
		for _, closestNode := range closestNodes {
			canConn, err := node.getGRPCConn(closestNode.IpAddress)
			if err != nil {
				continue
			}
			canServiceClient := pb.NewCANNodeClient(canConn)
			joinResponse, err := canServiceClient.Join(context.Background(), req)
			if err == nil {
				return joinResponse, nil
			}
		}
		return nil, status.Errorf(codes.Unavailable, "Could not forward join request")
	}

	// Split the zone and transfer data
	newZone, transferredData, err := node.splitZone(coords)
	if err != nil {
		return nil, err
	}

	// Update neighbors - pass the new node's ID and address
	neighbors := node.updateNeighbors(newZone)

	// Create response
	pbNeighbors := make([]*pb.Node, 0, len(neighbors))
	for _, neighbor := range neighbors {
		pbNeighbors = append(pbNeighbors, &pb.Node{
			NodeId:  neighbor.NodeId,
			Address: neighbor.IpAddress,
			Zone:    zoneToProto(neighbor.Zone),
		})
	}

	pbKeyValuePairs := make([]*pb.KeyValuePair, 0, len(transferredData))
	for key, value := range transferredData {
		pbKeyValuePairs = append(pbKeyValuePairs, &pb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	// Log the join event
	node.logger.Printf("Node %s joined the network with zone: %v", node.IPAddress, newZone)
	node.logger.Printf("Updated neighbors: %v", pbNeighbors)
	node.logger.Printf("Current zone: %v", node.Info.Zone)

	return &pb.JoinResponse{
		AssignedZone:    zoneToProto(newZone),
		Neighbors:       pbNeighbors,
		TransferredData: pbKeyValuePairs,
	}, nil
}

// AddNeighbor handles requests to add a node as a neighbor
func (node *Node) AddNeighbor(ctx context.Context, req *pb.AddNeighborRequest) (*pb.AddNeighborResponse, error) {
	node.mu.Lock()
	defer node.mu.Unlock()

	if req.Neighbor == nil {
		return &pb.AddNeighborResponse{Success: false}, status.Errorf(codes.InvalidArgument, "Neighbor information is missing")
	}

	// Convert proto node to topology node
	neighborZone := topology.NewZoneFromProto(req.Neighbor.Zone)
	neighborInfo := topology.NodeInfo{
		NodeId:    req.Neighbor.NodeId,
		IpAddress: req.Neighbor.Address,
		Zone:      neighborZone,
	}

	// Check if zones are adjacent before adding
	if !node.Info.Zone.IsAdjacent(neighborZone) {
		return &pb.AddNeighborResponse{Success: false}, nil
	}

	// Add the node to our routing table
	node.RoutingTable.AddNode(neighborInfo)

	node.logger.Printf("Added neighbor: %s with zone: %v", req.Neighbor.Address, neighborZone)

	return &pb.AddNeighborResponse{Success: true}, nil
}

// Helper function to convert Zone to proto message
func zoneToProto(zone topology.Zone) *pb.Zone {
	return &pb.Zone{
		MinCoordinates: zone.GetCoordMins(),
		MaxCoordinates: zone.GetCoordMaxs(),
	}
}

// Helper function to convert NodeInfo to proto message
func NodeInfoToProto(nodeInfo topology.NodeInfo) *pb.Node {
	return &pb.Node{
		NodeId:  nodeInfo.NodeId,
		Address: nodeInfo.IpAddress,
		Zone:    zoneToProto(nodeInfo.Zone),
	}
}

func (node *Node) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, err := node.GetImplementation(req.Key, int(req.HashToUse))
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{Value: value}, nil
}

func (node *Node) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	err := node.PutImplementation(req.Key, req.Value, int(req.HashToUse))
	if err != nil {
		return &pb.PutResponse{Success: false}, err
	}
	return &pb.PutResponse{Success: true}, nil
}

func (node *Node) SendNeighbourInfo(ctx context.Context, req *pb.NeighbourInfoRequest) (*pb.NeighbourInfoResponse, error) {
	node.mu.Lock()
	defer node.mu.Unlock()

	// Update the neighbour info
	neighbourInfo := make([]topology.NodeInfo, len(req.Neighbours))
	for i, n := range req.Neighbours {
		neighbourInfo[i] = topology.NodeInfo{
			NodeId:    n.NodeId,
			IpAddress: n.Address,
			Zone:      topology.NewZoneFromProto(n.Zone),
		}
	}
	node.NeighInfo = neighbourInfo

	return &pb.NeighbourInfoResponse{Success: true}, nil
}

func (node *Node) getClientConn(ip string) (*grpc.ClientConn, error) {
	// Check if the connection already exists
	node.mu.RLock()
	conn, exists := node.conns[ip]
	node.mu.RUnlock()
	if exists {
		return conn, nil
	}

	// Create a new connection
	conn, err := node.getGRPCConn(ip)
	if err != nil {
		return nil, status.Error(codes.Unavailable, fmt.Sprintf("Failed to connect to %s: %v", ip, err))
	}

	// Store the connection in the map
	node.mu.Lock()
	node.conns[ip] = conn
	node.mu.Unlock()

	// Return the connection
	return conn, nil
}
