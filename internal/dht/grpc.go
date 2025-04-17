package dht

import (
	"context"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Arnav-Negi/can/internal/topology"
	pb "github.com/Arnav-Negi/can/protofiles"
)

func (node *Node) StartGRPCServer(ip string, port int, bootstrapAddr string) error {
	// Make temp connection to boostrapper,
	// get the root CA and then start server using that
	bootstrapConn, err := grpc.NewClient(
		bootstrapAddr, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(LoggingUnaryClientInterceptor(node.logger)),
	)
	if err != nil { return err }

	bootstrapClient := pb.NewBootstrapServiceClient(bootstrapConn)
	if err := SetupNodeTLS(bootstrapClient, ip); err != nil {
		return fmt.Errorf("TLS setup failed: %w", err)
	}
	bootstrapConn.Close()

	// Setup for starting gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		node.logger.Fatalf("failed to listen: %v", err)
	}
	node.logger.Printf("Listening on IP %s", lis.Addr().String())

	// if port was given 0, it was selected randomly,
	port = lis.Addr().(*net.TCPAddr).Port

	// Start the gRPC server and
	// extract IP address from the listener
	node.IPAddress = fmt.Sprintf("%s:%d", ip, port)

	// Load TLS creds
	tlsCreds, err := LoadTLSCredentials()
	log.Printf("Loaded TLS credentials")
	if err != nil { 
		node.logger.Fatalf("failed to load TLS credentials: %v", err)
		return err 
	}

	// Start serving with creds
	s := grpc.NewServer(
		grpc.Creds(tlsCreds), 
		grpc.UnaryInterceptor(LoggingUnaryServerInterceptor(node.logger)),
	)
	pb.RegisterCANNodeServer(s, node)
	if err := s.Serve(lis); err != nil {
		node.logger.Fatalf("failed to serve: %v", err)
	}

	return nil
}

func (node *Node) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
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

	node.mu.Lock()
	defer node.mu.Unlock()
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
	node.logger.Printf("Node %s joined the network with zone: %v", req.Address, newZone)
	node.logger.Printf("Updated neighbors: %v", pbNeighbors)
	node.logger.Printf("Current zone: %v", node.Info.Zone)
	node.logger.Printf("My neighbors: %v", node.RoutingTable.Neighbours)

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

	// Iterate through existing neighbors to check if the node is already present
	// If present already, remove it
	for i, n := range node.RoutingTable.Neighbours {
		if n.IpAddress == req.Neighbor.Address {
			node.RoutingTable.Neighbours = append(node.RoutingTable.Neighbours[:i], node.RoutingTable.Neighbours[i+1:]...)
			break
		}
	}

	// Check if zones are adjacent before adding
	if !node.Info.Zone.IsAdjacent(neighborZone) {
		return &pb.AddNeighborResponse{Success: false}, nil
	}

	// Add the node to our routing table
	node.RoutingTable.AddNode(neighborInfo)

	node.logger.Printf("Added/Updated neighbor: %s with zone: %v", req.Neighbor.Address, neighborZone)
	node.logger.Printf("Current neighbors: %v", node.RoutingTable.Neighbours)

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

func (node *Node) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	err := node.DeleteImplementation(req.Key, int(req.HashToUse))
	if err != nil { 
		return &pb.DeleteResponse{Success: false}, err 
	}
	return &pb.DeleteResponse{Success: true}, nil
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
	if exists { return conn, nil }

	// Create a new connection
	conn, err := node.getGRPCConn(ip)
	if err != nil {
		return nil, status.Error(
			codes.Unavailable, 
			fmt.Sprintf("Failed to connect to %s: %v", ip, err),
		)
	}

	// Store the connection in the map
	node.mu.Lock()
	node.conns[ip] = conn
	node.mu.Unlock()

	// Return the connection
	return conn, nil
}
