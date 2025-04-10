package dht

import (
	"context"
	"fmt"
	"github.com/Arnav-Negi/can/internal/topology"
	"github.com/Arnav-Negi/can/internal/utils"
	pb "github.com/Arnav-Negi/can/protofiles"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"log"
	"net"
)

func (node *Node) StartGRPCServer(port int) error {
	ip, err := utils.GetIPAddress()
	if err != nil {
		return fmt.Errorf("failed to get IP address: %v", err)
	}

	// Start the gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))

	// extract IP address from the listener
	node.IPAddress = lis.Addr().String()

	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterCANNodeServer(s, node)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
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
		closestNodes := node.RoutingTable.GetNodesSorted(coords, 3)
		for _, closestNode := range closestNodes {
			canConn, err := grpc.NewClient(closestNode.IpAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

	// Update neighbors
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

	return &pb.JoinResponse{
		AssignedZone:    zoneToProto(newZone),
		Neighbors:       pbNeighbors,
		TransferredData: pbKeyValuePairs,
	}, nil
}

// Helper function to convert Zone to proto message
func zoneToProto(zone topology.Zone) *pb.Zone {
	return &pb.Zone{
		MinCoordinates: zone.GetCoordMins(),
		MaxCoordinates: zone.GetCoordMaxs(),
	}
}

func (node *Node) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (node *Node) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Put not implemented")
}
