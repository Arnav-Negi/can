package dht

import (
	"context"
	"fmt"
	"github.com/Arnav-Negi/can/internal/utils"
	pb "github.com/Arnav-Negi/can/protofiles"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	return nil, status.Errorf(codes.Unimplemented, "method Bootstrap not implemented")
}
func (node *Node) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (node *Node) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Put not implemented")
}
