package main

import (
	"context"
	"flag"
	"fmt"
	pb "github.com/Arnav-Negi/can/protofiles"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
)

const (
	defaultPort       = 5000 // Default port for the bootstrap server
	defaultDimensions = 2    // Default CAN dimensions
)

// BootstrapServer implements the BootstrapService gRPC service
type BootstrapServer struct {
	pb.UnimplementedBootstrapServiceServer
	dimensions uint32

	// Track active nodes in the network (IP addresses)
	activeNodes []string

	// Mutex for synchronizing access to activeNodes
	mu sync.RWMutex
}

// NewBootstrapServer creates a new bootstrap server
func NewBootstrapServer(dimensions uint32) *BootstrapServer {
	return &BootstrapServer{
		dimensions:  dimensions,
		activeNodes: make([]string, 0),
		mu:          sync.RWMutex{},
	}
}

// JoinQuery implements the JoinQuery RPC method
func (s *BootstrapServer) JoinQuery(ctx context.Context, req *pb.JoinQueryRequest) (*pb.JoinQueryResponse, error) {
	log.Printf("Received join request from: %s", req.Address)

	// Add the requesting node to active nodes if not already present
	s.addNode(req.Address)

	// Create a response with the CAN network dimensions, server's node ID,
	// and the list of active nodes
	response := &pb.JoinQueryResponse{
		NodeId:      uuid.New().String(),
		Dimensions:  s.dimensions,
		ActiveNodes: s.getActiveNodes(),
	}

	return response, nil
}

// addNode adds a node to the list of active nodes if not already present
func (s *BootstrapServer) addNode(address string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if the node is already in the list
	for _, node := range s.activeNodes {
		if node == address {
			return
		}
	}

	// Add the new node
	s.activeNodes = append(s.activeNodes, address)
	log.Printf("Added new node: %s, total nodes: %d", address, len(s.activeNodes))
}

// getActiveNodes returns a copy of the active nodes list
func (s *BootstrapServer) getActiveNodes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create a copy of the active nodes slice
	nodes := make([]string, len(s.activeNodes))
	copy(nodes, s.activeNodes)

	return nodes
}

func main() {
	// Parse command line arguments
	port := flag.Int("port", defaultPort, "Server port")
	dimensions := flag.Uint("dim", defaultDimensions, "CAN dimensions")
	flag.Parse()

	if *dimensions <= 0 {
		log.Fatalf("Invalid CAN dimensions: %d", *dimensions)
	}

	// Create a listener on the specified port
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Create a new gRPC server
	grpcServer := grpc.NewServer()

	// Create and register the bootstrap server
	server := NewBootstrapServer(uint32(*dimensions))
	pb.RegisterBootstrapServiceServer(grpcServer, server)

	log.Printf("Bootstrap server started on port %d", *port)
	log.Printf("CAN dimensions: %d", server.dimensions)

	// Start serving requests
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
