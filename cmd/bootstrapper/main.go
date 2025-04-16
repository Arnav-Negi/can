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
	defaultNumHashes  = 3    // Default number of hash functions
)

// BootstrapServer implements the BootstrapService gRPC service
type BootstrapServer struct {
	pb.UnimplementedBootstrapServiceServer
	dimensions uint32
	numHashes  uint32

	// Track active nodes in the network (IP addresses)
	activeNodes []string

	// Mutex for synchronizing access to activeNodes
	mu sync.RWMutex
}

// NewBootstrapServer creates a new bootstrap server
func NewBootstrapServer(dimensions uint32, numHashes uint32) *BootstrapServer {
	return &BootstrapServer{
		dimensions:  dimensions,
		numHashes:   numHashes,
		activeNodes: make([]string, 0),
		mu:          sync.RWMutex{},
	}
}

// JoinInfo implements the JoinInfo RPC method
func (s *BootstrapServer) JoinInfo(ctx context.Context, req *pb.JoinInfoRequest) (*pb.JoinInfoResponse, error) {
	log.Printf("Received join request from: %s", req.Address)

	// Create a response with the CAN network dimensions, server's node ID,
	// and the list of active nodes
	response := &pb.JoinInfoResponse{
		Dimensions:  s.dimensions,
		NumHashes:   s.numHashes,
		NodeId:      uuid.New().String(),
		ActiveNodes: s.getActiveNodes(),
	}

	// Add the requesting node to active nodes if not already present
	s.addNode(req.Address)

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

func PrintAllIPs() {
	ifaces, err := net.Interfaces()
	if err != nil {
		return
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue // ignore down or loopback interfaces
		}
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip != nil && ip.To4() != nil {
				log.Printf("Found IP: %s", ip)
			}
		}
	}
}

func main() {
	// Parse command line arguments
	port := flag.Int("port", defaultPort, "Server port")
	dimensions := flag.Uint("dim", defaultDimensions, "CAN dimensions")
	numHashes := flag.Uint("num_hashes", defaultNumHashes, "Number of hash functions")
	flag.Parse()

	if *dimensions <= 0 {
		log.Fatalf("Invalid CAN dimensions: %d", *dimensions)
	}

	// Create a listener on the specified port
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Create a new gRPC server
	grpcServer := grpc.NewServer()

	// Create and register the bootstrap server
	server := NewBootstrapServer(uint32(*dimensions), uint32(*numHashes))
	pb.RegisterBootstrapServiceServer(grpcServer, server)

	// print all IPs to be accessed
	PrintAllIPs()
	log.Printf("Port: %d", *port)
	log.Printf("CAN dimensions: %d", server.dimensions)

	// Start serving requests
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
