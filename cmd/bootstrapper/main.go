package main

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"sync"
	"time"

	pb "github.com/Arnav-Negi/can/protofiles"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
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

// GetActiveNodes implements the GetActiveNodes RPC method
func (s *BootstrapServer) GetRootCApem(ctx context.Context, req *emptypb.Empty) (*pb.RootCApemResponse, error) {
	log.Printf("Received request for root CA PEM")
	
	rootCAPem, err := os.ReadFile("certs/ca-cert.pem")
	if err != nil {
		return nil, err
	}
	return &pb.RootCApemResponse{
		RootCaPem: rootCAPem,
	}, nil
}

// SignCSR implements the SignCSR RPC method
func (s *BootstrapServer) SignCSR(ctx context.Context, req *pb.SignCSRRequest) (*pb.SignCSRResponse, error) {
	// Load CA key and cert
	caCertPEM, err := os.ReadFile("certs/ca-cert.pem")
	if err != nil {
		return nil, err
	}
	caKeyPEM, err := os.ReadFile("certs/ca-key.pem")
	if err != nil {
		return nil, err
	}

	caCertBlock, _ := pem.Decode(caCertPEM)
	caKeyBlock, _ := pem.Decode(caKeyPEM)

	caCert, err := x509.ParseCertificate(caCertBlock.Bytes)
	if err != nil {
		return nil, err
	}

	// Try parsing as PKCS1, fallback to PKCS8
	var caKey any
	caKey, err = x509.ParsePKCS1PrivateKey(caKeyBlock.Bytes)
	if err != nil {
		caKey, err = x509.ParsePKCS8PrivateKey(caKeyBlock.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing CA key failed: %v", err)
		}
	}

	// Decode the CSR
	csrBlock, _ := pem.Decode(req.CsrPem)
	if csrBlock == nil {
		return nil, fmt.Errorf("failed to decode CSR PEM")
	}
	csr, err := x509.ParseCertificateRequest(csrBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("invalid CSR: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("CSR signature invalid: %w", err)
	}

	// Parse IP SAN if provided
	var ipSANs []net.IP
	if req.IpSan != "" {
		ip := net.ParseIP(req.IpSan)
		if ip == nil {
			return nil, fmt.Errorf("invalid IP SAN: %s", req.IpSan)
		}
		ipSANs = append(ipSANs, ip)
	}

	// Sign the CSR
	certTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      csr.Subject,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		IPAddresses:  ipSANs,
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, certTemplate, caCert, csr.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("signing failed: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	return &pb.SignCSRResponse{SignedCertPem: certPEM}, nil
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
	numHashes := flag.Uint("num_hashes", defaultNumHashes, "Number of hash functions")
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
	server := NewBootstrapServer(uint32(*dimensions), uint32(*numHashes))
	pb.RegisterBootstrapServiceServer(grpcServer, server)

	log.Printf("Bootstrap server started on port %d", *port)
	log.Printf("CAN dimensions: %d", server.dimensions)

	// Start serving requests
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
