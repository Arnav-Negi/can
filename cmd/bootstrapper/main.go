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
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	defaultPort       = 5000 // Default port for the bootstrap server
	defaultDimensions = 2    // Default CAN dimensions
	defaultNumHashes  = 3    // Default number of hash functions
	defaultIp         = "0.0.0.0"
)

type IDService struct {
	mu      sync.Mutex
	counter int
}

func newIDService() *IDService {
	return &IDService{
		mu:      sync.Mutex{},
		counter: 0,
	}
}

func (idService *IDService) NewId() string {
	idService.mu.Lock()
	idService.counter++
	idService.mu.Unlock()
	return fmt.Sprintf("node-%d", idService.counter)
}

// BootstrapServer implements the BootstrapService gRPC service
type BootstrapServer struct {
	pb.UnimplementedBootstrapServiceServer
	dimensions uint32
	numHashes  uint32

	// Track active nodes in the network (IP addresses)
	activeNodes []string

	// Mutex for synchronizing access to activeNodes
	mu sync.RWMutex

	// id service to generate IDs
	idService *IDService
}

// NewBootstrapServer creates a new bootstrap server
func NewBootstrapServer(dimensions uint32, numHashes uint32) *BootstrapServer {
	return &BootstrapServer{
		dimensions:  dimensions,
		numHashes:   numHashes,
		activeNodes: make([]string, 0),
		mu:          sync.RWMutex{},
		idService:   newIDService(),
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
		NodeId:      s.idService.NewId(),
		ActiveNodes: s.getActiveNodes(),
	}

	// Add the requesting node to active nodes if not already present
	s.addNode(req.Address)

	return response, nil
}

// Leave implements the Leave RPC
func (s *BootstrapServer) Leave(ctx context.Context, req *pb.BootstrapLeaveInfo) (*pb.BootstrapLeaveResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nodeIP := req.NodeAddress
	log.Printf("Node with IP %s leaving the CAN", nodeIP)
	for i, node := range s.activeNodes {
		if nodeIP == node {
			s.activeNodes = append(s.activeNodes[:i], s.activeNodes[i+1:]...)
		}
	}
	return &pb.BootstrapLeaveResponse{}, nil
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
	// Replace localhost with 127.0.0.1 if present
	var ipSANs []net.IP
	ipStr := req.IpSan
	if ipStr == "localhost" {
		log.Printf("IP SAN: %s", ipStr)
		ipStr = "127.0.0.1"
	}
	if ipStr != "" {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			return nil, fmt.Errorf("invalid IP SAN: %q", ipStr)
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
	ip := flag.String("ip", defaultIp, "IP address")
	flag.Parse()

	if *dimensions <= 0 {
		log.Fatalf("Invalid CAN dimensions: %d", *dimensions)
	}

	// Create a listener on the specified port
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *ip, *port))
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
	log.Printf("IP: %s, Port: %d", *ip, *port)
	log.Printf("CAN dimensions: %d", server.dimensions)

	// Start serving requests
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
