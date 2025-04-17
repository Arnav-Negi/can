package dht

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"os/exec"

	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/Arnav-Negi/can/protofiles"
)

// Get the TLS config
func LoadTLSCredentials() (credentials.TransportCredentials, error) {
	// Root CA handling
	rootCA, err := os.ReadFile("certs/root/root-cert.pem")
	if err != nil {
		return nil, err
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(rootCA) {
		return nil, fmt.Errorf("failed to append client certs")
	}
	
	// Client certificate handling
	serverCert, err := tls.LoadX509KeyPair(
		"certs/node/node-cert.pem",
		"certs/node/node-key.pem",
	)
	if err != nil {
		return nil, err
	}

	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs: certPool,
		RootCAs: certPool,
	}), nil
}

func SetupNodeTLS(bootstrapClient pb.BootstrapServiceClient, ip string) error {
	// 1. Fetch root CA
	rootResp, err := bootstrapClient.GetRootCApem(context.Background(), &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("fetch root CA: %w", err)
	}
	if err := os.WriteFile("certs/root/root-cert.pem", rootResp.RootCaPem, 0644); err != nil {
		return fmt.Errorf("write root CA: %w", err)
	}

	// 2. Generate CSR externally via gen.sh
	cmd := exec.Command("certs/node/gen.sh")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to execute gen.sh: %w", err)
	}

	// 3. Read CSR from file
	csrPEM, err := os.ReadFile("certs/node/node.csr")
	if err != nil {
		return fmt.Errorf("read CSR: %w", err)
	}

	// 4. Send CSR to bootstrapper
	signedResp, err := bootstrapClient.SignCSR(context.Background(), &pb.SignCSRRequest{
		CsrPem: csrPEM,
		IpSan:     ip,
	})
	if err != nil {
		return fmt.Errorf("sign CSR: %w", err)
	}

	// 5. Save signed certificate
	if err := os.WriteFile("certs/node/node-cert.pem", signedResp.SignedCertPem, 0644); err != nil {
		return fmt.Errorf("write signed cert: %w", err)
	}

	log.Println("TLS certs set up")
	return nil
}
