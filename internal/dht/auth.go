package dht

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/Arnav-Negi/can/protofiles"
)

// The contents of the gen.sh script as a string
const certGenScript = `
#!/bin/bash
set -e

CERT_DIR=$1

mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

rm -f *.pem *.csr *.srl

openssl req -new -newkey rsa:2048 -nodes \
  -keyout node-key.pem \
  -out node.csr \
  -subj "/CN=$(hostname -f)"

echo "Generated node-key.pem and node.csr in $CERT_DIR"
`

// Get the TLS config
func LoadTLSCredentials(ipAddress string) (credentials.TransportCredentials, error) {
	certDir := fmt.Sprintf("certs/node-%s", ipAddress)

	rootCA, err := os.ReadFile("certs/root/root-cert.pem")
	if err != nil {
		return nil, err
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(rootCA) {
		return nil, fmt.Errorf("failed to append client certs")
	}

	serverCert, err := tls.LoadX509KeyPair(
		certDir+"/node-cert.pem",
		certDir+"/node-key.pem",
	)
	if err != nil {
		return nil, err
	}

	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    certPool,
		RootCAs:      certPool,
	}), nil
}

func SetupNodeTLS(bootstrapClient pb.BootstrapServiceClient, ip string, port int) error {
	// Construct node-specific cert directory
	certDir := fmt.Sprintf("certs/node-%s:%d", ip, port)
	if err := os.MkdirAll(certDir, 0755); err != nil {
		return fmt.Errorf("make cert dir: %w", err)
	}

	// Fetch root CA and write to shared root folder
	rootResp, err := bootstrapClient.GetRootCApem(context.Background(), &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("fetch root CA: %w", err)
	}
	if err := os.MkdirAll("certs/root", 0755); err != nil {
		return fmt.Errorf("make root cert dir: %w", err)
	}
	if err := os.WriteFile("certs/root/root-cert.pem", rootResp.RootCaPem, 0644); err != nil {
		return fmt.Errorf("write root CA: %w", err)
	}

	// Create a temporary shell script file
	tmpFile, err := os.CreateTemp("", "gen_*.sh")
	if err != nil {
		return fmt.Errorf("create temp file for gen.sh: %w", err)
	}

	// Write the embedded script to the temp file, ensuring Unix line endings
	script := strings.ReplaceAll(certGenScript, "\r\n", "\n")
	if _, err := tmpFile.WriteString(script); err != nil {
		tmpFile.Close()
		return fmt.Errorf("write embedded gen.sh: %w", err)
	}

	// Close the file before making it executable
	tmpFileName := tmpFile.Name()
	tmpFile.Close()

	// Make the temp file executable
	if err := os.Chmod(tmpFileName, 0755); err != nil {
		return fmt.Errorf("make temp script executable: %w", err)
	}

	// Call the shell script explicitly with bash
	cmd := exec.Command("bash", tmpFileName, certDir)
	log.Printf("Running gen.sh script...\n")

	// Capture the output of the command for debugging
	output, err := cmd.CombinedOutput() // This captures both stdout and stderr
	if err != nil {
		log.Printf("Error running gen.sh: %v", err)
		log.Printf("Script output: %s", output)
		return fmt.Errorf("failed to run gen.sh: %w", err)
	}

	// Read CSR from generated certDir
	csrPEM, err := os.ReadFile(filepath.Join(certDir, "node.csr"))
	if err != nil {
		return fmt.Errorf("read CSR: %w", err)
	}

	// Send CSR to bootstrapper
	signedResp, err := bootstrapClient.SignCSR(context.Background(), &pb.SignCSRRequest{
		CsrPem: csrPEM,
		IpSan:  ip,
	})
	if err != nil {
		return fmt.Errorf("sign CSR: %w", err)
	}

	// Save signed certificate
	if err := os.WriteFile(filepath.Join(certDir, "node-cert.pem"), signedResp.SignedCertPem, 0644); err != nil {
		return fmt.Errorf("write signed cert: %w", err)
	}

	log.Printf("TLS certs set up for %s:%d in %s\n", ip, port, certDir)
	return nil
}
