package dht

import (
	"context"
	"log"
	"time"

	pb "github.com/Arnav-Negi/can/protofiles"
)

func (node *Node) HeartbeatRoutine() {
	// Start a goroutine to send heartbeat messages periodically
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		node.mu.RLock()
		neighbours := node.RoutingTable.Neighbours 
		node.mu.RUnlock()

		for _, neighbour := range neighbours {
			// Get the client connection to the neighbour
			conn, err := node.getClientConn(neighbour.IpAddress)
			if err != nil {
				log.Printf("Failed to connect to %s: %v", neighbour.IpAddress, err)
				continue
			}
			client := pb.NewCANNodeClient(conn)
		
			// Send heartbeat request
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err = client.Heartbeat(ctx, &pb.HeartbeatRequest{
				Address: node.IPAddress,
			})
			cancel()
			if err != nil {
				log.Printf("Failed to send heartbeat: %v", err)
			}
		}

		node.CleanupStaleConnections()
	}
}

func (node *Node) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	node.mu.Lock()
	defer node.mu.Unlock()

	// Update the last heartbeat time for this node
	node.lastHeartbeat[req.Address] = time.Now()

	return &pb.HeartbeatResponse{
		Success: true,
	}, nil
}

func (node *Node) CleanupStaleConnections() {
	node.mu.Lock()
	defer node.mu.Unlock()

	// Check for stale connections and remove them
	for address, lastHeartbeat := range node.lastHeartbeat {
		if time.Since(lastHeartbeat) > 10*time.Second { // TODO : Make this configurable
			delete(node.conns, address)
			delete(node.lastHeartbeat, address)
			log.Printf("Removed stale connection to %s", address)
		}
	}
}