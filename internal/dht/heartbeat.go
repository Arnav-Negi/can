package dht

import (
	"context"
	"sync"
	"time"

	"github.com/Arnav-Negi/can/internal/topology"
	pb "github.com/Arnav-Negi/can/protofiles"
)

func (node *Node) HeartbeatRoutine() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// Initially, send 2-hop info to all neighbours
	node.NotifyAllNeighboursOfTwoHopInfo()

	for range ticker.C {
		node.mu.RLock()
		neighbours := node.RoutingTable.Neighbours
		node.mu.RUnlock()

		node.NotifyAllNeighboursOfTwoHopInfo()

		var wg sync.WaitGroup
		for _, neighbour := range neighbours {
			wg.Add(1)
			go func(nbr topology.NodeInfo) {
				defer wg.Done()

				conn, err := node.getClientConn(nbr.IpAddress)
				if err != nil {
					node.logger.Printf("Failed to connect to %s: %v", nbr.IpAddress, err)
					return
				}
				client := pb.NewCANNodeClient(conn)

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				_, err = client.Heartbeat(ctx, &pb.HeartbeatRequest{
					Address: node.IPAddress,
				})
				if err != nil {
					node.logger.Printf("Failed to send heartbeat to %s: %v", nbr.IpAddress, err)
				}
			}(neighbour)
		}

		wg.Wait() // Wait for all heartbeats to complete

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
	dirty := false
	crashedNodes := make([]struct {
		id      string
		address string
	}, 0)

	// Check for stale connections and remove them
	for address, lastHeartbeat := range node.lastHeartbeat {
		if time.Since(lastHeartbeat) > 10*time.Second { // TODO : Make this configurable
			// Find the nodeId for this address
			var nodeId string
			for _, neighbor := range node.RoutingTable.Neighbours {
				if neighbor.IpAddress == address {
					nodeId = neighbor.NodeId
					break
				}
			}

			delete(node.conns, address)
			delete(node.lastHeartbeat, address)
			node.RoutingTable.RemoveNeighbor(address)

			if nodeId != "" {
				crashedNodes = append(crashedNodes, struct {
					id      string
					address string
				}{id: nodeId, address: address})
			}

			node.logger.Printf("Removed stale connection to %s", address)
			dirty = true
		}
	}
	node.mu.Unlock()

	// Handle crashed nodes
	//for _, crashed := range crashedNodes {
	//	go node.HandleCrashDetection(crashed.id, crashed.address)
	//}

	if dirty {
		node.NotifyAllNeighboursOfTwoHopInfo()
	}
}
