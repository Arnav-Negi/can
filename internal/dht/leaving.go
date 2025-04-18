package dht

import (
	"context"
	"fmt"
	"github.com/Arnav-Negi/can/internal/topology"
	pb "github.com/Arnav-Negi/can/protofiles"
	"sort"
	"sync"
	//"google.golang.org/grpc/codes"
	//"google.golang.org/grpc/status"
)

// LeaveImplementation handles the graceful leaving of a node from the CAN network
func (node *Node) LeaveImplementation() error {
	node.logger.Printf("Node %s is leaving the network", node.Info.NodeId)
	node.logger.Printf("My current neighbors: %v", node.RoutingTable.Neighbours)

	// Find the smallest neighbor by volume
	smallestNeighbor, err := node.findSmallestNeighbor()
	if err != nil {
		return err
	}

	// Create connection to the smallest neighbor
	conn, err := node.getGRPCConn(smallestNeighbor.IpAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to smallest neighbor: %v", err)
	}
	client := pb.NewCANNodeClient(conn)

	// Send leave request to the smallest neighbor
	leaveResponse, err := client.InitiateLeave(context.Background(), &pb.LeaveRequest{
		LeavingNodeId: node.Info.NodeId,
		LeavingZone:   zoneToProto(node.Info.Zone),
	})
	if err != nil {
		return fmt.Errorf("failed to initiate leave process: %v", err)
	}

	// Wait for takeover to complete
	if !leaveResponse.Success {
		return fmt.Errorf("leave process failed: %s", leaveResponse.ErrorMessage)
	}

	// Gracefully shutdown by closing connections
	err = node.closeAllConnections()
	if err != nil {
		return err
	}
	node.logger.Printf("Node %s has successfully left the network", node.Info.NodeId)
	return nil
}

// findSmallestNeighbor returns the smallest neighbor by volume
func (node *Node) findSmallestNeighbor() (topology.NodeInfo, error) {
	node.mu.RLock()
	defer node.mu.RUnlock()

	if len(node.RoutingTable.Neighbours) == 0 {
		return topology.NodeInfo{}, fmt.Errorf("no neighbors available")
	}

	// Calculate volume for each neighbor
	type neighborWithVolume struct {
		info   topology.NodeInfo
		volume float32
	}

	neighbors := make([]neighborWithVolume, 0, len(node.RoutingTable.Neighbours))
	for _, neighbor := range node.RoutingTable.Neighbours {
		volume := neighbor.Zone.CalculateVolume()
		neighbors = append(neighbors, neighborWithVolume{
			info:   neighbor,
			volume: volume,
		})
	}

	// Sort by volume, with NodeId as tiebreaker
	sort.Slice(neighbors, func(i, j int) bool {
		if neighbors[i].volume == neighbors[j].volume {
			return neighbors[i].info.NodeId < neighbors[j].info.NodeId
		}
		return neighbors[i].volume < neighbors[j].volume
	})

	minVolume := neighbors[0].volume
	// Iterate through the neighbors which have minVolume as volume
	// If any of them is a sibling of Node, return that
	for _, neighbor := range neighbors {
		if neighbor.volume == minVolume && node.areSiblings(node.Info.Zone, neighbor.info.Zone) {
			return neighbor.info, nil
		}
	}

	return neighbors[0].info, nil
}

// closeAllConnections closes all gRPC connections
func (node *Node) closeAllConnections() error {
	node.mu.Lock()
	defer node.mu.Unlock()

	for addr, conn := range node.conns {
		err := conn.Close()
		if err != nil {
			return fmt.Errorf("failed to close connection to %s: %v", addr, err)
		}
		delete(node.conns, addr)
	}
	node.logger.Printf("Closed all connections")
	return nil
}

// findTakeoverNodeDFS performs a depth-first search to find a node for takeover
func (node *Node) findTakeoverNodeDFS(leavingZone topology.Zone, leavingNodeId string) (topology.NodeInfo, error) {
	// First check if this node and the leaving node are siblings
	if node.areSiblings(node.Info.Zone, leavingZone) {
		return *node.Info, nil
	}

	// Get the dimension with the shortest span (the last dimension that was split)
	splitDim := node.findLastSplitDimension(node.Info.Zone)

	// Find a neighbor along the split dimension
	for _, neighbor := range node.RoutingTable.Neighbours {
		// Skip the leaving node
		if neighbor.NodeId == leavingNodeId {
			continue
		}

		// Check if this neighbor abuts the leaving node along the split dimension
		if node.abutsDimension(neighbor.Zone, node.Info.Zone, splitDim) {
			// If this node's zone is smaller, forward the DFS request
			if neighbor.Zone.CalculateVolume() <= node.Info.Zone.CalculateVolume() {
				conn, err := node.getGRPCConn(neighbor.IpAddress)
				if err != nil {
					node.logger.Printf("Failed to connect to neighbor %s: %v", neighbor.NodeId, err)
					continue
				}

				client := pb.NewCANNodeClient(conn)
				response, err := client.PerformDFS(context.Background(), &pb.DFSRequest{
					LeavingNodeId: leavingNodeId,
					ParentZone:    zoneToProto(node.Info.Zone),
					ParentNodeId:  node.Info.NodeId,
				})

				if err == nil && response.FoundSibling {
					return topology.NodeInfo{
						NodeId:    response.TakeoverNodeId,
						IpAddress: response.TakeoverAddress,
						Zone:      topology.NewZoneFromProto(response.TakeoverZone),
					}, nil
				}
			}
		}
	}

	return topology.NodeInfo{}, fmt.Errorf("no suitable takeover node found")
}

// findLastSplitDimension finds the dimension with the shortest span
func (node *Node) findLastSplitDimension(zone topology.Zone) int {
	dims := len(zone.GetCoordMins())
	splitDim := 0
	minSpan := float32(1.1) // Max span in CAN is 1.0

	for i := 0; i < dims; i++ {
		span := zone.GetCoordMaxs()[i] - zone.GetCoordMins()[i]
		if span <= minSpan {
			minSpan = span
			splitDim = i
		}
	}

	return splitDim
}

// areSiblings checks if two zones can be combined (they are siblings)
func (node *Node) areSiblings(zone1, zone2 topology.Zone) bool {
	// Sibling zones must have equal volumes
	if zone1.CalculateVolume() != zone2.CalculateVolume() {
		return false
	}

	dims := len(zone1.GetCoordMins())
	matchingDims := 0
	splitDim := -1

	// Check each dimension
	for i := 0; i < dims; i++ {
		min1 := zone1.GetCoordMins()[i]
		max1 := zone1.GetCoordMaxs()[i]
		min2 := zone2.GetCoordMins()[i]
		max2 := zone2.GetCoordMaxs()[i]

		// If the zones share the same span in this dimension
		if min1 == min2 && max1 == max2 {
			matchingDims++
		} else if min1 == max2 || max1 == min2 {
			// If they abut perfectly in this dimension
			splitDim = i
		} else {
			// If they don't align in this dimension, they can't be siblings
			return false
		}
	}

	// To be siblings, zones must match in all dimensions except one,
	// and they must abut perfectly in that dimension
	return matchingDims == dims-1 && splitDim == node.findLastSplitDimension(zone1)
}

// abutsDimension checks if two zones abut along a specific dimension
func (node *Node) abutsDimension(zone1, zone2 topology.Zone, dim int) bool {
	min1 := zone1.GetCoordMins()[dim]
	max1 := zone1.GetCoordMaxs()[dim]
	min2 := zone2.GetCoordMins()[dim]
	max2 := zone2.GetCoordMaxs()[dim]

	return (min1 == max2 || max1 == min2)
}

// notifyTakeoverNode notifies a node to take over the zone of a leaving node
func (node *Node) notifyTakeoverNode(takingOverNode topology.NodeInfo, leavingNodeId string, leavingZone topology.Zone) error {
	// Get the leaving node's IP address from our routing table
	var leavingNodeIP string
	for _, neighbor := range node.RoutingTable.Neighbours {
		if neighbor.NodeId == leavingNodeId {
			leavingNodeIP = neighbor.IpAddress
			break
		}
	}

	if leavingNodeIP == "" {
		return fmt.Errorf("leaving node's IP not found in routing table")
	}

	// If takingOverNode is the sibling of the leaving node
	if takingOverNode.NodeId == node.Info.NodeId {
		return node.TakeoverSibling(leavingNodeIP, leavingNodeId, leavingZone, leavingNodeId)
	}

	// If takingOverNode is not the sibling of the leaving node
	conn, err := node.getGRPCConn(takingOverNode.IpAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to takeover node: %v", err)
	}

	client := pb.NewCANNodeClient(conn)

	node.logger.Printf("My current length of NeighInfo: %d", len(node.NeighInfo))
	node.logger.Printf("Current length of NeighInfo[leavingNodeId]: %d", len(node.NeighInfo[leavingNodeId]))
	node.logger.Printf("Current neighbours of leaving node: %v", node.NeighInfo[leavingNodeId])

	pbNeighbours := make([]*pb.Node, 0, len(node.NeighInfo[leavingNodeId]))
	for _, neighbor := range node.NeighInfo[leavingNodeId] {
		pbNeighbours = append(pbNeighbours, &pb.Node{
			NodeId:  neighbor.NodeId,
			Address: neighbor.IpAddress,
			Zone:    zoneToProto(neighbor.Zone),
		})
	}

	// Send takeover request
	response, err := client.TakeoverZone(context.Background(), &pb.TakeoverRequest{
		LeavingNodeId:      leavingNodeId,
		LeavingNodeAddress: leavingNodeIP,
		LeavingZone:        zoneToProto(leavingZone),
		LeavingNeighbors:   pbNeighbours,
		IsGraceful:         true,
	})

	if err != nil {
		return fmt.Errorf("takeover request failed: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("takeover node rejected request: %s", response.ErrorMessage)
	}

	return nil
}

// TakeoverSibling handles node takeover from a sibling
func (node *Node) TakeoverSibling(leavingNodeIP string, leavingNodeId string, leavingZone topology.Zone, avoidNodeID string) error {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.logger.Printf("Sibling takeover request from %s", leavingNodeId)

	mergedZone, err := node.mergeZones(node.Info.Zone, leavingZone)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to merge zones: %v", err)
		node.logger.Printf(errMsg)
		return err
	}

	err = node.fetchDataFromAnotherNode(leavingNodeIP)
	if err != nil {
		node.logger.Printf("Warning: Failed to fetch data from leaving node: %v", err)
		// Continue anyway - this is recoverable
	}

	oldZone := node.Info.Zone
	node.Info.Zone = mergedZone

	// Update neighbors based on the new merged zone
	// First collect all current neighbors from both zones
	neighborsToNotify := make(map[string]topology.NodeInfo)

	// Add our current neighbors
	for _, neighbor := range node.RoutingTable.Neighbours {
		if neighbor.NodeId != leavingNodeId && neighbor.NodeId != avoidNodeID {
			neighborsToNotify[neighbor.NodeId] = neighbor
		}
	}

	// Add the leaving node's neighbors
	for _, neighbor := range node.NeighInfo[leavingNodeId] {
		if neighbor.NodeId != node.Info.NodeId && neighbor.NodeId != avoidNodeID {
			neighborsToNotify[neighbor.NodeId] = neighbor
		}
	}

	// Rebuild our routing table with neighbors that are still adjacent
	var newNeighbors []topology.NodeInfo
	for _, neighbor := range neighborsToNotify {
		if mergedZone.IsAdjacent(neighbor.Zone) {
			newNeighbors = append(newNeighbors, neighbor)
		}
	}

	node.RoutingTable.Neighbours = newNeighbors

	// Notify all neighbors about our new zone
	node.notifyNeighborsAboutMerge(oldZone, mergedZone, leavingNodeId)

	node.logger.Printf("Successfully took over node %s's zone", leavingNodeId)
	return nil
}

// mergeZones combines two zones into one
func (node *Node) mergeZones(zone1, zone2 topology.Zone) (topology.Zone, error) {
	if !node.areSiblings(zone1, zone2) {
		return topology.Zone{}, fmt.Errorf("zones are not siblings, cannot merge")
	}

	dims := len(zone1.GetCoordMins())

	// Create a new zone with the combined dimensions
	newZone := topology.NewZone(uint(dims))
	Min := make([]float32, dims)
	Max := make([]float32, dims)

	for i := 0; i < dims; i++ {
		Min[i] = float32(min32(zone1.GetCoordMins()[i], zone2.GetCoordMins()[i]))
		Max[i] = float32(max32(zone1.GetCoordMaxs()[i], zone2.GetCoordMaxs()[i]))
	}

	newZone.SetCoordMins(Min)
	newZone.SetCoordMaxs(Max)

	return newZone, nil
}

// min32 returns the minimum of two float32 values
func min32(a, b float32) float32 {
	if a < b {
		return a
	}
	return b
}

// max32 returns the maximum of two float32 values
func max32(a, b float32) float32 {
	if a > b {
		return a
	}
	return b
}

// fetchDataFromLeavingNode retrieves data from a node that's leaving
func (node *Node) fetchDataFromAnotherNode(leavingNodeAddress string) error {
	conn, err := node.getGRPCConn(leavingNodeAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to leaving node: %v", err)
	}

	client := pb.NewCANNodeClient(conn)

	response, err := client.TransferData(context.Background(), &pb.TransferDataRequest{
		RequestingNodeId: node.Info.NodeId,
	})

	if err != nil {
		return fmt.Errorf("data transfer request failed: %v", err)
	}

	// Store the transferred data
	for _, kv := range response.Data {
		node.KVStore.Insert(kv.Key, kv.Value)
	}

	node.logger.Printf("Received %d key-value pairs from leaving node", len(response.Data))
	return nil
}

// notifyNeighborsAboutMerge notifies all neighbors about the zone merge
func (node *Node) notifyNeighborsAboutMerge(oldZone, newZone topology.Zone, leavingNodeId string) {
	var wg sync.WaitGroup

	for _, neighbor := range node.RoutingTable.Neighbours {
		wg.Add(1)
		go func(nbr topology.NodeInfo) {
			defer wg.Done()

			conn, err := node.getGRPCConn(nbr.IpAddress)
			if err != nil {
				node.logger.Printf("Failed to connect to neighbor %s: %v", nbr.NodeId, err)
				return
			}

			client := pb.NewCANNodeClient(conn)

			_, err = client.NotifyZoneMerge(context.Background(), &pb.ZoneMergeNotification{
				TakeoverNodeId:  node.Info.NodeId,
				TakeoverAddress: node.Info.IpAddress,
				OldZone:         zoneToProto(oldZone),
				NewZone:         zoneToProto(newZone),
				LeavingNodeId:   leavingNodeId,
			})

			if err != nil {
				node.logger.Printf("Failed to notify neighbor %s about zone merge: %v", nbr.NodeId, err)
			}
		}(neighbor)
	}

	wg.Wait()
}

//// HandleCrashDetection is called when a neighbor's heartbeat fails
//func (node *Node) HandleCrashDetection(crashedNodeId string, crashedNodeAddress string) {
//	node.logger.Printf("Detected crash of node %s", crashedNodeId)
//
//	// Get the crashed node's info from our routing table
//	var crashedNodeInfo topology.NodeInfo
//	found := false
//
//	node.mu.RLock()
//	for _, neighbor := range node.RoutingTable.Neighbours {
//		if neighbor.NodeId == crashedNodeId {
//			crashedNodeInfo = neighbor
//			found = true
//			break
//		}
//	}
//	node.mu.RUnlock()
//
//	if !found {
//		node.logger.Printf("Crashed node %s not found in routing table", crashedNodeId)
//		return
//	}
//
//	// Start takeover coordinator election
//	isCoordinator, err := node.electTakeoverCoordinator(crashedNodeInfo)
//	if err != nil {
//		node.logger.Printf("Error in coordinator election: %v", err)
//		return
//	}
//
//	if !isCoordinator {
//		// Not the coordinator, just wait for updates
//		return
//	}
//
//	// We are the coordinator, find a takeover node
//	takingOverNode, err := node.findTakeoverNodeDFS(crashedNodeInfo.Zone, crashedNodeId)
//	if err != nil {
//		node.logger.Printf("Failed to find takeover node for crashed node %s: %v", crashedNodeId, err)
//		return
//	}
//
//	// Notify the takeover node
//	err = node.notifyTakeoverNode(takingOverNode, crashedNodeId, crashedNodeInfo.Zone)
//	if err != nil {
//		node.logger.Printf("Failed to notify takeover node for crashed node %s: %v", crashedNodeId, err)
//		return
//	}
//
//	node.logger.Printf("Successfully coordinated recovery for crashed node %s", crashedNodeId)
//}
//
//// electTakeoverCoordinator decides if this node should be the coordinator for takeover
//// Returns true if this node is the coordinator, false otherwise
//func (node *Node) electTakeoverCoordinator(crashedNodeInfo topology.NodeInfo) (bool, error) {
//	node.mu.RLock()
//	myVolume := node.Info.Zone.CalculateVolume()
//	myNodeId := node.Info.NodeId
//	node.mu.RUnlock()
//
//	// Get the neighbors of the crashed node (from our 2-hop info)
//	crashedNodeNeighbors := make([]topology.NodeInfo, 0)
//
//	node.mu.RLock()
//	//for _, nbrInfo := range node.NeighInfo {
//	//	for _, neighbor := range node.RoutingTable.Neighbours {
//	//		if nbrInfo.NodeId == crashedNodeInfo.NodeId {
//	//			crashedNodeNeighbors = append(crashedNodeNeighbors, neighbor)
//	//		}
//	//	}
//	//}
//	node.mu.RUnlock()
//
//	// We need to add ourselves to the list if we're not already there
//	foundSelf := false
//	for _, nbr := range crashedNodeNeighbors {
//		if nbr.NodeId == myNodeId {
//			foundSelf = true
//			break
//		}
//	}
//
//	if !foundSelf {
//		node.mu.RLock()
//		crashedNodeNeighbors = append(crashedNodeNeighbors, *node.Info)
//		node.mu.RUnlock()
//	}
//
//	// Contact each neighbor to determine if we're the coordinator
//	var wg sync.WaitGroup
//	responses := make(chan bool, len(crashedNodeNeighbors))
//
//	for _, nbr := range crashedNodeNeighbors {
//		// Skip ourselves
//		if nbr.NodeId == myNodeId {
//			continue
//		}
//
//		wg.Add(1)
//		go func(neighbor topology.NodeInfo) {
//			defer wg.Done()
//
//			conn, err := node.getGRPCConn(neighbor.IpAddress)
//			if err != nil {
//				node.logger.Printf("Failed to connect to neighbor %s: %v", neighbor.NodeId, err)
//				// If we can't connect, assume they can't be coordinator
//				responses <- true
//				return
//			}
//
//			client := pb.NewCANNodeClient(conn)
//
//			response, err := client.ElectTakeoverCoordinator(context.Background(), &pb.CoordinatorElectionRequest{
//				CandidateNodeId: myNodeId,
//				CandidateVolume: myVolume,
//				CrashedNodeId:   crashedNodeInfo.NodeId,
//			})
//
//			if err != nil {
//				node.logger.Printf("Error in election with %s: %v", neighbor.NodeId, err)
//				// Assume we win if there's an error
//				responses <- true
//				return
//			}
//
//			// If false, the other node should be coordinator
//			responses <- response.ShouldBeCoordinator
//		}(nbr)
//	}
//
//	// Wait for all responses
//	wg.Wait()
//	close(responses)
//
//	// If all responses are true, we're the coordinator
//	isCoordinator := true
//	for r := range responses {
//		if !r {
//			isCoordinator = false
//			break
//		}
//	}
//
//	return isCoordinator, nil
//}
//
//// ElectTakeoverCoordinator handles election requests for takeover coordinator
//func (node *Node) ElectTakeoverCoordinator(ctx context.Context, req *pb.CoordinatorElectionRequest) (*pb.CoordinatorElectionResponse, error) {
//	node.mu.RLock()
//	defer node.mu.RUnlock()
//
//	myVolume := node.Info.Zone.CalculateVolume()
//	myNodeId := node.Info.NodeId
//
//	// Compare volumes - smaller volume wins (has priority to be coordinator)
//	if myVolume < req.CandidateVolume {
//		// I should be coordinator because I have smaller volume
//		return &pb.CoordinatorElectionResponse{
//			ShouldBeCoordinator: false,
//		}, nil
//	} else if myVolume > req.CandidateVolume {
//		// The candidate should be coordinator
//		return &pb.CoordinatorElectionResponse{
//			ShouldBeCoordinator: true,
//		}, nil
//	} else {
//		// Equal volumes, use node ID as tiebreaker (smaller ID wins)
//		return &pb.CoordinatorElectionResponse{
//			ShouldBeCoordinator: myNodeId > req.CandidateNodeId,
//		}, nil
//	}
//}
//
//// DetectNodeFailure is called periodically to check for failed nodes
//func (node *Node) DetectNodeFailure() {
//	node.mu.RLock()
//	neighbors := make([]topology.NodeInfo, len(node.RoutingTable.Neighbours))
//	copy(neighbors, node.RoutingTable.Neighbours)
//	node.mu.RUnlock()
//
//	for _, neighbor := range neighbors {
//		// Check if the neighbor is still in our routing table
//		//conn, err := node.getClientConn(neighbor.IpAddress)
//		_, err := node.getClientConn(neighbor.IpAddress)
//		if err != nil {
//			node.logger.Printf("Failed to connect to neighbor %s, marking as potentially failed", neighbor.NodeId)
//
//			// Try to contact node a few more times before declaring it crashed
//			failed := true
//			for i := 0; i < 3; i++ { // Try 3 times
//				time.Sleep(1 * time.Second)
//				conn, err := node.getGRPCConn(neighbor.IpAddress)
//				if err == nil {
//					err := conn.Close()
//					if err != nil {
//						return
//					}
//					failed = false
//					break
//				}
//			}
//
//			if failed {
//				node.logger.Printf("Confirmed failure of node %s, initiating crash recovery", neighbor.NodeId)
//				go node.HandleCrashDetection(neighbor.NodeId, neighbor.IpAddress)
//			}
//		} else {
//			// Neighbor is still up, update last heartbeat time
//			node.mu.Lock()
//			node.lastHeartbeat[neighbor.IpAddress] = time.Now()
//			node.mu.Unlock()
//		}
//	}
//}
//
//// StartCrashDetection starts a goroutine for detecting node failures
//func (node *Node) StartCrashDetection() {
//	go func() {
//		ticker := time.NewTicker(5 * time.Second)
//		defer ticker.Stop()
//
//		for range ticker.C {
//			node.DetectNodeFailure()
//		}
//	}()
//}
