package dht

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Arnav-Negi/can/internal/topology"
	pb "github.com/Arnav-Negi/can/protofiles"
)

func (node *Node) StartGRPCServer(ip string, port int) error {
	// Start the gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		node.logger.Fatalf("failed to listen: %v", err)
	}
	node.logger.Printf("Listening on IP %s", lis.Addr().String())

	// if port was given 0, it was selected randomly,
	port = lis.Addr().(*net.TCPAddr).Port

	// extract IP address from the listener
	node.IPAddress = fmt.Sprintf("%s:%d", ip, port)

	if err != nil {
		node.logger.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer(grpc.UnaryInterceptor(LoggingUnaryServerInterceptor(node.logger)))
	pb.RegisterCANNodeServer(s, node)
	if err := s.Serve(lis); err != nil {
		node.logger.Fatalf("failed to serve: %v", err)
	}
	return nil
}

func (node *Node) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	// Check if coordinates are in this node's zone
	coords := req.Coordinates
	if !node.Info.Zone.Contains(coords) {
		// Forward to the closest neighbor
		closestNodes := node.RoutingTable.GetNodesSorted(coords, node.Info.Zone, 3)
		for _, closestNode := range closestNodes {
			canConn, err := node.getGRPCConn(closestNode.IpAddress)
			if err != nil {
				continue
			}
			canServiceClient := pb.NewCANNodeClient(canConn)
			joinResponse, err := canServiceClient.Join(context.Background(), req)
			if err == nil {
				return joinResponse, nil
			}
		}
		return nil, status.Errorf(codes.Unavailable, "Could not forward join request")
	}

	node.mu.Lock()
	defer node.mu.Unlock()
	// Split the zone and transfer data
	newZone, transferredData, err := node.splitZone(coords)
	if err != nil {
		return nil, err
	}

	// Update neighbors - pass the new node's ID and address
	neighbors := node.updateNeighbors(newZone)

	// Create response
	pbNeighbors := make([]*pb.Node, 0, len(neighbors))
	for _, neighbor := range neighbors {
		pbNeighbors = append(pbNeighbors, &pb.Node{
			NodeId:  neighbor.NodeId,
			Address: neighbor.IpAddress,
			Zone:    zoneToProto(neighbor.Zone),
		})
	}

	pbKeyValuePairs := make([]*pb.KeyValuePair, 0, len(transferredData))
	for key, value := range transferredData {
		pbKeyValuePairs = append(pbKeyValuePairs, &pb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	// Log the join event
	node.logger.Printf("Node %s joined the network with zone: %v", req.Address, newZone)
	node.logger.Printf("Updated neighbors: %v", pbNeighbors)
	node.logger.Printf("Current zone: %v", node.Info.Zone)
	node.logger.Printf("My neighbors: %v", node.RoutingTable.Neighbours)

	return &pb.JoinResponse{
		AssignedZone:    zoneToProto(newZone),
		Neighbors:       pbNeighbors,
		TransferredData: pbKeyValuePairs,
	}, nil
}

// AddNeighbor handles requests to add a node as a neighbor
func (node *Node) AddNeighbor(ctx context.Context, req *pb.AddNeighborRequest) (*pb.AddNeighborResponse, error) {
	node.mu.Lock()
	defer node.mu.Unlock()

	if req.Neighbor == nil {
		return &pb.AddNeighborResponse{Success: false}, status.Errorf(codes.InvalidArgument, "Neighbor information is missing")
	}

	// Convert proto node to topology node
	neighborZone := topology.NewZoneFromProto(req.Neighbor.Zone)
	neighborInfo := topology.NodeInfo{
		NodeId:    req.Neighbor.NodeId,
		IpAddress: req.Neighbor.Address,
		Zone:      neighborZone,
	}

	// Iterate through existing neighbors to check if the node is already present
	// If present already, remove it
	for i, n := range node.RoutingTable.Neighbours {
		if n.IpAddress == req.Neighbor.Address {
			node.RoutingTable.Neighbours = append(node.RoutingTable.Neighbours[:i], node.RoutingTable.Neighbours[i+1:]...)
			break
		}
	}

	// Check if zones are adjacent before adding
	if !node.Info.Zone.IsAdjacent(neighborZone) {
		return &pb.AddNeighborResponse{Success: false}, nil
	}

	// Add the node to our routing table
	node.RoutingTable.AddNode(neighborInfo)

	node.logger.Printf("Added/Updated neighbor: %s with zone: %v", req.Neighbor.Address, neighborZone)
	node.logger.Printf("Current neighbors: %v", node.RoutingTable.Neighbours)

	return &pb.AddNeighborResponse{Success: true}, nil
}

// Helper function to convert Zone to proto message
func zoneToProto(zone topology.Zone) *pb.Zone {
	return &pb.Zone{
		MinCoordinates: zone.GetCoordMins(),
		MaxCoordinates: zone.GetCoordMaxs(),
	}
}

// Helper function to convert NodeInfo to proto message
func NodeInfoToProto(nodeInfo topology.NodeInfo) *pb.Node {
	return &pb.Node{
		NodeId:  nodeInfo.NodeId,
		Address: nodeInfo.IpAddress,
		Zone:    zoneToProto(nodeInfo.Zone),
	}
}

func (node *Node) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, err := node.GetImplementation(req.Key, int(req.HashToUse))
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{Value: value}, nil
}

func (node *Node) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	err := node.PutImplementation(req.Key, req.Value, int(req.HashToUse))
	if err != nil {
		return &pb.PutResponse{Success: false}, err
	}
	return &pb.PutResponse{Success: true}, nil
}

func (node *Node) SendNeighbourInfo(ctx context.Context, req *pb.NeighbourInfoRequest) (*pb.NeighbourInfoResponse, error) {
	node.mu.Lock()
	defer node.mu.Unlock()

	// Update the neighbour info
	neighbourInfo := make([]topology.NodeInfo, len(req.Neighbours))
	for i, n := range req.Neighbours {
		neighbourInfo[i] = topology.NodeInfo{
			NodeId:    n.NodeId,
			IpAddress: n.Address,
			Zone:      topology.NewZoneFromProto(n.Zone),
		}
	}
	node.NeighInfo[req.NodeId] = neighbourInfo

	return &pb.NeighbourInfoResponse{Success: true}, nil
}

func (node *Node) getClientConn(ip string) (*grpc.ClientConn, error) {
	// Check if the connection already exists
	node.mu.RLock()
	conn, exists := node.conns[ip]
	node.mu.RUnlock()
	if exists {
		return conn, nil
	}

	// Create a new connection
	conn, err := node.getGRPCConn(ip)
	if err != nil {
		return nil, status.Error(codes.Unavailable, fmt.Sprintf("Failed to connect to %s: %v", ip, err))
	}

	// Store the connection in the map
	node.mu.Lock()
	node.conns[ip] = conn
	node.mu.Unlock()

	// Return the connection
	return conn, nil
}

// InitiateLeave handles a request from a node that wants to leave
func (node *Node) InitiateLeave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	node.logger.Printf("Received leave request from node %s", req.LeavingNodeId)

	// Convert proto zone to our zone type
	leavingZone := topology.NewZoneFromProto(req.LeavingZone)

	// Start DFS to find a suitable takeover node
	takingOverNode, err := node.findTakeoverNodeDFS(leavingZone, req.LeavingNodeId)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to find takeover node: %v", err)
		node.logger.Printf(errMsg)
		return &pb.LeaveResponse{
			Success:      false,
			ErrorMessage: errMsg,
		}, nil
	}

	// Check if the found node is a sibling of Leaving node
	//isSibling := node.areSiblings(takingOverNode.Zone, leavingZone)

	// Notify the takeover node to take over the leaving node's zone
	//err = node.notifyTakeoverNode(takingOverNode, req.LeavingNodeId, leavingZone, isSibling)
	err = node.notifyTakeoverNode(takingOverNode, req.LeavingNodeId, leavingZone)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to notify takeover node: %v", err)
		node.logger.Printf(errMsg)
		return &pb.LeaveResponse{
			Success:      false,
			ErrorMessage: errMsg,
		}, nil
	}

	return &pb.LeaveResponse{
		Success:      true,
		ErrorMessage: "",
	}, nil
}

// PerformDFS handles a DFS request from another node
func (node *Node) PerformDFS(ctx context.Context, req *pb.DFSRequest) (*pb.DFSResponse, error) {
	node.logger.Printf("Received DFS request for node %s from node %s", req.LeavingNodeId, req.ParentNodeId)

	parentZone := topology.NewZoneFromProto(req.ParentZone)

	// Check if this node and the leaving node are siblings
	if node.areSiblings(node.Info.Zone, parentZone) {
		return &pb.DFSResponse{
			FoundSibling:    true,
			TakeoverNodeId:  node.Info.NodeId,
			TakeoverAddress: node.Info.IpAddress,
			TakeoverZone:    zoneToProto(node.Info.Zone),
		}, nil
	}

	splitDim := node.findLastSplitDimension(node.Info.Zone)

	// Continue DFS with neighbors
	for _, neighbor := range node.RoutingTable.Neighbours {
		// Skip the leaving node
		if neighbor.NodeId == req.LeavingNodeId {
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
					LeavingNodeId: req.LeavingNodeId,
					ParentZone:    zoneToProto(node.Info.Zone),
					ParentNodeId:  node.Info.NodeId,
				})

				if err == nil && response.FoundSibling {
					return response, nil
				}
			}
		}
	}

	return &pb.DFSResponse{
		FoundSibling: false,
	}, nil
}

// TakeoverZone handles a request to take over another node's zone
func (node *Node) TakeoverZone(ctx context.Context, req *pb.TakeoverRequest) (*pb.TakeoverResponse, error) {
	node.logger.Printf("Received takeover request for node %s", req.LeavingNodeId)
	node.mu.Lock()
	defer node.mu.Unlock()

	var siblingNeighbor topology.NodeInfo
	for _, neighbor := range node.RoutingTable.Neighbours {
		if node.areSiblings(node.Info.Zone, neighbor.Zone) {
			siblingNeighbor = neighbor
			break
		}
	}

	conn, err := node.getGRPCConn(siblingNeighbor.IpAddress)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to connect to sibling neighbor %s: %v", siblingNeighbor.NodeId, err)
		node.logger.Printf(errMsg)
		return &pb.TakeoverResponse{
			Success:      false,
			ErrorMessage: errMsg,
		}, nil
	}
	client := pb.NewCANNodeClient(conn)

	nodeInfo := &pb.Node{
		NodeId:  node.Info.NodeId,
		Address: node.Info.IpAddress,
		Zone:    zoneToProto(node.Info.Zone),
	}

	// Notify the sibling neighbor about the takeover
	var response *pb.UpdateSiblingResponse
	response, err = client.UpdateSibling(context.Background(), &pb.UpdateSiblingRequest{
		Sibling:       nodeInfo,
		LeavingNodeId: req.LeavingNodeId,
	})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to notify sibling neighbor: %v", response.ErrorMessage)
		node.logger.Printf(errMsg)
		return &pb.TakeoverResponse{
			Success:      false,
			ErrorMessage: errMsg,
		}, nil
	}

	zoneToBeTakenOver := topology.NewZoneFromProto(req.LeavingZone)

	// If this is a graceful leave, fetch data from the leaving node
	if req.IsGraceful {
		err = node.fetchDataFromAnotherNode(req.LeavingNodeAddress)
		if err != nil {
			node.logger.Printf("Warning: Failed to fetch data from leaving node: %v", err)
			// Continue anyway - this is recoverable
		}
	}

	// Update our zone
	oldZone := node.Info.Zone
	node.Info.Zone = zoneToBeTakenOver

	neighborsToNotify := make(map[string]topology.NodeInfo)

	for _, neighbor := range req.LeavingNeighbors {
		if neighbor.NodeId != node.Info.NodeId {
			neighborsToNotify[neighbor.NodeId] = topology.NodeInfo{
				NodeId:    neighbor.NodeId,
				IpAddress: neighbor.Address,
				Zone:      topology.NewZoneFromProto(neighbor.Zone),
			}
		}
	}

	// Rebuild our routing table with neighbors that are still adjacent
	var newNeighbors []topology.NodeInfo
	for _, neighbor := range neighborsToNotify {
		if zoneToBeTakenOver.IsAdjacent(neighbor.Zone) {
			newNeighbors = append(newNeighbors, neighbor)
		}
	}

	node.RoutingTable.Neighbours = newNeighbors

	// Notify all neighbors about our new zone
	node.notifyNeighborsAboutMerge(oldZone, zoneToBeTakenOver, req.LeavingNodeId)

	node.logger.Printf("Successfully took over node %s's zone", req.LeavingNodeId)
	return &pb.TakeoverResponse{
		Success:      true,
		ErrorMessage: "",
	}, nil
}

func (node *Node) UpdateSibling(ctx context.Context, req *pb.UpdateSiblingRequest) (*pb.UpdateSiblingResponse, error) {
	node.logger.Printf("Sibling update request from %s", req.Sibling.NodeId)

	leavingNodeIP := req.Sibling.Address
	leavingNodeId := req.Sibling.NodeId
	leavingZone := topology.NewZoneFromProto(req.Sibling.Zone)
	avoidNodeID := req.LeavingNodeId

	err := node.TakeoverSibling(leavingNodeIP, leavingNodeId, leavingZone, avoidNodeID)

	if err != nil {
		errMsg := fmt.Sprintf("Failed to take over sibling: %v", err)
		node.logger.Printf(errMsg)
		return &pb.UpdateSiblingResponse{
			Success:      false,
			ErrorMessage: errMsg,
		}, nil
	}
	node.logger.Printf("Successfully took over sibling %s", req.Sibling.NodeId)
	return &pb.UpdateSiblingResponse{
		Success:      true,
		ErrorMessage: "",
	}, nil
}

// TransferData handles a request to transfer all data to another node
func (node *Node) TransferData(ctx context.Context, req *pb.TransferDataRequest) (*pb.TransferDataResponse, error) {
	node.logger.Printf("Transferring data to node %s", req.RequestingNodeId)
	node.mu.RLock()
	defer node.mu.RUnlock()

	// Collect all key-value pairs
	kvPairs := make([]*pb.KeyValuePair, 0)
	node.KVStore.ForEach(func(key string, value []byte) {
		kvPairs = append(kvPairs, &pb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	})

	return &pb.TransferDataResponse{
		Data: kvPairs,
	}, nil
}

// NotifyZoneMerge handles a notification about a zone merge
func (node *Node) NotifyZoneMerge(ctx context.Context, notification *pb.ZoneMergeNotification) (*pb.ZoneMergeResponse, error) {
	node.logger.Printf("Received zone merge notification from node %s", notification.TakeoverNodeId)
	node.mu.Lock()
	defer node.mu.Unlock()

	// Update the routing table by removing the leaving node
	for i, neighbor := range node.RoutingTable.Neighbours {
		if neighbor.NodeId == notification.LeavingNodeId {
			// Remove this neighbor
			node.RoutingTable.Neighbours = append(
				node.RoutingTable.Neighbours[:i],
				node.RoutingTable.Neighbours[i+1:]...,
			)
			break
		}
	}

	// Update the zone of the takeover node if it's in our routing table
	for i, neighbor := range node.RoutingTable.Neighbours {
		if neighbor.NodeId == notification.TakeoverNodeId {
			node.RoutingTable.Neighbours = append(
				node.RoutingTable.Neighbours[:i],
				node.RoutingTable.Neighbours[i+1:]...,
			)
			break
		}
	}
	// Add the takeover node with its new zone
	takeoverZone := topology.NewZoneFromProto(notification.NewZone)
	takeoverNode := topology.NodeInfo{
		NodeId:    notification.TakeoverNodeId,
		IpAddress: notification.TakeoverAddress,
		Zone:      takeoverZone,
	}
	node.RoutingTable.Neighbours = append(node.RoutingTable.Neighbours, takeoverNode)

	return &pb.ZoneMergeResponse{
		Success: true,
	}, nil
}
