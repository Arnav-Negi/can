package routing

import (
	"github.com/Arnav-Negi/can/internal/topology"
	"github.com/Arnav-Negi/can/internal/utils"
	"sort"
)

type RoutingTable struct {
	Dimensions   uint
	HashFunction MultiHash
	Neighbours   []topology.NodeInfo
}

func NewRoutingTable(dimensions uint) *RoutingTable {
	return &RoutingTable{
		Dimensions:   dimensions,
		HashFunction: *NewMultiHash(int(dimensions)),
		Neighbours:   []topology.NodeInfo{},
	}
}

func (rt *RoutingTable) AddNode(nodeInfo topology.NodeInfo) {
	rt.Neighbours = append(rt.Neighbours, nodeInfo)
}

// GetNodesSorted Sort the neighbours based on their distance to the given coordinates
func (rt *RoutingTable) GetNodesSorted(coords []float32, numNodes int) []topology.NodeInfo {
	utils.Assert(len(coords) == int(rt.Dimensions), "Coordinates length must match dimensions")
	utils.Assert(len(rt.Neighbours) > 0, "No neighbours to sort")
	sortedNeighbors := rt.Neighbours
	sort.Slice(sortedNeighbors, func(i, j int) bool {
		node1 := sortedNeighbors[i]
		node2 := sortedNeighbors[j]
		distance1 := node1.Zone.Distance(coords)
		distance2 := node2.Zone.Distance(coords)
		return distance1 < distance2
	})
	if numNodes > len(sortedNeighbors) {
		numNodes = len(sortedNeighbors)
	}
	return sortedNeighbors[:numNodes]
}
