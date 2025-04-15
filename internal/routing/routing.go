package routing

import (
	"sort"
	"sync"

	"github.com/Arnav-Negi/can/internal/topology"
	"github.com/Arnav-Negi/can/internal/utils"
)

type RoutingTable struct {
	mu sync.RWMutex

	Dimensions    uint
	NumHashes     uint
	HashFunctions []MultiHash
	Neighbours    []topology.NodeInfo
}

func NewRoutingTable(dimensions uint, NumHashes uint) *RoutingTable {
	HashFunctions := make([]MultiHash, NumHashes)
	for i := 0; i < int(NumHashes); i++ {
		HashFunctions[i] = *NewMultiHash(int(dimensions), i)
	}

	return &RoutingTable{
		mu:            sync.RWMutex{},
		Dimensions:    dimensions,
		NumHashes:     NumHashes,
		HashFunctions: HashFunctions,
		Neighbours:    []topology.NodeInfo{},
	}
}

func (rt *RoutingTable) AddNode(nodeInfo topology.NodeInfo) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.Neighbours = append(rt.Neighbours, nodeInfo)
}

// GetNodesSorted Sort the neighbours based on their distance to the given coordinates
func (rt *RoutingTable) GetNodesSorted(coords []float32, selZone topology.Zone, numNodes int) []topology.NodeInfo {
	utils.Assert(len(coords) == int(rt.Dimensions), "Coordinates length must match dimensions")
	utils.Assert(len(rt.Neighbours) > 0, "No neighbours to sort")

	rt.mu.RLock()
	sortedNeighbors := rt.Neighbours
	rt.mu.RUnlock()

	sort.Slice(sortedNeighbors, func(i, j int) bool {
		node1 := sortedNeighbors[i]
		node2 := sortedNeighbors[j]
		distance1 := node1.Zone.Distance(coords)
		distance2 := node2.Zone.Distance(coords)
		return distance1 < distance2
	})

	for i := len(sortedNeighbors) - 1; i >= 0; i-- {
		if sortedNeighbors[i].Zone.Distance(coords) >= selZone.Distance(coords) {
			sortedNeighbors = sortedNeighbors[:i]
		}
	}

	// remove zones farther from coords than selfZone is from coords
	if numNodes > len(sortedNeighbors) {
		numNodes = len(sortedNeighbors)
	}

	return sortedNeighbors[:numNodes]
}

func (rt *RoutingTable) RemoveNeighbor(ip string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	for i, node := range rt.Neighbours {
		if node.IpAddress == ip {
			rt.Neighbours = append(rt.Neighbours[:i], rt.Neighbours[i+1:]...)
			break
		}
	}
}
