package routing

import "github.com/Arnav-Negi/can/internal/topology"

type Neighbour struct {
	Coordinates []uint32 // d-dimensional toroidal coord space
	Address     string   // Address of the neighbour -> Replace with Client conn maybe [TODODODODO]
}

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
