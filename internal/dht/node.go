package dht

import (
	"github.com/Arnav-Negi/can/internal/routing"
	"github.com/Arnav-Negi/can/internal/store"
	"github.com/Arnav-Negi/can/internal/utils"
	"strconv"
)

type Node struct {
	IPAddress string

	KVStore      *store.MemoryStore
	RoutingTable *routing.RoutingTable
}

// NewNode This function initializes a new Node instance.
func NewNode(port int) (*Node, error) {
	ip, err := utils.GetIPAddress()
	if err != nil {
		return nil, err
	}

	ipAddress := ip + ":" + strconv.Itoa(port)

	return &Node{
		IPAddress: ipAddress,

		KVStore:      store.NewMemoryStore(),
		RoutingTable: nil,
	}, nil
}
