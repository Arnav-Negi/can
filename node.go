package can

import (
	"log"

	"github.com/Arnav-Negi/can/internal/routing"
	"github.com/Arnav-Negi/can/internal/store"
)

type Node[K comparable, V any] struct {
	NodeID    		uint8
	IPAddress 		string

	KVStore   		*store.KVStore[K, V]
	RoutingTable 	*routing.RoutingTable[K]
}

func NewNode[K comparable, V any](nodeID uint8, ipAddress string, coordDimensions uint) (*Node[K, V], error) {
	// Initialize the KVStore, this must be present 
	// in the Node struct for future accesses
	kvStore, err := store.NewKVStore[K, V]()
	if err != nil {
		log.Fatalf("Failed to initialize KVStore: %v", err)
		return nil, err
	}

	// Initialize the RoutingTable
	routingTable := routing.NewRoutingTable[K](coordDimensions)
	if routingTable == nil {
		log.Fatalf("Failed to initialize RoutingTable")
		return nil, err
	}

	return &Node[K, V]{
		NodeID:    nodeID,
		IPAddress: ipAddress,

		KVStore:   kvStore,
		RoutingTable: routingTable,
	}, nil
}