/*
	This file contains implementation of the DHT type.
	DHT is the user facing API for the CAN network.
	DHT uses other modules internally for routing, encryption,
	fault tolerance etc.
*/

package can

import (
	"strconv"
	"sync"
)

type DHT struct {
	//config       *config.Config
	//store        *store.Store
	//routingTable *routing.RoutingTable
	address string
	mu      sync.Mutex
}

// NewDHT This function is used to create a new DHT instance.
// It initializes the DHT with the provided configuration.
func NewDHT(port int) *DHT {
	return &DHT{
		address: "localhost:" + strconv.Itoa(port),
	}
}

// Bootstrap This starts the DHT, might take some time to set up and join
// the overlay network.
// If no bootstrap node is provided, it returns an error
func (dht *DHT) Bootstrap() error {
	//TODO implement me
	panic("implement me")
}

// Leave This function is used to leave the CAN network.
// It should gracefully leave the network and clean up resources.
func (dht *DHT) Leave() error {
	//TODO implement me
	panic("implement me")
}

// Put This function is used to store a value in the DHT.
// Overwrites the value if the key already exists.
func (dht *DHT) Put(key string, value []byte) error {
	//TODO implement me
	panic("implement me")
}

// Get This function is used to retrieve a value from the DHT.
// Error if the key does not exist or unable to retrieve the value.
func (dht *DHT) Get(key string) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}
