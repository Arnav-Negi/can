package can

import (
	"github.com/Arnav-Negi/can/internal/dht"
)

type DHT struct {
	Node *dht.Node
}

// NewDHT This function initializes a new DHT instance.
// Bootstrap must be called to join the network.
func NewDHT(port int) (*DHT, error) {
	// Initialize a new node, not yet connected to the network
	node, err := dht.NewNode(port)
	if err != nil {
		return nil, err
	}

	return &DHT{
		Node: node,
	}, nil
}

// StartNode This function starts the gRPC server for the DHT node.
// It listens for incoming requests and handles them.
// To be called using goroutines
func (dht *DHT) StartNode() error {
	return dht.Node.StartGRPCServer()
}

// Bootstrap This starts the DHT, might take some time to set up and join
// the overlay network.
// If no bootstrap node is provided, it returns an error
func (dht *DHT) Join(bootstrapAddr string) error {
	return dht.Node.JoinImplementation(bootstrapAddr)
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
	return dht.Node.PutImplementation(key, value)
}

// Get This function is used to retrieve a value from the DHT.
// Error if the key does not exist or unable to retrieve the value.
func (dht *DHT) Get(key string) ([]byte, error) {
	return dht.Node.GetImplementation(key)
}
