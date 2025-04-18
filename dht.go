package can

import (
	"github.com/Arnav-Negi/can/internal/dht"
	"github.com/Arnav-Negi/can/internal/store"
	"github.com/Arnav-Negi/can/internal/topology"
)

type DHT struct {
	Node *dht.Node
}

// NewDHT This function initializes a new DHT instance.
// Bootstrap must be called to join the network.
func NewDHT() *DHT {
	// Initialize a new node, not yet connected to the network
	return &DHT{
		Node: dht.NewNode(),
	}
}

func (dht *DHT) Info() (string, string, []float32, []float32, []topology.NodeInfo, *store.MemoryStore) {
	return dht.Node.GetInfo()
}

// StartNode This function starts the gRPC server for the DHT node.
// It listens for incoming requests and handles them.
// To be called using goroutines
// port: Port to listen on, 0 for random port
func (dht *DHT) StartNode(ip string, port int, bootstrapAddress string) error {
	return dht.Node.StartGRPCServer(ip, port, bootstrapAddress)
}

// Join Bootstrap This starts the DHT, might take some time to set up and join
// the overlay network.
// If no bootstrap node is provided, it returns an error
func (dht *DHT) Join(bootstrapAddr string) error {
	return dht.Node.JoinImplementation(bootstrapAddr)
}

// Leave This function is used to leave the CAN network.
// It should gracefully leave the network and clean up resources.
func (dht *DHT) Leave() error {
	return dht.Node.LeaveImplementation()
}

// Put This function is used to store a value in the DHT.
// Overwrites the value if the key already exists.
func (dht *DHT) Put(key string, value []byte) error {
	return dht.Node.PutImplementation(
		key, value,
		-1, // The hash ID to query, -1 for all
	)
}

// Get This function is used to retrieve a value from the DHT.
// Error if the key does not exist or unable to retrieve the value.
func (dht *DHT) Get(key string) ([]byte, error) {
	return dht.Node.GetImplementation(
		key,
		-1, // The hash ID to query, -1 for all
	)
}

// Delete This function is used to delete a value from the DHT.
// Error if the key does not exist or unable to delete the value.
func (dht *DHT) Delete(key string) error {
	return dht.Node.DeleteImplementation(
		key,
		-1, // The hash ID to query, -1 for all
	)
}
