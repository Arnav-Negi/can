package routing

type Neighbour struct {
	Coordinates []uint32 // d-dimensional toroidal coord space
	Address   string   // Address of the neighbour -> Replace with Client conn maybe [TODODODODO]
}

type RoutingTable[K comparable] struct {
	Dimensions 	uint
	
	Hashers 	[]Hasher[K]
	Neighbours 	[]Neighbour
}

func NewRoutingTable[K comparable](dimensions uint) *RoutingTable[K] {
	// Create a new hasher for each dimension
	hashers := make([]Hasher[K], dimensions)
	for i := uint(0); i < dimensions; i++ {
		hashers[i] = *NewHasher[K]()
	}

	// Make a new neighbour in each direction
	// initialize with empty coordinates
	neighbours := make([]Neighbour, dimensions)
	for i := uint(0); i < dimensions; i++ {
		neighbours[i] = Neighbour{
			Coordinates: make([]uint32, dimensions),
			Address:     "",
		}
	}

	return &RoutingTable[K]{
		Dimensions: dimensions,
		Hashers:   hashers,
		Neighbours: neighbours,
	}
}