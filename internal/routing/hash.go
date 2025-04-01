package routing

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"strconv"
)

// MultiHash provides multiple hash functions to map keys to coordinates in a CAN space
type MultiHash struct {
	dimensions int
	seeds      []uint64
}

// NewMultiHash creates a new MultiHash with the specified number of dimensions
func NewMultiHash(dimensions int) *MultiHash {
	mh := &MultiHash{
		dimensions: dimensions,
		seeds:      make([]uint64, dimensions),
	}
	// deterministically initialize the seeds
	mh.initSeeds()

	return mh
}

// initSeeds initializes deterministic random seeds for each dimension
func (mh *MultiHash) initSeeds() {
	for i := 0; i < mh.dimensions; i++ {
		// Use a deterministic method to generate seed for dimension i
		// Here we're using a simple approach: hash the dimension index
		h := fnv.New64a()
		h.Write([]byte("dimension-seed-" + strconv.Itoa(i)))
		mh.seeds[i] = binary.BigEndian.Uint64(h.Sum(nil))
	}
}

// GetCoordinates maps a string key to coordinates in the CAN space
func (mh *MultiHash) GetCoordinates(key string) []float32 {
	coordinates := make([]float32, mh.dimensions)

	for i := 0; i < mh.dimensions; i++ {
		h := fnv.New64a()
		h.Write([]byte(key))
		keyHash := binary.BigEndian.Uint64(h.Sum(nil))
		result := keyHash ^ mh.seeds[i]

		// Map the result to the range [0, 1]
		coordinates[i] = float32(result) / float32(math.MaxUint64)
	}

	return coordinates
}
