package routing

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// MultiHash provides multiple hash functions to map keys to coordinates in a CAN space
type MultiHash struct {
	dimensions int
	seeds      []uint64
}

// NewMultiHash creates a new MultiHash with the specified number of dimensions
func NewMultiHash(dimensions int, hashId int) *MultiHash {
	mh := &MultiHash{
		dimensions: dimensions,
		seeds:      make([]uint64, dimensions),
	}
	// deterministically initialize the seeds
	mh.initSeeds(hashId)

	return mh
}

func splitMix64(x uint64) uint64 {
	z := x + 0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (mh *MultiHash) initSeeds(hashId int) {
	for i := 0; i < mh.dimensions; i++ {
		// pack simply, then do two rounds:
		state := uint64(i)<<32 | uint64(hashId)
		z := splitMix64(state)
		mh.seeds[i] = splitMix64(z + 0x9e3779b97f4a7c15)
		//log.Printf("dim %v, hashId %v, seed %v\n", i, hashId, mh.seeds[i])
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

		// Map the result to the range [0, 1)
		coordinates[i] = float32(result) / float32(math.MaxUint64)
	}

	return coordinates
}
