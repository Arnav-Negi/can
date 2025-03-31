package routing

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/google/uuid"
)

type Hasher[K comparable] struct {
	seed uint32
}

// NewHasher generates a new hasher with a UUID-based seed
func NewHasher[K comparable]() *Hasher[K] {
	u, err := uuid.NewRandom()
	if err != nil {
		log.Fatalf("Failed to generate UUID: %v", err)
	}
	seed := binary.BigEndian.Uint32(u[:4]) // First 4 bytes

	return &Hasher[K]{seed: seed}
}

// Hash generates a hash for the given key using the seed
func (h *Hasher[K]) Hash(key K) uint32 {
	// Convert key to a byte slice bcoz md5 needs that ugh
	keyBytes := []byte(fmt.Sprintf("%v", key))

	hash := md5.Sum(keyBytes)
	return binary.BigEndian.Uint32(hash[:4]) ^ h.seed
}
