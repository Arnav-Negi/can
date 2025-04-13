package cache

import (
	"fmt"
	"log"

	"github.com/hashicorp/golang-lru/v2"
)

type Cache struct {
	Cache *lru.Cache[string, []byte]
}

func evictionLogger(key string, value []byte) {
	fmt.Printf("Evicted key: %s, value: %x\n from the cache", key, value)
}

func NewCache(size int) *Cache {
	cache, err := lru.NewWithEvict[string, []byte](size, evictionLogger)
	if err != nil {
		log.Fatalf("Failed to create cache: %v", err)
		return nil
	}
	return &Cache{
		Cache: cache,
	}
}