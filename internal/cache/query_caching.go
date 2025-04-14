package cache

import (
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
)

type Cache struct {
	Cache *lru.LRU[string, []byte]
}

func evictionLogger(key string, value []byte) {
	fmt.Printf("Evicted key: %s, value: %x\n from the cache", key, value)
}

func NewCache(size int, ttl time.Duration) *Cache {
	return &Cache{
		Cache: lru.NewLRU(size, evictionLogger, ttl),
	}
}