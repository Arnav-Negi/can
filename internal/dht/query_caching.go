package dht

import (
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
)

type Cache struct {
	Cache *lru.LRU[string, []byte]
}

func (node *Node) getEvictionLogger() func(key string, value []byte) {
	return func(key string, value []byte) {
		node.logger.Printf("Evicted key: %s, value: %v\n from the cache", key, value)
	}
}

func (node *Node) GetNewCache(size int, ttl time.Duration) *Cache {
	return &Cache{
		Cache: lru.NewLRU(size, node.getEvictionLogger(), ttl),
	}
}
