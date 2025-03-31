package store

import (
	"database/sql"
	"log"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

// Credits for some of the design decisions
// https://brandonrozek.com/blog/simple-kv-store-sqlite/
type KVStore[K comparable, V any] struct {
	DB		*sql.DB
	Mutex	*sync.Mutex
}

func InitDB[K comparable, V any]() (*KVStore[K, V], error) {
	DB, err := sql.Open("sqlite3", "kv.db")
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
		return nil, err
	}

	createUserTable := `
		CREATE TABLE IF NOT EXISTS kvstore (
			key TEXT NOT NULL UNIQUE,
			value TEXT NOT NULL,
		);
	`

	_, err = DB.Exec(createUserTable)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
		return nil, err
	}

	log.Println("Database initialized successfully")
	return &KVStore[K, V]{
		DB: DB,
	}, nil
}

func NewKVStore[K comparable, V any]() (*KVStore[K, V], error) {
	kvStore, err := InitDB[K, V]()
	if err != nil {
		log.Fatalf("Failed to initialize KVStore: %v", err)
		return nil, err
	}
	kvStore.Mutex = &sync.Mutex{}
	return kvStore, nil
}


// Insert/Set/Put method for the KV Store
func (kv *KVStore[K, V]) Insert(key K, value V) error {
	kv.Mutex.Lock()
	defer kv.Mutex.Unlock()
	
	// Insert NEW KEY-VALUE pair if key does not exist
	// else update value for that key on conflicting key
	_, err := kv.DB.Exec(`INSERT OR REPLACE INTO kvstore 
	(key, value) VALUES (?, ?) 
	ON CONFLICT (key) DO UPDATE 
	SET value=?`, 
	key, value, value)
	if err != nil {
		log.Printf("Failed to insert key-value pair: %v", err)
		return err
	}
	return nil
}

// Retrieve/Get/Lookup method for the KV Store
func (kv *KVStore[K, V]) Retrieve(key K) (V, error) {
	kv.Mutex.Lock()
	defer kv.Mutex.Unlock()
	
	var value V
	err := kv.DB.QueryRow("SELECT value FROM kvstore WHERE key = ?", key).Scan(&value)
	if err != nil {
		log.Printf("Failed to retrieve value for key %v: %v", key, err)

		var nilV V
		return nilV, err
	}
	return value, nil
}

// Delete/Remove method for the KV Store
func (kv *KVStore[K, V]) Delete(key K) error {
	kv.Mutex.Lock()
	defer kv.Mutex.Unlock()

	_, err := kv.DB.Exec("DELETE FROM kvstore WHERE key = ?", key)
	if err != nil {
		log.Printf("Failed to delete key %v: %v", key, err)
		return err
	}
	return nil
}