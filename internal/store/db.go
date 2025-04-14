package store

import (
	// "database/sql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	// "log"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type Store interface {
	Insert(string, []byte) error
	Retrieve(string) ([]byte, error)
	Delete(string) error
}

type MemoryStore struct {
	mu    sync.RWMutex
	Store map[string][]byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		mu:    sync.RWMutex{},
		Store: make(map[string][]byte),
	}
}

func (m *MemoryStore) Insert(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Store[key] = value
}

func (m *MemoryStore) Retrieve(key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	Value, ok := m.Store[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "Key not found")
	}
	return Value, nil
}

func (m *MemoryStore) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.Store, key)
}

func (m *MemoryStore) ForEach(callback func(key string, value []byte)) {
	for key, value := range m.Store {
		callback(key, value)
	}
}

// // Credits for some of the design decisions
// // https://brandonrozek.com/blog/simple-kv-store-sqlite/
// type KVStore struct {
// 	DB    *sql.DB
// 	Mutex *sync.Mutex
// }

// func InitDB() (*KVStore, error) {
// 	DB, err := sql.Open("sqlite3", "kv.db")
// 	if err != nil {
// 		log.Fatalf("Failed to connect to database: %v", err)
// 		return nil, err
// 	}

// 	createUserTable := `
// 		CREATE TABLE IF NOT EXISTS kvstore (
// 			key TEXT NOT NULL UNIQUE,
// 			value TEXT NOT NULL,
// 		);
// 	`

// 	_, err = DB.Exec(createUserTable)
// 	if err != nil {
// 		log.Fatalf("Failed to create table: %v", err)
// 		return nil, err
// 	}

// 	log.Println("Database initialized successfully")
// 	return &KVStore{
// 		DB: DB,
// 	}, nil
// }

// func NewKVStore() (*KVStore, error) {
// 	kvStore, err := InitDB()
// 	if err != nil {
// 		log.Fatalf("Failed to initialize KVStore: %v", err)
// 		return nil, err
// 	}
// 	kvStore.Mutex = &sync.Mutex{}
// 	return kvStore, nil
// }

// // Insert/Set/Put method for the KV Store
// func (kv *KVStore) Insert(key string, value []byte) error {
// 	kv.Mutex.Lock()
// 	defer kv.Mutex.Unlock()

// 	// Insert NEW KEY-VALUE pair if key does not exist
// 	// else update value for that key on conflicting key
// 	_, err := kv.DB.Exec(`INSERT OR REPLACE INTO kvstore 
// 	(key, value) VALUES (?, ?) 
// 	ON CONFLICT (key) DO UPDATE 
// 	SET value=?`,
// 		key, value, value)
// 	if err != nil {
// 		log.Printf("Failed to insert key-value pair: %v", err)
// 		return err
// 	}
// 	return nil
// }

// // Retrieve/Get/Lookup method for the KV Store
// func (kv *KVStore) Retrieve(key string) ([]byte, error) {
// 	kv.Mutex.Lock()
// 	defer kv.Mutex.Unlock()

// 	var value []byte
// 	err := kv.DB.QueryRow("SELECT value FROM kvstore WHERE key = ?", key).Scan(&value)
// 	if err != nil {
// 		log.Printf("Failed to retrieve value for key %v: %v", key, err)

// 		var nilV []byte
// 		return nilV, err
// 	}
// 	return value, nil
// }

// // Delete/Remove method for the KV Store
// func (kv *KVStore) Delete(key string) error {
// 	kv.Mutex.Lock()
// 	defer kv.Mutex.Unlock()

// 	_, err := kv.DB.Exec("DELETE FROM kvstore WHERE key = ?", key)
// 	if err != nil {
// 		log.Printf("Failed to delete key %v: %v", key, err)
// 		return err
// 	}
// 	return nil
// }
