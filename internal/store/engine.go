package store

import (
	"sync"
	"time"
)

// Item represents a single value in our database, along with its expiration metadata.
type Item struct {
	Value     []byte
	ExpiresAt time.Time // The absolute time when this key becomes invalid
	IsDurable bool      // If true, this key never expires
}

// Engine manages the thread-safe in-memory database.
type Engine struct {
	mu    sync.RWMutex
	cache map[string]Item
}

// NewEngine initializes a storage engine and starts the background TTL janitor.
func NewEngine(cleanupInterval time.Duration) *Engine {
	e := &Engine{
		cache: make(map[string]Item),
	}

	// Spawn the background cleaner goroutine
	go e.startJanitor(cleanupInterval)

	return e
}

// Set inserts or updates a key-value pair with an optional duration (TTL).
// If duration is 0, the key lives forever.
func (e *Engine) Set(key string, value []byte, ttl time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()

	item := Item{
		Value: value,
	}

	if ttl > 0 {
		item.ExpiresAt = time.Now().Add(ttl)
		item.IsDurable = false
	} else {
		item.IsDurable = true
	}

	e.cache[key] = item
}

// Get retrieves a value by its key. It returns the value and a boolean
// indicating if the key was found and still valid.
func (e *Engine) Get(key string) ([]byte, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	item, exists := e.cache[key]
	if !exists {
		return nil, false
	}

	// On-the-fly expiration check: if a client requests an expired key
	// before the janitor has deleted it, treat it as non-existent.
	if !item.IsDurable && time.Now().After(item.ExpiresAt) {
		return nil, false
	}

	return item.Value, true
}

// Delete explicitly removes a key from the memory engine.
func (e *Engine) Delete(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.cache, key)
}

// startJanitor runs a loop that periodically sweeps memory to remove expired keys.
func (e *Engine) startJanitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		e.evictExpiredKeys()
	}
}

// evictExpiredKeys scans the database and purges dead records.
func (e *Engine) evictExpiredKeys() {
	// Acquire a full write lock because we are modifying the map structure
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	evictedCount := 0

	for key, item := range e.cache {
		if !item.IsDurable && now.After(item.ExpiresAt) {
			delete(e.cache, key)
			evictedCount++
		}
	}

	if evictedCount > 0 {
		println("[Janitor] Purged", evictedCount, "expired keys from memory.")
	}
}
