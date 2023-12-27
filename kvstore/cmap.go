package kvstore

import (
	"bytes"
	"maps"
	"slices"
	"sync"

	"github.com/h5law/kvwal"
)

var _ kvwal.KVStore = (*ConcurrentMap)(nil)

// ConcurrentMap is a struct that implements the KVStore interface.
// Using a RWMutex around the map we can ensure the map is concurrency safe.
type ConcurrentMap struct {
	mu sync.RWMutex
	m  map[string]kvwal.Value
}

// NewKVStore creates a new instance of the KVStore interface.
func NewKVStore() kvwal.KVStore {
	return &ConcurrentMap{
		mu: sync.RWMutex{},
		m:  make(map[string]kvwal.Value),
	}
}

// Get returns the value associated with the given key.
func (c *ConcurrentMap) Get(key kvwal.Key) (kvwal.Value, error) {
	if len(key) == 0 {
		return nil, ErrEmptyStoreKey
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v, ok := c.m[string(key)]; ok {
		return v, nil
	}
	return nil, ErrKeyNotFound
}

// GetAll returns all keys and values in the store.
func (c *ConcurrentMap) GetAll() ([]kvwal.Key, []kvwal.Value) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]kvwal.Key, 0, len(c.m))
	values := make([]kvwal.Value, 0, len(c.m))
	for k, v := range c.m {
		keys = append(keys, []byte(k))
		values = append(values, v)
	}
	return keys, values
}

// GetPrefix returns all values in the store who's key has the given prefix.
func (c *ConcurrentMap) GetPrefix(prefix kvwal.KeyPrefix) []kvwal.Value {
	c.mu.RLock()
	defer c.mu.RUnlock()
	values := make([]kvwal.Value, 0, len(c.m))
	for k, v := range c.m {
		if bytes.HasPrefix([]byte(k), prefix) {
			values = append(values, v)
		}
	}
	return values
}

// Has checks whether the given key exists in the store or not.
func (c *ConcurrentMap) Has(key kvwal.Key) (bool, error) {
	if len(key) == 0 {
		return false, ErrEmptyStoreKey
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.m[string(key)]
	return ok, nil
}

// Set sets/updates the value associated with the given key in the store.
func (c *ConcurrentMap) Set(key kvwal.Key, value kvwal.Value) error {
	if len(key) == 0 {
		return ErrEmptyStoreKey
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[string(key)] = value
	return nil
}

// Delete deletes the given key from the store.
func (c *ConcurrentMap) Delete(key kvwal.Key) error {
	if len(key) == 0 {
		return ErrEmptyStoreKey
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	k := string(key)
	if _, ok := c.m[k]; ok {
		delete(c.m, k)
		return nil
	}
	return ErrKeyNotFound
}

// DeletePrefix deletes all keys with the given prefix from the store.
// To delete all keys from the store, pass in a nil prefix.
func (c *ConcurrentMap) DeletePrefix(prefix kvwal.KeyPrefix) {
	c.mu.Lock()
	defer c.mu.Unlock()
	maps.DeleteFunc(c.m, func(key string, _ kvwal.Value) bool {
		return bytes.HasPrefix([]byte(key), prefix)
	})
}

// ClearAll deletes all key-value pairs from the store.
func (c *ConcurrentMap) ClearAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m = make(map[string]kvwal.Value)
}

// Iterate iterates over all key-value pairs in the store with the
// provided prefix, in the specified direction (or forwards if not
// specified), and invokes the provided consumer function on each
// key-value pair. If the consumer function returns false, then the
// iteration is stopped.
// The consumer function is invoked with a copy of the key and value
// and does not mutate the store.
func (c *ConcurrentMap) Iterate(
	prefix kvwal.KeyPrefix,
	consumer kvwal.IteratorConsumerFn,
	direction ...kvwal.IterDirection,
) (err error) {
	iterDir, err := getIterDirection(direction...)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(c.m))
	c.mu.RLock()
	for k := range c.m {
		if bytes.HasPrefix([]byte(k), prefix) {
			keys = append(keys, k)
		}
	}
	c.mu.RUnlock()
	slices.Sort(keys)
	if iterDir == kvwal.IterDirectionReverse {
		slices.Reverse(keys)
	}
	for _, k := range keys {
		key := bytes.Clone([]byte(k)) // Copy the key
		value := bytes.Clone(c.m[k])  // Copy the value
		if !consumer(key, value) {
			return nil
		}
	}
	return nil
}

// IterateKeys iterates over all keys in the store with the provided
// prefix, in the specified direction (or forwards if not specified),
// and invokes the provided consumer function on each key. If the
// consumer function returns false, then the iteration is stopped.
// The consumer function is invoked with a copy of the key, and does
// not mutate the store.
func (c *ConcurrentMap) IterateKeys(
	prefix kvwal.KeyPrefix,
	consumer kvwal.IteratorKeysConsumerFn,
	direction ...kvwal.IterDirection,
) (err error) {
	iterDir, err := getIterDirection(direction...)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(c.m))
	c.mu.RLock()
	for k := range c.m {
		if bytes.HasPrefix([]byte(k), prefix) {
			keys = append(keys, k)
		}
	}
	c.mu.RUnlock()
	slices.Sort(keys)
	if iterDir == kvwal.IterDirectionReverse {
		slices.Reverse(keys)
	}
	for _, k := range keys {
		if !consumer(bytes.Clone([]byte(k))) {
			return nil
		}
	}
	return nil
}

// Len returns the number of key-value pairs in the store.
func (c *ConcurrentMap) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}

// Clone returns a copy of the store.
func (c *ConcurrentMap) Clone() kvwal.KVStore {
	c.mu.RLock()
	defer c.mu.RUnlock()
	newMap := maps.Clone(c.m)
	return &ConcurrentMap{
		mu: sync.RWMutex{},
		m:  newMap,
	}
}

// Equal returns true if the provided store is equal to the current store,
// by using an iterator to compare the key-value pairs in both stores.
func (c *ConcurrentMap) Equal(other kvwal.KVStore) (bool, error) {
	c.mu.RLock()
	selfKeys, selfValues := c.GetAll()
	c.mu.RUnlock()
	otherKeys, otherValues := other.GetAll()
	if len(selfKeys) != len(otherKeys) || len(selfValues) != len(otherValues) {
		return false, nil
	}
	eq := true
	if err := other.Iterate(nil, func(key kvwal.Key, value kvwal.Value) bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		if v, ok := c.m[string(key)]; !ok || !bytes.Equal(v, value) {
			eq = false
			return false
		}
		return true
	}); err != nil {
		return false, err
	}
	return eq, nil
}

// getIterDirection returns the iteration direction from the provided set
// of directions. If no direction is provided, then the default forward
// direction is returned. If more than one direction is provided, then an
// error is returned, or if the provided direction is unknown, then an error
// is returned.
func getIterDirection(direction ...kvwal.IterDirection) (kvwal.IterDirection, error) {
	if len(direction) == 0 {
		return kvwal.IterDirectionForward, nil
	} else if len(direction) > 1 {
		return kvwal.IterDirectionForward, ErrInvalidIterDirections
	}
	switch direction[0] {
	case kvwal.IterDirectionForward:
		return kvwal.IterDirectionForward, nil
	case kvwal.IterDirectionReverse:
		return kvwal.IterDirectionReverse, nil
	default:
		return kvwal.IterDirectionForward, ErrUnknownIterDirection
	}
}
