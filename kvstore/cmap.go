package kvstore

import (
	"bytes"
	"maps"
	"slices"
	"sync"
)

var _ KVStore = (*cmap)(nil)

// cmap is a struct that implements the KVStore interface.
// Using a RWMutex around the map we can ensure the map is concurrency safe.
type cmap struct {
	mu sync.RWMutex
	m  map[string][]byte
}

// NewKVStore creates a new instace of the KVStore interface.
func NewKVStore() KVStore {
	return &cmap{
		mu: sync.RWMutex{},
		m:  make(map[string][]byte),
	}
}

// Get returns the value associated with the given key.
func (c *cmap) Get(key Key) (Value, error) {
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

// GetPrefix returns all keys in the store with the given prefix
// To retrieve all keys in the store, pass in an a nil prefix.
func (c *cmap) GetPrefix(prefix Key) ([]Value, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	values := make([]Value, 0, len(c.m))
	for k, v := range c.m {
		if bytes.HasPrefix([]byte(k), prefix) {
			values = append(values, v)
		}
	}
	return values, nil
}

// Has checks whether the given key exists in the store or not.
func (c *cmap) Has(key Key) (bool, error) {
	if len(key) == 0 {
		return false, ErrEmptyStoreKey
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.m[string(key)]
	return ok, nil
}

// Set sets/updates the value associated with the given key in the store.
func (c *cmap) Set(key Key, value Value) error {
	if len(key) == 0 {
		return ErrEmptyStoreKey
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[string(key)] = value
	return nil
}

// Delete deletes the given key from the store.
func (c *cmap) Delete(key Key) error {
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
func (c *cmap) DeletePrefix(prefix Key) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	maps.DeleteFunc(c.m, func(key string, _ []byte) bool {
		return bytes.HasPrefix([]byte(key), prefix)
	})
	return nil
}

// ClearAll deletes all key-value pairs from the store.
func (c *cmap) ClearAll() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m = make(map[string][]byte)
	return nil
}

// Iterate iterates over all key-value pairs in the store with the
// provided prefix, in the specified direction (or forwards if not
// specified), and invokes the provided consumer function on each
// key-value pair. If the consumer function returns false, then the
// iteration is stopped.
// The consumer function is invoked with a copy of the key and value.
// and does not mutate the store.
func (c *cmap) Iterate(
	prefix Key,
	consumer IteratorConsumerFn,
	direction ...IterDirection,
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
	switch iterDir {
	case IterDirectionReverse:
		slices.Reverse(keys)
	}
	for _, k := range keys {
		v := bytes.Clone(c.m[k])
		if !consumer(bytes.Clone([]byte(k)), v) {
			break
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
func (c *cmap) IterateKeys(
	prefix Key,
	consumer IteratorKeysConsumerFn,
	direction ...IterDirection,
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
	switch iterDir {
	case IterDirectionReverse:
		slices.Reverse(keys)
	}
	for _, k := range keys {
		if !consumer(bytes.Clone([]byte(k))) {
			break
		}
	}
	return nil
}

// Len returns the number of key-value pairs in the store.
func (c *cmap) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}

// Clone returns a copy of the store.
func (c *cmap) Clone() KVStore {
	c.mu.RLock()
	defer c.mu.RUnlock()
	newMap := maps.Clone(c.m)
	return &cmap{
		mu: sync.RWMutex{},
		m:  newMap,
	}
}

// getIterDirection returns the iteration direction from the provided spead
// of directions. If no direction is provided, then the default forward
// direction is returned. If more than one direction is provided, then an
// error is returned, or if the provided direction is unknown, then an error
// is returned.
func getIterDirection(direction ...IterDirection) (IterDirection, error) {
	if len(direction) == 0 {
		return IterDirectionForward, nil
	} else if len(direction) > 1 {
		return IterDirectionForward, ErrInvalidIterDirections
	}
	switch direction[0] {
	case IterDirectionForward:
		return IterDirectionForward, nil
	case IterDirectionReverse:
		return IterDirectionReverse, nil
	default:
		return IterDirectionForward, ErrUnknownIterDirection
	}
}
