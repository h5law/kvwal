package kvstore

type (
	// Key is a type alias for a byte slice
	Key []byte
	// Value is a type alias for a byte slice
	Value []byte
	// IterDirection is an alias for the enum that defines the direction of iteration
	IterDirection byte
)

const (
	// IterDirectionForward is used to iterate over keys sorted in ascending order
	IterDirectionForward IterDirection = iota
	// IterDirectionReverse is used to iterate over keys sorted in descending order
	IterDirectionReverse
)

type (
	// IteratorConsumerFn is a function that is invoked on each key-value pair
	// during iteration. If the function returns false, then the iteration is
	// stopped. It is invoked with a copy of the key and value, and does not
	// mutate the store.
	IteratorConsumerFn func(key Key, value Value) bool
	// IteratorKeysConsumerFn is a function that is invoked on each key during
	// iteration. If the function returns false, then the iteration is stopped.
	// It is invoked with a copy of the key, and does not mutate the store.
	IteratorKeysConsumerFn func(key Key) bool
)

// KVStore is an interface that defines the behaviour for a key-value store.
type KVStore interface {
	// --- Accessors ---

	// Get retrieves the key from the store
	Get(key Key) (Value, error)
	// GetPrefix retrieves all keys with the given prefix
	GetPrefix(prefix Key) ([]Value, error)
	// Has checks whether the key exists in the store
	Has(key Key) (bool, error)

	// --- Mutations ---

	// Set sets/updates a key-value pair in the store
	Set(key Key, val Value) error
	// Delete removes a key-value pair from the store
	Delete(key Key) error
	// DeletePrefix removes all key-value pairs with the given prefix
	DeletePrefix(prefix Key) error
	// ClearAll removes all key-value pairs from the store
	ClearAll() error

	// --- Iteration ---

	// Iterate iterates over all key-value pairs in the store with the
	// provided prefix, in the specified direction (or forwards if not
	// specified), and invokes the provided consumer function on each
	// key-value pair. If the consumer function returns false, then the
	// iteration is stopped.
	Iterate(prefix Key, consumer IteratorConsumerFn, direction ...IterDirection) error
	// IterateKeys iterates over all keys in the store with the provided
	// prefix, in the specified direction (or forwards if not specified),
	// and invokes the provided consumer function on each key. If the
	// consumer function returns false, then the iteration is stopped.
	IterateKeys(prefix Key, consumer IteratorKeysConsumerFn, direction ...IterDirection) error

	// --- Operations ---

	// Len returns the number of key-value pairs in the store
	Len() int
	// Clone returns a shallow copy of the store
	Clone() KVStore
}
