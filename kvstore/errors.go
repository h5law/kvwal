package kvstore

import (
	"errors"
)

var (
	// ErrKVStoreKeyNotFound is returned when a key is not found
	ErrKVStoreKeyNotFound = errors.New("key not found")
	// ErrKVStoreEmptyStoreKey is returned when attempting to use an empty key
	ErrKVStoreEmptyStoreKey = errors.New("empty store key")
	// ErrKVStoreInvalidIterDirections is returned when an invalid number of
	// iteration directions are provided to the Iterate functions
	ErrKVStoreInvalidIterDirections = errors.New("invalid number of iteration directions")
	// ErrKVStoreUnknownIterDirection is returned when an unknown iteration
	// direction is provided to the Iterate functions
	ErrKVStoreUnknownIterDirection = errors.New("unknown iteration direction")
)
