package kvstore

import (
	"errors"
)

var (
	// ErrKeyNotFound is returned when a key is not found
	ErrKeyNotFound = errors.New("key not found")
	// ErrEmptyStoreKey is returned when attempting to use an empty key
	ErrEmptyStoreKey = errors.New("empty store key")
	// ErrInvalidIterDirections is returned when an invalid number of
	// iteration directions are provided to the Iterate functions
	ErrInvalidIterDirections = errors.New("invalid number of iteration directions")
	// ErrUnknownIterDirection is returned when an unknown iteration
	// direction is provided to the Iterate functions
	ErrUnknownIterDirection = errors.New("unknown iteration direction")
)
