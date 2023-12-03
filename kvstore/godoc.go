// Package kvstore provides a simple key-value store safe for concurrent reads
// and writes and and interface for interacting with the store.
// The KVStore uses []byte for both keys and values, aliased as the Key and Value
// types. This allows for the KVStore to be used with any type that can be
// serialised or converted into a byte slice.
package kvstore
