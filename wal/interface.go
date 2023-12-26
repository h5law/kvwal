package wal

import (
	"context"
)

// Entry is a type that represents a single entry in the log.
type Entry []byte

// WAL is an interface that represents a write-ahead-log.
type WAL interface {
	// Start the Write Ahead Logger, it takes a cancellable context.
	Start(context.Context) error
	// Write the give entry to the log.
	Write(Entry) error
	// Close the Write Ahead Logger and cancels the context.
	Close() error
}
