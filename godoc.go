// Package kvwal provides both a concurrency safe key-value store and an
// (optional) append-only write-ahead log that can be used to persist changes
// to the store and rebuild it from the log, in case of a crash or shutdown.
package kvwal
