// Package wal provides a write-ahead-logger that can be used for a key-value
// store. It itself is concurrency safe and uses batched writes to improve
// performance - the batch size can be configured on creation. A single routine
// is used to perform the writes to the log, and multiple goroutines are used
// to queue the writes. The log is flushed to disk when the batch size is
// reached.
package wal
