package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// defaultBatchSize is the default number of entries to batch together before
// writing to the log file, this can be changed by utilising the WithBatchSize
// option, when creating a new WAL.
const defaultBatchSize = 32

var _ WAL = (*wal)(nil)

type wal struct {
	errGroup    *errgroup.Group
	logPath     string
	batchSize   int
	entryClosed atomic.Bool
	entryChan   chan Entry
}

// NewWriteAheadLogger creates a new Write Ahead Logger with the given options.
func NewWriteAheadLogger(ctx context.Context, opts ...walOption) (WAL, error) {
	wal := &wal{
		batchSize: defaultBatchSize,
		entryChan: make(chan Entry),
	}
	for _, opt := range opts {
		opt(wal)
	}
	if wal.logPath == "" {
		return nil, ErrWALNoLogPath
	}
	return wal, nil
}

// Start starts the WAL, opening the log file and starting the batch writing
// goroutine. This is non-blocking, only returning a synchronous error if the
// writer fails to open the log file.
func (w *wal) Start(ctx context.Context) error {
	// Create an error group to handle the batch writing goroutine.
	g, ctx := errgroup.WithContext(ctx)
	// Store the error group on the WAL
	w.errGroup = g
	// Call the writer function in a goroutine, so as to not block the caller.
	w.errGroup.Go(func() error {
		return w.writer(ctx)
	})
	return nil
}

// Write sends the given entry to the entry channel, to be written to the log
// file, once the batch size is reached.
func (w *wal) Write(entry Entry) error {
	if w.entryClosed.Load() {
		return errors.Join(ErrWALWritingEntry, errors.New("entry channel closed"))
	}
	w.entryChan <- entry
	return nil
}

// Close stops the WAL, closing the entry channel and flushing any remaining
// entries to the log file.
func (w *wal) Close() error {
	// Close the entry channel, signalling to the writer that no more entries
	// will be sent, once this channel is closed all pending batch entries will
	// be written to the log file.
	close(w.entryChan)
	// Set the entryClosed flag to true, so that any subsequent calls to Write
	// will return an error.
	w.entryClosed.CompareAndSwap(false, true)
	// Wait for the all goroutines to finish, returning any errors.
	return w.errGroup.Wait()
}

func (w *wal) writer(ctx context.Context) error {
	// Attempt to open the log file, creating it if it doesn't exist.
	logFile, err := os.OpenFile(w.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Join(ErrWALOpeningFile, err)
	}
	// Defer closing the log file.
	defer logFile.Close()
	w.errGroup.Go(func() error {
		return w.goWriteBatch(ctx, logFile)
	})
	return w.errGroup.Wait()
}

// goWriteBatch is a goroutine that collects entries from the entries channel
// and writes them to the log file, according to the batch size.
// It is intended to be run in a goroutine.
func (w *wal) goWriteBatch(ctx context.Context, logFile *os.File) error {
	batch := make([]Entry, 0, w.batchSize)
	for {
		select {
		case entry, ok := <-w.entryChan:
			if !ok {
				// Ensure the entryClosed flag to true
				w.entryClosed.CompareAndSwap(false, true)
				// If the entry channel is closed, flush any remaining entries
				// to the log file and return.
				return w.writeBatchToFile(logFile, batch)
			}
			batch = append(batch, entry) // append the entry to the batch
			if len(batch) == w.batchSize {
				if err := w.writeBatchToFile(logFile, batch); err != nil {
					return err // handle and return the error
				}
				batch = make([]Entry, 0, w.batchSize) // reset the batch
			}
		case <-ctx.Done():
			return ctx.Err() // handle context cancellation
		}
	}
}

// writeBatchToFile writes the given batch of entries to the log file, returning
// any errors that occur or if the number of bytes written does not match the
// expected number of bytes per entry.
func (w *wal) writeBatchToFile(logFile *os.File, batch []Entry) error {
	for _, entry := range batch {
		// Write the entry to the log file, appending a newline.
		n, err := logFile.Write(append(entry, '\n'))
		// Send any errors to the errChan.
		if err != nil {
			return errors.Join(ErrWALWritingEntry, err)
		}
		// Ensure the correct number of bytes were written (including the newline).
		if n != len(entry)+1 {
			return errors.Join(
				ErrWALWritingEntry,
				fmt.Errorf("wrote %d bytes, expected %d", n, len(entry)),
			)
		}
	}
	return nil
}
