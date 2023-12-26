package wal

type walOption func(*wal)

// WithLogPath sets the log path of the WAL.
func WithLogPath(path string) walOption {
	return func(w *wal) {
		w.logPath = path
	}
}

// WithBatchSize sets the batch size of the WAL.
func WithBatchSize(size int) walOption {
	return func(w *wal) {
		w.batchSize = size
	}
}
