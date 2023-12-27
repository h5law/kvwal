package wal

type WalOption func(*Logger)

// WithLogPath sets the log path of the WAL.
func WithLogPath(path string) WalOption {
	return func(w *Logger) {
		w.logPath = path
	}
}

// WithBatchSize sets the batch size of the WAL.
func WithBatchSize(size int) WalOption {
	return func(w *Logger) {
		w.batchSize = size
	}
}
