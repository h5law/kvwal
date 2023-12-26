package wal

import (
	"errors"
)

var (
	// ErrWALNoLogPath is returned when the WAL is created without a log path.
	ErrWALNoLogPath = errors.New("no log path provided")
	// ErrWALOpeningFile is returned when the WAL cannot open the file.
	ErrWALOpeningFile = errors.New("error opening file")
	// ErrWALWritingEntry is returned when the WAL cannot write an entry.
	ErrWALWritingEntry = errors.New("error writing entry")
)
