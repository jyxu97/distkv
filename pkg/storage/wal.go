// Package storage - Write-Ahead Log (WAL)
//
// The WAL closes the durability gap in the LSM write path. Without it, a write
// that has been acknowledged (and, at the cluster level, counted toward a W
// quorum) lives only in the in-memory MemTable until the next flush. If the
// process crashes before that flush, the write is lost even though the client
// was told it succeeded.
//
// With the WAL, every mutation is appended to an on-disk log and fsync'd
// BEFORE it is inserted into the MemTable. On restart the engine replays the
// log to rebuild the MemTable, so any acknowledged-but-unflushed write is
// recovered. Once a MemTable is durably flushed to an SSTable, the WAL that
// covered it can be truncated.
//
// Record format (all integers little-endian):
//
//	+---------------+---------------+-----------------------+
//	| length uint32 |  crc32 uint32 |  payload ([length]B)  |
//	+---------------+---------------+-----------------------+
//
// The payload is the JSON encoding of an Entry (the same encoding used by
// SSTables, so vector clocks are preserved). The CRC is computed over the
// payload only and lets replay detect a torn tail record from a crash mid-append.
package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// walRecordHeaderSize is the fixed size of the per-record header:
// 4 bytes length + 4 bytes CRC32.
const walRecordHeaderSize = 8

// walFileName is the name of the active write-ahead log file inside the data dir.
const walFileName = "wal.log"

// WAL is an append-only, crash-safe log of storage mutations.
//
// It is safe for concurrent use: appends are serialized by an internal mutex.
// The engine already serializes writes under its own lock, but the WAL guards
// itself so it remains correct if used independently (e.g. in tests).
type WAL struct {
	mu sync.Mutex

	path   string
	file   *os.File
	writer *bufio.Writer

	// syncOnAppend controls whether each Append fsyncs the file before
	// returning. This MUST be true for real durability; tests may disable it
	// to isolate behavior unrelated to fsync cost.
	syncOnAppend bool

	// closed guards against use-after-close.
	closed bool
}

// OpenWAL opens (creating if necessary) the write-ahead log at the given path.
// Existing contents are preserved and new records are appended after them, so
// callers should Replay before appending if they need the prior contents.
func OpenWAL(path string, syncOnAppend bool) (*WAL, error) {
	if path == "" {
		return nil, fmt.Errorf("wal: path cannot be empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("wal: failed to create directory: %w", err)
	}

	// O_APPEND guarantees each write lands at the current end of file even if
	// something else writes concurrently at the OS level.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open log file: %w", err)
	}

	return &WAL{
		path:         path,
		file:         file,
		writer:       bufio.NewWriter(file),
		syncOnAppend: syncOnAppend,
	}, nil
}

// Append serializes the entry, writes a length-prefixed, CRC-checked record,
// and (when syncOnAppend is set) fsyncs the file so the record is durable
// before the method returns. The engine calls this before touching the
// MemTable, so a returned nil error means the write will survive a crash.
func (w *WAL) Append(entry *Entry) error {
	if entry == nil {
		return fmt.Errorf("wal: cannot append nil entry")
	}

	payload, err := serializeEntry(entry)
	if err != nil {
		return fmt.Errorf("wal: failed to serialize entry: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("wal: append on closed log")
	}

	var header [walRecordHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))

	if _, err := w.writer.Write(header[:]); err != nil {
		return fmt.Errorf("wal: failed to write record header: %w", err)
	}
	if _, err := w.writer.Write(payload); err != nil {
		return fmt.Errorf("wal: failed to write record payload: %w", err)
	}
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: failed to flush record to OS: %w", err)
	}
	if w.syncOnAppend {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("wal: failed to fsync record: %w", err)
		}
	}
	return nil
}

// Sync flushes buffered data and fsyncs the underlying file. Useful when
// syncOnAppend is disabled and the caller wants an explicit durability point.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return fmt.Errorf("wal: sync on closed log")
	}
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Truncate resets the log to empty. It is called after the MemTable it covered
// has been durably flushed to an SSTable, so the records are no longer needed
// for recovery. The file handle is reopened so subsequent appends start at
// offset zero.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return fmt.Errorf("wal: truncate on closed log")
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush before truncate failed: %w", err)
	}
	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("wal: truncate failed: %w", err)
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek after truncate failed: %w", err)
	}
	// Persist the truncation itself.
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync after truncate failed: %w", err)
	}
	w.writer.Reset(w.file)
	return nil
}

// Rotate seals the current log by renaming it to a sidecar segment and starts a
// fresh, empty active log at the original path. It returns the path of the
// sealed segment. Records appended after Rotate go to the new active log; the
// sealed segment still describes the MemTable being flushed and must be
// retained until that flush is durable, then removed with RemoveSegment.
//
// This is the mechanism that makes WAL truncation safe under concurrent writes:
// a write that arrives during a flush lands in the new active log and is never
// discarded when the sealed segment for the flushed MemTable is removed.
func (w *WAL) Rotate() (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return "", fmt.Errorf("wal: rotate on closed log")
	}

	if err := w.writer.Flush(); err != nil {
		return "", fmt.Errorf("wal: flush before rotate failed: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return "", fmt.Errorf("wal: sync before rotate failed: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return "", fmt.Errorf("wal: close before rotate failed: %w", err)
	}

	sealedPath := fmt.Sprintf("%s.%d.sealed", w.path, time.Now().UnixNano())
	if err := os.Rename(w.path, sealedPath); err != nil {
		// Best-effort reopen so the WAL remains usable on failure.
		if f, reopenErr := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644); reopenErr == nil {
			w.file = f
			w.writer = bufio.NewWriter(f)
		}
		return "", fmt.Errorf("wal: rename during rotate failed: %w", err)
	}

	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return "", fmt.Errorf("wal: failed to open fresh log after rotate: %w", err)
	}
	w.file = file
	w.writer = bufio.NewWriter(file)
	return sealedPath, nil
}

// RemoveSegment deletes a sealed WAL segment produced by Rotate. Call it only
// after the MemTable that segment covered has been durably flushed to an SSTable.
func RemoveSegment(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("wal: failed to remove sealed segment: %w", err)
	}
	return nil
}

// Close flushes and closes the log file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	flushErr := w.writer.Flush()
	closeErr := w.file.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// Replay reads every intact record from the log file at path and invokes fn for
// each recovered Entry, in append order. It tolerates a torn final record: if
// the log ends mid-header or mid-payload, or the final record's CRC does not
// match (both classic symptoms of a crash during append), replay stops cleanly
// at the last good record and returns the number of records recovered without
// an error. A CRC mismatch in a non-final record is treated as corruption and
// returned as an error, since data after it cannot be trusted.
//
// Replay opens its own read-only handle and does not require an open WAL, so it
// can run during engine startup before the writable WAL is opened.
func ReplayWAL(path string, fn func(*Entry) error) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // No log yet: nothing to recover.
		}
		return 0, fmt.Errorf("wal: failed to open log for replay: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	recovered := 0

	for {
		var header [walRecordHeaderSize]byte
		n, err := io.ReadFull(reader, header[:])
		if err == io.EOF {
			// Clean end of log on a record boundary.
			return recovered, nil
		}
		if err == io.ErrUnexpectedEOF || (err == nil && n < walRecordHeaderSize) {
			// Partial header: torn tail from a crash. Stop cleanly.
			return recovered, nil
		}
		if err != nil {
			return recovered, fmt.Errorf("wal: failed to read record header: %w", err)
		}

		length := binary.LittleEndian.Uint32(header[0:4])
		wantCRC := binary.LittleEndian.Uint32(header[4:8])

		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			// Truncated payload: torn tail. Stop cleanly at the last good record.
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return recovered, nil
			}
			return recovered, fmt.Errorf("wal: failed to read record payload: %w", err)
		}

		if crc32.ChecksumIEEE(payload) != wantCRC {
			// A CRC mismatch on what turns out to be the final bytes of the file
			// is a torn write; anything after a bad record in the middle of the
			// file is untrustworthy. Peek to decide which case we're in.
			if _, peekErr := reader.Peek(1); peekErr == io.EOF {
				return recovered, nil // Torn tail record: tolerate.
			}
			return recovered, fmt.Errorf("wal: checksum mismatch in non-final record at record %d", recovered)
		}

		entry, err := deserializeEntry(payload)
		if err != nil {
			return recovered, fmt.Errorf("wal: failed to deserialize record %d: %w", recovered, err)
		}
		if err := fn(entry); err != nil {
			return recovered, fmt.Errorf("wal: replay callback failed at record %d: %w", recovered, err)
		}
		recovered++
	}
}
