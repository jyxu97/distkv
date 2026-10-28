// Package consensus - Persistent counter for this node's vector clock entries
//
// When a node coordinates a write it advances its own entry in the write's
// vector clock. That entry must never repeat: if two different writes received
// the same clock they would compare as Equal and one value would be silently
// treated as a replica of the other. Using context[node]+1 is not enough —
// two blind writes, or two writes carrying the same context, would collide.
//
// DotCounter issues values from a single per-node sequence shared by all keys.
// Every value is larger than every value issued before, including before a
// restart: the counter persists a reservation (a high-water mark) before
// handing out values beneath it, and on restart resumes above the reservation.
// Values skipped by a restart are simply never used, which is harmless — a
// vector clock entry only needs to grow, not to be dense.
package consensus

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// DefaultDotReservation is how many values a DotCounter reserves per fsync.
const DefaultDotReservation = 1000

// DotCounter hands out strictly increasing, never-reused counter values for
// this node's entry in new vector clocks. It is safe for concurrent use.
type DotCounter struct {
	mu sync.Mutex

	path        string
	next        uint64 // next value to issue
	reserved    uint64 // highest value covered by the persisted reservation
	reservation uint64 // values reserved per persist
}

// OpenDotCounter loads the counter persisted at path, or starts a new one if
// the file does not exist. reservation values are persisted per fsync.
func OpenDotCounter(path string, reservation uint64) (*DotCounter, error) {
	if reservation == 0 {
		reservation = DefaultDotReservation
	}

	var reserved uint64
	data, err := os.ReadFile(filepath.Clean(path))
	switch {
	case err == nil:
		reserved, err = strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("dot counter: corrupt file %s: %w", path, err)
		}
	case os.IsNotExist(err):
		// Fresh node: start from zero
	default:
		return nil, fmt.Errorf("dot counter: failed to read %s: %w", path, err)
	}

	return &DotCounter{
		path: path,
		// Values up to the reservation may have been issued before a crash
		next:        reserved + 1,
		reserved:    reserved,
		reservation: reservation,
	}, nil
}

// Next returns a value greater than every value previously issued and greater
// than floor. Callers pass the node's entry from the write's causal context as
// floor, so the new entry always advances past what the client has seen.
func (c *DotCounter) Next(floor uint64) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.next <= floor {
		c.next = floor + 1
	}
	if c.next > c.reserved {
		reserved := c.next + c.reservation - 1
		if err := c.persist(reserved); err != nil {
			return 0, err
		}
		c.reserved = reserved
	}

	value := c.next
	c.next++
	return value, nil
}

// persist durably records the reservation: write a temp file, fsync it, rename
// it over the old one, and fsync the directory so the rename survives a crash.
func (c *DotCounter) persist(reserved uint64) error {
	dir := filepath.Dir(c.path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("dot counter: failed to create directory: %w", err)
	}

	tmp := c.path + ".tmp"
	f, err := os.OpenFile(filepath.Clean(tmp), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("dot counter: failed to open temp file: %w", err)
	}
	if _, err := f.WriteString(strconv.FormatUint(reserved, 10)); err != nil {
		_ = f.Close()
		return fmt.Errorf("dot counter: failed to write: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("dot counter: failed to fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("dot counter: failed to close: %w", err)
	}
	if err := os.Rename(tmp, c.path); err != nil {
		return fmt.Errorf("dot counter: failed to rename: %w", err)
	}

	d, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return fmt.Errorf("dot counter: failed to open directory: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("dot counter: failed to fsync directory: %w", err)
	}
	return nil
}

// NewVersion builds the vector clock for a new write coordinated by nodeID:
// the causal context the client supplied (nil or empty for a blind write) with
// nodeID's entry advanced to a fresh value from counter. The context is not
// modified.
func NewVersion(context *VectorClock, nodeID string, counter *DotCounter) (*VectorClock, error) {
	clocks := map[string]uint64{}
	if context != nil {
		clocks = context.GetClocks()
	}

	dot, err := counter.Next(clocks[nodeID])
	if err != nil {
		return nil, err
	}
	clocks[nodeID] = dot
	return NewVectorClockFromMap(clocks), nil
}
