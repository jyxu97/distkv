// Package storage - MemTable implementation
// MemTable is an in-memory sorted data structure that buffers writes
// before they are flushed to disk as SSTables.
package storage

import (
	"distkv/pkg/consensus"
	"sync"
)

// MemTable stores key-value pairs in memory using a sorted structure.
// All writes go to the MemTable first for fast write performance.
// When MemTable gets too large, it's flushed to an SSTable on disk.
type MemTable struct {
	// data stores entries sorted by key for efficient range queries
	// We use a slice instead of a tree for simplicity in this implementation
	data []Entry

	// index maps keys to positions in data slice for O(log n) lookups
	index map[string]int

	// size tracks the approximate memory usage in bytes
	size int64

	// mutex protects concurrent access to the MemTable
	mutex sync.RWMutex

	// readOnly indicates if this MemTable is being flushed (no more writes)
	readOnly bool

	// accounted is how many bytes the engine recorded with its memory monitor
	// for this table, released once the table is flushed. Guarded by the
	// engine's mutex, not by mutex.
	accounted int64
}

// NewMemTable creates a new empty MemTable.
func NewMemTable() *MemTable {
	return &MemTable{
		data:     make([]Entry, 0),
		index:    make(map[string]int),
		size:     0,
		readOnly: false,
	}
}

// Put adds or updates an entry in the MemTable.
// This is called for every write operation in the system.
func (mt *MemTable) Put(entry Entry) error {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	// Check if MemTable is read-only (being flushed)
	if mt.readOnly {
		return ErrMemTableReadOnly
	}

	key := entry.Key

	// Check if key already exists
	if existingPos, exists := mt.index[key]; exists {
		// Update existing entry
		oldEntry := &mt.data[existingPos]

		// Calculate size difference
		oldSize := mt.calculateEntrySize(oldEntry)
		newSize := mt.calculateEntrySize(&entry)

		// Update the entry
		mt.data[existingPos] = entry
		mt.size = mt.size - oldSize + newSize

		return nil
	}

	// Add new entry
	newPos := len(mt.data)
	mt.data = append(mt.data, entry)
	mt.index[key] = newPos
	mt.size += mt.calculateEntrySize(&entry)

	// Keep data sorted by key (simple insertion sort for this position)
	mt.insertSorted(newPos)

	return nil
}

// Get retrieves an entry from the MemTable by key.
// Returns the entry if found, nil if not found.
func (mt *MemTable) Get(key string) *Entry {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	// Use index for O(1) lookup
	if pos, exists := mt.index[key]; exists {
		entry := mt.data[pos]
		return &entry
	}

	return nil
}

// Delete marks an entry as deleted by inserting a tombstone.
// We don't actually remove the data until compaction.
func (mt *MemTable) Delete(key string, vectorClock *consensus.VectorClock) error {
	// Create a tombstone entry
	deleteEntry := NewDeleteEntry(key, vectorClock)

	// Add the tombstone to the MemTable
	return mt.Put(*deleteEntry)
}

// Size returns the approximate memory usage of the MemTable in bytes.
func (mt *MemTable) Size() int64 {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	return mt.size
}

// Count returns the number of entries in the MemTable.
func (mt *MemTable) Count() int {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	return len(mt.data)
}

// SetReadOnly marks the MemTable as read-only (no more writes allowed).
// This is called when the MemTable is about to be flushed to disk.
func (mt *MemTable) SetReadOnly() {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	mt.readOnly = true
}

// IsReadOnly returns true if the MemTable is read-only.
func (mt *MemTable) IsReadOnly() bool {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	return mt.readOnly
}

// Iterator returns an iterator to scan all entries in key order.
// This is used when flushing the MemTable to an SSTable.
func (mt *MemTable) Iterator() Iterator {
	return NewMemTableIterator(mt)
}

// insertSorted maintains the sorted order of data slice after insertion.
// This is a simple implementation - production code might use a tree structure.
func (mt *MemTable) insertSorted(pos int) {
	if pos == 0 {
		return // Already in correct position
	}

	// Move the new entry to its correct sorted position
	entry := mt.data[pos]
	key := entry.Key

	// Find where this key should be inserted
	insertPos := 0
	for i := 0; i < pos; i++ {
		if mt.data[i].Key <= key {
			insertPos = i + 1
		} else {
			break
		}
	}

	if insertPos == pos {
		return // Already in correct position
	}

	// Shift entries and update index
	copy(mt.data[insertPos+1:pos+1], mt.data[insertPos:pos])
	mt.data[insertPos] = entry

	// Update index to reflect new positions
	mt.rebuildIndex()
}

// rebuildIndex reconstructs the index map after data rearrangement.
func (mt *MemTable) rebuildIndex() {
	mt.index = make(map[string]int, len(mt.data))
	for i, entry := range mt.data {
		mt.index[entry.Key] = i
	}
}

// calculateEntrySize estimates the memory usage of an entry.
func (mt *MemTable) calculateEntrySize(entry *Entry) int64 {
	size := int64(len(entry.Key))   // Key string
	size += int64(len(entry.Value)) // Value bytes
	size += int64(8)                // Timestamp (int64)
	size += int64(1)                // Deleted flag (bool)

	// VectorClock size (approximate)
	if entry.VectorClock != nil {
		clockMap := entry.VectorClock.GetClocks()
		for nodeID := range clockMap {
			size += int64(len(nodeID)) + 8 // nodeID + uint64
		}
	}

	return size
}
