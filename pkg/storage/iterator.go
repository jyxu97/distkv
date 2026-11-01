// Package storage - Iterator implementations for scanning key-value pairs
// This provides efficient range queries and compaction support by iterating
// through data in sorted order across MemTables and SSTables.
package storage

import (
	"container/heap"
	"fmt"
	"sort"
)

// MemTableIterator iterates through entries in a MemTable in sorted order.
type MemTableIterator struct {
	memTable *MemTable
	index    int  // Current position in the sorted data slice
	valid    bool // Whether the iterator points to a valid entry
}

// NewMemTableIterator creates a new iterator for the given MemTable.
func NewMemTableIterator(memTable *MemTable) *MemTableIterator {
	memTable.mutex.RLock() // Hold read lock during iteration
	return &MemTableIterator{
		memTable: memTable,
		index:    0,
		valid:    len(memTable.data) > 0,
	}
}

// Valid returns true if the iterator points to a valid entry.
func (it *MemTableIterator) Valid() bool {
	return it.valid && it.index < len(it.memTable.data)
}

// Key returns the current key (only valid if Valid() is true).
func (it *MemTableIterator) Key() string {
	if !it.Valid() {
		return ""
	}
	return it.memTable.data[it.index].Key
}

// Value returns the current entry (only valid if Valid() is true).
func (it *MemTableIterator) Value() *Entry {
	if !it.Valid() {
		return nil
	}
	entry := it.memTable.data[it.index]
	return &entry
}

// Next advances the iterator to the next entry.
func (it *MemTableIterator) Next() {
	if it.Valid() {
		it.index++
		it.valid = it.index < len(it.memTable.data)
	}
}

// Close releases resources held by the iterator.
func (it *MemTableIterator) Close() error {
	it.memTable.mutex.RUnlock() // Release the read lock
	it.valid = false
	return nil
}

// SSTableIterator iterates through entries in an SSTable in sorted order.
type SSTableIterator struct {
	sstable *SSTable
	keys    []string // Sorted list of keys in this SSTable
	index   int      // Current position in the keys slice
	valid   bool     // Whether the iterator points to a valid entry
	entry   *Entry   // Cached current entry
}

// NewSSTableIterator creates a new iterator for the given SSTable.
func NewSSTableIterator(sstable *SSTable) *SSTableIterator {
	sstable.mutex.RLock() // Hold read lock during iteration

	// Extract and sort keys from the index
	keys := make([]string, 0, len(sstable.index))
	for key := range sstable.index {
		keys = append(keys, key)
	}

	// The index is a map, so its iteration order is random. The merge
	// iterator requires every source in key order; without this sort it emits
	// a key more than once and compaction keeps whichever version comes last.
	sort.Strings(keys)

	return &SSTableIterator{
		sstable: sstable,
		keys:    keys,
		index:   0,
		valid:   len(keys) > 0,
		entry:   nil,
	}
}

// Valid returns true if the iterator points to a valid entry.
func (it *SSTableIterator) Valid() bool {
	return it.valid && it.index < len(it.keys)
}

// Key returns the current key (only valid if Valid() is true).
func (it *SSTableIterator) Key() string {
	if !it.Valid() {
		return ""
	}
	return it.keys[it.index]
}

// Value returns the current entry (only valid if Valid() is true).
func (it *SSTableIterator) Value() *Entry {
	if !it.Valid() {
		return nil
	}

	// Lazy load the entry when requested
	if it.entry == nil {
		key := it.keys[it.index]
		entry, err := it.sstable.Get(key)
		if err != nil {
			return nil // Entry not found or error
		}
		it.entry = entry
	}

	return it.entry
}

// Next advances the iterator to the next entry.
func (it *SSTableIterator) Next() {
	if it.Valid() {
		it.index++
		it.entry = nil // Clear cached entry
		it.valid = it.index < len(it.keys)
	}
}

// Close releases resources held by the iterator.
func (it *SSTableIterator) Close() error {
	it.sstable.mutex.RUnlock() // Release the read lock
	it.valid = false
	it.entry = nil
	return nil
}

// MergeIterator combines multiple iterators and returns entries in sorted order.
// This is essential for reading data that spans multiple MemTables and SSTables.
type MergeIterator struct {
	iterators []Iterator    // All source iterators
	heap      *iteratorHeap // Min-heap for efficient merging
	valid     bool          // Whether the iterator has a current entry
	current   *heapItem     // Current entry from the heap
}

// heapItem represents an entry from one of the source iterators.
type heapItem struct {
	key      string   // Key of the entry
	entry    *Entry   // The actual entry
	iterator Iterator // Which iterator this entry came from
	index    int      // Index of the source iterator
}

// iteratorHeap implements heap.Interface for merging sorted iterators.
type iteratorHeap []*heapItem

func (h iteratorHeap) Len() int { return len(h) }

// Less orders by key, and for equal keys by source index: sources are passed
// newest first, so the lowest index holds the newest version of a key and
// must be popped first. Ordering by key alone left the choice to heap layout,
// which returned stale versions to iteration and compaction.
func (h iteratorHeap) Less(i, j int) bool {
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}
	return h[i].index < h[j].index
}
func (h iteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *iteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(*heapItem))
}

func (h *iteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// NewMergeIterator creates a new merge iterator that combines multiple source iterators.
func NewMergeIterator(iterators []Iterator) *MergeIterator {
	// Initialize heap with first entry from each valid iterator
	h := &iteratorHeap{}
	heap.Init(h)

	for i, it := range iterators {
		if it.Valid() {
			item := &heapItem{
				key:      it.Key(),
				entry:    it.Value(),
				iterator: it,
				index:    i,
			}
			heap.Push(h, item)
		}
	}

	mi := &MergeIterator{
		iterators: iterators,
		heap:      h,
		valid:     h.Len() > 0,
		current:   nil,
	}

	// Position at first entry
	if mi.valid {
		mi.current = heap.Pop(mi.heap).(*heapItem)
		mi.advanceIterator(mi.current)
	}

	return mi
}

// Valid returns true if the iterator points to a valid entry.
func (mi *MergeIterator) Valid() bool {
	return mi.valid
}

// Key returns the current key (only valid if Valid() is true).
func (mi *MergeIterator) Key() string {
	if !mi.Valid() {
		return ""
	}
	return mi.current.key
}

// Value returns the current entry (only valid if Valid() is true).
func (mi *MergeIterator) Value() *Entry {
	if !mi.Valid() {
		return nil
	}
	return mi.current.entry
}

// Next advances the iterator to the next entry.
// This handles duplicate keys by taking the newest entry (highest timestamp).
func (mi *MergeIterator) Next() {
	if !mi.Valid() {
		return
	}

	currentKey := mi.current.key

	// Skip all entries with the same key, keeping only the newest
	// (entries from more recent sources should have higher timestamps)
	for mi.heap.Len() > 0 {
		next := heap.Pop(mi.heap).(*heapItem)
		mi.advanceIterator(next)

		if next.key != currentKey {
			// Found next unique key
			mi.current = next
			return
		}
		// Skip duplicate keys (they're older versions)
	}

	// No more entries
	mi.valid = false
	mi.current = nil
}

// advanceIterator moves the given iterator to its next entry and adds a new item to heap.
// Creates a new heapItem instead of mutating the passed item, so the caller's item is unchanged.
func (mi *MergeIterator) advanceIterator(item *heapItem) {
	item.iterator.Next()
	if item.iterator.Valid() {
		next := &heapItem{
			key:      item.iterator.Key(),
			entry:    item.iterator.Value(),
			iterator: item.iterator,
			index:    item.index,
		}
		heap.Push(mi.heap, next)
	}
}

// Close releases resources held by the iterator.
func (mi *MergeIterator) Close() error {
	var lastErr error

	// Close all source iterators
	for _, it := range mi.iterators {
		if err := it.Close(); err != nil {
			lastErr = err // Keep track of last error
		}
	}

	mi.valid = false
	mi.current = nil
	mi.heap = nil

	return lastErr
}

// EngineIterator provides a complete view of all data in the storage engine.
// It merges data from the active MemTable, flushing MemTables, and all SSTables.
type EngineIterator struct {
	mergeIterator *MergeIterator
}

// NewEngineIterator creates an iterator that scans all data in the storage engine.
func NewEngineIterator(engine *Engine) (*EngineIterator, error) {
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()

	if engine.closed {
		return nil, ErrStorageClosed
	}

	var iterators []Iterator

	// Add active MemTable iterator
	if engine.activeMemTable != nil {
		iterators = append(iterators, NewMemTableIterator(engine.activeMemTable))
	}

	// Add flushing MemTable iterators, newest first
	for i := len(engine.flushingMemTables) - 1; i >= 0; i-- {
		iterators = append(iterators, NewMemTableIterator(engine.flushingMemTables[i]))
	}

	// Add SSTable iterators (newest level/files first for proper conflict resolution)
	for level := 0; level < len(engine.sstables); level++ {
		for i := len(engine.sstables[level]) - 1; i >= 0; i-- {
			iterators = append(iterators, NewSSTableIterator(engine.sstables[level][i]))
		}
	}

	if len(iterators) == 0 {
		return nil, fmt.Errorf("no data sources available for iteration")
	}

	ei := &EngineIterator{
		mergeIterator: NewMergeIterator(iterators),
	}
	ei.skipPurged()
	return ei, nil
}

// skipPurged advances past keys whose newest entry is a purge marker: they
// are absent from this node's point of view.
func (ei *EngineIterator) skipPurged() {
	for ei.mergeIterator.Valid() {
		if entry := ei.mergeIterator.Value(); entry == nil || !entry.Purged {
			return
		}
		ei.mergeIterator.Next()
	}
}

// Valid returns true if the iterator points to a valid entry.
func (ei *EngineIterator) Valid() bool {
	return ei.mergeIterator.Valid()
}

// Key returns the current key (only valid if Valid() is true).
func (ei *EngineIterator) Key() string {
	return ei.mergeIterator.Key()
}

// Value returns the current entry (only valid if Valid() is true).
func (ei *EngineIterator) Value() *Entry {
	return ei.mergeIterator.Value()
}

// Next advances the iterator to the next entry.
func (ei *EngineIterator) Next() {
	ei.mergeIterator.Next()
	ei.skipPurged()
}

// Close releases resources held by the iterator.
func (ei *EngineIterator) Close() error {
	return ei.mergeIterator.Close()
}
