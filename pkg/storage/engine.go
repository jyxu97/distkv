// Package storage - Main storage engine that coordinates MemTables and SSTables
// This is the main interface that the rest of the system uses for data operations.
package storage

import (
	"bytes"
	"distkv/pkg/consensus"
	"distkv/pkg/errors"
	"distkv/pkg/logging"
	"distkv/pkg/metrics"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Engine is the main storage engine that implements the LSM-tree.
// It coordinates between MemTables (memory) and SSTables (disk).
type Engine struct {
	// config holds storage configuration parameters
	config *StorageConfig

	// dataDir is the directory where SSTable files are stored
	dataDir string

	// activeMemTable receives all new writes
	activeMemTable *MemTable

	// flushingMemTables are sealed, read-only MemTables waiting to be written
	// to disk, oldest first. The flush worker writes them in this order, so a
	// MemTable never outlives a newer one: reads search them newest first,
	// ahead of every SSTable.
	flushingMemTables []*MemTable

	// flushCond is signalled (with mutex) whenever a flush finishes or fails
	// and when the engine closes. Writers held back by backpressure wait on it.
	flushCond *sync.Cond

	// flushErr is the error of the last flush attempt if it failed, nil once
	// a flush succeeds. While set, writes that would have to wait for a
	// flush fail instead of waiting for a retry that may never succeed.
	flushErr error

	// stalledWriters counts writers waiting on flushCond (for tests and stats)
	stalledWriters int

	// createSSTable writes a MemTable to an SSTable file. A field so tests
	// can delay or fail flushes.
	createSSTable func(memTable *MemTable, path string) (*SSTable, error)

	// flushRetryMin and flushRetryMax bound the delay between attempts to
	// flush a MemTable whose flush failed; the delay doubles per failure.
	flushRetryMin, flushRetryMax time.Duration

	// crashed makes the flush worker stop without flushing (CrashForTesting)
	crashed bool

	// sstables contains all disk-based storage files organized by level
	sstables [][]*SSTable

	// mutex protects concurrent access to engine state
	mutex sync.RWMutex

	// flushChan signals when a flush operation is needed
	flushChan chan struct{}

	// compactionChan signals when compaction is needed
	compactionChan chan struct{}

	// stopChan signals shutdown
	stopChan chan struct{}

	// wg tracks background goroutines
	wg sync.WaitGroup

	// nextGen is the generation for the next MemTable flushed to an SSTable.
	// Guarded by mutex.
	nextGen uint64

	// stats tracks performance metrics
	stats *StorageStats

	// statsMu guards stats updates made while holding only mutex.RLock
	// (concurrent Get and Stats calls); writers holding mutex.Lock are exclusive
	statsMu sync.Mutex

	// closed indicates if the engine is shut down
	closed bool

	// logger is the component logger
	logger *logging.Logger

	// metrics collector
	metrics *metrics.MetricsCollector

	// memoryMonitor tracks and enforces memory limits
	memoryMonitor *MemoryMonitor

	// wal is the write-ahead log. When enabled, every mutation is appended and
	// fsync'd here before being inserted into the active MemTable, so an
	// acknowledged write survives a crash and is replayed on the next startup.
	// nil when WAL is disabled via config.
	wal *WAL
}

// NewEngine creates a new storage engine with the specified configuration.
func NewEngine(dataDir string, config *StorageConfig) (*Engine, error) {
	logger := logging.WithComponent("storage.engine")

	if config == nil {
		config = DefaultStorageConfig()
		logger.Info("Using default storage configuration")
	}

	// Validate configuration
	if dataDir == "" {
		return nil, errors.NewInvalidConfigError("data directory cannot be empty")
	}

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeInternal,
			"failed to create data directory").WithContext("dataDir", dataDir)
	}

	logger.WithField("dataDir", dataDir).Info("Initializing storage engine")

	// Initialize sstables array for all levels
	maxLevels := config.MaxLevels
	if maxLevels <= 0 {
		maxLevels = 7
	}
	sstables := make([][]*SSTable, maxLevels)
	for i := range sstables {
		sstables[i] = make([]*SSTable, 0)
	}

	// Create memory monitor
	memoryConfig := DefaultMemoryConfig()
	memoryMonitor := NewMemoryMonitor(memoryConfig)

	// Register memory pressure callback
	memoryMonitor.RegisterPressureCallback(func(level MemoryPressureLevel) {
		if level >= MemoryPressureHigh {
			logger.WithField("level", level).Warn("High memory pressure detected, triggering flush")
			// Trigger flush on high memory pressure (will be handled by the engine instance)
		}
	})

	engine := &Engine{
		config:            config,
		dataDir:           dataDir,
		activeMemTable:    NewMemTable(),
		flushingMemTables: make([]*MemTable, 0),
		sstables:          sstables,
		flushChan:         make(chan struct{}, 1),
		compactionChan:    make(chan struct{}, 1),
		stopChan:          make(chan struct{}),
		stats:             &StorageStats{},
		closed:            false,
		logger:            logger,
		metrics:           metrics.GetGlobalMetrics(),
		memoryMonitor:     memoryMonitor,
		flushRetryMin:     100 * time.Millisecond,
		flushRetryMax:     10 * time.Second,
	}
	engine.flushCond = sync.NewCond(&engine.mutex)
	engine.createSSTable = func(memTable *MemTable, path string) (*SSTable, error) {
		return CreateSSTable(memTable, path, config)
	}

	// Load existing SSTables from disk
	if err := engine.loadExistingSSTables(); err != nil {
		logger.WithError(err).Error("Failed to load existing SSTables")
		return nil, errors.Wrap(err, errors.ErrCodeStorageCorrupted,
			"failed to load existing SSTables").WithContext("dataDir", dataDir)
	}

	// Recover the write-ahead log, if enabled. Any writes that were
	// acknowledged but not yet flushed to an SSTable live only in the WAL after
	// a crash; replaying them into the active MemTable restores durability.
	if config.WALEnabled {
		if err := engine.recoverWAL(); err != nil {
			logger.WithError(err).Error("Failed to recover write-ahead log")
			return nil, errors.Wrap(err, errors.ErrCodeStorageCorrupted,
				"failed to recover write-ahead log").WithContext("dataDir", dataDir)
		}
	}
	engine.flattenLevelsIfOutOfOrder()

	// Start background workers
	engine.startBackgroundWorkers()

	logger.Info("Storage engine initialized successfully")
	return engine, nil
}

// Put stores a key-value pair in the storage engine.
// This is the main write path for the LSM-tree.
func (e *Engine) Put(key string, value []byte, vectorClock *consensus.VectorClock) error {
	if value == nil {
		e.metrics.Storage().WriteErrors.Add(1)
		return errors.New(errors.ErrCodeInvalidValue, "value cannot be nil")
	}
	_, _, err := e.WriteIf(NewEntry(key, value, vectorClock), nil)
	return err
}

// Delete marks a key as deleted by inserting a tombstone.
func (e *Engine) Delete(key string, vectorClock *consensus.VectorClock) error {
	_, _, err := e.WriteIf(NewDeleteEntry(key, vectorClock), nil)
	return err
}

// WriteIf stores entry (a value or a tombstone) only if cond approves the key's
// current local entry. current is nil when the key is absent, and is the raw
// entry otherwise, tombstones included. cond runs under the engine's write
// lock, so the check and the write are atomic with respect to every other
// writer; a nil cond always writes. It returns whether the entry was written
// and the entry that was current before the call.
func (e *Engine) WriteIf(entry *Entry, cond func(current *Entry) bool) (bool, *Entry, error) {
	// Track latency
	tracker := metrics.NewLatencyTracker()
	defer func() {
		latency := tracker.Finish()
		e.metrics.Storage().WriteLatencyNs.Store(latency)
	}()

	// Validate inputs
	key := entry.Key
	if key == "" {
		e.metrics.Storage().WriteErrors.Add(1)
		return false, nil, errors.New(errors.ErrCodeInvalidKey, "key cannot be empty")
	}

	// The MemTable keeps the entry after this returns, and the flush worker
	// encodes it later, so it must not share the caller's value or clock
	stored := *entry
	stored.Value = bytes.Clone(entry.Value)
	if entry.VectorClock != nil {
		stored.VectorClock = entry.VectorClock.Copy()
	}
	entry = &stored

	e.mutex.Lock()

	if e.closed {
		e.mutex.Unlock()
		e.metrics.Storage().WriteErrors.Add(1)
		return false, nil, errors.NewStorageClosedError()
	}
	if err := e.waitForMemTableRoomLocked(); err != nil {
		e.mutex.Unlock()
		e.metrics.Storage().WriteErrors.Add(1)
		return false, nil, err
	}

	var current *Entry
	if cond != nil {
		found, _, err := e.lookupLocked(key)
		if err != nil && err != ErrKeyNotFound {
			e.mutex.Unlock()
			e.metrics.Storage().WriteErrors.Add(1)
			return false, nil, err
		}
		current = found
		if !cond(current) {
			e.mutex.Unlock()
			return false, current, nil
		}
	}

	// Estimate memory usage and check limits. This comes before the WAL: a
	// write refused here must not be in the log, or recovery would apply a
	// write the caller was told had failed.
	estimatedSize := int64(len(key) + len(entry.Value) + 64) // Rough estimate
	if err := e.memoryMonitor.RecordMemTableAllocation(estimatedSize); err != nil {
		e.stats.WriteErrors++
		e.metrics.Storage().WriteErrors.Add(1)
		e.mutex.Unlock()
		e.logger.WithError(err).WithField("estimatedSize", estimatedSize).
			Error("Memory limit exceeded, cannot write entry")
		return false, current, err
	}

	// Durability: append to the write-ahead log and fsync BEFORE inserting into
	// the MemTable. If we crash after this returns, the write is recoverable on
	// restart; if the append fails, we reject the write rather than acknowledge
	// something we can't recover. This is what makes an ack'd write survive a crash.
	if e.wal != nil {
		if err := e.wal.Append(entry); err != nil {
			e.memoryMonitor.RecordMemTableDeallocation(estimatedSize)
			e.mutex.Unlock()
			e.stats.WriteErrors++
			e.metrics.Storage().WriteErrors.Add(1)
			e.logger.WithError(err).WithField("key", key).Error("Failed to append to WAL")
			return false, current, errors.Wrap(err, errors.ErrCodeInternal, "failed to append to WAL")
		}
	}

	// Add to active MemTable
	if err := e.activeMemTable.Put(*entry); err != nil {
		// Rollback memory allocation on error
		e.memoryMonitor.RecordMemTableDeallocation(estimatedSize)
		e.stats.WriteErrors++
		e.metrics.Storage().WriteErrors.Add(1)
		e.mutex.Unlock()
		e.logger.WithError(err).WithField("key", key).Error("Failed to put entry to MemTable")
		return false, current, errors.Wrap(err, errors.ErrCodeInternal, "failed to put entry to MemTable")
	}
	e.activeMemTable.accounted += estimatedSize

	// Update stats and metrics
	e.stats.WriteCount++
	e.metrics.Storage().WriteOps.Add(1)

	e.metrics.Storage().MemTableSize.Store(e.activeMemTable.Size())

	// Hand a full MemTable to the flush worker; this write does not wait for
	// the flush
	e.maybeSealLocked()
	e.mutex.Unlock()

	return true, current, nil
}

// Get retrieves a value by key from the storage engine.
// This implements the LSM-tree read path: MemTable first, then SSTables.
// Deleted keys report ErrKeyNotFound; use GetEntry to see tombstones.
func (e *Engine) Get(key string) (*Entry, error) {
	entry, err := e.GetEntry(key)
	if err != nil {
		return nil, err
	}
	if entry.Deleted {
		return nil, ErrKeyNotFound // Tombstone found
	}
	return entry, nil
}

// GetEntry is like Get but returns tombstones instead of hiding them, so
// callers comparing versions can see a delete's vector clock.
func (e *Engine) GetEntry(key string) (*Entry, error) {
	// Track latency
	tracker := metrics.NewLatencyTracker()
	defer func() {
		latency := tracker.Finish()
		e.metrics.Storage().ReadLatencyNs.Store(latency)
	}()

	// Validate input
	if key == "" {
		e.metrics.Storage().ReadErrors.Add(1)
		return nil, errors.New(errors.ErrCodeInvalidKey, "key cannot be empty")
	}

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if e.closed {
		e.metrics.Storage().ReadErrors.Add(1)
		return nil, errors.NewStorageClosedError()
	}

	startTime := time.Now()
	defer func() {
		e.statsMu.Lock()
		e.stats.ReadCount++
		// Update average read latency (simplified)
		e.stats.AvgReadLatency = time.Since(startTime)
		e.statsMu.Unlock()
		e.metrics.Storage().ReadOps.Add(1)
	}()

	entry, inMemory, err := e.lookupLocked(key)
	if inMemory {
		e.metrics.Storage().CacheHits.Add(1) // MemTable is effectively a cache
	} else {
		e.metrics.Storage().CacheMisses.Add(1)
	}
	if err != nil && err != ErrKeyNotFound {
		e.statsMu.Lock()
		e.stats.ReadErrors++
		e.statsMu.Unlock()
		e.metrics.Storage().ReadErrors.Add(1)
		e.logger.WithError(err).WithField("key", key).
			Error("Error reading from SSTable")
		return nil, errors.Wrap(err, errors.ErrCodeInternal,
			"failed to read from SSTable")
	}
	return entry, err
}

// lookupLocked finds the newest entry for key, tombstones included, and
// reports whether it came from a MemTable. A purge marker means the key is
// absent. Caller must hold e.mutex.
func (e *Engine) lookupLocked(key string) (*Entry, bool, error) {
	entry, inMemory, err := e.lookupNewestLocked(key)
	if err == nil && entry.Purged {
		return nil, inMemory, ErrKeyNotFound
	}
	return entry, inMemory, err
}

// lookupNewestLocked returns the newest stored entry for key, purge markers
// included. Caller must hold e.mutex.
func (e *Engine) lookupNewestLocked(key string) (*Entry, bool, error) {
	// First check active MemTable
	if entry := e.activeMemTable.Get(key); entry != nil {
		return entry, true, nil
	}

	// Check flushing MemTables, newest first
	for i := len(e.flushingMemTables) - 1; i >= 0; i-- {
		if entry := e.flushingMemTables[i].Get(key); entry != nil {
			return entry, true, nil
		}
	}

	// Check SSTables level by level (lower levels first - more recent data)
	for level := 0; level < len(e.sstables); level++ {
		for i := len(e.sstables[level]) - 1; i >= 0; i-- {
			entry, err := e.sstables[level][i].Get(key)
			if err == nil {
				return entry, false, nil
			}
			if err != ErrKeyNotFound {
				return nil, false, err
			}
		}
	}

	return nil, false, ErrKeyNotFound
}

// Iterator returns an iterator that scans all key-value pairs.
// This merges data from MemTables and SSTables in sorted order.
func (e *Engine) Iterator() (Iterator, error) {
	return NewEngineIterator(e)
}

// Stats returns current storage engine statistics.
func (e *Engine) Stats() *StorageStats {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	e.statsMu.Lock()
	defer e.statsMu.Unlock()

	// Update current stats
	e.stats.MemTableSize = e.activeMemTable.Size()
	e.stats.MemTableCount = 1 + len(e.flushingMemTables)

	// Update memory stats
	memUsage := e.memoryMonitor.GetMemoryUsage()
	e.stats.MemoryUsage = memUsage.TotalUsage
	e.stats.HeapUsage = memUsage.HeapAlloc

	// Count total SSTables across all levels
	totalSSTables := 0
	var totalSize int64 = e.activeMemTable.Size()
	for level := 0; level < len(e.sstables); level++ {
		totalSSTables += len(e.sstables[level])
		for _, sst := range e.sstables[level] {
			totalSize += sst.Size()
		}
	}
	e.stats.SSTableCount = totalSSTables
	e.stats.TotalDataSize = totalSize

	// Copy stats to avoid race conditions
	statsCopy := *e.stats
	return &statsCopy
}

// Close shuts down the storage engine gracefully.
func (e *Engine) Close() error {
	e.logger.Info("Initiating graceful shutdown of storage engine")

	e.mutex.Lock()
	if e.closed {
		e.mutex.Unlock()
		e.logger.Warn("Storage engine already closed")
		return nil
	}
	e.closed = true

	// Queue the remaining writes for the flush worker, which drains its queue
	// before stopping, and release writers held back by backpressure
	if e.activeMemTable.Size() > 0 {
		e.sealLocked()
	}
	e.flushCond.Broadcast()
	e.mutex.Unlock()

	var shutdownErrors []error

	// Signal shutdown to background workers
	e.logger.Debug("Stopping background workers")
	close(e.stopChan)

	// Wait for background workers to finish with timeout
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		e.logger.Debug("Background workers stopped successfully")
	case <-time.After(30 * time.Second):
		e.logger.Error("Timeout waiting for background workers to stop")
		shutdownErrors = append(shutdownErrors,
			errors.New(errors.ErrCodeTimeout, "timeout waiting for background workers"))
	}

	// Stop memory monitor
	e.logger.Debug("Stopping memory monitor")
	e.memoryMonitor.Stop()

	// A MemTable the worker could not flush keeps its writes in its sealed
	// WAL segment, which the next startup recovers
	e.mutex.Lock()
	if unflushed := len(e.flushingMemTables); unflushed > 0 {
		lost := "they are recovered from the WAL on the next start"
		if e.wal == nil {
			lost = "their writes are lost because the WAL is disabled"
		}
		err := fmt.Errorf("%d MemTables could not be flushed (%v); %s", unflushed, e.flushErr, lost)
		e.logger.WithError(err).Error("Unflushed MemTables at shutdown")
		shutdownErrors = append(shutdownErrors, err)
	}
	e.mutex.Unlock()

	// Close the write-ahead log now that no further writes will occur.
	if e.wal != nil {
		if err := e.wal.Close(); err != nil {
			e.logger.WithError(err).Error("Failed to close WAL")
			shutdownErrors = append(shutdownErrors, err)
		}
	}

	// Close all SSTable files
	e.mutex.Lock()
	sstables := e.sstables
	totalSSTables := 0
	for level := range sstables {
		totalSSTables += len(sstables[level])
	}
	e.mutex.Unlock()

	e.logger.WithField("sstableCount", totalSSTables).Debug("Closing SSTable files")
	for level := range sstables {
		for i, sst := range sstables[level] {
			if err := sst.Close(); err != nil {
				e.logger.WithError(err).WithFields(map[string]interface{}{
					"level":        level,
					"sstableIndex": i,
				}).Error("Failed to close SSTable")
				shutdownErrors = append(shutdownErrors, err)
			}
		}
	}

	if len(shutdownErrors) > 0 {
		e.logger.WithField("errorCount", len(shutdownErrors)).
			Error("Storage engine closed with errors")
		return errors.New(errors.ErrCodeInternal,
			fmt.Sprintf("storage engine shutdown completed with %d errors", len(shutdownErrors)))
	}

	e.logger.Info("Storage engine shutdown completed successfully")
	return nil
}

// CrashForTesting simulates an abrupt process crash for tests: it stops the
// background workers and closes the WAL file handle WITHOUT performing the
// graceful final flush that Close does. Any data still in the active MemTable
// therefore exists only in the WAL, exactly as it would after a real crash, so
// a subsequent NewEngine on the same directory must recover it by replaying the
// WAL. It must only be used in tests.
func (e *Engine) CrashForTesting() {
	e.mutex.Lock()
	if e.closed {
		e.mutex.Unlock()
		return
	}
	e.closed = true
	e.crashed = true
	e.flushCond.Broadcast()
	e.mutex.Unlock()

	close(e.stopChan)
	e.wg.Wait()
	e.memoryMonitor.Stop()

	// Release the WAL file handle but do NOT truncate it: its records are the
	// only copy of the unflushed writes and must survive to be replayed.
	if e.wal != nil {
		_ = e.wal.Close()
	}
}

// triggerCompaction signals that compaction is needed.
func (e *Engine) triggerCompaction() {
	select {
	case e.compactionChan <- struct{}{}:
	default:
		// Channel full, compaction already pending
	}
}

// startBackgroundWorkers starts goroutines for flush and compaction.
func (e *Engine) startBackgroundWorkers() {
	// Flush worker
	e.wg.Add(1)
	go e.flushWorker()

	// Compaction worker
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			select {
			case <-e.compactionChan:
				e.performCompaction()
			case <-e.stopChan:
				return
			}
		}
	}()
}

// memTableFullLocked reports whether the active MemTable has reached its size
// limit. Caller must hold e.mutex.
func (e *Engine) memTableFullLocked() bool {
	return e.activeMemTable.Size() >= int64(e.config.MemTableMaxSize)
}

// maxSealedMemTables is how many sealed MemTables may wait for the flush
// worker: MaxMemTables counts the active one too.
func (e *Engine) maxSealedMemTables() int {
	if e.config.MaxMemTables > 1 {
		return e.config.MaxMemTables - 1
	}
	return 1
}

// maybeSealLocked seals the active MemTable if it is full and the flush queue
// has room. Caller must hold e.mutex.
func (e *Engine) maybeSealLocked() {
	if e.memTableFullLocked() && len(e.flushingMemTables) < e.maxSealedMemTables() {
		e.sealLocked()
	}
}

// waitForMemTableRoomLocked applies backpressure: while the active MemTable is
// full and cannot be sealed because the flush queue is full, the writer waits
// for a flush to finish. It returns an error instead if the engine closes or
// the last flush failed, rather than waiting on retries that may never
// succeed. Writes do not wait while the active MemTable has room, even during
// a flush. Caller must hold e.mutex; it is released while waiting.
func (e *Engine) waitForMemTableRoomLocked() error {
	for e.memTableFullLocked() {
		if e.closed {
			return errors.NewStorageClosedError()
		}
		if len(e.flushingMemTables) < e.maxSealedMemTables() {
			e.sealLocked()
			return nil
		}
		if e.flushErr != nil {
			return errors.Wrap(e.flushErr, errors.ErrCodeInternal,
				"MemTables are full and flushing them is failing")
		}
		e.stalledWriters++
		e.flushCond.Wait()
		e.stalledWriters--
		if e.closed {
			// Close may have sealed the MemTable this writer waited for
			return errors.NewStorageClosedError()
		}
	}
	return nil
}

// sealLocked makes the active MemTable read-only, queues it for the flush
// worker and starts a new one. In the same critical section it rotates the
// WAL, so the sealed segment holds exactly the sealed MemTable's writes and
// every later write goes to the new log. The segment is named after the
// MemTable's generation, which recovery uses to order it among SSTables.
// Caller must hold e.mutex.
func (e *Engine) sealLocked() {
	sealed := e.activeMemTable
	sealed.SetReadOnly()
	sealed.gen = e.nextGen
	e.nextGen++

	if e.wal != nil {
		segment, err := e.wal.Rotate(sealed.gen)
		if err != nil {
			// If the rename did not happen, the sealed MemTable's records stay
			// in the active log ahead of later writes; recovery replays them in
			// order, so nothing is lost, only that space is not reclaimed
			e.logger.WithError(err).Error("Failed to rotate WAL while sealing a MemTable")
		}
		sealed.walSegment = segment
	}

	e.flushingMemTables = append(e.flushingMemTables, sealed)
	e.activeMemTable = NewMemTable()
	e.metrics.Storage().MemTableCount.Store(int64(len(e.flushingMemTables) + 1))

	select {
	case e.flushChan <- struct{}{}:
	default: // The worker is already signalled
	}
}

// flushWorker writes sealed MemTables to SSTables, oldest first and one at a
// time. A failed flush is retried with a growing delay, and later MemTables
// wait: flushing a newer one first would leave the older one in memory, where
// reads find it ahead of the newer SSTable. On stop it first drains the queue,
// giving up at the first failure; the WAL segments of anything not flushed
// remain for recovery.
func (e *Engine) flushWorker() {
	defer e.wg.Done()
	var delay time.Duration // Until the next retry; 0 after a success
	for {
		memTable := e.nextMemTableToFlush()
		if memTable == nil {
			select {
			case <-e.flushChan:
			case <-e.stopChan:
				if e.nextMemTableToFlush() == nil {
					return
				}
			}
			continue
		}

		if err := e.flushMemTable(memTable); err != nil {
			if delay = 2 * delay; delay == 0 {
				delay = e.flushRetryMin
			}
			if delay > e.flushRetryMax {
				delay = e.flushRetryMax
			}
			e.mutex.Lock()
			e.flushErr = err
			e.flushCond.Broadcast()
			e.mutex.Unlock()
			e.metrics.Storage().FlushErrors.Add(1)
			e.logger.WithError(err).WithFields(map[string]interface{}{
				"generation": memTable.gen,
				"retryIn":    delay.String(),
			}).Error("MemTable flush failed")

			timer := time.NewTimer(delay)
			select {
			case <-e.stopChan:
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}
		delay = 0
	}
}

// nextMemTableToFlush returns the oldest sealed MemTable, or nil if there is
// none or the engine crashed.
func (e *Engine) nextMemTableToFlush() *MemTable {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	if e.crashed || len(e.flushingMemTables) == 0 {
		return nil
	}
	return e.flushingMemTables[0]
}

// flushMemTable writes a sealed MemTable to an SSTable and installs it. On
// failure the MemTable stays queued and readable, and its WAL segment is kept.
func (e *Engine) flushMemTable(memTable *MemTable) error {
	tracker := metrics.NewLatencyTracker()
	e.mutex.RLock()
	gen, segment := memTable.gen, memTable.walSegment
	e.mutex.RUnlock()

	sstablesDir := filepath.Join(e.dataDir, "sstables")
	if err := os.MkdirAll(sstablesDir, 0755); err != nil {
		return fmt.Errorf("failed to create sstables directory: %w", err)
	}
	// The name records the generation for reload
	filePath := filepath.Join(sstablesDir, fmt.Sprintf("sstable_%d.db", gen))
	sstable, err := e.createSSTable(memTable, filePath)
	if err != nil {
		return fmt.Errorf("failed to create SSTable %s: %w", filePath, err)
	}

	// Swap the MemTable for its SSTable in one critical section, so reads
	// find the data in exactly one of them
	e.mutex.Lock()
	sstable.level = 0
	sstable.gen = gen
	e.sstables[0] = insertByGen(e.sstables[0], sstable)
	e.flushingMemTables = e.flushingMemTables[1:] // memTable is always the oldest
	e.memoryMonitor.RecordMemTableDeallocation(memTable.accounted)
	e.flushErr = nil

	totalSSTables := 0
	for level := range e.sstables {
		totalSSTables += len(e.sstables[level])
	}
	e.metrics.Storage().SSTableCount.Store(int64(totalSSTables))
	e.metrics.Storage().MemTableCount.Store(int64(len(e.flushingMemTables) + 1))
	level0Count := len(e.sstables[0])
	needsCompaction := level0Count >= e.config.CompactionThreshold

	// The queue has room again: seal a MemTable that filled up meanwhile and
	// release writers held back by backpressure
	if !e.closed {
		e.maybeSealLocked()
	}
	e.flushCond.Broadcast()
	e.mutex.Unlock()

	// The SSTable is durable (CreateSSTable fsyncs its files and directory),
	// so the sealed WAL segment that covered it can go. Writes that arrived
	// during the flush are in newer segments or the active log.
	if segment != "" {
		if err := RemoveSegment(segment); err != nil {
			e.logger.WithError(err).WithField("segment", segment).
				Warn("Failed to remove sealed WAL segment after flush")
		}
	}

	e.metrics.Storage().FlushCount.Add(1)
	e.metrics.Storage().FlushDurationNs.Store(tracker.Finish())
	e.logger.WithFields(map[string]interface{}{
		"path":               filePath,
		"level0SSTableCount": level0Count,
		"totalSSTableCount":  totalSSTables,
	}).Info("SSTable created successfully")

	if needsCompaction {
		e.logger.WithField("level0SSTableCount", level0Count).
			Info("Level 0 SSTable count exceeded threshold, triggering compaction")
		e.triggerCompaction()
	}
	return nil
}

// performCompaction merges multiple SSTables into fewer, larger files.
// This is essential for maintaining good read performance in an LSM-tree.
func (e *Engine) performCompaction() {
	tracker := metrics.NewLatencyTracker()
	e.logger.Info("Starting compaction")

	switch e.config.CompactionStrategy {
	case CompactionLevelBased:
		// Compacting one level can push the next one over its limit, and only
		// level 0 triggers compaction, so continue until every level fits
		for e.performLevelBasedCompaction(tracker) {
		}
	case CompactionSizeTiered:
		e.performSizeTieredCompaction(tracker)
	default:
		e.performSimpleCompaction(tracker)
	}
}

// performLevelBasedCompaction implements level-based compaction strategy.
// Level 0 files are compacted into level 1, level 1 into level 2, etc. It
// compacts the first level over its limit and reports whether it did.
func (e *Engine) performLevelBasedCompaction(tracker *metrics.LatencyTracker) bool {
	e.mutex.Lock()

	// Find the first level that needs compaction
	sourceLevel := -1
	for level := 0; level < len(e.sstables)-1; level++ {
		maxFilesForLevel := e.maxFilesForLevel(level)
		if len(e.sstables[level]) >= maxFilesForLevel {
			sourceLevel = level
			break
		}
	}

	if sourceLevel == -1 {
		e.mutex.Unlock()
		e.logger.Debug("No compaction needed at any level")
		return false
	}

	targetLevel := sourceLevel + 1
	e.logger.WithFields(map[string]interface{}{
		"sourceLevel": sourceLevel,
		"targetLevel": targetLevel,
		"sourceCount": len(e.sstables[sourceLevel]),
	}).Info("Level-based compaction starting")

	// Select SSTables from source level
	sourceTables := make([]*SSTable, len(e.sstables[sourceLevel]))
	copy(sourceTables, e.sstables[sourceLevel])

	// Select overlapping SSTables from target level
	targetTables := e.selectOverlappingSSTables(targetLevel, sourceTables)
	bottom := e.isBottomLevelLocked(targetLevel)

	e.mutex.Unlock()

	// Compact the selected tables, newest first: the source level is newer
	// than the target level, and within a level later tables are newer
	allTables := append(newestFirst(sourceTables), newestFirst(targetTables)...)
	newSSTables, err := e.compactSSTablesForLevel(allTables, targetLevel, bottom)
	if err != nil {
		e.logger.WithError(err).Error("Level-based compaction failed")
		e.metrics.Storage().CompactionErrors.Add(1)
		return false
	}

	// Update the engine state
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// Remove only the compacted tables from source level (not any newly added ones)
	e.sstables[sourceLevel] = e.removeCompactedTables(e.sstables[sourceLevel], sourceTables)

	// Remove compacted tables from target level
	e.sstables[targetLevel] = e.removeCompactedTables(e.sstables[targetLevel], targetTables)

	// Add new tables to target level, in generation order
	for _, table := range newSSTables {
		e.sstables[targetLevel] = insertByGen(e.sstables[targetLevel], table)
	}

	// Update metrics
	totalSSTables := 0
	for level := range e.sstables {
		totalSSTables += len(e.sstables[level])
	}
	e.metrics.Storage().SSTableCount.Store(int64(totalSSTables))
	e.stats.CompactionCount++
	e.metrics.Storage().CompactionCount.Add(1)
	e.metrics.Storage().CompactionDurationNs.Store(tracker.Finish())

	e.logger.WithFields(map[string]interface{}{
		"sourceLevel":    sourceLevel,
		"targetLevel":    targetLevel,
		"compactedCount": len(allTables),
		"newCount":       len(newSSTables),
	}).Info("Level-based compaction completed")

	// Clean up old tables
	e.cleanupInBackground(allTables)
	return true
}

// performSimpleCompaction merges all SSTables in level 0 into one.
func (e *Engine) performSimpleCompaction(tracker *metrics.LatencyTracker) {
	e.mutex.Lock()

	// Check if compaction is needed
	if len(e.sstables[0]) < e.config.CompactionThreshold {
		e.mutex.Unlock()
		e.logger.Debug("Simple compaction not needed")
		return
	}

	e.logger.WithField("sstableCount", len(e.sstables[0])).Info("Simple compaction starting")

	tablesToCompact := make([]*SSTable, len(e.sstables[0]))
	copy(tablesToCompact, e.sstables[0])
	bottom := e.isBottomLevelLocked(0)

	e.mutex.Unlock()

	// Perform compaction (later tables in a level are newer)
	newSSTables, err := e.compactSSTablesForLevel(newestFirst(tablesToCompact), 0, bottom)
	if err != nil {
		e.logger.WithError(err).Error("Simple compaction failed")
		e.metrics.Storage().CompactionErrors.Add(1)
		return
	}

	// Update engine state
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// Keep tables flushed while compaction ran, and place the output by
	// generation: it holds data no newer than its newest input, so tables
	// flushed meanwhile stay ahead of it
	level0 := e.removeCompactedTables(e.sstables[0], tablesToCompact)
	for _, table := range newSSTables {
		level0 = insertByGen(level0, table)
	}
	e.sstables[0] = level0

	totalSSTables := 0
	for level := range e.sstables {
		totalSSTables += len(e.sstables[level])
	}
	e.metrics.Storage().SSTableCount.Store(int64(totalSSTables))
	e.stats.CompactionCount++
	e.metrics.Storage().CompactionCount.Add(1)
	e.metrics.Storage().CompactionDurationNs.Store(tracker.Finish())

	e.logger.WithFields(map[string]interface{}{
		"oldCount": len(tablesToCompact),
		"newCount": len(newSSTables),
	}).Info("Simple compaction completed")

	e.cleanupInBackground(tablesToCompact)
}

// performSizeTieredCompaction implements size-tiered compaction (placeholder).
func (e *Engine) performSizeTieredCompaction(tracker *metrics.LatencyTracker) {
	// For now, fall back to simple compaction
	e.performSimpleCompaction(tracker)
}

// maxFilesForLevel returns the maximum number of files allowed at a level.
func (e *Engine) maxFilesForLevel(level int) int {
	if level == 0 {
		return e.config.CompactionThreshold
	}
	// Exponential growth: level 1 = 10 files, level 2 = 100 files, etc.
	multiplier := e.config.LevelSizeMultiplier
	if multiplier <= 0 {
		multiplier = 10
	}
	result := e.config.CompactionThreshold
	for i := 0; i < level; i++ {
		result *= multiplier
	}
	return result
}

// selectOverlappingSSTables finds SSTables in targetLevel that overlap with sourceTables.
func (e *Engine) selectOverlappingSSTables(targetLevel int, sourceTables []*SSTable) []*SSTable {
	if targetLevel >= len(e.sstables) {
		return []*SSTable{}
	}

	// Find min and max keys from source tables
	var minKey, maxKey string
	for _, sst := range sourceTables {
		if minKey == "" || sst.minKey < minKey {
			minKey = sst.minKey
		}
		if maxKey == "" || sst.maxKey > maxKey {
			maxKey = sst.maxKey
		}
	}

	// Select overlapping tables from target level
	overlapping := make([]*SSTable, 0)
	for _, sst := range e.sstables[targetLevel] {
		// Check if key ranges overlap
		if sst.maxKey >= minKey && sst.minKey <= maxKey {
			overlapping = append(overlapping, sst)
		}
	}

	return overlapping
}

// removeCompactedTables removes specified tables from a slice.
func (e *Engine) removeCompactedTables(allTables []*SSTable, toRemove []*SSTable) []*SSTable {
	removeMap := make(map[*SSTable]bool)
	for _, sst := range toRemove {
		removeMap[sst] = true
	}

	result := make([]*SSTable, 0)
	for _, sst := range allTables {
		if !removeMap[sst] {
			result = append(result, sst)
		}
	}

	return result
}

// compactSSTablesForLevel merges multiple SSTables into new SSTables for a specific level.
// newestFirst returns a level's tables in reverse order: tables are appended
// as they are created, so the last one is the newest.
func newestFirst(tables []*SSTable) []*SSTable {
	reversed := make([]*SSTable, len(tables))
	for i, table := range tables {
		reversed[len(tables)-1-i] = table
	}
	return reversed
}

// isBottomLevelLocked reports whether no level below level holds any table.
// Only a compaction into the bottom level may drop expired tombstones: above
// it, a tombstone may still hide an older value in a deeper level, which
// would come back if the tombstone were dropped. Caller must hold e.mutex;
// compactions run one at a time, so the answer holds for the whole compaction.
func (e *Engine) isBottomLevelLocked(level int) bool {
	for deeper := level + 1; deeper < len(e.sstables); deeper++ {
		if len(e.sstables[deeper]) > 0 {
			return false
		}
	}
	return true
}

// compactSSTablesForLevel merges tables, which must be ordered newest first,
// keeping only the newest version of each key. Expired tombstones are dropped
// only if bottom is set (see isBottomLevelLocked).
func (e *Engine) compactSSTablesForLevel(tables []*SSTable, targetLevel int, bottom bool) ([]*SSTable, error) {
	if len(tables) == 0 {
		return nil, errors.New(errors.ErrCodeInternal, "no SSTables to compact")
	}

	e.logger.WithField("tableCount", len(tables)).Debug("Starting SSTable compaction for level")

	// Create iterators for all SSTables
	iterators := make([]Iterator, len(tables))
	var gen uint64
	for i, table := range tables {
		iterators[i] = NewSSTableIterator(table)
		if table.gen > gen {
			gen = table.gen
		}
	}

	// Create merge iterator
	mergeIterator := NewMergeIterator(iterators)
	defer mergeIterator.Close()

	// Create temporary MemTable to collect compacted entries
	tempMemTable := NewMemTable()
	processedCount := 0
	tombstonesRemoved := 0

	// Split into multiple SSTables if too large
	newSSTables := make([]*SSTable, 0)
	maxSSTableSize := e.config.SSTableMaxSize

	for mergeIterator.Valid() {
		entry := mergeIterator.Value()
		if entry == nil {
			mergeIterator.Next()
			continue
		}

		// Skip expired tombstones, unless they may hide data in a deeper level
		if bottom && entry.Deleted && entry.IsExpired(e.config.TombstoneTTL) {
			tombstonesRemoved++
			mergeIterator.Next()
			continue
		}

		// Add entry to temporary MemTable
		if err := tempMemTable.Put(*entry); err != nil {
			e.logger.WithError(err).Error("Failed to add entry during compaction")
			return nil, errors.Wrap(err, errors.ErrCodeCompactionFailed,
				"failed to add entry during compaction")
		}

		processedCount++

		// Check if we need to flush this MemTable to a new SSTable
		if tempMemTable.Size() >= maxSSTableSize {
			newSSTable, err := e.flushCompactedMemTable(tempMemTable, targetLevel, gen)
			if err != nil {
				return nil, err
			}
			newSSTables = append(newSSTables, newSSTable)
			tempMemTable = NewMemTable()
		}

		mergeIterator.Next()
	}

	// Flush remaining data
	if tempMemTable.Size() > 0 {
		newSSTable, err := e.flushCompactedMemTable(tempMemTable, targetLevel, gen)
		if err != nil {
			return nil, err
		}
		newSSTables = append(newSSTables, newSSTable)
	}

	e.metrics.Storage().TombstonesCollected.Add(uint64(tombstonesRemoved))
	e.logger.WithFields(map[string]interface{}{
		"processedCount":    processedCount,
		"tombstonesRemoved": tombstonesRemoved,
		"newSSTableCount":   len(newSSTables),
	}).Info("Compaction processing completed")

	return newSSTables, nil
}

// flushCompactedMemTable creates an SSTable from a compacted MemTable. The
// output takes gen, the generation of the newest input table, so it sorts
// before any table flushed while the compaction ran.
func (e *Engine) flushCompactedMemTable(memTable *MemTable, level int, gen uint64) (*SSTable, error) {
	timestamp := time.Now().UnixNano()
	compactedPath := filepath.Join(e.dataDir, "sstables",
		fmt.Sprintf("compacted_L%d_g%d_%d.db", level, gen, timestamp))

	newSSTable, err := CreateSSTable(memTable, compactedPath, e.config)
	if err != nil {
		e.logger.WithError(err).Error("Failed to create compacted SSTable")
		return nil, errors.Wrap(err, errors.ErrCodeCompactionFailed,
			"failed to create compacted SSTable")
	}

	newSSTable.level = level
	newSSTable.gen = gen
	return newSSTable, nil
}

// cleanupInBackground deletes compacted tables without holding up the
// compaction worker; closing a table waits for open iterators on it. Close
// waits for the cleanup, so a clean shutdown leaves no compacted inputs behind.
func (e *Engine) cleanupInBackground(tables []*SSTable) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.cleanupOldSSTables(tables)
	}()
}

// cleanupOldSSTables closes and deletes old SSTable files after compaction.
func (e *Engine) cleanupOldSSTables(oldTables interface{}) {
	// Handle both []*SSTable and [][]*SSTable
	var tables []*SSTable
	switch v := oldTables.(type) {
	case []*SSTable:
		tables = v
	case [][]*SSTable:
		for _, level := range v {
			tables = append(tables, level...)
		}
	default:
		e.logger.Error("Invalid type for oldTables in cleanupOldSSTables")
		return
	}

	if len(tables) == 0 {
		return
	}
	e.logger.WithField("tableCount", len(tables)).Debug("Cleaning up old SSTables")

	successCount := 0
	errorCount := 0

	for _, table := range tables {
		// Close the SSTable
		if err := table.Close(); err != nil {
			e.logger.WithError(err).WithField("path", table.filePath).
				Warn("Error closing old SSTable")
			errorCount++
		}

		// Delete the file
		if err := os.Remove(table.filePath); err != nil {
			e.logger.WithError(err).WithField("path", table.filePath).
				Warn("Error deleting old SSTable file")
			errorCount++
		} else {
			successCount++
			e.logger.WithField("path", table.filePath).Debug("Deleted old SSTable file")
		}

		// Delete metadata file if it exists
		metadataPath := table.filePath + ".meta"
		if err := os.Remove(metadataPath); err != nil && !os.IsNotExist(err) {
			e.logger.WithError(err).WithField("path", metadataPath).
				Warn("Error deleting SSTable metadata file")
		}
	}

	e.logger.WithFields(map[string]interface{}{
		"successCount": successCount,
		"errorCount":   errorCount,
	}).Info("SSTable cleanup completed")
}

// walPath returns the path to the engine's write-ahead log file.
func (e *Engine) walPath() string {
	return filepath.Join(e.dataDir, walFileName)
}

// recoverWAL recovers the writes held only in the write-ahead log, then opens
// the log for appending. It runs during NewEngine, after SSTables are loaded.
//
// Sealed segments belong to MemTables that were sealed but not flushed. Each
// is written to the SSTable its MemTable would have become, with the
// generation in its name, so it takes its place among the other SSTables by
// age; replaying it into the active MemTable instead would put old writes
// ahead of every newer SSTable. A segment whose SSTable already exists (a
// crash after the flush, before the segment's removal) is just removed. Each
// segment is removed only once its SSTable is durable.
//
// The active log holds the newest writes and is replayed into the active
// MemTable. Replay tolerates a torn final record (a crash mid-append); see
// ReplayWAL.
func (e *Engine) recoverWAL() error {
	path := e.walPath()

	// Older versions rewrote the log during recovery via this temporary file;
	// one left by a crash is incomplete, and the files it came from are intact
	if err := os.Remove(path + ".rewrite"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove leftover WAL rewrite: %w", err)
	}

	segments, err := filepath.Glob(path + ".*.sealed")
	if err != nil {
		return fmt.Errorf("failed to list sealed WAL segments: %w", err)
	}

	// Segments from older versions ("<wal>.<nanos>.sealed") carry no
	// generation. They predate this version's first start, so no segment
	// with a generation can exist alongside them; as before, they rank above
	// every SSTable, in name (time) order.
	type sealedSegment struct {
		path string
		gen  uint64
	}
	var ordered, legacy []sealedSegment
	for _, segment := range segments {
		if gen, ok := parseSealedSegmentGen(path, segment); ok {
			ordered = append(ordered, sealedSegment{segment, gen})
			if gen >= e.nextGen {
				e.nextGen = gen + 1
			}
		} else {
			legacy = append(legacy, sealedSegment{path: segment})
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].gen < ordered[j].gen })
	sort.Slice(legacy, func(i, j int) bool { return legacy[i].path < legacy[j].path })
	for i := range legacy {
		legacy[i].gen = e.nextGen
		e.nextGen++
	}

	for _, segment := range append(ordered, legacy...) {
		if err := e.recoverSegment(segment.path, segment.gen); err != nil {
			return err
		}
	}

	// Replay the active log last: its writes are the most recent. Insert
	// directly into the active MemTable so they are not re-appended to the
	// log. Later records for the same key overwrite earlier ones, matching
	// the original write order.
	recovered, err := ReplayWAL(path, func(entry *Entry) error {
		return e.activeMemTable.Put(*entry)
	})
	if err != nil {
		return err
	}
	if recovered > 0 {
		e.logger.WithField("recoveredRecords", recovered).
			Info("Recovered writes from write-ahead log")
	}

	// Open (or create) the log for subsequent appends. Existing records are
	// preserved: they still describe the current MemTable contents and must not
	// be truncated until that MemTable is flushed to an SSTable.
	wal, err := OpenWAL(path, e.config.WALSyncOnPut)
	if err != nil {
		return err
	}
	e.wal = wal
	return nil
}

// recoverSegment writes a sealed WAL segment to an SSTable of generation gen
// and removes the segment.
func (e *Engine) recoverSegment(segment string, gen uint64) error {
	memTable := NewMemTable()
	if !e.hasSSTableGen(gen) {
		recovered, err := ReplayWAL(segment, func(entry *Entry) error {
			return memTable.Put(*entry)
		})
		if err != nil {
			return err
		}
		e.logger.WithFields(map[string]interface{}{
			"segment":          segment,
			"generation":       gen,
			"recoveredRecords": recovered,
		}).Info("Recovering sealed WAL segment into an SSTable")
	}

	if memTable.Size() > 0 {
		sstablesDir := filepath.Join(e.dataDir, "sstables")
		if err := os.MkdirAll(sstablesDir, 0755); err != nil {
			return fmt.Errorf("failed to create sstables directory: %w", err)
		}
		sstable, err := e.createSSTable(memTable, filepath.Join(sstablesDir, fmt.Sprintf("sstable_%d.db", gen)))
		if err != nil {
			return fmt.Errorf("failed to write SSTable for WAL segment %s: %w", segment, err)
		}
		sstable.level = 0
		sstable.gen = gen
		e.sstables[0] = insertByGen(e.sstables[0], sstable)
	}

	// Only now are the segment's records durable elsewhere. Persist the
	// removal too: a legacy segment that came back would be recovered again
	// with a newer generation than the data written after it
	if err := RemoveSegment(segment); err != nil {
		return err
	}
	return syncDir(filepath.Dir(segment))
}

// hasSSTableGen reports whether any loaded SSTable has generation gen. A
// compaction output takes the generation of its newest input, so it also
// counts: it holds everything that input held.
func (e *Engine) hasSSTableGen(gen uint64) bool {
	for _, tables := range e.sstables {
		for _, table := range tables {
			if table.gen == gen {
				return true
			}
		}
	}
	return false
}

// loadExistingSSTables scans the data directory and opens existing SSTables.
func (e *Engine) loadExistingSSTables() error {
	sstablesDir := filepath.Join(e.dataDir, "sstables")
	files, err := filepath.Glob(filepath.Join(sstablesDir, "*.db"))
	if err != nil {
		return err
	}

	e.logger.WithField("fileCount", len(files)).Debug("Loading existing SSTables")

	for _, filePath := range files {
		sstable, err := OpenSSTable(filePath)
		if err != nil {
			e.logger.WithError(err).WithField("path", filePath).
				Warn("Failed to open SSTable, skipping")
			continue
		}

		sstable.gen = parseSSTableGen(filepath.Base(filePath))
		sstable.level = parseSSTableLevel(filepath.Base(filePath))
		if sstable.gen >= e.nextGen {
			e.nextGen = sstable.gen + 1
		}

		// Add to appropriate level, in generation order
		level := sstable.level
		if level < 0 || level >= len(e.sstables) {
			e.logger.WithField("level", level).Warn("Invalid SSTable level, adding to level 0")
			level = 0
		}
		e.sstables[level] = insertByGen(e.sstables[level], sstable)
	}

	return nil
}

// flattenLevelsIfOutOfOrder moves every SSTable into level 0, in generation
// order, if the levels are out of order (see levelsOrderedLocked). Run at
// startup, after recovery has added its SSTables.
func (e *Engine) flattenLevelsIfOutOfOrder() {
	if e.levelsOrderedLocked() {
		return
	}
	e.logger.Warn("SSTable levels out of order (compaction or flush interrupted by a crash), loading all tables into level 0")
	var all []*SSTable
	for level := range e.sstables {
		for _, sstable := range e.sstables[level] {
			all = insertByGen(all, sstable)
		}
		e.sstables[level] = make([]*SSTable, 0)
	}
	e.sstables[0] = all
}

// levelsOrderedLocked reports whether every table holds newer data than every
// table in a deeper level, which reads rely on: they search levels top down.
// A compaction keeps this true in memory, but a crash after writing its
// output and before deleting its inputs leaves inputs on disk at a shallower
// level than an output with the same or newer generation; the output may
// even be partial. Likewise, recovery may write a sealed WAL segment to a
// level-0 table older than data already compacted below it. Generation order
// alone stays correct in those cases, since an output's generation is that of
// its newest input.
func (e *Engine) levelsOrderedLocked() bool {
	var maxBelow uint64
	seenBelow := false
	for level := len(e.sstables) - 1; level >= 0; level-- {
		tables := e.sstables[level]
		if len(tables) == 0 {
			continue
		}
		if seenBelow && tables[0].gen <= maxBelow {
			return false
		}
		if gen := tables[len(tables)-1].gen; !seenBelow || gen > maxBelow {
			maxBelow = gen
		}
		seenBelow = true
	}
	return true
}

// parseSSTableGen extracts the generation from an SSTable file name:
// sstable_<gen>.db or compacted_L<level>_g<gen>_<nanos>.db. Files written
// before generations existed (sstable_<nanos>.db, sstable_final_<nanos>.db,
// compacted_L<level>_<nanos>.db) use their creation time, which keeps their
// previous relative order; new generations continue above them.
func parseSSTableGen(name string) uint64 {
	name = strings.TrimSuffix(name, ".db")
	parts := strings.Split(name, "_")
	for i, part := range parts {
		if strings.HasPrefix(part, "g") && i > 0 && strings.HasPrefix(parts[0], "compacted") {
			if gen, err := strconv.ParseUint(part[1:], 10, 64); err == nil {
				return gen
			}
		}
	}
	gen, _ := strconv.ParseUint(parts[len(parts)-1], 10, 64)
	return gen
}

// parseSSTableLevel extracts the level from a compacted_L<level>_... file
// name; flushed tables (sstable_...) are in level 0.
func parseSSTableLevel(name string) int {
	rest, ok := strings.CutPrefix(name, "compacted_L")
	if !ok {
		return 0
	}
	digits, _, _ := strings.Cut(rest, "_")
	level, err := strconv.Atoi(digits)
	if err != nil {
		return 0
	}
	return level
}

// insertByGen inserts table into a level kept sorted by generation, oldest first.
func insertByGen(tables []*SSTable, table *SSTable) []*SSTable {
	i := sort.Search(len(tables), func(i int) bool { return tables[i].gen > table.gen })
	tables = append(tables, nil)
	copy(tables[i+1:], tables[i:])
	tables[i] = table
	return tables
}
