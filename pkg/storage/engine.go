// Package storage - Main storage engine that coordinates MemTables and SSTables
// This is the main interface that the rest of the system uses for data operations.
package storage

import (
	"distkv/pkg/consensus"
	"distkv/pkg/errors"
	"distkv/pkg/logging"
	"distkv/pkg/metrics"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

	// flushingMemTables are being written to disk (read-only)
	flushingMemTables []*MemTable

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

	// stats tracks performance metrics
	stats *StorageStats

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

	// Start background workers
	engine.startBackgroundWorkers()

	logger.Info("Storage engine initialized successfully")
	return engine, nil
}

// Put stores a key-value pair in the storage engine.
// This is the main write path for the LSM-tree.
func (e *Engine) Put(key string, value []byte, vectorClock *consensus.VectorClock) error {
	// Track latency
	tracker := metrics.NewLatencyTracker()
	defer func() {
		latency := tracker.Finish()
		e.metrics.Storage().WriteLatencyNs.Store(latency)
	}()

	// Validate inputs
	if key == "" {
		e.metrics.Storage().WriteErrors.Add(1)
		return errors.New(errors.ErrCodeInvalidKey, "key cannot be empty")
	}
	if value == nil {
		e.metrics.Storage().WriteErrors.Add(1)
		return errors.New(errors.ErrCodeInvalidValue, "value cannot be nil")
	}

	e.mutex.Lock()

	if e.closed {
		e.mutex.Unlock()
		e.metrics.Storage().WriteErrors.Add(1)
		return errors.NewStorageClosedError()
	}

	// Create entry
	entry := NewEntry(key, value, vectorClock)

	// Durability: append to the write-ahead log and fsync BEFORE inserting into
	// the MemTable. If we crash after this returns, the write is recoverable on
	// restart; if the append fails, we reject the write rather than acknowledge
	// something we can't recover. This is what makes an ack'd write survive a crash.
	if e.wal != nil {
		if err := e.wal.Append(entry); err != nil {
			e.mutex.Unlock()
			e.stats.WriteErrors++
			e.metrics.Storage().WriteErrors.Add(1)
			e.logger.WithError(err).WithField("key", key).Error("Failed to append to WAL")
			return errors.Wrap(err, errors.ErrCodeInternal, "failed to append to WAL")
		}
	}

	// Estimate memory usage and check limits
	estimatedSize := int64(len(key) + len(value) + 64) // Rough estimate
	if err := e.memoryMonitor.RecordMemTableAllocation(estimatedSize); err != nil {
		e.stats.WriteErrors++
		e.metrics.Storage().WriteErrors.Add(1)
		e.mutex.Unlock()
		e.logger.WithError(err).WithField("estimatedSize", estimatedSize).
			Error("Memory limit exceeded, cannot write entry")
		return err
	}

	// Add to active MemTable
	if err := e.activeMemTable.Put(*entry); err != nil {
		// Rollback memory allocation on error
		e.memoryMonitor.RecordMemTableDeallocation(estimatedSize)
		e.stats.WriteErrors++
		e.metrics.Storage().WriteErrors.Add(1)
		e.mutex.Unlock()
		e.logger.WithError(err).WithField("key", key).Error("Failed to put entry to MemTable")
		return errors.Wrap(err, errors.ErrCodeInternal, "failed to put entry to MemTable")
	}

	// Update stats and metrics
	e.stats.WriteCount++
	e.metrics.Storage().WriteOps.Add(1)

	memTableSize := e.activeMemTable.Size()
	e.metrics.Storage().MemTableSize.Store(memTableSize)

	// Check if MemTable needs to be flushed
	needsFlush := memTableSize >= int64(e.config.MemTableMaxSize)
	if needsFlush {
		e.logger.WithFields(map[string]interface{}{
			"memTableSize": memTableSize,
			"threshold":    e.config.MemTableMaxSize,
		}).Info("MemTable threshold reached, triggering flush")
	}

	// Release the mutex before flushing to avoid deadlock
	e.mutex.Unlock()

	if needsFlush {
		// For testing: force synchronous flush
		e.performFlush()
	}

	return nil
}

// Get retrieves a value by key from the storage engine.
// This implements the LSM-tree read path: MemTable first, then SSTables.
func (e *Engine) Get(key string) (*Entry, error) {
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
		e.stats.ReadCount++
		e.metrics.Storage().ReadOps.Add(1)
		// Update average read latency (simplified)
		e.stats.AvgReadLatency = time.Since(startTime)
	}()

	// First check active MemTable
	if entry := e.activeMemTable.Get(key); entry != nil {
		e.metrics.Storage().CacheHits.Add(1) // MemTable is effectively a cache
		if entry.Deleted {
			return nil, ErrKeyNotFound // Tombstone found
		}
		return entry, nil
	}

	// Check flushing MemTables
	for _, memTable := range e.flushingMemTables {
		if entry := memTable.Get(key); entry != nil {
			e.metrics.Storage().CacheHits.Add(1)
			if entry.Deleted {
				return nil, ErrKeyNotFound // Tombstone found
			}
			return entry, nil
		}
	}

	// Check SSTables level by level (lower levels first - more recent data)
	e.metrics.Storage().CacheMisses.Add(1)
	for level := 0; level < len(e.sstables); level++ {
		for i := len(e.sstables[level]) - 1; i >= 0; i-- {
			entry, err := e.sstables[level][i].Get(key)
			if err == nil {
				if entry.Deleted {
					return nil, ErrKeyNotFound // Tombstone found
				}
				return entry, nil
			}
			if err != ErrKeyNotFound {
				e.stats.ReadErrors++
				e.metrics.Storage().ReadErrors.Add(1)
				e.logger.WithError(err).WithField("key", key).
					Error("Error reading from SSTable")
				return nil, errors.Wrap(err, errors.ErrCodeInternal,
					"failed to read from SSTable")
			}
		}
	}

	return nil, ErrKeyNotFound
}

// Delete marks a key as deleted by inserting a tombstone.
func (e *Engine) Delete(key string, vectorClock *consensus.VectorClock) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.closed {
		return ErrStorageClosed
	}

	// Create tombstone entry
	entry := NewDeleteEntry(key, vectorClock)

	// Durability: persist the tombstone to the WAL before inserting it into the
	// MemTable, so an acknowledged delete survives a crash just like a put.
	if e.wal != nil {
		if err := e.wal.Append(entry); err != nil {
			e.stats.WriteErrors++
			e.logger.WithError(err).WithField("key", key).Error("Failed to append delete to WAL")
			return errors.Wrap(err, errors.ErrCodeInternal, "failed to append delete to WAL")
		}
	}

	// Add tombstone to active MemTable
	if err := e.activeMemTable.Put(*entry); err != nil {
		e.stats.WriteErrors++
		return fmt.Errorf("failed to put delete entry: %v", err)
	}

	// Update stats
	e.stats.WriteCount++

	// Check if MemTable needs to be flushed
	if e.activeMemTable.Size() >= int64(e.config.MemTableMaxSize) {
		e.triggerFlush()
	}

	return nil
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

	// Flush any remaining data in active MemTable
	e.mutex.Lock()
	if e.activeMemTable.Size() > 0 {
		e.logger.WithField("memTableSize", e.activeMemTable.Size()).
			Info("Flushing remaining MemTable data")
		flushingMemTable := e.activeMemTable
		flushingMemTable.SetReadOnly()
		e.mutex.Unlock()

		// Perform final flush
		sstablesDir := filepath.Join(e.dataDir, "sstables")
		if err := os.MkdirAll(sstablesDir, 0755); err != nil {
			e.logger.WithError(err).Error("Failed to create sstables directory during shutdown")
			shutdownErrors = append(shutdownErrors, err)
		}
		timestamp := time.Now().UnixNano()
		fileName := fmt.Sprintf("sstable_final_%d.db", timestamp)
		filePath := filepath.Join(sstablesDir, fileName)

		if _, err := CreateSSTable(flushingMemTable, filePath, e.config); err != nil {
			e.logger.WithError(err).Error("Failed to flush final MemTable")
			shutdownErrors = append(shutdownErrors, err)
		} else {
			e.logger.Info("Final MemTable flushed successfully")
			// The remaining data is now durable in an SSTable, so the WAL that
			// covered it can be discarded. If truncation fails we keep the WAL;
			// replay on next startup is idempotent (same keys re-inserted).
			if e.wal != nil {
				if err := e.wal.Truncate(); err != nil {
					e.logger.WithError(err).Warn("Failed to truncate WAL after final flush")
				}
			}
		}
	} else {
		e.mutex.Unlock()
	}

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

// triggerFlush signals that a MemTable flush is needed.
func (e *Engine) triggerFlush() {
	select {
	case e.flushChan <- struct{}{}:
	default:
		// Channel full, flush already pending
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
	go func() {
		defer e.wg.Done()
		for {
			select {
			case <-e.flushChan:
				e.performFlush()
			case <-e.stopChan:
				return
			}
		}
	}()

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

// performFlush flushes the active MemTable to an SSTable.
func (e *Engine) performFlush() {
	tracker := metrics.NewLatencyTracker()
	e.logger.Info("Starting MemTable flush")

	e.mutex.Lock()

	// Move active MemTable to flushing list
	flushingMemTable := e.activeMemTable
	flushingMemTable.SetReadOnly()
	e.flushingMemTables = append(e.flushingMemTables, flushingMemTable)

	// Create new active MemTable
	e.activeMemTable = NewMemTable()
	memTableCount := len(e.flushingMemTables) + 1
	e.metrics.Storage().MemTableCount.Store(int64(memTableCount))

	// Seal the current WAL segment atomically with the MemTable swap. Records
	// for the MemTable we're about to flush are now in the sealed segment;
	// writes that arrive during the flush go to the fresh active log and are
	// preserved. We remove the sealed segment only after the SSTable is durable.
	var sealedWALSegment string
	if e.wal != nil {
		var walErr error
		sealedWALSegment, walErr = e.wal.Rotate()
		if walErr != nil {
			e.logger.WithError(walErr).Error("Failed to rotate WAL during flush")
			// Continue with the flush: correctness of the data is unaffected,
			// we just won't be able to reclaim the old WAL space this round.
		}
	}

	e.mutex.Unlock()

	// Create sstables subdirectory if it doesn't exist
	sstablesDir := filepath.Join(e.dataDir, "sstables")
	if err := os.MkdirAll(sstablesDir, 0755); err != nil {
		e.logger.WithError(err).Error("Failed to create sstables directory")
		e.metrics.Storage().FlushErrors.Add(1)
		return
	}

	// Generate SSTable file path
	timestamp := time.Now().UnixNano()
	fileName := fmt.Sprintf("sstable_%d.db", timestamp)
	filePath := filepath.Join(sstablesDir, fileName)

	// Create SSTable from MemTable
	e.logger.WithField("path", filePath).Info("Creating SSTable")
	sstable, err := CreateSSTable(flushingMemTable, filePath, e.config)
	if err != nil {
		e.logger.WithError(err).Error("Failed to create SSTable")
		e.metrics.Storage().FlushErrors.Add(1)
		return
	}

	// Add SSTable to level 0 (newest level)
	e.mutex.Lock()
	sstable.level = 0
	e.sstables[0] = append(e.sstables[0], sstable)

	totalSSTables := 0
	for level := range e.sstables {
		totalSSTables += len(e.sstables[level])
	}
	e.metrics.Storage().SSTableCount.Store(int64(totalSSTables))

	// Remove from flushing list
	for i, mt := range e.flushingMemTables {
		if mt == flushingMemTable {
			e.flushingMemTables = append(e.flushingMemTables[:i], e.flushingMemTables[i+1:]...)
			break
		}
	}

	level0Count := len(e.sstables[0])
	needsCompaction := level0Count >= e.config.CompactionThreshold
	e.mutex.Unlock()

	// The flushed MemTable is now durable in an SSTable, so the sealed WAL
	// segment that covered it is no longer needed for recovery and can be
	// reclaimed. Writes that arrived during the flush are in the fresh active
	// log and are untouched.
	if sealedWALSegment != "" {
		if err := RemoveSegment(sealedWALSegment); err != nil {
			e.logger.WithError(err).WithField("segment", sealedWALSegment).
				Warn("Failed to remove sealed WAL segment after flush")
		}
	}

	// Update metrics
	e.metrics.Storage().FlushCount.Add(1)
	e.metrics.Storage().FlushDurationNs.Store(tracker.Finish())
	e.logger.WithFields(map[string]interface{}{
		"path":               filePath,
		"level0SSTableCount": level0Count,
		"totalSSTableCount":  totalSSTables,
	}).Info("SSTable created successfully")

	// Check if compaction is needed (for level 0)
	if needsCompaction {
		e.logger.WithField("level0SSTableCount", level0Count).
			Info("Level 0 SSTable count exceeded threshold, triggering compaction")
		e.triggerCompaction()
	}
}

// performCompaction merges multiple SSTables into fewer, larger files.
// This is essential for maintaining good read performance in an LSM-tree.
func (e *Engine) performCompaction() {
	tracker := metrics.NewLatencyTracker()
	e.logger.Info("Starting compaction")

	switch e.config.CompactionStrategy {
	case CompactionLevelBased:
		e.performLevelBasedCompaction(tracker)
	case CompactionSizeTiered:
		e.performSizeTieredCompaction(tracker)
	default:
		e.performSimpleCompaction(tracker)
	}
}

// performLevelBasedCompaction implements level-based compaction strategy.
// Level 0 files are compacted into level 1, level 1 into level 2, etc.
func (e *Engine) performLevelBasedCompaction(tracker *metrics.LatencyTracker) {
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
		return
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

	e.mutex.Unlock()

	// Compact the selected tables
	allTables := append(sourceTables, targetTables...)
	newSSTables, err := e.compactSSTablesForLevel(allTables, targetLevel)
	if err != nil {
		e.logger.WithError(err).Error("Level-based compaction failed")
		e.metrics.Storage().CompactionErrors.Add(1)
		return
	}

	// Update the engine state
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// Remove only the compacted tables from source level (not any newly added ones)
	e.sstables[sourceLevel] = e.removeCompactedTables(e.sstables[sourceLevel], sourceTables)

	// Remove compacted tables from target level
	e.sstables[targetLevel] = e.removeCompactedTables(e.sstables[targetLevel], targetTables)

	// Add new tables to target level
	e.sstables[targetLevel] = append(e.sstables[targetLevel], newSSTables...)

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
	go e.cleanupOldSSTables(allTables)
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

	e.mutex.Unlock()

	// Perform compaction
	newSSTables, err := e.compactSSTablesForLevel(tablesToCompact, 0)
	if err != nil {
		e.logger.WithError(err).Error("Simple compaction failed")
		e.metrics.Storage().CompactionErrors.Add(1)
		return
	}

	// Update engine state
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.sstables[0] = newSSTables

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

	go e.cleanupOldSSTables(tablesToCompact)
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
func (e *Engine) compactSSTablesForLevel(tables []*SSTable, targetLevel int) ([]*SSTable, error) {
	if len(tables) == 0 {
		return nil, errors.New(errors.ErrCodeInternal, "no SSTables to compact")
	}

	e.logger.WithField("tableCount", len(tables)).Debug("Starting SSTable compaction for level")

	// Create iterators for all SSTables
	iterators := make([]Iterator, len(tables))
	for i, table := range tables {
		iterators[i] = NewSSTableIterator(table)
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

		// Skip expired tombstones
		if entry.Deleted && entry.IsExpired(e.config.TombstoneTTL) {
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
			newSSTable, err := e.flushCompactedMemTable(tempMemTable, targetLevel)
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
		newSSTable, err := e.flushCompactedMemTable(tempMemTable, targetLevel)
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

// flushCompactedMemTable creates an SSTable from a compacted MemTable.
func (e *Engine) flushCompactedMemTable(memTable *MemTable, level int) (*SSTable, error) {
	timestamp := time.Now().UnixNano()
	compactedPath := filepath.Join(e.dataDir, "sstables",
		fmt.Sprintf("compacted_L%d_%d.db", level, timestamp))

	newSSTable, err := CreateSSTable(memTable, compactedPath, e.config)
	if err != nil {
		e.logger.WithError(err).Error("Failed to create compacted SSTable")
		return nil, errors.Wrap(err, errors.ErrCodeCompactionFailed,
			"failed to create compacted SSTable")
	}

	newSSTable.level = level
	return newSSTable, nil
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

// recoverWAL replays any records in the write-ahead log into the active
// MemTable, then opens the log for appending. It runs during NewEngine, after
// SSTables are loaded, so replayed entries correctly shadow older on-disk
// versions. Replaying before opening the writable handle ensures we don't
// re-append the records we're recovering.
//
// Replay tolerates a torn final record (a crash mid-append); see ReplayWAL.
func (e *Engine) recoverWAL() error {
	path := e.walPath()

	replayInto := func(p string) (int, error) {
		return ReplayWAL(p, func(entry *Entry) error {
			// Insert directly into the active MemTable, bypassing Put so we don't
			// re-append to the WAL during recovery. Later records for the same key
			// overwrite earlier ones, matching the original write order.
			return e.activeMemTable.Put(*entry)
		})
	}

	total := 0

	// Replay any sealed segments left behind by a crash that occurred after a
	// WAL rotation but before the corresponding SSTable was durable. They are
	// named "<wal>.<nanos>.sealed"; sorting by name orders them oldest-first,
	// which is the order in which their writes originally happened.
	sealed, err := filepath.Glob(path + ".*.sealed")
	if err != nil {
		return fmt.Errorf("failed to list sealed WAL segments: %w", err)
	}
	sort.Strings(sealed)
	for _, seg := range sealed {
		n, err := replayInto(seg)
		if err != nil {
			return err
		}
		total += n
		// The segment's writes are now back in the MemTable and will be
		// re-flushed and re-logged, so the stale segment can be removed.
		if err := RemoveSegment(seg); err != nil {
			e.logger.WithError(err).WithField("segment", seg).
				Warn("Failed to remove replayed sealed WAL segment")
		}
	}

	// Replay the active log last: its writes are the most recent.
	n, err := replayInto(path)
	if err != nil {
		return err
	}
	total += n

	if total > 0 {
		e.logger.WithField("recoveredRecords", total).
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

		// Add to appropriate level
		level := sstable.level
		if level < 0 || level >= len(e.sstables) {
			e.logger.WithField("level", level).Warn("Invalid SSTable level, adding to level 0")
			level = 0
		}
		e.sstables[level] = append(e.sstables[level], sstable)
	}

	return nil
}
