package storage

import (
	"fmt"
	"path/filepath"
	"testing"
)

// memoryLimitedEngine returns an engine whose MemTables may hold at most
// limit bytes in total, as counted by its memory monitor.
func memoryLimitedEngine(t *testing.T, dir string, memTableSize int, limit int64) *Engine {
	t.Helper()
	cfg := DefaultStorageConfig()
	cfg.MemTableMaxSize = memTableSize
	cfg.CompactionThreshold = 1 << 30
	e, err := NewEngine(dir, cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	e.memoryMonitor.config.MaxMemTableMemory = limit
	return e
}

// TestFlushReleasesMemTableMemory covers the MemTable memory limit across
// flushes: a flushed MemTable must stop counting against the limit, or the
// engine refuses every write once its lifetime writes reach the limit.
func TestFlushReleasesMemTableMemory(t *testing.T) {
	const limit = 1 << 20
	e := memoryLimitedEngine(t, filepath.Join(t.TempDir(), "data"), 64<<10, limit)
	defer e.Close()

	value := make([]byte, 16<<10)
	for i := 0; i < 4*limit/len(value); i++ { // 4x the limit in total
		if err := e.Put(fmt.Sprintf("key-%03d", i), value, nil); err != nil {
			t.Fatalf("write %d refused after %d flushes: %v", i, e.metrics.Storage().FlushCount.Load(), err)
		}
	}

	e.mutex.RLock()
	defer e.mutex.RUnlock()
	held := e.activeMemTable.accounted
	for _, mt := range e.flushingMemTables {
		held += mt.accounted
	}
	if usage := e.memoryMonitor.memTableUsage.Load(); usage != held {
		t.Fatalf("memory monitor counts %d MemTable bytes, MemTables hold %d", usage, held)
	}
}

// TestRefusedWriteIsNotRecorded covers a write refused for lack of MemTable
// memory: it must not count against the limit afterwards, and must not reach
// the WAL, or recovery would apply a write the caller was told had failed.
func TestRefusedWriteIsNotRecorded(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	e := memoryLimitedEngine(t, dir, 64<<20, 1024)

	if err := e.Put("small", make([]byte, 100), nil); err != nil {
		t.Fatalf("Put small: %v", err)
	}
	if err := e.Put("large", make([]byte, 4096), nil); err != ErrMemoryLimitExceeded {
		t.Fatalf("Put large = %v, want %v", err, ErrMemoryLimitExceeded)
	}
	if err := e.Put("small-2", make([]byte, 100), nil); err != nil {
		t.Fatalf("write within the limit refused after an earlier refusal: %v", err)
	}

	e.CrashForTesting()
	reopened, err := NewEngine(dir, e.config)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer reopened.Close()
	for _, key := range []string{"small", "small-2"} {
		if _, err := reopened.Get(key); err != nil {
			t.Fatalf("acknowledged write %s lost: %v", key, err)
		}
	}
	if _, err := reopened.Get("large"); err != ErrKeyNotFound {
		t.Fatalf("refused write visible after recovery: err=%v", err)
	}
}
