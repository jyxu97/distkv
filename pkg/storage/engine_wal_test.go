package storage_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"distkv/pkg/consensus"
	"distkv/pkg/storage"
)

// walConfig returns a config with a large MemTable so writes stay in memory
// (and thus only in the WAL) until we decide to flush — the scenario where a
// crash would lose data without a WAL.
func walConfig() *storage.StorageConfig {
	cfg := storage.DefaultStorageConfig()
	cfg.MemTableMaxSize = 1 << 30 // 1GiB: never auto-flush during these tests
	cfg.WALEnabled = true
	cfg.WALSyncOnPut = true
	return cfg
}

// TestEngine_CrashRecovery is the core durability test: writes that were
// acknowledged but never flushed to an SSTable must be recovered from the WAL
// after a simulated crash. Before the WAL existed, this data was lost.
func TestEngine_CrashRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	engine, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	const n = 50
	for i := 0; i < n; i++ {
		vc := consensus.NewVectorClock()
		vc.Increment("node1")
		key := fmt.Sprintf("key%02d", i)
		if err := engine.Put(key, []byte(fmt.Sprintf("value%02d", i)), vc); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	// Simulate a crash: no graceful Close, so nothing is flushed to an SSTable.
	// The only durable copy of these writes is the WAL.
	engine.CrashForTesting()

	// Restart on the same directory.
	recovered, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("NewEngine after crash: %v", err)
	}
	defer recovered.Close()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%02d", i)
		entry, err := recovered.Get(key)
		if err != nil {
			t.Fatalf("Get %s after recovery: %v (write was lost!)", key, err)
		}
		if want := fmt.Sprintf("value%02d", i); string(entry.Value) != want {
			t.Errorf("%s = %q, want %q", key, entry.Value, want)
		}
	}
}

// TestEngine_CrashRecoveryPreservesVectorClock ensures causality metadata
// survives the WAL round-trip — this is what makes recovered data usable for
// conflict resolution rather than just opaque bytes.
func TestEngine_CrashRecoveryPreservesVectorClock(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	engine, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	vc := consensus.NewVectorClock()
	vc.Increment("node1")
	vc.Increment("node1")
	vc.Increment("node2")
	if err := engine.Put("k", []byte("v"), vc); err != nil {
		t.Fatalf("Put: %v", err)
	}
	engine.CrashForTesting()

	recovered, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("NewEngine after crash: %v", err)
	}
	defer recovered.Close()

	entry, err := recovered.Get("k")
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if entry.VectorClock == nil {
		t.Fatal("recovered entry lost its vector clock")
	}
	if !entry.VectorClock.Equal(vc) {
		t.Errorf("recovered clock = %s, want %s", entry.VectorClock.String(), vc.String())
	}
}

// TestEngine_CrashRecoveryWithDelete verifies a tombstone written before a
// crash is recovered, so a deleted key stays deleted after restart.
func TestEngine_CrashRecoveryWithDelete(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	engine, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	vc := consensus.NewVectorClock()
	vc.Increment("node1")
	if err := engine.Put("k", []byte("v"), vc); err != nil {
		t.Fatalf("Put: %v", err)
	}
	vc2 := vc.Copy()
	vc2.Increment("node1")
	if err := engine.Delete("k", vc2); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	engine.CrashForTesting()

	recovered, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("NewEngine after crash: %v", err)
	}
	defer recovered.Close()

	if _, err := recovered.Get("k"); err != storage.ErrKeyNotFound {
		t.Fatalf("expected deleted key to stay deleted after recovery, got err=%v", err)
	}
}

// TestEngine_WALTruncatedAfterGracefulClose verifies that a clean shutdown
// flushes to an SSTable and truncates the WAL, so recovery reads the data from
// the SSTable (not a stale WAL) and the log does not grow without bound.
func TestEngine_WALTruncatedAfterGracefulClose(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	engine, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	vc := consensus.NewVectorClock()
	vc.Increment("node1")
	if err := engine.Put("k", []byte("v"), vc); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After graceful close the WAL should be empty (data is in an SSTable).
	walPath := filepath.Join(dir, "wal.log")
	got := 0
	if _, err := storage.ReplayWAL(walPath, func(*storage.Entry) error { got++; return nil }); err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}
	if got != 0 {
		t.Errorf("expected WAL to be truncated after graceful close, but it has %d records", got)
	}

	// And the data is still readable via the SSTable path.
	recovered, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer recovered.Close()
	entry, err := recovered.Get("k")
	if err != nil {
		t.Fatalf("Get after clean restart: %v", err)
	}
	if string(entry.Value) != "v" {
		t.Errorf("value = %q, want \"v\"", entry.Value)
	}
}

// TestEngine_CrashRecoveryAcrossFlush covers the rotation path: some writes are
// flushed to an SSTable (WAL rotated), then more writes land in the new MemTable
// and only the WAL, then a crash. Recovery must return BOTH the flushed and the
// unflushed writes.
func TestEngine_CrashRecoveryAcrossFlush(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	// Small MemTable so the first batch of writes triggers a real flush+rotate.
	cfg := walConfig()
	cfg.MemTableMaxSize = 200

	engine, err := storage.NewEngine(dir, cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	// Enough writes to force at least one flush (each entry is well over the
	// 200-byte threshold once key+value+clock+overhead are counted).
	const n = 30
	for i := 0; i < n; i++ {
		vc := consensus.NewVectorClock()
		vc.Increment("node1")
		key := fmt.Sprintf("key%02d", i)
		if err := engine.Put(key, []byte(fmt.Sprintf("value-%02d", i)), vc); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	engine.CrashForTesting()

	recovered, err := storage.NewEngine(dir, cfg)
	if err != nil {
		t.Fatalf("NewEngine after crash: %v", err)
	}
	defer recovered.Close()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%02d", i)
		entry, err := recovered.Get(key)
		if err != nil {
			t.Fatalf("Get %s after recovery across flush: %v", key, err)
		}
		if want := fmt.Sprintf("value-%02d", i); string(entry.Value) != want {
			t.Errorf("%s = %q, want %q", key, entry.Value, want)
		}
	}
}

// TestEngine_WALDisabled confirms the durability behavior is opt-out: with the
// WAL disabled, unflushed writes are lost on a crash (documents the trade-off).
func TestEngine_WALDisabled(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	cfg := walConfig()
	cfg.WALEnabled = false

	engine, err := storage.NewEngine(dir, cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	vc := consensus.NewVectorClock()
	vc.Increment("node1")
	if err := engine.Put("k", []byte("v"), vc); err != nil {
		t.Fatalf("Put: %v", err)
	}
	engine.CrashForTesting()

	recovered, err := storage.NewEngine(dir, cfg)
	if err != nil {
		t.Fatalf("NewEngine after crash: %v", err)
	}
	defer recovered.Close()

	if _, err := recovered.Get("k"); err != storage.ErrKeyNotFound {
		t.Fatalf("with WAL disabled, expected write to be lost after crash, got err=%v", err)
	}
}
