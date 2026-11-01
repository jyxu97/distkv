package storage_test

import (
	"fmt"
	"os"
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

// TestEngine_WriteIfSeesTombstonesAndRefusesStale verifies the conditional
// write used by replicas: the condition sees the current entry including a
// tombstone, a refused write leaves the key untouched, and a tombstone's clock
// survives crash recovery so it can still supersede stale values afterwards.
func TestEngine_WriteIfSeesTombstonesAndRefusesStale(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	engine, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	older := consensus.NewVectorClockFromMap(map[string]uint64{"A": 1})
	newer := consensus.NewVectorClockFromMap(map[string]uint64{"A": 2})

	if err := engine.Put("k", []byte("v1"), older); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := engine.Delete("k", newer); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	notSuperseded := func(incoming *consensus.VectorClock) func(*storage.Entry) bool {
		return func(current *storage.Entry) bool {
			return current == nil || !current.VectorClock.IsAfter(incoming)
		}
	}

	// A delayed copy of the older value must not resurrect the deleted key
	applied, current, err := engine.WriteIf(storage.NewEntry("k", []byte("v1"), older), notSuperseded(older))
	if err != nil {
		t.Fatalf("WriteIf: %v", err)
	}
	if applied {
		t.Fatal("Expected stale write to be refused")
	}
	if current == nil || !current.Deleted || !current.VectorClock.Equal(newer) {
		t.Fatalf("Expected the condition to see the tombstone at %s, got %+v", newer, current)
	}

	// The tombstone and its clock survive a crash
	engine.CrashForTesting()
	recovered, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("NewEngine after crash: %v", err)
	}
	defer recovered.Close()

	entry, err := recovered.GetEntry("k")
	if err != nil {
		t.Fatalf("GetEntry after recovery: %v", err)
	}
	if !entry.Deleted || !entry.VectorClock.Equal(newer) {
		t.Fatalf("Expected recovered tombstone at %s, got deleted=%v clock=%s", newer, entry.Deleted, entry.VectorClock)
	}
	if applied, _, _ := recovered.WriteIf(storage.NewEntry("k", []byte("v1"), older), notSuperseded(older)); applied {
		t.Error("Expected stale write to be refused after recovery")
	}

	// A causally newer value is accepted
	newest := consensus.NewVectorClockFromMap(map[string]uint64{"A": 3})
	if applied, _, _ := recovered.WriteIf(storage.NewEntry("k", []byte("v3"), newest), notSuperseded(newest)); !applied {
		t.Error("Expected newer write to be applied")
	}
	if got, err := recovered.Get("k"); err != nil || string(got.Value) != "v3" {
		t.Errorf("Expected v3 after newer write, got %v, %v", got, err)
	}
}

// TestEngine_PurgeMarker verifies the local purge marker cleanup uses: the
// key reads as absent (no tombstone, no clock), stays absent across flush,
// compaction and crash recovery, and a later write makes it visible again.
func TestEngine_PurgeMarker(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	cfg := walConfig()
	engine, err := storage.NewEngine(dir, cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	vc := consensus.NewVectorClockFromMap(map[string]uint64{"A": 1})
	for _, key := range []string{"kept", "purged"} {
		if err := engine.Put(key, []byte("v1"), vc); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	purged, _, err := engine.WriteIf(storage.NewPurgeEntry("purged"), func(current *storage.Entry) bool {
		return current != nil
	})
	if err != nil || !purged {
		t.Fatalf("purge failed: applied=%v err=%v", purged, err)
	}

	assertAbsent := func(e *storage.Engine, when string) {
		t.Helper()
		if _, err := e.GetEntry("purged"); err != storage.ErrKeyNotFound {
			t.Fatalf("%s: GetEntry(purged) err = %v, want ErrKeyNotFound", when, err)
		}
		it, err := e.Iterator()
		if err != nil {
			t.Fatalf("%s: Iterator: %v", when, err)
		}
		defer it.Close()
		var keys []string
		for ; it.Valid(); it.Next() {
			keys = append(keys, it.Key())
		}
		if len(keys) != 1 || keys[0] != "kept" {
			t.Fatalf("%s: iterator returned %v, want only [kept]", when, keys)
		}
	}
	assertAbsent(engine, "after purge")

	// Survives a crash: the marker is in the WAL
	engine.CrashForTesting()
	recovered, err := storage.NewEngine(dir, cfg)
	if err != nil {
		t.Fatalf("NewEngine after crash: %v", err)
	}
	assertAbsent(recovered, "after crash recovery")

	// Survives a clean shutdown, which flushes the marker to an SSTable
	if err := recovered.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := storage.NewEngine(dir, cfg)
	if err != nil {
		t.Fatalf("NewEngine after close: %v", err)
	}
	defer reopened.Close()
	assertAbsent(reopened, "after flush to SSTable")

	// A new write makes the key visible again
	newer := consensus.NewVectorClockFromMap(map[string]uint64{"A": 2})
	if err := reopened.Put("purged", []byte("v2"), newer); err != nil {
		t.Fatalf("Put after purge: %v", err)
	}
	if entry, err := reopened.Get("purged"); err != nil || string(entry.Value) != "v2" {
		t.Fatalf("Get after re-write = %v, %v; want v2", entry, err)
	}
}

// TestEngine_RecoveredSegmentSurvivesSecondCrash covers a crash right after a
// WAL rotation (the sealed segment is the only durable copy of its writes),
// followed by a second crash before those recovered writes are flushed. The
// first recovery must not discard the segment's records before they are
// durable again.
func TestEngine_RecoveredSegmentSurvivesSecondCrash(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	walPath := filepath.Join(dir, "wal.log")

	first, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	v1 := consensus.NewVectorClockFromMap(map[string]uint64{"A": 1})
	for _, key := range []string{"k1", "k2"} {
		if err := first.Put(key, []byte("v1"), v1); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	first.CrashForTesting()

	// Crash right after rotation: the records sit in a sealed segment and no
	// SSTable holds them yet
	if err := os.Rename(walPath, walPath+".1.sealed"); err != nil {
		t.Fatalf("simulate rotation: %v", err)
	}

	// First recovery, then a newer write to k2, then a second crash before any flush
	second, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("first recovery: %v", err)
	}
	v2 := consensus.NewVectorClockFromMap(map[string]uint64{"A": 2})
	if err := second.Put("k2", []byte("v2"), v2); err != nil {
		t.Fatalf("Put after recovery: %v", err)
	}
	second.CrashForTesting()

	third, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("second recovery: %v", err)
	}
	defer third.Close()
	for key, want := range map[string]string{"k1": "v1", "k2": "v2"} {
		entry, err := third.Get(key)
		if err != nil || string(entry.Value) != want {
			t.Fatalf("after second crash %s = %v (%v), want %s", key, entry, err, want)
		}
	}
}

// TestEngine_RecoveryIgnoresIncompleteRewrite covers a crash while recovery
// was rewriting the log: the leftover temporary file must be ignored and the
// original segment and log recovered.
func TestEngine_RecoveryIgnoresIncompleteRewrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	walPath := filepath.Join(dir, "wal.log")

	first, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	vc := consensus.NewVectorClockFromMap(map[string]uint64{"A": 1})
	if err := first.Put("k", []byte("v"), vc); err != nil {
		t.Fatalf("Put: %v", err)
	}
	first.CrashForTesting()
	if err := os.Rename(walPath, walPath+".1.sealed"); err != nil {
		t.Fatalf("simulate rotation: %v", err)
	}
	if err := os.WriteFile(walPath+".rewrite", []byte("torn garbage"), 0o600); err != nil {
		t.Fatal(err)
	}

	recovered, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("recovery with leftover rewrite file: %v", err)
	}
	defer recovered.Close()
	if entry, err := recovered.Get("k"); err != nil || string(entry.Value) != "v" {
		t.Fatalf("k = %v (%v), want v", entry, err)
	}
}

// TestEngine_RecoveryCrashBeforeSegmentRemoval covers a crash after the
// rewritten log was installed but before the sealed segment was removed: the
// segment then replays before a log that already contains its records, which
// must still end with the newest values.
func TestEngine_RecoveryCrashBeforeSegmentRemoval(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	walPath := filepath.Join(dir, "wal.log")
	segment := walPath + ".1.sealed"

	first, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	v1 := consensus.NewVectorClockFromMap(map[string]uint64{"A": 1})
	if err := first.Put("k", []byte("v1"), v1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	first.CrashForTesting()
	if err := os.Rename(walPath, segment); err != nil {
		t.Fatalf("simulate rotation: %v", err)
	}
	segmentCopy, err := os.ReadFile(segment)
	if err != nil {
		t.Fatal(err)
	}

	second, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if _, err := os.Stat(segment); !os.IsNotExist(err) {
		t.Fatalf("sealed segment still present after recovery made it durable: %v", err)
	}
	v2 := consensus.NewVectorClockFromMap(map[string]uint64{"A": 2})
	if err := second.Put("k", []byte("v2"), v2); err != nil {
		t.Fatalf("Put: %v", err)
	}
	second.CrashForTesting()

	// As if the crash had hit before the segment was removed
	if err := os.WriteFile(segment, segmentCopy, 0o600); err != nil {
		t.Fatal(err)
	}

	third, err := storage.NewEngine(dir, walConfig())
	if err != nil {
		t.Fatalf("recovery with a leftover segment: %v", err)
	}
	defer third.Close()
	if entry, err := third.Get("k"); err != nil || string(entry.Value) != "v2" {
		t.Fatalf("k = %v (%v), want v2", entry, err)
	}
}
