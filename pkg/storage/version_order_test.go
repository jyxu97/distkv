package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"distkv/pkg/consensus"
)

// versionOrderEngine returns an engine that flushes after every write and never
// compacts on its own, so tests decide when compaction runs.
func versionOrderEngine(t *testing.T, dir string, strategy CompactionStrategy) *Engine {
	t.Helper()
	cfg := DefaultStorageConfig()
	cfg.MemTableMaxSize = 1 // Every write becomes its own SSTable
	cfg.CompactionThreshold = 1 << 30
	cfg.CompactionStrategy = strategy
	e, err := NewEngine(dir, cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return e
}

// writeRounds writes every key once per round, so each key has `rounds`
// versions spread over many interleaved SSTables; the newest is "v<rounds>".
func writeRounds(t *testing.T, e *Engine, keys, rounds int) {
	t.Helper()
	for v := 1; v <= rounds; v++ {
		for k := 0; k < keys; k++ {
			vc := consensus.NewVectorClockFromMap(map[string]uint64{"A": uint64(v)})
			if err := e.Put(fmt.Sprintf("key-%02d", k), []byte(fmt.Sprintf("v%d", v)), vc); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
	}
	waitForFlushes(e)
}

// assertNewest checks that both point lookups and full iteration return the
// newest version of every key.
func assertNewest(t *testing.T, e *Engine, keys, rounds int, when string) {
	t.Helper()
	want := fmt.Sprintf("v%d", rounds)
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key-%02d", k)
		entry, err := e.Get(key)
		if err != nil || string(entry.Value) != want {
			t.Fatalf("%s: Get(%s) = %v, %v; want %s", when, key, entry, err, want)
		}
	}

	it, err := e.Iterator()
	if err != nil {
		t.Fatalf("%s: Iterator: %v", when, err)
	}
	defer it.Close()
	seen := 0
	for ; it.Valid(); it.Next() {
		seen++
		if got := string(it.Value().Value); got != want {
			t.Fatalf("%s: iterator returned %s=%s, want %s", when, it.Key(), got, want)
		}
	}
	if seen != keys {
		t.Fatalf("%s: iterator returned %d keys, want %d", when, seen, keys)
	}
}

// TestIteratorReturnsNewestVersion covers the merge iterator: with the same
// key in many SSTables it used to pick a version by heap layout, returning
// stale values to anti-entropy, bootstrap streaming and compaction.
func TestIteratorReturnsNewestVersion(t *testing.T) {
	e := versionOrderEngine(t, filepath.Join(t.TempDir(), "data"), CompactionSimple)
	defer e.Close()

	writeRounds(t, e, 26, 5)
	assertNewest(t, e, 26, 5, "before compaction")
}

// TestCompactionKeepsNewestVersion covers both compaction strategies and a
// restart afterwards: compaction must keep the newest version of each key, and
// reloading the tables must preserve which table is newer.
func TestCompactionKeepsNewestVersion(t *testing.T) {
	for _, strategy := range []CompactionStrategy{CompactionSimple, CompactionLevelBased} {
		t.Run(fmt.Sprintf("strategy=%d", strategy), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "data")
			e := versionOrderEngine(t, dir, strategy)

			writeRounds(t, e, 26, 5)
			e.config.CompactionThreshold = 4
			e.performCompaction()
			if e.Stats().CompactionCount == 0 {
				t.Fatal("compaction did not run")
			}
			assertNewest(t, e, 26, 5, "after compaction")

			// Newer writes after compaction must still win over the compacted output
			writeRounds(t, e, 26, 6)
			assertNewest(t, e, 26, 6, "after more writes")

			if err := e.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			reopened := versionOrderEngine(t, dir, strategy)
			defer reopened.Close()
			assertNewest(t, reopened, 26, 6, "after restart")
		})
	}
}

// TestParseSSTableGen covers current and legacy SSTable file names.
func TestParseSSTableGen(t *testing.T) {
	cases := map[string]uint64{
		"sstable_42.db":                    42,
		"compacted_L1_g42_1730000000.db":   42,
		"sstable_1730000000000000000.db":   1730000000000000000,
		"sstable_final_1730000000.db":      1730000000,
		"compacted_L0_1730000000123456.db": 1730000000123456,
	}
	for name, want := range cases {
		if got := parseSSTableGen(name); got != want {
			t.Errorf("parseSSTableGen(%q) = %d, want %d", name, got, want)
		}
	}
}

// TestCompactionKeepsPurgeMarker covers a purge marker in a newer SSTable than
// the value it hides: compaction must keep the marker, so the key stays absent.
func TestCompactionKeepsPurgeMarker(t *testing.T) {
	for _, strategy := range []CompactionStrategy{CompactionSimple, CompactionLevelBased} {
		t.Run(fmt.Sprintf("strategy=%d", strategy), func(t *testing.T) {
			e := versionOrderEngine(t, filepath.Join(t.TempDir(), "data"), strategy)
			defer e.Close()

			writeRounds(t, e, 10, 3)
			if _, _, err := e.WriteIf(NewPurgeEntry("key-03"), nil); err != nil {
				t.Fatalf("purge: %v", err)
			}
			writeRounds(t, e, 1, 1) // More tables after the marker

			e.config.CompactionThreshold = 4
			e.performCompaction()
			if _, err := e.GetEntry("key-03"); err != ErrKeyNotFound {
				t.Fatalf("purged key visible after compaction: err=%v", err)
			}
			it, _ := e.Iterator()
			defer it.Close()
			for ; it.Valid(); it.Next() {
				if it.Key() == "key-03" {
					t.Fatal("iterator returned the purged key after compaction")
				}
			}
		})
	}
}

// TestIteratorMergesUnsortedMemTable covers the active MemTable as a merge
// source: it stores keys in insertion order, but the merge iterator needs
// every source in key order to return each key once with its newest version.
func TestIteratorMergesUnsortedMemTable(t *testing.T) {
	cfg := DefaultStorageConfig()
	cfg.CompactionThreshold = 1 << 30
	e, err := NewEngine(filepath.Join(t.TempDir(), "data"), cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer e.Close()

	const keys = 26
	put := func(k, v int) {
		vc := consensus.NewVectorClockFromMap(map[string]uint64{"A": uint64(v)})
		if err := e.Put(fmt.Sprintf("key-%02d", k), []byte(fmt.Sprintf("v%d", v)), vc); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	for k := 0; k < keys; k++ {
		put(k, 1)
	}
	flushNow(e) // v1 for every key now lives in an SSTable
	for k := keys - 1; k >= 0; k-- {
		put(k, 2) // Newer versions in the MemTable, in reverse key order
	}

	assertNewest(t, e, keys, 2, "with an unsorted MemTable")
}

// TestSyncDir covers the directory fsync used to make new and renamed WAL and
// SSTable files survive power loss.
func TestSyncDir(t *testing.T) {
	if err := syncDir(t.TempDir()); err != nil {
		t.Fatalf("syncDir on an existing directory: %v", err)
	}
	if err := syncDir(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("expected an error for a missing directory")
	}
}

// levelEngine returns a level-based engine that flushes every write, puts
// every compacted entry in its own SSTable, and allows 2 tables in level 0, 4
// in level 1, 8 in level 2 and so on. Tests decide when compaction runs.
func levelEngine(t *testing.T, dir string) *Engine {
	t.Helper()
	cfg := DefaultStorageConfig()
	cfg.MemTableMaxSize = 1
	cfg.SSTableMaxSize = 1
	cfg.CompactionThreshold = 1 << 30
	cfg.CompactionStrategy = CompactionLevelBased
	cfg.LevelSizeMultiplier = 2
	e, err := NewEngine(dir, cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return e
}

func levelCounts(e *Engine) []int {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	counts := make([]int, len(e.sstables))
	for level, tables := range e.sstables {
		counts[level] = len(tables)
	}
	return counts
}

// TestLevelCompactionCascades covers a compaction whose output pushes the next
// level over its limit: that level must be compacted too, not left to grow
// until the next level-0 trigger (which would compact level 0 again first).
func TestLevelCompactionCascades(t *testing.T) {
	e := levelEngine(t, filepath.Join(t.TempDir(), "data"))
	defer e.Close()

	writeRounds(t, e, 26, 3)
	e.config.CompactionThreshold = 2
	e.performCompaction()

	counts := levelCounts(e)
	for level := 0; level < len(counts)-1; level++ {
		if counts[level] >= e.maxFilesForLevel(level) {
			t.Fatalf("level %d holds %d tables, limit %d (levels: %v)", level, counts[level], e.maxFilesForLevel(level), counts)
		}
	}
	assertNewest(t, e, 26, 3, "after cascading compaction")
}

// TestCompactionKeepsExpiredTombstoneAboveOlderData covers an expired
// tombstone compacted into a level with older data below it: dropping the
// tombstone there would bring the deleted value back.
func TestCompactionKeepsExpiredTombstoneAboveOlderData(t *testing.T) {
	e := levelEngine(t, filepath.Join(t.TempDir(), "data"))
	defer e.Close()
	e.config.TombstoneTTL = time.Nanosecond // Every tombstone is expired

	writeRounds(t, e, 4, 1)
	e.config.CompactionThreshold = 2
	e.performCompaction()

	// Move everything to the last level, below the level the tombstone goes to
	e.mutex.Lock()
	last := len(e.sstables) - 1
	for level := 0; level < last; level++ {
		for _, table := range e.sstables[level] {
			e.sstables[last] = insertByGen(e.sstables[last], table)
		}
		e.sstables[level] = nil
	}
	e.mutex.Unlock()

	vc := consensus.NewVectorClockFromMap(map[string]uint64{"A": 2})
	if err := e.Delete("key-01", vc); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := e.Put("key-02", []byte("v2"), vc); err != nil {
		t.Fatalf("Put: %v", err)
	}
	e.performCompaction()

	if entry, err := e.Get("key-01"); err == nil {
		t.Fatalf("deleted key-01 is back after compaction: %s", entry.Value)
	}
}

// TestRestartKeepsSSTableLevels covers reopening an engine after level-based
// compaction: tables must return to their levels, not all to level 0.
func TestRestartKeepsSSTableLevels(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	e := levelEngine(t, dir)
	writeRounds(t, e, 26, 3)
	e.config.CompactionThreshold = 2
	e.performCompaction()
	before := levelCounts(e)
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := levelEngine(t, dir)
	defer reopened.Close()
	after := levelCounts(reopened)
	for level := 1; level < len(before); level++ {
		if before[level] != after[level] {
			t.Fatalf("levels before restart %v, after %v", before, after)
		}
	}
	assertNewest(t, reopened, 26, 3, "after restart")
}

// TestRestartAfterInterruptedCompaction covers a crash after compaction wrote
// its output but before it deleted its inputs: old level-0 inputs are then on
// disk next to the newer level-1 output, and must not be read ahead of it.
func TestRestartAfterInterruptedCompaction(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	e := levelEngine(t, dir)

	// Keep copies of the tables holding the oldest version of every key
	writeRounds(t, e, 26, 1)
	tables, _ := filepath.Glob(filepath.Join(dir, "sstables", "sstable_*"))
	saved := make(map[string][]byte)
	for _, path := range tables {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		saved[path] = data
	}

	writeRounds(t, e, 26, 3)
	e.config.CompactionThreshold = 2
	e.performCompaction()
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Put the deleted inputs back, as a crash before their deletion would
	for path, data := range saved {
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}

	reopened := levelEngine(t, dir)
	defer reopened.Close()
	assertNewest(t, reopened, 26, 3, "after an interrupted compaction")
}
