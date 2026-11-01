package storage

import (
	"fmt"
	"path/filepath"
	"testing"

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
	e.performFlush() // v1 for every key now lives in an SSTable
	for k := keys - 1; k >= 0; k-- {
		put(k, 2) // Newer versions in the MemTable, in reverse key order
	}

	assertNewest(t, e, keys, 2, "with an unsorted MemTable")
}
