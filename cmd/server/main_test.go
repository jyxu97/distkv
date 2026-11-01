// Unit tests for server flag parsing
package main

import (
	"flag"
	"reflect"
	"testing"

	"distkv/pkg/storage"
)

// TestParseFlagsEnablesWALByDefault tests that a server started with default
// flags writes a WAL and fsyncs it before acknowledging writes
func TestParseFlagsEnablesWALByDefault(t *testing.T) {
	config, err := parseFlags(flag.NewFlagSet("test", flag.ContinueOnError), []string{"-node-id=n1"})
	if err != nil {
		t.Fatalf("parseFlags failed: %v", err)
	}

	if !config.StorageConfig.WALEnabled {
		t.Error("Expected WAL to be enabled by default")
	}
	if !config.StorageConfig.WALSyncOnPut {
		t.Error("Expected WAL fsync on every write by default")
	}
}

// TestParseFlagsWALOverrides tests that the WAL flags can be turned off
func TestParseFlagsWALOverrides(t *testing.T) {
	config, err := parseFlags(flag.NewFlagSet("test", flag.ContinueOnError),
		[]string{"-node-id=n1", "-wal-enabled=false", "-wal-sync=false"})
	if err != nil {
		t.Fatalf("parseFlags failed: %v", err)
	}

	if config.StorageConfig.WALEnabled {
		t.Error("Expected -wal-enabled=false to disable the WAL")
	}
	if config.StorageConfig.WALSyncOnPut {
		t.Error("Expected -wal-sync=false to disable per-write fsync")
	}
}

// TestParseFlagsStorageDefaults tests that default flags give the storage
// engine's defaults: level-based compaction and a full-size MemTable, not the
// zero-value simple compaction and the 1KB MemTable the server used to pass
func TestParseFlagsStorageDefaults(t *testing.T) {
	config, err := parseFlags(flag.NewFlagSet("test", flag.ContinueOnError), []string{"-node-id=n1"})
	if err != nil {
		t.Fatalf("parseFlags failed: %v", err)
	}

	want := storage.DefaultStorageConfig()
	if !reflect.DeepEqual(config.StorageConfig, want) {
		t.Errorf("storage config = %+v, want the engine defaults %+v", config.StorageConfig, want)
	}
}

// TestParseFlagsCompactionStrategy tests the -compaction-strategy flag
func TestParseFlagsCompactionStrategy(t *testing.T) {
	config, err := parseFlags(flag.NewFlagSet("test", flag.ContinueOnError),
		[]string{"-node-id=n1", "-compaction-strategy=simple", "-mem-table-size=4096"})
	if err != nil {
		t.Fatalf("parseFlags failed: %v", err)
	}
	if config.StorageConfig.CompactionStrategy != storage.CompactionSimple {
		t.Error("Expected -compaction-strategy=simple to select simple compaction")
	}
	if config.StorageConfig.MemTableMaxSize != 4096 {
		t.Errorf("MemTableMaxSize = %d, want 4096", config.StorageConfig.MemTableMaxSize)
	}

	if _, err := parseFlags(flag.NewFlagSet("test", flag.ContinueOnError),
		[]string{"-node-id=n1", "-compaction-strategy=tiered"}); err == nil {
		t.Error("Expected an unknown compaction strategy to be rejected")
	}
}
