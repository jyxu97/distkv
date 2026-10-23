// Unit tests for server flag parsing
package main

import (
	"flag"
	"testing"
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
