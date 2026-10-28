package consensus

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestDotCounterIncreases tests that values strictly increase and respect the floor
func TestDotCounterIncreases(t *testing.T) {
	counter, err := OpenDotCounter(filepath.Join(t.TempDir(), "counter"), 10)
	if err != nil {
		t.Fatalf("OpenDotCounter failed: %v", err)
	}

	first, _ := counter.Next(0)
	second, _ := counter.Next(0)
	if first != 1 || second != 2 {
		t.Errorf("Expected 1 then 2, got %d then %d", first, second)
	}

	// A context that already contains a larger entry for this node pushes past it
	third, _ := counter.Next(50)
	if third != 51 {
		t.Errorf("Expected floor 50 to yield 51, got %d", third)
	}
	if fourth, _ := counter.Next(0); fourth != 52 {
		t.Errorf("Expected 52 after the floor jump, got %d", fourth)
	}
}

// TestDotCounterNeverReusesAfterRestart tests that reopening the counter, as
// after a crash or restart, never issues a value handed out before
func TestDotCounterNeverReusesAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "counter")

	counter, err := OpenDotCounter(path, 10)
	if err != nil {
		t.Fatalf("OpenDotCounter failed: %v", err)
	}
	var last uint64
	for i := 0; i < 25; i++ { // Crosses several reservation boundaries
		last, _ = counter.Next(0)
	}

	// Simulate a crash: drop the in-memory counter without any shutdown hook
	restarted, err := OpenDotCounter(path, 10)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	next, err := restarted.Next(0)
	if err != nil {
		t.Fatalf("Next after restart failed: %v", err)
	}
	if next <= last {
		t.Errorf("Value %d after restart does not exceed last issued value %d", next, last)
	}
}

// TestDotCounterConcurrentUnique tests that concurrent callers never get the same value
func TestDotCounterConcurrentUnique(t *testing.T) {
	counter, err := OpenDotCounter(filepath.Join(t.TempDir(), "counter"), 7)
	if err != nil {
		t.Fatalf("OpenDotCounter failed: %v", err)
	}

	const goroutines, perGoroutine = 8, 200
	values := make(chan uint64, goroutines*perGoroutine)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				v, err := counter.Next(0)
				if err != nil {
					t.Errorf("Next failed: %v", err)
					return
				}
				values <- v
			}
		}()
	}
	wg.Wait()
	close(values)

	seen := make(map[uint64]bool)
	for v := range values {
		if seen[v] {
			t.Fatalf("Value %d issued twice", v)
		}
		seen[v] = true
	}
}

// TestDotCounterCorruptFile tests that a corrupt counter file is an error, not a silent reset
func TestDotCounterCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "counter")
	if err := os.WriteFile(path, []byte("not-a-number"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDotCounter(path, 10); err == nil {
		t.Error("Expected an error for a corrupt counter file")
	}
}

// TestNewVersion tests that a write's version keeps the causal context and
// advances only the coordinator's entry
func TestNewVersion(t *testing.T) {
	counter, err := OpenDotCounter(filepath.Join(t.TempDir(), "counter"), 10)
	if err != nil {
		t.Fatalf("OpenDotCounter failed: %v", err)
	}

	// Blind write: no context
	blind, err := NewVersion(nil, "A", counter)
	if err != nil {
		t.Fatalf("NewVersion failed: %v", err)
	}
	if blind.Size() != 1 || blind.GetClock("A") == 0 {
		t.Errorf("Expected a single fresh entry for A, got %s", blind)
	}

	// Update: context from a previous version, written through another coordinator
	context := NewVectorClockFromMap(map[string]uint64{"A": 3, "B": 5})
	updated, err := NewVersion(context, "B", counter)
	if err != nil {
		t.Fatalf("NewVersion failed: %v", err)
	}
	if !updated.IsAfter(context) {
		t.Errorf("Expected %s to descend from context %s", updated, context)
	}
	if updated.GetClock("A") != 3 || updated.GetClock("B") <= 5 {
		t.Errorf("Expected A unchanged and B advanced past 5, got %s", updated)
	}
	if context.GetClock("B") != 5 {
		t.Error("NewVersion must not modify the caller's context")
	}

	// Two writes from the same context through different coordinators are concurrent
	viaA, _ := NewVersion(context, "A", counter)
	viaB, _ := NewVersion(context, "B", counter)
	if viaA.Compare(viaB) != Concurrent {
		t.Errorf("Expected %s and %s to be concurrent", viaA, viaB)
	}
}
