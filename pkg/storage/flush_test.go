package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// flushTestEngine returns an engine whose MemTable is full at memTableSize
// bytes, with up to MaxMemTables (2) MemTables: the active one and one
// waiting for the flush worker. Failed flushes are retried after 1-4 ms.
func flushTestEngine(t *testing.T, dir string, memTableSize int) *Engine {
	t.Helper()
	cfg := DefaultStorageConfig()
	cfg.MemTableMaxSize = memTableSize
	cfg.CompactionThreshold = 1 << 30
	e, err := NewEngine(dir, cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	e.flushRetryMin = time.Millisecond
	e.flushRetryMax = 4 * time.Millisecond
	return e
}

// flushGate holds every flush attempt until the test opens it.
type flushGate struct {
	started chan uint64 // generation of each attempt, as it starts
	release chan struct{}
}

func holdFlushes(e *Engine) *flushGate {
	g := &flushGate{started: make(chan uint64, 64), release: make(chan struct{})}
	e.createSSTable = func(memTable *MemTable, path string) (*SSTable, error) {
		g.started <- memTable.gen
		<-g.release
		return CreateSSTable(memTable, path, e.config)
	}
	return g
}

func (g *flushGate) open() { close(g.release) }

// failFlushes makes flush attempts fail while failing is set and reports
// each attempt on attempts.
func failFlushes(e *Engine, failing *atomic.Bool) (attempts chan uint64) {
	attempts = make(chan uint64, 1024)
	e.createSSTable = func(memTable *MemTable, path string) (*SSTable, error) {
		attempts <- memTable.gen
		if failing.Load() {
			return nil, errors.New("injected flush failure")
		}
		return CreateSSTable(memTable, path, e.config)
	}
	return attempts
}

// within fails the test unless done is closed within 5 seconds.
func within(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not happen", what)
	}
}

// waitUntil polls cond until it holds, failing the test after 5 seconds.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func put(t *testing.T, e *Engine, key string, size int) {
	t.Helper()
	if err := e.Put(key, make([]byte, size), nil); err != nil {
		t.Fatalf("Put %s: %v", key, err)
	}
}

func assertPresent(t *testing.T, e *Engine, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if _, err := e.Get(key); err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
	}
}

func sealedSegments(t *testing.T, dir string) []string {
	t.Helper()
	segments, err := filepath.Glob(filepath.Join(dir, walFileName+".*.sealed"))
	if err != nil {
		t.Fatal(err)
	}
	return segments
}

// TestWritesDoNotWaitForFlush covers the normal flush path: the write that
// fills the MemTable returns while its flush is still running, writes during
// the flush go to the new MemTable without waiting, and reads see the data in
// both MemTables and then in the SSTable.
func TestWritesDoNotWaitForFlush(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	e := flushTestEngine(t, dir, 64<<10)
	defer e.Close()
	gate := holdFlushes(e)

	done := make(chan struct{})
	go func() {
		defer close(done)
		put(t, e, "big", 70<<10) // Fills the MemTable
	}()
	gen := <-gate.started
	within(t, done, "the write that filled the MemTable returning before its flush")

	var wg sync.WaitGroup
	var keys []string
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("during-%03d", i)
		keys = append(keys, key)
		wg.Add(1)
		go func() {
			defer wg.Done()
			put(t, e, key, 100)
		}()
	}
	writes := make(chan struct{})
	go func() { wg.Wait(); close(writes) }()
	within(t, writes, "writes during the flush completing")

	// The flushing MemTable and the new one are both readable
	assertPresent(t, e, append(keys, "big")...)
	if segments := sealedSegments(t, dir); len(segments) != 1 {
		t.Fatalf("sealed segments during the flush: %v", segments)
	}

	gate.open()
	waitForFlushes(e)
	assertPresent(t, e, append(keys, "big")...)
	e.mutex.RLock()
	tables := len(e.sstables[0])
	e.mutex.RUnlock()
	if tables != 1 {
		t.Fatalf("%d SSTables after one flush", tables)
	}
	if segments := sealedSegments(t, dir); len(segments) != 0 {
		t.Fatalf("segment of flushed generation %d kept: %v", gen, segments)
	}
}

// TestBackpressureBoundsMemTables covers writes outrunning the flush worker:
// once the active MemTable is full and the flush queue is too, writers wait
// rather than growing memory, and continue as soon as a flush finishes.
func TestBackpressureBoundsMemTables(t *testing.T) {
	e := flushTestEngine(t, filepath.Join(t.TempDir(), "data"), 1<<10)
	defer e.Close()
	gate := holdFlushes(e)

	put(t, e, "k1", 4<<10) // Sealed and queued
	<-gate.started
	put(t, e, "k2", 4<<10) // Fills the new MemTable; the queue is full

	done := make(chan struct{})
	go func() {
		defer close(done)
		put(t, e, "k3", 4<<10)
	}()
	waitUntil(t, "the third writer waits", func() bool {
		e.mutex.RLock()
		defer e.mutex.RUnlock()
		return e.stalledWriters == 1
	})
	e.mutex.RLock()
	sealed := len(e.flushingMemTables)
	e.mutex.RUnlock()
	if sealed != 1 {
		t.Fatalf("%d sealed MemTables, limit 1", sealed)
	}

	gate.open()
	within(t, done, "the waiting writer continuing after the flush")
	waitForFlushes(e)
	assertPresent(t, e, "k1", "k2", "k3")
}

// TestSustainedWriteLoad covers many writers filling MemTables faster than a
// slowed flush worker drains them: every write succeeds and stays readable,
// and the flush queue never exceeds its limit.
func TestSustainedWriteLoad(t *testing.T) {
	e := flushTestEngine(t, filepath.Join(t.TempDir(), "data"), 8<<10)
	defer e.Close()
	var maxSealed atomic.Int64
	e.createSSTable = func(memTable *MemTable, path string) (*SSTable, error) {
		e.mutex.RLock()
		if n := int64(len(e.flushingMemTables)); n > maxSealed.Load() {
			maxSealed.Store(n)
		}
		e.mutex.RUnlock()
		time.Sleep(2 * time.Millisecond)
		return CreateSSTable(memTable, path, e.config)
	}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				put(t, e, fmt.Sprintf("w%d-%02d", w, i), 1<<10)
			}
		}()
	}
	wg.Wait()
	waitForFlushes(e)

	if got := maxSealed.Load(); got > int64(e.maxSealedMemTables()) {
		t.Fatalf("%d sealed MemTables queued, limit %d", got, e.maxSealedMemTables())
	}
	for w := 0; w < 8; w++ {
		for i := 0; i < 50; i++ {
			assertPresent(t, e, fmt.Sprintf("w%d-%02d", w, i))
		}
	}
}

// TestFlushFailureIsRetried covers a failing flush: the MemTable stays
// readable and its WAL segment stays on disk, a write that would have to wait
// for the flush fails instead of hanging, and once flushing works again the
// retry succeeds and normal writes resume.
func TestFlushFailureIsRetried(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	e := flushTestEngine(t, dir, 1<<10)
	defer e.Close()
	var failing atomic.Bool
	failing.Store(true)
	attempts := failFlushes(e, &failing)

	put(t, e, "k1", 4<<10)
	<-attempts
	<-attempts // Failed twice
	assertPresent(t, e, "k1")
	if segments := sealedSegments(t, dir); len(segments) != 1 {
		t.Fatalf("segment of the failing flush not kept: %v", segments)
	}

	put(t, e, "k2", 4<<10) // The active MemTable had room
	if err := e.Put("k3", make([]byte, 4<<10), nil); err == nil {
		t.Fatal("write accepted although it needs a flush that keeps failing")
	}

	failing.Store(false)
	waitForFlushes(e)
	put(t, e, "k3", 4<<10)
	waitForFlushes(e)
	assertPresent(t, e, "k1", "k2", "k3")
	if segments := sealedSegments(t, dir); len(segments) != 0 {
		t.Fatalf("segments left after successful flushes: %v", segments)
	}
}

// TestCrashWithUnflushedMemTables covers a crash while a flush keeps failing:
// both the sealed MemTable and the newer active one exist only in the WAL, and
// recovery must restore both with the newer value winning.
func TestCrashWithUnflushedMemTables(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	e := flushTestEngine(t, dir, 1<<10)
	var failing atomic.Bool
	failing.Store(true)
	attempts := failFlushes(e, &failing)

	if err := e.Put("k", []byte("v1"+string(make([]byte, 4<<10))), nil); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	<-attempts
	if err := e.Put("k", []byte("v2"), nil); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	e.CrashForTesting()

	recovered := flushTestEngine(t, dir, 1<<10)
	defer recovered.Close()
	if entry, err := recovered.Get("k"); err != nil || string(entry.Value) != "v2" {
		t.Fatalf("k = %v (%v), want v2", entry, err)
	}
	if segments := sealedSegments(t, dir); len(segments) != 0 {
		t.Fatalf("recovered segments not removed: %v", segments)
	}
}

// TestRecoveredSegmentDoesNotShadowNewerSSTable covers a sealed segment left
// by a crash after an older flush, alongside an SSTable from a newer one.
// The segment's writes are older and must not hide the SSTable's, whether or
// not the older SSTable was written before the crash.
func TestRecoveredSegmentDoesNotShadowNewerSSTable(t *testing.T) {
	for _, olderTableWritten := range []bool{true, false} {
		t.Run(fmt.Sprintf("olderTableWritten=%v", olderTableWritten), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "data")
			e := flushTestEngine(t, dir, 1<<10)

			// Keep a copy of the first flush's segment, as a crash before its
			// removal would
			var segment string
			var segmentData []byte
			var firstTable string
			e.createSSTable = func(memTable *MemTable, path string) (*SSTable, error) {
				if segment == "" {
					segment = sealedSegments(t, dir)[0]
					segmentData, _ = os.ReadFile(segment)
					firstTable = path
				}
				return CreateSSTable(memTable, path, e.config)
			}

			old := append([]byte("v1"), make([]byte, 4<<10)...)
			if err := e.Put("k", old, nil); err != nil {
				t.Fatalf("Put v1: %v", err)
			}
			waitForFlushes(e)
			if err := e.Put("k", append([]byte("v2"), make([]byte, 4<<10)...), nil); err != nil {
				t.Fatalf("Put v2: %v", err)
			}
			waitForFlushes(e)
			if err := e.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			if err := os.WriteFile(segment, segmentData, 0o600); err != nil {
				t.Fatal(err)
			}
			if !olderTableWritten {
				os.Remove(firstTable)
				os.Remove(firstTable + ".meta")
			}

			recovered := flushTestEngine(t, dir, 1<<10)
			defer recovered.Close()
			entry, err := recovered.Get("k")
			if err != nil || string(entry.Value[:2]) != "v2" {
				t.Fatalf("k = %.2q (%v), want v2", entry.Value, err)
			}
			if segments := sealedSegments(t, dir); len(segments) != 0 {
				t.Fatalf("recovered segment not removed: %v", segments)
			}
		})
	}
}

// TestCloseDrainsPendingFlushes covers shutdown with flush work outstanding:
// Close releases writers waiting on backpressure, flushes the queued and
// active MemTables, and leaves nothing for recovery.
func TestCloseDrainsPendingFlushes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	e := flushTestEngine(t, dir, 1<<10)
	gate := holdFlushes(e)

	put(t, e, "k1", 4<<10)
	<-gate.started
	put(t, e, "k2", 4<<10)
	stalled := make(chan error, 1)
	go func() { stalled <- e.Put("k3", make([]byte, 4<<10), nil) }()
	waitUntil(t, "the third writer waits", func() bool {
		e.mutex.RLock()
		defer e.mutex.RUnlock()
		return e.stalledWriters == 1
	})

	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()
	select {
	case err := <-stalled:
		if err == nil {
			t.Fatal("write waiting on backpressure succeeded after Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not release the waiting writer")
	}

	gate.open()
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if segments := sealedSegments(t, dir); len(segments) != 0 {
		t.Fatalf("segments left after a clean shutdown: %v", segments)
	}

	reopened := flushTestEngine(t, dir, 1<<10)
	defer reopened.Close()
	assertPresent(t, reopened, "k1", "k2")
}

// TestCloseWithFailingFlush covers shutdown while flushing fails: Close
// reports it, and the writes are recovered from the WAL on the next start.
func TestCloseWithFailingFlush(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	e := flushTestEngine(t, dir, 1<<10)
	var failing atomic.Bool
	failing.Store(true)
	attempts := failFlushes(e, &failing)

	put(t, e, "k1", 4<<10)
	<-attempts
	put(t, e, "k2", 100)
	if err := e.Close(); err == nil {
		t.Fatal("Close reported success with unflushed MemTables")
	}

	reopened := flushTestEngine(t, dir, 1<<10)
	defer reopened.Close()
	assertPresent(t, reopened, "k1", "k2")
}

// TestFlushesInOrder covers a queue of several sealed MemTables whose oldest
// fails to flush: the newer ones must wait. Flushing a newer one first would
// leave the older MemTable in memory, where reads find it ahead of the newer
// SSTable and return its stale value.
func TestFlushesInOrder(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	cfg := DefaultStorageConfig()
	cfg.MemTableMaxSize = 1 << 10
	cfg.MaxMemTables = 3 // Two sealed MemTables may wait
	cfg.CompactionThreshold = 1 << 30
	e, err := NewEngine(dir, cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer e.Close()
	e.flushRetryMin = time.Millisecond
	e.flushRetryMax = time.Millisecond

	var failing atomic.Bool
	failing.Store(true)
	e.mutex.RLock()
	oldest := e.nextGen // The generation the first sealed MemTable gets
	e.mutex.RUnlock()
	attempts := make(chan uint64, 1024)
	e.createSSTable = func(memTable *MemTable, path string) (*SSTable, error) {
		attempts <- memTable.gen
		if failing.Load() && memTable.gen == oldest {
			return nil, errors.New("injected flush failure")
		}
		return CreateSSTable(memTable, path, e.config)
	}

	value := func(v string) []byte { return append([]byte(v), make([]byte, 4<<10)...) }
	if err := e.Put("k", value("v1"), nil); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := e.Put("k", value("v2"), nil); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	// Attempts already reported may predate the second MemTable's sealing
	for len(attempts) > 0 {
		<-attempts
	}
	for i := 0; i < 5; i++ {
		if gen := <-attempts; gen != oldest {
			t.Fatalf("flushed generation %d while %d keeps failing", gen, oldest)
		}
		if entry, err := e.Get("k"); err != nil || string(entry.Value[:2]) != "v2" {
			t.Fatalf("k = %.2q (%v) while the older flush fails, want v2", entry.Value, err)
		}
	}

	failing.Store(false)
	waitForFlushes(e)
	if entry, err := e.Get("k"); err != nil || string(entry.Value[:2]) != "v2" {
		t.Fatalf("k = %.2q (%v) after both flushes, want v2", entry.Value, err)
	}
}
