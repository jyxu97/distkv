// WAL unit tests (internal package: exercises unexported helpers and the
// on-disk record format directly).
package storage

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"distkv/pkg/consensus"
)

func tempWALPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "wal.log")
}

func mkEntry(key, value string, node string) *Entry {
	vc := consensus.NewVectorClock()
	vc.Increment(node)
	return NewEntry(key, []byte(value), vc)
}

// TestWAL_AppendReplayRoundTrip verifies records survive an append/close/replay
// cycle in order, and that the recovered vector clock is intact (the whole
// point of the serialization fix).
func TestWAL_AppendReplayRoundTrip(t *testing.T) {
	path := tempWALPath(t)

	w, err := OpenWAL(path, true)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	want := []*Entry{
		mkEntry("a", "1", "node1"),
		mkEntry("b", "2", "node2"),
		mkEntry("c", "3", "node1"),
	}
	for _, e := range want {
		if err := w.Append(e); err != nil {
			t.Fatalf("Append(%s): %v", e.Key, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var got []*Entry
	n, err := ReplayWAL(path, func(e *Entry) error {
		got = append(got, e)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}
	if n != len(want) {
		t.Fatalf("recovered %d records, want %d", n, len(want))
	}
	for i := range want {
		if got[i].Key != want[i].Key || string(got[i].Value) != string(want[i].Value) {
			t.Errorf("record %d = (%s,%s), want (%s,%s)", i,
				got[i].Key, got[i].Value, want[i].Key, want[i].Value)
		}
		if got[i].VectorClock == nil {
			t.Errorf("record %d lost its vector clock", i)
			continue
		}
		if !got[i].VectorClock.Equal(want[i].VectorClock) {
			t.Errorf("record %d clock = %s, want %s", i,
				got[i].VectorClock.String(), want[i].VectorClock.String())
		}
	}
}

// TestWAL_ReplayMissingFile confirms a not-yet-created log is not an error.
func TestWAL_ReplayMissingFile(t *testing.T) {
	n, err := ReplayWAL(filepath.Join(t.TempDir(), "does-not-exist.log"), func(*Entry) error { return nil })
	if err != nil {
		t.Fatalf("expected nil error for missing file, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 records, got %d", n)
	}
}

// TestWAL_TornTailPartialPayload simulates a crash mid-append: a valid record
// followed by a header promising more bytes than exist. Replay must recover the
// good record and stop cleanly.
func TestWAL_TornTailPartialPayload(t *testing.T) {
	path := tempWALPath(t)
	w, err := OpenWAL(path, true)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	if err := w.Append(mkEntry("good", "value", "node1")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append a torn record: a header claiming 100 bytes, but only 10 written.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	var hdr [walRecordHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], 100)
	binary.LittleEndian.PutUint32(hdr[4:8], 0xdeadbeef)
	f.Write(hdr[:])
	f.Write([]byte("truncated!"))
	f.Close()

	var got []string
	n, err := ReplayWAL(path, func(e *Entry) error {
		got = append(got, e.Key)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayWAL should tolerate torn tail, got error: %v", err)
	}
	if n != 1 || len(got) != 1 || got[0] != "good" {
		t.Fatalf("expected to recover exactly the 'good' record, got %v (n=%d)", got, n)
	}
}

// TestWAL_TornTailPartialHeader simulates a crash that wrote only part of a
// record header. Replay must stop cleanly after the last good record.
func TestWAL_TornTailPartialHeader(t *testing.T) {
	path := tempWALPath(t)
	w, _ := OpenWAL(path, true)
	_ = w.Append(mkEntry("good", "value", "node1"))
	_ = w.Close()

	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write([]byte{0x01, 0x02, 0x03}) // 3 bytes: partial 8-byte header
	f.Close()

	n, err := ReplayWAL(path, func(*Entry) error { return nil })
	if err != nil {
		t.Fatalf("expected clean stop on partial header, got %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 recovered record, got %d", n)
	}
}

// TestWAL_CorruptFinalRecordCRC writes a record whose payload has been flipped
// so its CRC no longer matches. As the final record (torn write), replay must
// tolerate it and recover the preceding good record.
func TestWAL_CorruptFinalRecordCRC(t *testing.T) {
	path := tempWALPath(t)
	w, _ := OpenWAL(path, true)
	_ = w.Append(mkEntry("good", "value", "node1"))
	_ = w.Close()

	// Manually append a record with a deliberately wrong CRC.
	payload := []byte(`{"Key":"bad","Value":null,"VectorClock":{},"Timestamp":1,"Deleted":false}`)
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	var hdr [walRecordHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(payload)^0xffffffff) // wrong CRC
	f.Write(hdr[:])
	f.Write(payload)
	f.Close()

	var got []string
	n, err := ReplayWAL(path, func(e *Entry) error {
		got = append(got, e.Key)
		return nil
	})
	if err != nil {
		t.Fatalf("expected torn final CRC to be tolerated, got %v", err)
	}
	if n != 1 || got[0] != "good" {
		t.Fatalf("expected only 'good' recovered, got %v (n=%d)", got, n)
	}
}

// TestWAL_CorruptMiddleRecordCRC verifies that a CRC mismatch in a non-final
// record is reported as corruption (data after it cannot be trusted).
func TestWAL_CorruptMiddleRecordCRC(t *testing.T) {
	path := tempWALPath(t)
	w, _ := OpenWAL(path, true)
	_ = w.Append(mkEntry("first", "v1", "node1"))
	_ = w.Close()

	// Append a bad-CRC record, then a valid record after it.
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	badPayload := []byte(`{"Key":"bad","Value":null,"VectorClock":{},"Timestamp":1,"Deleted":false}`)
	var badHdr [walRecordHeaderSize]byte
	binary.LittleEndian.PutUint32(badHdr[0:4], uint32(len(badPayload)))
	binary.LittleEndian.PutUint32(badHdr[4:8], 12345) // wrong CRC
	f.Write(badHdr[:])
	f.Write(badPayload)
	// A valid trailing record so the bad one is NOT the final record.
	goodPayload, _ := serializeEntry(mkEntry("third", "v3", "node1"))
	var goodHdr [walRecordHeaderSize]byte
	binary.LittleEndian.PutUint32(goodHdr[0:4], uint32(len(goodPayload)))
	binary.LittleEndian.PutUint32(goodHdr[4:8], crc32.ChecksumIEEE(goodPayload))
	f.Write(goodHdr[:])
	f.Write(goodPayload)
	f.Close()

	n, err := ReplayWAL(path, func(*Entry) error { return nil })
	if err == nil {
		t.Fatalf("expected corruption error for mid-file CRC mismatch, got nil (n=%d)", n)
	}
	if n != 1 {
		t.Fatalf("expected 1 good record before corruption, got %d", n)
	}
}

// TestWAL_Truncate verifies Truncate empties the log so replay recovers nothing.
func TestWAL_Truncate(t *testing.T) {
	path := tempWALPath(t)
	w, _ := OpenWAL(path, true)
	_ = w.Append(mkEntry("a", "1", "node1"))
	_ = w.Append(mkEntry("b", "2", "node1"))
	if err := w.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	// Appends after truncate should still work and be the only survivors.
	_ = w.Append(mkEntry("c", "3", "node1"))
	_ = w.Close()

	var got []string
	n, _ := ReplayWAL(path, func(e *Entry) error { got = append(got, e.Key); return nil })
	if n != 1 || got[0] != "c" {
		t.Fatalf("after truncate expected only 'c', got %v (n=%d)", got, n)
	}
}

// TestWAL_Rotate verifies rotation seals current records into a segment and the
// fresh active log starts empty, with both readable.
func TestWAL_Rotate(t *testing.T) {
	path := tempWALPath(t)
	w, _ := OpenWAL(path, true)
	_ = w.Append(mkEntry("old", "1", "node1"))

	sealed, err := w.Rotate()
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if sealed == "" {
		t.Fatal("Rotate returned empty sealed path")
	}
	_ = w.Append(mkEntry("new", "2", "node1"))
	_ = w.Close()

	// Sealed segment holds the pre-rotation record.
	var sealedKeys []string
	ns, _ := ReplayWAL(sealed, func(e *Entry) error { sealedKeys = append(sealedKeys, e.Key); return nil })
	if ns != 1 || sealedKeys[0] != "old" {
		t.Fatalf("sealed segment: expected [old], got %v", sealedKeys)
	}

	// Fresh active log holds only the post-rotation record.
	var activeKeys []string
	na, _ := ReplayWAL(path, func(e *Entry) error { activeKeys = append(activeKeys, e.Key); return nil })
	if na != 1 || activeKeys[0] != "new" {
		t.Fatalf("active log: expected [new], got %v", activeKeys)
	}
}
