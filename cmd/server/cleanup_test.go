// Integration tests for local cleanup of keys a node no longer replicates.
package main

import (
	"context"
	"sort"
	"strings"
	"testing"

	"distkv/pkg/consensus"
	"distkv/pkg/storage"
	"distkv/proto"
)

// startHandoff writes keys to A, B and C (N=3, W=3, R=1), then adds D and
// waits until it has bootstrapped and every node sees it readable.
func startHandoff(t *testing.T) (a, b, c, d *DistKVServer, values map[string]string) {
	t.Helper()
	a, b, c = startVersionCluster(t, 1, 3)
	values = writeKeys(t, a, 120)
	d = startJoiner(t, a, nil)
	waitReady(t, d, []*DistKVServer{a, b, c})
	return a, b, c, d, values
}

// notOwned returns, sorted, the keys node holds data for but no longer replicates.
func notOwned(node *DistKVServer, values map[string]string) []string {
	owned := make(map[string]bool)
	for _, key := range ownedKeys(node, values) {
		owned[key] = true
	}
	var keys []string
	for key := range values {
		if !owned[key] {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func eligibleKeys(report *cleanupReport) []string {
	var keys []string
	for _, candidate := range report.eligible {
		keys = append(keys, candidate.key)
	}
	sort.Strings(keys)
	return keys
}

func runCleanup(t *testing.T, node *DistKVServer, dryRun bool) *cleanupReport {
	t.Helper()
	report, err := node.cleanup(context.Background(), dryRun)
	if err != nil {
		t.Fatalf("cleanup on %s (dryRun=%v) refused: %v", node.config.NodeID, dryRun, err)
	}
	return report
}

func assertLocal(t *testing.T, node *DistKVServer, keys []string, present bool) {
	t.Helper()
	for _, key := range keys {
		_, err := node.storageEngine.GetEntry(key)
		if present && err != nil {
			t.Fatalf("%s lost %s: %v", node.config.NodeID, key, err)
		}
		if !present && err != storage.ErrKeyNotFound {
			t.Fatalf("%s still stores %s (err=%v)", node.config.NodeID, key, err)
		}
	}
}

// TestCleanupAfterHandoff covers the successful path: a dry run reports
// exactly the keys a node no longer replicates and removes nothing; applying
// it removes only those, and every key stays readable on its owners.
func TestCleanupAfterHandoff(t *testing.T) {
	a, b, c, d, values := startHandoff(t)
	all := []*DistKVServer{a, b, c, d}

	for _, node := range []*DistKVServer{a, b, c} {
		want := notOwned(node, values)
		if len(want) == 0 {
			t.Fatalf("%s handed off no keys", node.config.NodeID)
		}

		report := runCleanup(t, node, true)
		if got := eligibleKeys(report); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("%s dry run reports %d eligible keys, want %d", node.config.NodeID, len(got), len(want))
		}
		if report.removed != 0 {
			t.Fatalf("dry run removed %d keys", report.removed)
		}
		assertLocal(t, node, want, true)
	}

	for _, node := range []*DistKVServer{a, b, c} {
		want := notOwned(node, values)
		report := runCleanup(t, node, false)
		if report.removed != len(want) {
			t.Fatalf("%s removed %d keys, want %d (skipped %v)", node.config.NodeID, report.removed, len(want), report.skipped)
		}
		assertLocal(t, node, want, false)
		assertLocal(t, node, ownedKeys(node, values), true)
	}

	// The new owner gave nothing up, and every key is still readable everywhere
	if report := runCleanup(t, d, true); len(report.eligible) != 0 {
		t.Fatalf("node-d reports %d eligible keys, want 0", len(report.eligible))
	}
	readAll(t, all, values)
}

// TestCleanupRefusedWhileBootstrapIncomplete covers a failed bootstrap: while
// the new node is still joining, no old owner may remove anything.
func TestCleanupRefusedWhileBootstrapIncomplete(t *testing.T) {
	a, b, c := startVersionCluster(t, 1, 3)
	values := writeKeys(t, a, 60)

	faults := newFaultyStreamer()
	faults.set("*", unavailable)
	d := startJoiner(t, a, faults)
	waitForCalls(t, d, faults, 6)

	for _, node := range []*DistKVServer{a, b, c} {
		for _, dryRun := range []bool{true, false} {
			_, err := node.cleanup(context.Background(), dryRun)
			if err == nil || !strings.Contains(err.Error(), "joining") {
				t.Fatalf("cleanup on %s (dryRun=%v) not refused while node-d joins: %v", node.config.NodeID, dryRun, err)
			}
		}
		var keys []string
		for key := range values {
			keys = append(keys, key)
		}
		assertLocal(t, node, keys, true)
	}
	if _, err := d.cleanup(context.Background(), true); err == nil {
		t.Fatal("cleanup ran on a node that has not finished bootstrapping")
	}
}

// TestCleanupRefusedOnStaleRingView covers ownership views that have not
// converged: a peer whose ring differs, and a peer that cannot be reached.
func TestCleanupRefusedOnStaleRingView(t *testing.T) {
	a, b, c, _, _ := startHandoff(t)

	// node-b has not learned about node-d
	b.consistentHash.RemoveNode("node-d")
	if _, err := a.cleanup(context.Background(), true); err == nil || !strings.Contains(err.Error(), "node-b") {
		t.Fatalf("cleanup not refused for node-b's stale ring: %v", err)
	}
	b.consistentHash.AddNode("node-d")
	runCleanup(t, a, true)

	// node-c cannot confirm its view
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := a.cleanup(context.Background(), false); err == nil || !strings.Contains(err.Error(), "node-c") {
		t.Fatalf("cleanup not refused with node-c unreachable: %v", err)
	}
}

// TestCleanupKeepsConcurrentWrites covers writes that reach an old owner
// around cleanup, as from a coordinator with a stale ring: a write after
// verification must survive removal, and a version newer than the replicas'
// must never be eligible.
func TestCleanupKeepsConcurrentWrites(t *testing.T) {
	a, _, c, _, values := startHandoff(t)
	handedOff := notOwned(c, values)
	if len(handedOff) < 2 {
		t.Fatal("node-c handed off too few keys")
	}
	replicate := func(key, value string, version *consensus.VectorClock) {
		t.Helper()
		resp, err := (&NodeServiceImpl{server: c}).Replicate(context.Background(), &proto.ReplicateRequest{
			Key: key, Value: []byte(value), VectorClock: convertVectorClockToProto(version),
		})
		if err != nil || !resp.Success {
			t.Fatalf("write to node-c failed: %v %v", err, resp)
		}
	}
	localVersion := func(key string) *consensus.VectorClock {
		entry, err := c.storageEngine.GetEntry(key)
		if err != nil {
			t.Fatalf("node-c lacks %s: %v", key, err)
		}
		return entry.VectorClock
	}

	// A newer version only node-c has must not be eligible
	ahead := handedOff[1]
	newer, _ := consensus.NewVersion(localVersion(ahead), "node-a", a.dotCounter)
	replicate(ahead, "only-on-c", newer)

	report := runCleanup(t, c, true)
	for _, key := range eligibleKeys(report) {
		if key == ahead {
			t.Fatalf("%s is eligible although node-c holds a newer version than its replicas", ahead)
		}
	}
	if report.skipped[skipReplicaOlder] == 0 {
		t.Fatalf("expected %s to be kept as %s, skipped=%v", ahead, skipReplicaOlder, report.skipped)
	}

	// A write that lands after verification must survive the removal
	late := eligibleKeys(report)[0]
	lateVersion, _ := consensus.NewVersion(localVersion(late), "node-a", a.dotCounter)
	replicate(late, "late", lateVersion)

	ring, err := c.confirmRingConverged(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.removeCandidates(report, ring); err != nil {
		t.Fatalf("removal failed: %v", err)
	}
	if report.skipped[skipChangedLocally] != 1 || report.removed != len(report.eligible)-1 {
		t.Fatalf("removed %d of %d, skipped %v; want all but the late write", report.removed, len(report.eligible), report.skipped)
	}
	for key, want := range map[string]string{late: "late", ahead: "only-on-c"} {
		if entry, err := c.storageEngine.Get(key); err != nil || string(entry.Value) != want {
			t.Fatalf("node-c has %s = %v (%v), want %q", key, entry, err, want)
		}
	}
}

// TestCleanupSurvivesRestarts covers restarts after cleanup: the old owner
// keeps the keys it removed absent (they are not streamed or repaired back),
// and a restarted new owner still serves everything it took over.
func TestCleanupSurvivesRestarts(t *testing.T) {
	a, b, c, d, values := startHandoff(t)
	removed := notOwned(c, values)
	if report := runCleanup(t, c, false); report.removed != len(removed) {
		t.Fatalf("removed %d keys, want %d", report.removed, len(removed))
	}

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop node-c: %v", err)
	}
	c = startServerWith(t, c.config, nil)
	waitReady(t, c, []*DistKVServer{a, b, d})
	assertLocal(t, c, removed, false)
	assertLocal(t, c, ownedKeys(c, values), true)

	if err := d.Stop(); err != nil {
		t.Fatalf("Stop node-d: %v", err)
	}
	d = startServerWith(t, d.config, nil)
	waitReady(t, d, []*DistKVServer{a, b, c})
	assertLocal(t, d, ownedKeys(d, values), true)

	readAll(t, []*DistKVServer{a, b, c, d}, values)
}

// TestCleanupDeletedKeys covers tombstones: a handed-off key deleted on its
// replicas may be removed, and a delete that only the old owner holds must be
// kept rather than lost.
func TestCleanupDeletedKeys(t *testing.T) {
	a, b, c, d, values := startHandoff(t)
	handedOff := notOwned(c, values)
	if len(handedOff) < 2 {
		t.Fatal("node-c handed off too few keys")
	}
	deletedOnReplicas, deletedOnlyOnC := handedOff[0], handedOff[1]

	// A causal delete through the replicas; node-c, no longer a replica, keeps the old value
	read := get(t, a, deletedOnReplicas)
	resp, err := (&DistKVServiceImpl{server: a}).Delete(context.Background(),
		&proto.DeleteRequest{Key: deletedOnReplicas, VectorClock: read.VectorClock})
	if err != nil || !resp.Success {
		t.Fatalf("delete failed: %v %s", err, resp.GetErrorMessage())
	}

	// A delete that reached only node-c
	entry, _ := c.storageEngine.GetEntry(deletedOnlyOnC)
	tombstone, _ := consensus.NewVersion(entry.VectorClock, "node-c", c.dotCounter)
	ack, err := (&NodeServiceImpl{server: c}).Replicate(context.Background(), &proto.ReplicateRequest{
		Key: deletedOnlyOnC, VectorClock: convertVectorClockToProto(tombstone), IsDelete: true,
	})
	if err != nil || !ack.Success {
		t.Fatalf("tombstone on node-c failed: %v %v", err, ack)
	}

	report := runCleanup(t, c, false)
	assertLocal(t, c, []string{deletedOnReplicas}, false)
	kept, err := c.storageEngine.GetEntry(deletedOnlyOnC)
	if err != nil || !kept.Deleted || !kept.VectorClock.Equal(tombstone) {
		t.Fatalf("node-c lost the delete only it holds: %v (%v), skipped=%v", kept, err, report.skipped)
	}

	for _, node := range []*DistKVServer{a, b, c, d} {
		if after := get(t, node, deletedOnReplicas); after.Found {
			t.Fatalf("deleted key %s visible via %s after cleanup", deletedOnReplicas, node.config.NodeID)
		}
	}
	delete(values, deletedOnReplicas)
	delete(values, deletedOnlyOnC)
	readAll(t, []*DistKVServer{a, b, c, d}, values)
}
