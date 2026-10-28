// Integration tests for vector clock handling on the write path:
// Client Put → coordinator → quorum write → replica Replicate → storage → ACK.
package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"distkv/pkg/consensus"
	"distkv/pkg/replication"
	"distkv/proto"
)

// startVersionCluster starts node-a, node-b and node-c with N=3 and the given
// read and write quorums, and waits until every node sees all three as readable.
func startVersionCluster(t *testing.T, r, w int) (a, b, c *DistKVServer) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping multi-node integration test in short mode")
	}

	quorum := &replication.QuorumConfig{
		N:              3,
		R:              r,
		W:              w,
		RequestTimeout: 5 * time.Second,
		RetryAttempts:  3,
		RetryDelay:     100 * time.Millisecond,
	}
	a = startTestNode(t, "node-a", quorum)
	b = startTestNode(t, "node-b", quorum, a.config.Address)
	c = startTestNode(t, "node-c", quorum, a.config.Address)
	waitForCluster(t, []*DistKVServer{a, b, c}, 3)
	return a, b, c
}

func put(t *testing.T, coordinator *DistKVServer, key, value string, causalContext *consensus.VectorClock) *consensus.VectorClock {
	t.Helper()
	req := &proto.PutRequest{Key: key, Value: []byte(value)}
	if causalContext != nil {
		req.VectorClock = convertVectorClockToProto(causalContext)
	}
	resp, err := (&DistKVServiceImpl{server: coordinator}).Put(context.Background(), req)
	if err != nil || !resp.Success {
		t.Fatalf("put %s=%s via %s failed: %v %s", key, value, coordinator.config.NodeID, err, resp.GetErrorMessage())
	}
	return convertVectorClockFromProto(resp.VectorClock)
}

func get(t *testing.T, coordinator *DistKVServer, key string) *proto.GetResponse {
	t.Helper()
	resp, err := (&DistKVServiceImpl{server: coordinator}).Get(context.Background(), &proto.GetRequest{Key: key})
	if err != nil || resp.ErrorMessage != "" {
		t.Fatalf("get %s via %s failed: %v %s", key, coordinator.config.NodeID, err, resp.GetErrorMessage())
	}
	return resp
}

// waitForReplicas waits until every server stores value for key, then checks
// that each stored clock equals want: one logical write, one version everywhere.
func waitForReplicas(t *testing.T, servers []*DistKVServer, key, value string, want *consensus.VectorClock) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for _, s := range servers {
		for {
			entry, err := s.storageEngine.Get(key)
			if err == nil && string(entry.Value) == value {
				if !entry.VectorClock.Equal(want) {
					t.Fatalf("%s stores %s=%s at %s, want %s", s.config.NodeID, key, value, entry.VectorClock, want)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never stored %s=%s (last: %v, %v)", s.config.NodeID, key, value, entry, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// TestBlindPutStoresCoordinatorVersionOnAllReplicas covers a first write with
// no client clock: it must get a real version (not an empty clock), the ACK
// must report that version, and every replica must store exactly it.
func TestBlindPutStoresCoordinatorVersionOnAllReplicas(t *testing.T) {
	a, b, c := startVersionCluster(t, 2, 2)

	version := put(t, a, "blind", "v1", nil)
	if version.Size() != 1 || version.GetClock("node-a") == 0 {
		t.Fatalf("Expected a version with only a fresh node-a entry, got %s", version)
	}

	waitForReplicas(t, []*DistKVServer{a, b, c}, "blind", "v1", version)

	resp := get(t, b, "blind")
	if !resp.Found || string(resp.Value) != "v1" || resp.HasConflict {
		t.Fatalf("Expected v1 without conflict, got found=%v value=%q conflict=%v", resp.Found, resp.Value, resp.HasConflict)
	}
	if got := convertVectorClockFromProto(resp.VectorClock); !got.Equal(version) {
		t.Errorf("Get returned %s, want the written version %s", got, version)
	}
}

// TestPutWithContextAdvancesVersion covers an update that passes the previous
// version as its causal context through a different coordinator.
func TestPutWithContextAdvancesVersion(t *testing.T) {
	a, b, c := startVersionCluster(t, 2, 2)

	v1 := put(t, a, "update", "v1", nil)
	v2 := put(t, b, "update", "v2", v1)

	if !v2.IsAfter(v1) {
		t.Fatalf("Expected %s to descend from %s", v2, v1)
	}
	if v2.GetClock("node-a") != v1.GetClock("node-a") || v2.GetClock("node-b") == 0 {
		t.Errorf("Expected node-a entry kept and node-b entry added, got %s from %s", v2, v1)
	}

	waitForReplicas(t, []*DistKVServer{a, b, c}, "update", "v2", v2)

	resp := get(t, c, "update")
	if string(resp.Value) != "v2" || resp.HasConflict {
		t.Fatalf("Expected v2 without conflict, got %q conflict=%v", resp.Value, resp.HasConflict)
	}
	if got := convertVectorClockFromProto(resp.VectorClock); !got.Equal(v2) {
		t.Errorf("Get returned %s, want %s", got, v2)
	}
}

// TestRepeatedWritesThroughSameCoordinator covers sequential and concurrent
// blind writes entering through one coordinator: no two may share a version.
func TestRepeatedWritesThroughSameCoordinator(t *testing.T) {
	a, b, c := startVersionCluster(t, 2, 2)

	first := put(t, a, "repeat", "one", nil)
	second := put(t, a, "repeat", "two", nil)
	if first.Equal(second) {
		t.Fatalf("Two writes through node-a received the same version %s", first)
	}
	if !second.IsAfter(first) {
		t.Errorf("Expected the later write %s to follow %s", second, first)
	}

	// Concurrent writes to one key through one coordinator. A write that a
	// replica has already seen superseded may be refused, but every version
	// that is acknowledged must be unique.
	const writers = 16
	type result struct {
		value   string
		version *consensus.VectorClock
	}
	results := make(chan result, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			value := fmt.Sprintf("w%d", i)
			resp, err := (&DistKVServiceImpl{server: a}).Put(context.Background(),
				&proto.PutRequest{Key: "hot", Value: []byte(value)})
			if err == nil && resp.Success {
				results <- result{value, convertVectorClockFromProto(resp.VectorClock)}
			}
		}(i)
	}
	wg.Wait()
	close(results)

	var acked []result
	for r := range results {
		for _, other := range acked {
			if r.version.Equal(other.version) {
				t.Fatalf("Writes %s and %s were both acknowledged at version %s", r.value, other.value, r.version)
			}
		}
		acked = append(acked, r)
	}
	if len(acked) == 0 {
		t.Fatal("Expected at least one concurrent write to succeed")
	}

	// Replicas refuse superseded versions, so they converge on the newest one
	newest := acked[0]
	for _, r := range acked[1:] {
		if r.version.IsAfter(newest.version) {
			newest = r
		}
	}
	waitForReplicas(t, []*DistKVServer{a, b, c}, "hot", newest.value, newest.version)
}

// TestConcurrentWritesThroughDifferentCoordinators covers two writes that both
// start from the same version, coordinated by different nodes: their versions
// must be concurrent, not equal and not ordered.
func TestConcurrentWritesThroughDifferentCoordinators(t *testing.T) {
	a, b, c := startVersionCluster(t, 2, 2)

	base := put(t, a, "fork", "base", nil)
	viaA := put(t, a, "fork", "from-a", base)
	viaB := put(t, b, "fork", "from-b", base)

	if !viaA.IsAfter(base) || !viaB.IsAfter(base) {
		t.Fatalf("Expected both %s and %s to descend from %s", viaA, viaB, base)
	}
	if viaA.Compare(viaB) != consensus.Concurrent {
		t.Fatalf("Expected %s and %s to be concurrent", viaA, viaB)
	}

	// Whichever write each replica kept, it must be stored at that write's own
	// version, so the two stay distinguishable across replicas
	time.Sleep(200 * time.Millisecond) // Let the third replica's writes land
	for _, s := range []*DistKVServer{a, b, c} {
		entry, err := s.storageEngine.Get("fork")
		if err != nil {
			t.Fatalf("%s: %v", s.config.NodeID, err)
		}
		switch string(entry.Value) {
		case "from-a":
			if !entry.VectorClock.Equal(viaA) {
				t.Errorf("%s stores from-a at %s, want %s", s.config.NodeID, entry.VectorClock, viaA)
			}
		case "from-b":
			if !entry.VectorClock.Equal(viaB) {
				t.Errorf("%s stores from-b at %s, want %s", s.config.NodeID, entry.VectorClock, viaB)
			}
		default:
			t.Errorf("%s stores unexpected value %q", s.config.NodeID, entry.Value)
		}
	}
}

// TestSiblingsPreservedAndResolved covers conflict detection: when replicas
// hold concurrent versions, a read returns both as siblings with a context that
// covers both, and a write with that context resolves the conflict.
func TestSiblingsPreservedAndResolved(t *testing.T) {
	a, b, c := startVersionCluster(t, 3, 2) // R=3 so a read sees every replica
	servers := []*DistKVServer{a, b, c}

	base := put(t, a, "conflict", "base", nil)
	waitForReplicas(t, servers, "conflict", "base", base)

	x := put(t, a, "conflict", "x", base)
	waitForReplicas(t, servers, "conflict", "x", x)

	// A concurrent write coordinated by node-b that only reached node-c, as
	// under a partition: same context, its own version, partial replication.
	y, err := consensus.NewVersion(base, "node-b", b.dotCounter)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := b.replicaClient.WriteReplica(context.Background(), "node-c", "conflict", []byte("y"), y)
	if err != nil || !resp.Success {
		t.Fatalf("partial replication of y failed: %v %v", err, resp)
	}

	read := get(t, a, "conflict")
	if !read.HasConflict || len(read.Siblings) != 2 {
		t.Fatalf("Expected 2 siblings, got conflict=%v siblings=%d", read.HasConflict, len(read.Siblings))
	}
	values := map[string]bool{}
	for _, sibling := range read.Siblings {
		values[string(sibling.Value)] = true
	}
	if !values["x"] || !values["y"] {
		t.Fatalf("Expected siblings x and y, got %v", values)
	}
	readContext := convertVectorClockFromProto(read.VectorClock)
	if !readContext.Equal(x.Merge(y)) {
		t.Fatalf("Expected read context %s to cover both siblings (%s)", readContext, x.Merge(y))
	}

	// Writing back with the read context supersedes both siblings
	resolved := put(t, c, "conflict", "resolved", readContext)
	if !resolved.IsAfter(x) || !resolved.IsAfter(y) {
		t.Fatalf("Expected %s to descend from both %s and %s", resolved, x, y)
	}
	waitForReplicas(t, servers, "conflict", "resolved", resolved)

	final := get(t, b, "conflict")
	if final.HasConflict || string(final.Value) != "resolved" {
		t.Fatalf("Expected resolved value without conflict, got %q conflict=%v", final.Value, final.HasConflict)
	}
}

// TestReplicaAckReportsPersistedVersion covers the replica side directly: the
// ACK echoes exactly the stored clock, and an older version is refused rather
// than rolling the key back.
func TestReplicaAckReportsPersistedVersion(t *testing.T) {
	_, b, _ := startVersionCluster(t, 2, 2)
	replica := &NodeServiceImpl{server: b}

	version := consensus.NewVectorClockFromMap(map[string]uint64{"node-a": 42})
	resp, err := replica.Replicate(context.Background(), &proto.ReplicateRequest{
		Key: "ack", Value: []byte("v42"), VectorClock: convertVectorClockToProto(version),
	})
	if err != nil || !resp.Success {
		t.Fatalf("Replicate failed: %v %v", err, resp)
	}
	if acked := convertVectorClockFromProto(resp.VectorClock); !acked.Equal(version) {
		t.Errorf("ACK reported %s, want the version sent %s", acked, version)
	}
	stored, err := b.storageEngine.GetEntry("ack")
	if err != nil || !stored.VectorClock.Equal(version) {
		t.Fatalf("Stored %v (%v), want clock %s", stored, err, version)
	}

	// A delayed, older version must not overwrite the newer one
	older := consensus.NewVectorClockFromMap(map[string]uint64{"node-a": 41})
	resp, err = replica.Replicate(context.Background(), &proto.ReplicateRequest{
		Key: "ack", Value: []byte("v41"), VectorClock: convertVectorClockToProto(older),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Success {
		t.Error("Expected a superseded version to be refused")
	}
	if stored, _ := b.storageEngine.Get("ack"); string(stored.Value) != "v42" {
		t.Errorf("Superseded write rolled the key back to %q", stored.Value)
	}
}

// TestDeleteWithContextSupersedesValue covers a causal delete: once applied, a
// delayed copy of the deleted value must not bring the key back.
func TestDeleteWithContextSupersedesValue(t *testing.T) {
	a, b, c := startVersionCluster(t, 3, 2)

	written := put(t, a, "gone", "v1", nil)
	waitForReplicas(t, []*DistKVServer{a, b, c}, "gone", "v1", written)

	read := get(t, b, "gone")
	resp, err := (&DistKVServiceImpl{server: b}).Delete(context.Background(),
		&proto.DeleteRequest{Key: "gone", VectorClock: read.VectorClock})
	if err != nil || !resp.Success {
		t.Fatalf("Delete failed: %v %s", err, resp.GetErrorMessage())
	}

	// Redeliver the original value to a replica, e.g. a retried message
	ack, err := (&NodeServiceImpl{server: c}).Replicate(context.Background(), &proto.ReplicateRequest{
		Key: "gone", Value: []byte("v1"), VectorClock: convertVectorClockToProto(written),
	})
	if err != nil {
		t.Fatal(err)
	}
	if ack.Success {
		t.Error("Expected the deleted value to be refused by the replica")
	}

	if after := get(t, a, "gone"); after.Found || after.HasConflict {
		t.Fatalf("Expected key deleted, got found=%v value=%q conflict=%v", after.Found, after.Value, after.HasConflict)
	}
}

// TestCoordinatorRestartNeverReusesVersion covers persistence: after a restart
// the stored version is recovered intact, and new writes through the restarted
// coordinator get versions that follow, never repeat, the earlier ones.
func TestCoordinatorRestartNeverReusesVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	quorum := &replication.QuorumConfig{N: 1, R: 1, W: 1, RequestTimeout: 5 * time.Second,
		RetryAttempts: 3, RetryDelay: 100 * time.Millisecond}
	config := newTestConfig(t, "node-a", quorum, t.TempDir())

	// Started without startServer's cleanup hook: this test stops it itself
	server, err := NewDistKVServer(config)
	if err != nil {
		t.Fatalf("NewDistKVServer failed: %v", err)
	}
	if err := server.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	before := put(t, server, "restart", "before", nil)
	if err := server.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	restarted := startServer(t, config)
	recovered := get(t, restarted, "restart")
	if string(recovered.Value) != "before" || !convertVectorClockFromProto(recovered.VectorClock).Equal(before) {
		t.Fatalf("Expected recovered value 'before' at %s, got %q at %v", before, recovered.Value, recovered.VectorClock)
	}

	after := put(t, restarted, "restart", "after", nil)
	if after.GetClock("node-a") <= before.GetClock("node-a") {
		t.Fatalf("Version %s after restart does not advance past %s", after, before)
	}
	if final := get(t, restarted, "restart"); string(final.Value) != "after" {
		t.Errorf("Expected the post-restart write to win, got %q", final.Value)
	}
}
