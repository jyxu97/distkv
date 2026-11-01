// Integration tests for node bootstrap: a node joining an existing cluster must
// not serve reads for its new key ranges until it has streamed their data.
package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"distkv/pkg/consensus"
	"distkv/pkg/gossip"
	"distkv/pkg/partition"
	"distkv/pkg/replication"
	"distkv/pkg/storage"
	"distkv/pkg/tls"
	"distkv/proto"
)

// freeAddress returns a localhost address with a currently unused port.
func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

// newTestConfig returns a server config on a free localhost port with fast gossip.
func newTestConfig(t *testing.T, nodeID string, quorum *replication.QuorumConfig, dataDir string, seeds ...string) *ServerConfig {
	t.Helper()
	address := freeAddress(t)
	return &ServerConfig{
		NodeID:           nodeID,
		Address:          address,
		AdvertiseAddress: address,
		DataDir:          dataDir,
		SeedNodes:        seeds,
		VirtualNodes:     50,
		StorageConfig:    storage.DefaultStorageConfig(),
		QuorumConfig:     quorum,
		GossipConfig: &gossip.GossipConfig{
			HeartbeatInterval:   100 * time.Millisecond,
			SuspectTimeout:      10 * time.Second,
			DeadTimeout:         30 * time.Second,
			GossipInterval:      100 * time.Millisecond,
			GossipFanout:        3,
			MaxGossipPacketSize: 64 * 1024,
		},
		TLSConfig: &tls.Config{Enabled: false},
	}
}

// startServer creates and starts a DistKV server from config, stopping it at test end.
func startServer(t *testing.T, config *ServerConfig) *DistKVServer {
	t.Helper()
	return startServerWith(t, config, nil)
}

// startServerWith is startServer with a hook to adjust the server before Start.
func startServerWith(t *testing.T, config *ServerConfig, configure func(*DistKVServer)) *DistKVServer {
	t.Helper()
	server, err := NewDistKVServer(config)
	if err != nil {
		t.Fatalf("failed to create node %s: %v", config.NodeID, err)
	}
	if configure != nil {
		configure(server)
	}
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start node %s: %v", config.NodeID, err)
	}
	t.Cleanup(func() { _ = server.Stop() })
	return server
}

// startTestNode creates and starts a DistKV server joined via the given seeds.
func startTestNode(t *testing.T, nodeID string, quorum *replication.QuorumConfig, seeds ...string) *DistKVServer {
	t.Helper()
	return startServer(t, newTestConfig(t, nodeID, quorum, t.TempDir(), seeds...))
}

// waitForCluster blocks until every server's hash ring contains size nodes and
// no server still sees any node as joining.
func waitForCluster(t *testing.T, servers []*DistKVServer, size int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if clusterConverged(servers, size) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("cluster did not converge to %d readable nodes", size)
}

func clusterConverged(servers []*DistKVServer, size int) bool {
	for _, s := range servers {
		if s.consistentHash.Size() != size {
			return false
		}
		for _, nodeInfo := range s.gossipManager.GetNodes() {
			if nodeInfo.IsJoining() {
				return false
			}
		}
	}
	return true
}

// fault is how an injected stream from one peer misbehaves.
type fault int

const (
	healthy     fault = iota
	unavailable       // The RPC fails as if the peer were down
	hang              // The RPC blocks until the bootstrap peer timeout
	badEntry          // The stream includes an entry that cannot be applied
)

// faultyStreamer wraps the real range streamer so a test can decide, per peer,
// how streaming misbehaves. The key "*" applies to every peer without its own fault.
type faultyStreamer struct {
	inner rangeStreamer

	mu     sync.Mutex
	faults map[string]fault
	calls  map[string]int
}

func newFaultyStreamer() *faultyStreamer {
	return &faultyStreamer{faults: map[string]fault{}, calls: map[string]int{}}
}

func (f *faultyStreamer) set(peer string, mode fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[peer] = mode
}

func (f *faultyStreamer) clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults = map[string]fault{}
}

func (f *faultyStreamer) totalCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	total := 0
	for _, n := range f.calls {
		total += n
	}
	return total
}

func (f *faultyStreamer) StreamRange(ctx context.Context, peer string, ranges []partition.TokenRange) ([]replication.AntiEntropyEntry, error) {
	f.mu.Lock()
	mode, ok := f.faults[peer]
	if !ok {
		mode = f.faults["*"]
	}
	f.calls[peer]++
	f.mu.Unlock()

	switch mode {
	case unavailable:
		return nil, fmt.Errorf("injected: peer %s unavailable", peer)
	case hang:
		<-ctx.Done()
		return nil, ctx.Err()
	case badEntry:
		entries, err := f.inner.StreamRange(ctx, peer, ranges)
		if err != nil {
			return nil, err
		}
		// Storage rejects an empty key, so applying this entry fails
		return append(entries, replication.AntiEntropyEntry{Key: "", Value: []byte("x")}), nil
	}
	return f.inner.StreamRange(ctx, peer, ranges)
}

// startJoiner starts node-d joining through seed with fast bootstrap retries.
// A non-nil faults wraps its range streamer.
func startJoiner(t *testing.T, seed *DistKVServer, faults *faultyStreamer) *DistKVServer {
	t.Helper()
	config := newTestConfig(t, "node-d", seed.config.QuorumConfig, t.TempDir(), seed.config.Address)
	return startServerWith(t, config, func(s *DistKVServer) {
		s.bootstrapTiming = bootstrapTiming{
			PeerTimeout: 150 * time.Millisecond,
			RetryBase:   20 * time.Millisecond,
			RetryMax:    100 * time.Millisecond,
		}
		if faults != nil {
			faults.inner = s.rangeStreamer
			s.rangeStreamer = faults
		}
	})
}

// writeKeys writes n blind keys through coordinator and returns key -> value.
func writeKeys(t *testing.T, coordinator *DistKVServer, n int) map[string]string {
	t.Helper()
	values := make(map[string]string, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%03d", i)
		values[key] = "value-" + key
		put(t, coordinator, key, values[key], nil)
	}
	return values
}

// readAll reads every key through every coordinator and fails on any miss or
// wrong value: the cluster must serve complete data whatever node-d's state.
func readAll(t *testing.T, coordinators []*DistKVServer, values map[string]string) {
	t.Helper()
	var missing int
	for _, coordinator := range coordinators {
		for key, want := range values {
			resp, err := coordinator.quorumManager.Read(&replication.ReadRequest{Key: key})
			if err != nil {
				t.Fatalf("read %s via %s failed: %v", key, coordinator.config.NodeID, err)
			}
			if !resp.Found || string(resp.Value) != want {
				missing++
			}
		}
	}
	if missing > 0 {
		t.Fatalf("%d of %d reads returned a missing or wrong value", missing, len(values)*len(coordinators))
	}
}

// ownedKeys returns the keys node is a replica of on its own ring.
func ownedKeys(node *DistKVServer, values map[string]string) []string {
	var keys []string
	for key := range values {
		for _, id := range node.consistentHash.GetNodes(key, node.config.QuorumConfig.N) {
			if id == node.config.NodeID {
				keys = append(keys, key)
				break
			}
		}
	}
	return keys
}

// missingLocally counts owned keys that node does not store with the expected value.
func missingLocally(node *DistKVServer, values map[string]string) int {
	missing := 0
	for _, key := range ownedKeys(node, values) {
		entry, err := node.storageEngine.Get(key)
		if err != nil || string(entry.Value) != values[key] {
			missing++
		}
	}
	return missing
}

// waitJoined waits until node's ring holds size nodes, i.e. its join finished.
func waitJoined(t *testing.T, node *DistKVServer, size int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for node.consistentHash.Size() != size {
		if time.Now().After(deadline) {
			t.Fatalf("%s never saw %d nodes in its ring", node.config.NodeID, size)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitForCalls waits until faults has seen at least n stream calls, failing
// at once if joiner becomes ready instead of retrying the faulty streams.
func waitForCalls(t *testing.T, joiner *DistKVServer, faults *faultyStreamer, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for faults.totalCalls() < n {
		select {
		case <-joiner.bootstrapDone:
			t.Fatalf("%s became ready after %d faulty stream calls", joiner.config.NodeID, faults.totalCalls())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d stream calls, want %d", faults.totalCalls(), n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// assertJoining checks that joiner has not become readable: its bootstrap has
// not completed, it advertises itself as joining, and no peer routes reads
// for its ranges to it.
func assertJoining(t *testing.T, joiner *DistKVServer, peers []*DistKVServer, values map[string]string) {
	t.Helper()
	select {
	case <-joiner.bootstrapDone:
		t.Fatal("bootstrap completed despite the injected fault")
	default:
	}
	if !joiner.gossipManager.GetLocalNode().IsJoining() {
		t.Fatal("joiner cleared its joining flag despite the injected fault")
	}
	id := joiner.config.NodeID
	for _, peer := range append(peers, joiner) {
		if info, ok := peer.gossipManager.GetNodes()[id]; !ok || !info.IsJoining() {
			t.Fatalf("%s does not see %s as joining", peer.config.NodeID, id)
		}
		for _, key := range ownedKeys(joiner, values) {
			for _, replica := range peer.nodeSelector.GetReplicas(key, joiner.config.QuorumConfig.N) {
				if replica.NodeID == id {
					t.Fatalf("%s would read %s from joining %s", peer.config.NodeID, key, id)
				}
			}
		}
	}
}

// waitReady waits for joiner to finish bootstrap and for every node to see it readable.
func waitReady(t *testing.T, joiner *DistKVServer, peers []*DistKVServer) {
	t.Helper()
	select {
	case <-joiner.bootstrapDone:
	case <-time.After(10 * time.Second):
		t.Fatal("bootstrap did not complete")
	}
	waitForCluster(t, append(peers, joiner), len(peers)+1)
}

// TestJoiningNodeDoesNotServeStaleReads starts A, B, C, writes keys with W=N,
// then adds D and immediately reads every key with R=1 through every
// coordinator. Without bootstrap, D would answer not-found for keys in its
// new ranges. Once bootstrap completes, D itself must hold every key it owns.
func TestJoiningNodeDoesNotServeStaleReads(t *testing.T) {
	a, b, c := startVersionCluster(t, 1, 3)
	peers := []*DistKVServer{a, b, c}
	values := writeKeys(t, a, 200)

	d := startJoiner(t, a, nil)
	waitJoined(t, d, 4)
	readAll(t, append(peers, d), values)

	waitReady(t, d, peers)
	if missing := missingLocally(d, values); missing > 0 {
		t.Fatalf("node-d is readable but lacks %d of the keys it owns", missing)
	}
	readAll(t, append(peers, d), values)
}

// TestBootstrapToleratesOneUnavailablePeer covers a peer failing during
// streaming when the rest still intersect every write quorum: with N=3 and
// W=2, two of a range's three previous replicas hold every acknowledged write.
func TestBootstrapToleratesOneUnavailablePeer(t *testing.T) {
	a, b, c := startVersionCluster(t, 2, 2)
	peers := []*DistKVServer{a, b, c}
	values := writeKeys(t, a, 200)

	faults := newFaultyStreamer()
	faults.set("node-b", unavailable)
	d := startJoiner(t, a, faults)

	waitReady(t, d, peers)
	if missing := missingLocally(d, values); missing > 0 {
		t.Fatalf("node-d became readable without %d of its keys", missing)
	}
	readAll(t, append(peers, d), values)
}

// TestBootstrapStaysJoiningWhilePeersUnavailable covers streaming failing for
// every peer: the node must stay joining across retries, reads must keep
// avoiding it while it is incomplete, and it must recover once peers return.
func TestBootstrapStaysJoiningWhilePeersUnavailable(t *testing.T) {
	a, b, c := startVersionCluster(t, 1, 3)
	peers := []*DistKVServer{a, b, c}
	values := writeKeys(t, a, 200)

	faults := newFaultyStreamer()
	faults.set("*", unavailable)
	d := startJoiner(t, a, faults)

	waitForCalls(t, d, faults, 9) // At least three failed attempts
	assertJoining(t, d, peers, values)
	if missingLocally(d, values) == 0 {
		t.Fatal("expected node-d to be incomplete while streaming fails")
	}
	readAll(t, append(peers, d), values)

	faults.clear()
	waitReady(t, d, peers)
	if missing := missingLocally(d, values); missing > 0 {
		t.Fatalf("node-d recovered but lacks %d of its keys", missing)
	}
	readAll(t, append(peers, d), values)
}

// TestBootstrapTimeoutDoesNotMarkReady covers streams that never finish: each
// attempt hits the per-peer timeout and the node must stay joining.
func TestBootstrapTimeoutDoesNotMarkReady(t *testing.T) {
	a, b, c := startVersionCluster(t, 1, 3)
	peers := []*DistKVServer{a, b, c}
	values := writeKeys(t, a, 50)

	faults := newFaultyStreamer()
	faults.set("*", hang)
	d := startJoiner(t, a, faults)

	waitForCalls(t, d, faults, 6) // At least two timed-out attempts
	assertJoining(t, d, peers, values)

	faults.clear()
	waitReady(t, d, peers)
	if missing := missingLocally(d, values); missing > 0 {
		t.Fatalf("node-d recovered but lacks %d of its keys", missing)
	}
}

// TestBootstrapApplyFailureDoesNotMarkReady covers a stream that arrives but
// cannot be fully applied. Most entries may already be stored; the node must
// still stay joining because the transfer is not known to be complete.
func TestBootstrapApplyFailureDoesNotMarkReady(t *testing.T) {
	a, b, c := startVersionCluster(t, 1, 3)
	peers := []*DistKVServer{a, b, c}
	values := writeKeys(t, a, 50)

	faults := newFaultyStreamer()
	faults.set("*", badEntry)
	d := startJoiner(t, a, faults)

	waitForCalls(t, d, faults, 6)
	assertJoining(t, d, peers, values)

	faults.clear()
	waitReady(t, d, peers)
	if missing := missingLocally(d, values); missing > 0 {
		t.Fatalf("node-d recovered but lacks %d of its keys", missing)
	}
}

// TestWritesDuringJoinSurviveStreaming covers updates made while the node is
// joining: it must receive them as a pending replica, and the older versions
// it streams afterwards must not overwrite them.
func TestWritesDuringJoinSurviveStreaming(t *testing.T) {
	a, b, c := startVersionCluster(t, 1, 2)
	peers := []*DistKVServer{a, b, c}
	values := writeKeys(t, a, 100)

	faults := newFaultyStreamer()
	faults.set("*", unavailable)
	d := startJoiner(t, a, faults)
	waitForCalls(t, d, faults, 6)
	assertJoining(t, d, peers, values)

	// Update every key node-d will own, each built on the version it replaces
	updated := make(map[string]*consensus.VectorClock)
	for _, key := range ownedKeys(d, values) {
		previous := convertVectorClockFromProto(get(t, b, key).VectorClock)
		values[key] = "updated-" + key
		updated[key] = put(t, a, key, values[key], previous)
	}
	if len(updated) == 0 {
		t.Fatal("node-d owns no keys")
	}

	// The joining node receives them as a pending replica
	deadline := time.Now().Add(5 * time.Second)
	for missingLocally(d, values) > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("joining node-d is missing %d updates written during its join", missingLocally(d, values))
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Streaming the older versions must not roll them back
	faults.clear()
	waitReady(t, d, peers)
	for key, version := range updated {
		entry, err := d.storageEngine.Get(key)
		if err != nil || string(entry.Value) != values[key] || !entry.VectorClock.Equal(version) {
			t.Fatalf("after bootstrap node-d has %s = %v (%v), want %q at %s", key, entry, err, values[key], version)
		}
	}
	readAll(t, append(peers, d), values)
}

// TestFailedJoinStaysJoining covers a node whose seed is unreachable: it must
// not fall back to serving reads as a standalone cluster, and must join and
// bootstrap once the seed comes up.
func TestFailedJoinStaysJoining(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node integration test in short mode")
	}
	quorum := &replication.QuorumConfig{N: 2, R: 1, W: 1, RequestTimeout: 5 * time.Second,
		RetryAttempts: 3, RetryDelay: 100 * time.Millisecond}

	seedConfig := newTestConfig(t, "node-a", quorum, t.TempDir())
	joinerConfig := newTestConfig(t, "node-d", quorum, t.TempDir(), seedConfig.Address)
	d := startServerWith(t, joinerConfig, func(s *DistKVServer) {
		s.bootstrapTiming = bootstrapTiming{PeerTimeout: 150 * time.Millisecond,
			RetryBase: 20 * time.Millisecond, RetryMax: 100 * time.Millisecond}
	})

	time.Sleep(300 * time.Millisecond) // Several failed join attempts
	select {
	case <-d.bootstrapDone:
		t.Fatal("node-d finished bootstrap without reaching its seed")
	default:
	}
	if !d.gossipManager.GetLocalNode().IsJoining() {
		t.Fatal("node-d left the joining state without reaching its seed")
	}
	if _, err := d.quorumManager.Read(&replication.ReadRequest{Key: "any"}); err == nil {
		t.Fatal("node-d served a read from its own empty store")
	}

	// The seed comes up later; node-d must join it and become readable
	a := startServer(t, seedConfig)
	put(t, a, "k", "v", nil)
	waitReady(t, d, []*DistKVServer{a})
	if resp := get(t, d, "k"); !resp.Found || string(resp.Value) != "v" {
		t.Fatalf("expected node-d to read k=v after joining, got found=%v value=%q", resp.Found, resp.Value)
	}
}

// TestBootstrapStreamsTombstones covers a delete that reached only some
// previous replicas. The joining node must store the tombstone, not the stale
// value the lagging replica still has.
func TestBootstrapStreamsTombstones(t *testing.T) {
	a, b, c := startVersionCluster(t, 1, 3)
	peers := []*DistKVServer{a, b, c}

	// Pick a key node-d will own once it joins
	ring := partition.NewConsistentHash(50)
	for _, id := range []string{"node-a", "node-b", "node-c", "node-d"} {
		ring.AddNode(id)
	}
	key := ""
	for i := 0; key == ""; i++ {
		candidate := fmt.Sprintf("deleted-%d", i)
		for _, id := range ring.GetNodes(candidate, 3) {
			if id == "node-d" {
				key = candidate
			}
		}
	}

	written := put(t, a, key, "stale", nil)
	waitForReplicas(t, peers, key, "stale", written)

	// The delete reaches node-a and node-b; node-c keeps the old value
	tombstone, err := consensus.NewVersion(written, "node-a", a.dotCounter)
	if err != nil {
		t.Fatal(err)
	}
	for _, replica := range []*DistKVServer{a, b} {
		resp, err := (&NodeServiceImpl{server: replica}).Replicate(context.Background(), &proto.ReplicateRequest{
			Key: key, VectorClock: convertVectorClockToProto(tombstone), IsDelete: true,
		})
		if err != nil || !resp.Success {
			t.Fatalf("delete on %s failed: %v %v", replica.config.NodeID, err, resp)
		}
	}

	d := startJoiner(t, a, nil)
	waitReady(t, d, peers)

	entry, err := d.storageEngine.GetEntry(key)
	if err != nil {
		t.Fatalf("node-d has no entry for deleted key %s: %v", key, err)
	}
	if !entry.Deleted || !entry.VectorClock.Equal(tombstone) {
		t.Fatalf("node-d stores %s as deleted=%v value=%q at %s, want tombstone at %s",
			key, entry.Deleted, entry.Value, entry.VectorClock, tombstone)
	}
}
