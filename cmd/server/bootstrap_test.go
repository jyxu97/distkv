// Integration tests for node bootstrap: a node joining an existing cluster must
// not serve reads for its new key ranges until it has streamed their data.
package main

import (
	"fmt"
	"net"
	"testing"
	"time"

	"distkv/pkg/consensus"
	"distkv/pkg/gossip"
	"distkv/pkg/replication"
	"distkv/pkg/storage"
	"distkv/pkg/tls"
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

// startTestNode creates and starts a DistKV server joined via the given seeds.
func startTestNode(t *testing.T, nodeID string, quorum *replication.QuorumConfig, seeds ...string) *DistKVServer {
	t.Helper()
	address := freeAddress(t)
	config := &ServerConfig{
		NodeID:           nodeID,
		Address:          address,
		AdvertiseAddress: address,
		DataDir:          t.TempDir(),
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

	server, err := NewDistKVServer(config)
	if err != nil {
		t.Fatalf("failed to create node %s: %v", nodeID, err)
	}
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start node %s: %v", nodeID, err)
	}
	t.Cleanup(func() { _ = server.Stop() })
	return server
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

// TestJoiningNodeDoesNotServeStaleReads starts A, B, C, writes keys with W=N,
// then adds D and immediately reads every key with R=1 through every
// coordinator. Without bootstrap, D would answer not-found for keys in its
// new ranges.
func TestJoiningNodeDoesNotServeStaleReads(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node integration test in short mode")
	}

	quorum := &replication.QuorumConfig{
		N:              3,
		R:              1,
		W:              3,
		RequestTimeout: 5 * time.Second,
		RetryAttempts:  3,
		RetryDelay:     100 * time.Millisecond,
	}

	a := startTestNode(t, "node-a", quorum)
	b := startTestNode(t, "node-b", quorum, a.config.Address)
	c := startTestNode(t, "node-c", quorum, a.config.Address)
	waitForCluster(t, []*DistKVServer{a, b, c}, 3)

	const numKeys = 200
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%03d", i)
		vc := consensus.NewVectorClock()
		vc.Increment(a.config.NodeID)
		_, err := a.quorumManager.Write(&replication.WriteRequest{
			Key:         key,
			Value:       []byte("value-" + key),
			VectorClock: vc,
		})
		if err != nil {
			t.Fatalf("write %s failed: %v", key, err)
		}
	}

	d := startTestNode(t, "node-d", quorum, a.config.Address)
	servers := []*DistKVServer{a, b, c, d}

	var missing int
	for _, coordinator := range servers {
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key-%03d", i)
			resp, err := coordinator.quorumManager.Read(&replication.ReadRequest{Key: key})
			if err != nil {
				t.Fatalf("read %s via %s failed: %v", key, coordinator.config.NodeID, err)
			}
			if !resp.Found {
				missing++
			}
		}
	}

	if missing > 0 {
		t.Fatalf("%d of %d reads returned not-found after node-d joined", missing, numKeys*len(servers))
	}
}
