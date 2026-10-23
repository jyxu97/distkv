// Unit tests for NodeSelector
package main

import (
	"fmt"
	"testing"
	"time"

	"distkv/pkg/gossip"
	"distkv/pkg/partition"
	"distkv/pkg/replication"
)

// TestNewNodeSelector tests NodeSelector creation
func TestNewNodeSelector(t *testing.T) {
	ch := partition.NewConsistentHash(150)
	gm := gossip.NewGossip("test-node", "localhost:8080", nil)

	ns := NewNodeSelector(ch, gm)

	if ns == nil {
		t.Fatal("Expected non-nil NodeSelector")
	}

	if ns.consistentHash != ch {
		t.Error("ConsistentHash not set correctly")
	}

	if ns.gossipManager != gm {
		t.Error("GossipManager not set correctly")
	}
}

// TestGetReplicas tests getting replica nodes for a key
func TestGetReplicas(t *testing.T) {
	// Create consistent hash and add nodes
	ch := partition.NewConsistentHash(150)
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	// Create gossip manager and add node info
	gm := gossip.NewGossip("coordinator", "localhost:9999", nil)
	gm.AddNode("node1", "localhost:8080")
	gm.AddNode("node2", "localhost:8081")
	gm.AddNode("node3", "localhost:8082")

	ns := NewNodeSelector(ch, gm)

	// Test getting replicas
	replicas := ns.GetReplicas("test-key", 3)

	if len(replicas) != 3 {
		t.Errorf("Expected 3 replicas, got %d", len(replicas))
	}

	// Verify replica structure
	for _, replica := range replicas {
		if replica.NodeID == "" {
			t.Error("Replica has empty NodeID")
		}
		if replica.Address == "" {
			t.Error("Replica has empty Address")
		}
		// Note: All nodes are marked as alive due to connection-based failure detection
		if !replica.IsAlive {
			t.Error("Expected all replicas to be marked as alive")
		}
	}
}

// TestGetReplicasWithUnknownNode tests behavior when node is not in gossip
func TestGetReplicasWithUnknownNode(t *testing.T) {
	// Create consistent hash with nodes not in gossip
	ch := partition.NewConsistentHash(150)
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("unknown-node")

	// Create gossip with only some nodes
	gm := gossip.NewGossip("coordinator", "localhost:9999", nil)
	gm.AddNode("node1", "localhost:8080")

	ns := NewNodeSelector(ch, gm)

	replicas := ns.GetReplicas("test-key", 3)

	// Should still return 3 replicas
	if len(replicas) != 3 {
		t.Errorf("Expected 3 replicas, got %d", len(replicas))
	}

	// Check that unknown nodes have proper defaults
	foundUnknown := false
	for _, replica := range replicas {
		if replica.NodeID == "unknown-node" {
			foundUnknown = true
			if replica.Address != "unknown" {
				t.Errorf("Expected address 'unknown' for unknown node, got %s", replica.Address)
			}
			if replica.IsAlive {
				t.Error("Expected unknown node to be marked as dead")
			}
		}
	}

	if !foundUnknown {
		t.Error("Expected to find unknown-node in replicas")
	}
}

// TestGetAliveReplicas tests filtering for alive replicas only
func TestGetAliveReplicas(t *testing.T) {
	// Create consistent hash
	ch := partition.NewConsistentHash(150)
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")
	ch.AddNode("node4")

	// Create gossip with mixed node states
	gm := gossip.NewGossip("coordinator", "localhost:9999", nil)
	gm.AddNode("node1", "localhost:8080")
	gm.AddNode("node2", "localhost:8081")
	// node3 and node4 not in gossip (will be marked dead)

	ns := NewNodeSelector(ch, gm)

	// Get alive replicas
	aliveReplicas := ns.GetAliveReplicas("test-key", 4)

	// Should only return alive nodes
	// Note: Due to connection-based detection, only nodes not in gossip are dead
	if len(aliveReplicas) < 2 {
		t.Errorf("Expected at least 2 alive replicas, got %d", len(aliveReplicas))
	}

	// Verify all returned replicas are alive
	for _, replica := range aliveReplicas {
		if !replica.IsAlive {
			t.Errorf("GetAliveReplicas returned dead node: %s", replica.NodeID)
		}
	}
}

// TestGetReplicasConsistency tests that replicas are consistent for same key
func TestGetReplicasConsistency(t *testing.T) {
	ch := partition.NewConsistentHash(150)
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	gm := gossip.NewGossip("coordinator", "localhost:9999", nil)
	gm.AddNode("node1", "localhost:8080")
	gm.AddNode("node2", "localhost:8081")
	gm.AddNode("node3", "localhost:8082")

	ns := NewNodeSelector(ch, gm)

	// Get replicas multiple times for same key
	replicas1 := ns.GetReplicas("consistent-key", 3)
	replicas2 := ns.GetReplicas("consistent-key", 3)

	if len(replicas1) != len(replicas2) {
		t.Error("Replica count changed for same key")
	}

	// Verify same nodes in same order
	for i := range replicas1 {
		if replicas1[i].NodeID != replicas2[i].NodeID {
			t.Errorf("Replica order changed: %s != %s", replicas1[i].NodeID, replicas2[i].NodeID)
		}
	}
}

// TestConvertUnixToTime tests time conversion helper
func TestConvertUnixToTime(t *testing.T) {
	tests := []struct {
		name     string
		unixTime int64
		expected time.Time
	}{
		{"zero time", 0, time.Unix(0, 0)},
		{"current time", time.Now().Unix(), time.Unix(time.Now().Unix(), 0)},
		{"specific time", 1609459200, time.Unix(1609459200, 0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertUnixToTime(tt.unixTime)
			if result.Unix() != tt.expected.Unix() {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestGetReplicasReturnsReplicaInfo tests the returned type
func TestGetReplicasReturnsReplicaInfo(t *testing.T) {
	ch := partition.NewConsistentHash(150)
	ch.AddNode("node1")

	gm := gossip.NewGossip("coordinator", "localhost:9999", nil)
	gm.AddNode("node1", "localhost:8080")

	ns := NewNodeSelector(ch, gm)
	replicas := ns.GetReplicas("test-key", 1)

	if len(replicas) == 0 {
		t.Fatal("Expected at least one replica")
	}

	// Verify it implements the interface by checking fields
	replica := replicas[0]
	_ = replica.NodeID
	_ = replica.Address
	_ = replica.IsAlive
	_ = replica.LastSeen

	// Type assertion to ensure it's ReplicaInfo
	var _ replication.ReplicaInfo = replica
}

// TestJoiningNodeIsPendingNotReadable tests that a joining node is skipped for
// reads but returned as a pending replica for writes
func TestJoiningNodeIsPendingNotReadable(t *testing.T) {
	ch := partition.NewConsistentHash(50)
	gm := gossip.NewGossip("coordinator", "localhost:9999", nil)
	for i, node := range []string{"node1", "node2", "node3"} {
		ch.AddNode(node)
		gm.AddNode(node, fmt.Sprintf("localhost:%d", 8080+i))
	}
	ch.AddNode("node4")
	gm.AddJoiningNode("node4", "localhost:8083")

	ns := NewNodeSelector(ch, gm)

	pendingKeys := 0
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%d", i)

		replicas := ns.GetReplicas(key, 3)
		if len(replicas) != 3 {
			t.Fatalf("key %s: expected 3 readable replicas, got %d", key, len(replicas))
		}
		for _, r := range replicas {
			if r.NodeID == "node4" {
				t.Fatalf("key %s: joining node returned as readable replica", key)
			}
		}

		pending := ns.GetPendingReplicas(key, 3)
		inRing := false
		for _, id := range ch.GetNodes(key, 3) {
			inRing = inRing || id == "node4"
		}
		if inRing != (len(pending) == 1 && pending[0].NodeID == "node4") {
			t.Fatalf("key %s: pending=%v but node4 in preference list=%v", key, pending, inRing)
		}
		if inRing {
			pendingKeys++
		}
	}
	if pendingKeys == 0 {
		t.Fatal("Expected node4 to be pending for some keys")
	}

	// Once node4 finishes bootstrapping it becomes readable and is no longer pending
	nodes := gm.GetNodes()
	update := nodes["node4"]
	update.HeartbeatCounter++
	update.Joining = false
	gm.ProcessGossipMessage("node4", []*gossip.NodeInfo{update})

	for i := 0; i < 200; i++ {
		if len(ns.GetPendingReplicas(fmt.Sprintf("key-%d", i), 3)) != 0 {
			t.Fatal("Expected no pending replicas after node4 finished joining")
		}
	}
}
