package partition

import (
	"fmt"
	"testing"
)

// TestNewConsistentHash tests creating a new consistent hash ring
func TestNewConsistentHash(t *testing.T) {
	ch := NewConsistentHash(150)

	if ch == nil {
		t.Fatal("NewConsistentHash returned nil")
	}

	if ch.virtualNodes != 150 {
		t.Errorf("Expected 150 virtual nodes, got %d", ch.virtualNodes)
	}

	if ch.Size() != 0 {
		t.Errorf("Expected size 0 for new hash ring, got %d", ch.Size())
	}
}

// TestNewConsistentHashDefaultVirtualNodes tests default virtual nodes
func TestNewConsistentHashDefaultVirtualNodes(t *testing.T) {
	// Zero or negative should use default
	ch := NewConsistentHash(0)
	if ch.virtualNodes != 150 {
		t.Errorf("Expected default 150 virtual nodes, got %d", ch.virtualNodes)
	}

	ch = NewConsistentHash(-5)
	if ch.virtualNodes != 150 {
		t.Errorf("Expected default 150 virtual nodes for negative input, got %d", ch.virtualNodes)
	}
}

// TestAddNode tests adding nodes to the hash ring
func TestAddNode(t *testing.T) {
	ch := NewConsistentHash(150)

	ch.AddNode("node1")

	if ch.Size() != 1 {
		t.Errorf("Expected size 1 after adding node, got %d", ch.Size())
	}

	// Should have 150 virtual nodes in the ring
	if len(ch.sortedKeys) != 150 {
		t.Errorf("Expected 150 virtual nodes in ring, got %d", len(ch.sortedKeys))
	}

	// Keys should be sorted
	for i := 1; i < len(ch.sortedKeys); i++ {
		if ch.sortedKeys[i] < ch.sortedKeys[i-1] {
			t.Error("Keys should be sorted")
			break
		}
	}
}

// TestAddDuplicateNode tests adding the same node twice
func TestAddDuplicateNode(t *testing.T) {
	ch := NewConsistentHash(150)

	ch.AddNode("node1")
	initialKeys := len(ch.sortedKeys)

	ch.AddNode("node1")

	if ch.Size() != 1 {
		t.Errorf("Expected size 1 after duplicate add, got %d", ch.Size())
	}

	if len(ch.sortedKeys) != initialKeys {
		t.Error("Duplicate node should not add more virtual nodes")
	}
}

// TestAddMultipleNodes tests adding multiple nodes
func TestAddMultipleNodes(t *testing.T) {
	ch := NewConsistentHash(150)

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	if ch.Size() != 3 {
		t.Errorf("Expected size 3, got %d", ch.Size())
	}

	// Should have 450 virtual nodes (3 * 150)
	if len(ch.sortedKeys) != 450 {
		t.Errorf("Expected 450 virtual nodes, got %d", len(ch.sortedKeys))
	}

	// Verify sorted
	for i := 1; i < len(ch.sortedKeys); i++ {
		if ch.sortedKeys[i] < ch.sortedKeys[i-1] {
			t.Error("Keys should remain sorted after adding multiple nodes")
			break
		}
	}
}

// TestRemoveNode tests removing a node from the hash ring
func TestRemoveNode(t *testing.T) {
	ch := NewConsistentHash(150)

	ch.AddNode("node1")
	ch.AddNode("node2")

	ch.RemoveNode("node1")

	if ch.Size() != 1 {
		t.Errorf("Expected size 1 after removal, got %d", ch.Size())
	}

	// Should have 150 virtual nodes (only node2)
	if len(ch.sortedKeys) != 150 {
		t.Errorf("Expected 150 virtual nodes after removal, got %d", len(ch.sortedKeys))
	}

	// All remaining virtual nodes should belong to node2
	for _, key := range ch.sortedKeys {
		if ch.ring[key] != "node2" {
			t.Errorf("All virtual nodes should belong to node2, got %s", ch.ring[key])
		}
	}
}

// TestRemoveNonExistentNode tests removing a node that doesn't exist
func TestRemoveNonExistentNode(t *testing.T) {
	ch := NewConsistentHash(150)

	ch.AddNode("node1")
	initialSize := ch.Size()

	ch.RemoveNode("nonexistent")

	if ch.Size() != initialSize {
		t.Error("Removing non-existent node should not change size")
	}
}

// TestGetNode tests getting the node for a key
func TestGetNode(t *testing.T) {
	ch := NewConsistentHash(150)

	// Empty ring should return empty string
	node := ch.GetNode("test-key")
	if node != "" {
		t.Errorf("Expected empty string for empty ring, got %s", node)
	}

	// Add nodes
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	// Get node for key
	node = ch.GetNode("test-key")
	if node == "" {
		t.Error("Should return a node for non-empty ring")
	}

	// Same key should always map to same node
	node2 := ch.GetNode("test-key")
	if node != node2 {
		t.Error("Same key should always map to same node")
	}

	// Different keys might map to different nodes
	// (not guaranteed, but with 3 nodes it's likely)
	differentNode := false
	for i := 0; i < 100; i++ {
		testNode := ch.GetNode(fmt.Sprintf("key-%d", i))
		if testNode != node {
			differentNode = true
			break
		}
	}

	if !differentNode {
		t.Log("Warning: All test keys mapped to same node (unlikely but possible)")
	}
}

// TestGetNodes tests getting multiple nodes for replication
func TestGetNodes(t *testing.T) {
	ch := NewConsistentHash(150)

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	// Get 3 nodes for replication
	nodes := ch.GetNodes("test-key", 3)

	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(nodes))
	}

	// All nodes should be unique
	seen := make(map[string]bool)
	for _, node := range nodes {
		if seen[node] {
			t.Errorf("Duplicate node in result: %s", node)
		}
		seen[node] = true
	}

	// Should return all available nodes
	if len(seen) != 3 {
		t.Errorf("Expected 3 unique nodes, got %d", len(seen))
	}
}

// TestGetNodesMoreThanAvailable tests requesting more nodes than available
func TestGetNodesMoreThanAvailable(t *testing.T) {
	ch := NewConsistentHash(150)

	ch.AddNode("node1")
	ch.AddNode("node2")

	// Request 5 nodes but only 2 available
	nodes := ch.GetNodes("test-key", 5)

	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes (capped at available), got %d", len(nodes))
	}
}

// TestGetNodesEmptyRing tests getting nodes from empty ring
func TestGetNodesEmptyRing(t *testing.T) {
	ch := NewConsistentHash(150)

	nodes := ch.GetNodes("test-key", 3)

	if nodes != nil {
		t.Errorf("Expected nil for empty ring, got %v", nodes)
	}
}

// TestGetAllNodes tests retrieving all nodes
func TestGetAllNodes(t *testing.T) {
	ch := NewConsistentHash(150)

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	allNodes := ch.GetAllNodes()

	if len(allNodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(allNodes))
	}

	// Verify all nodes are present
	nodeMap := make(map[string]bool)
	for _, node := range allNodes {
		nodeMap[node] = true
	}

	expectedNodes := []string{"node1", "node2", "node3"}
	for _, expected := range expectedNodes {
		if !nodeMap[expected] {
			t.Errorf("Expected node %s in result", expected)
		}
	}
}

// TestGetNodeDistribution tests the distribution of virtual nodes
func TestGetNodeDistribution(t *testing.T) {
	ch := NewConsistentHash(150)

	ch.AddNode("node1")
	ch.AddNode("node2")

	dist := ch.GetNodeDistribution()

	if len(dist) != 2 {
		t.Errorf("Expected distribution for 2 nodes, got %d", len(dist))
	}

	if dist["node1"] != 150 {
		t.Errorf("Expected 150 virtual nodes for node1, got %d", dist["node1"])
	}

	if dist["node2"] != 150 {
		t.Errorf("Expected 150 virtual nodes for node2, got %d", dist["node2"])
	}
}

// TestConsistency tests that keys remain on same node after adding/removing others
func TestConsistency(t *testing.T) {
	ch := NewConsistentHash(150)

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	// Map some keys
	keyMapping := make(map[string]string)
	testKeys := []string{"key1", "key2", "key3", "key4", "key5"}

	for _, key := range testKeys {
		keyMapping[key] = ch.GetNode(key)
	}

	// Add a new node
	ch.AddNode("node4")

	// Check how many keys moved
	movedCount := 0
	for _, key := range testKeys {
		newNode := ch.GetNode(key)
		if newNode != keyMapping[key] {
			movedCount++
		}
	}

	// With consistent hashing, only a fraction of keys should move
	// Not all keys should move to the new node
	if movedCount == len(testKeys) {
		t.Error("All keys moved after adding node (should be distributed)")
	}

	t.Logf("Keys moved after adding node: %d/%d", movedCount, len(testKeys))
}

// TestRemovalConsistency tests minimal key movement when removing nodes
func TestRemovalConsistency(t *testing.T) {
	ch := NewConsistentHash(150)

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")
	ch.AddNode("node4")

	// Map some keys
	keyMapping := make(map[string]string)
	testKeys := []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8"}

	for _, key := range testKeys {
		keyMapping[key] = ch.GetNode(key)
	}

	// Remove a node
	ch.RemoveNode("node3")

	// Keys that were on node3 should move, others should stay
	movedCount := 0
	for _, key := range testKeys {
		newNode := ch.GetNode(key)
		if keyMapping[key] != "node3" && newNode != keyMapping[key] {
			t.Errorf("Key %s moved from %s to %s even though node3 was removed",
				key, keyMapping[key], newNode)
		}
		if newNode != keyMapping[key] {
			movedCount++
		}
	}

	t.Logf("Keys moved after removing node: %d/%d", movedCount, len(testKeys))
}

// TestHashDistribution tests that hash function produces reasonable distribution
func TestHashDistribution(t *testing.T) {
	ch := NewConsistentHash(150)

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	// Test many keys and see distribution
	distribution := make(map[string]int)
	numKeys := 1000

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		node := ch.GetNode(key)
		distribution[node]++
	}

	// Each node should get roughly 1/3 of keys (with some variance)
	expectedPerNode := numKeys / 3
	tolerance := numKeys / 10 // Allow 10% variance

	for node, count := range distribution {
		if count < expectedPerNode-tolerance || count > expectedPerNode+tolerance {
			t.Logf("Warning: Node %s has %d keys (expected ~%d)", node, count, expectedPerNode)
		}
	}

	t.Logf("Distribution for %d keys: %v", numKeys, distribution)
}

// TestWrapAround tests that the ring wraps around correctly
func TestWrapAround(t *testing.T) {
	ch := NewConsistentHash(10) // Use fewer virtual nodes for easier testing

	ch.AddNode("node1")

	// Should always return node1 regardless of key
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		node := ch.GetNode(key)
		if node != "node1" {
			t.Errorf("Expected node1, got %s", node)
		}
	}
}

// TestVirtualNodeDifferentiation tests that virtual nodes are unique
func TestVirtualNodeDifferentiation(t *testing.T) {
	ch := NewConsistentHash(100)

	ch.AddNode("node1")

	// Verify all virtual node hashes are unique
	seen := make(map[uint32]bool)
	for _, hash := range ch.sortedKeys {
		if seen[hash] {
			t.Error("Duplicate hash value in ring (hash collision)")
		}
		seen[hash] = true
	}
}

// TestSize tests the Size method
func TestSize(t *testing.T) {
	ch := NewConsistentHash(150)

	if ch.Size() != 0 {
		t.Errorf("Expected size 0, got %d", ch.Size())
	}

	ch.AddNode("node1")
	if ch.Size() != 1 {
		t.Errorf("Expected size 1, got %d", ch.Size())
	}

	ch.AddNode("node2")
	ch.AddNode("node3")
	if ch.Size() != 3 {
		t.Errorf("Expected size 3, got %d", ch.Size())
	}

	ch.RemoveNode("node2")
	if ch.Size() != 2 {
		t.Errorf("Expected size 2, got %d", ch.Size())
	}
}

// TestGetNodesOrder tests that GetNodes returns nodes in consistent order
func TestGetNodesOrder(t *testing.T) {
	ch := NewConsistentHash(150)

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	// Get nodes multiple times for same key
	nodes1 := ch.GetNodes("test-key", 3)
	nodes2 := ch.GetNodes("test-key", 3)

	// Should return same order
	if len(nodes1) != len(nodes2) {
		t.Fatal("Different number of nodes returned")
	}

	for i := range nodes1 {
		if nodes1[i] != nodes2[i] {
			t.Error("GetNodes should return nodes in consistent order")
		}
	}
}

// TestGetNodesExcluding tests that excluded nodes are skipped as if absent
func TestGetNodesExcluding(t *testing.T) {
	full := NewConsistentHash(50)
	without := NewConsistentHash(50)
	for _, node := range []string{"node1", "node2", "node3", "node4"} {
		full.AddNode(node)
		if node != "node4" {
			without.AddNode(node)
		}
	}

	exclude := func(nodeID string) bool { return nodeID == "node4" }
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%d", i)
		got := full.GetNodesExcluding(key, 3, exclude)
		want := without.GetNodes(key, 3)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("key %s: GetNodesExcluding = %v, want %v", key, got, want)
		}
	}
}

// TestTokenRangesFor tests that a key falls in a node's ranges exactly when
// the node is one of the key's replicas
func TestTokenRangesFor(t *testing.T) {
	ch := NewConsistentHash(50)
	nodes := []string{"node1", "node2", "node3", "node4"}
	for _, node := range nodes {
		ch.AddNode(node)
	}

	for _, node := range nodes {
		ranges := ch.TokenRangesFor(node, 3)
		for i := 0; i < 2000; i++ {
			key := fmt.Sprintf("key-%d", i)
			h := ch.HashKey(key)

			inRange := false
			for _, r := range ranges {
				if r.Contains(h) {
					inRange = true
					break
				}
			}
			isReplica := false
			for _, id := range ch.GetNodes(key, 3) {
				if id == node {
					isReplica = true
					break
				}
			}

			if inRange != isReplica {
				t.Fatalf("node %s key %s: inRange=%v, isReplica=%v", node, key, inRange, isReplica)
			}
		}
	}
}

// TestTokenRangesForWholeRing tests that a node replicating every key gets a
// single whole-ring range
func TestTokenRangesForWholeRing(t *testing.T) {
	ch := NewConsistentHash(50)
	ch.AddNode("node1")
	ch.AddNode("node2")

	ranges := ch.TokenRangesFor("node1", 3)
	if len(ranges) != 1 || ranges[0].Start != ranges[0].End {
		t.Fatalf("Expected one whole-ring range, got %v", ranges)
	}
	if ch.TokenRangesFor("unknown", 3) != nil {
		t.Error("Expected no ranges for a node not on the ring")
	}
}

// TestTokenRangeContainsWrap tests the wrap-around range semantics
func TestTokenRangeContainsWrap(t *testing.T) {
	r := TokenRange{Start: 100, End: 10}
	for _, h := range []uint32{101, 1 << 31, 0, 10} {
		if !r.Contains(h) {
			t.Errorf("Expected wrapped range to contain %d", h)
		}
	}
	for _, h := range []uint32{11, 50, 100} {
		if r.Contains(h) {
			t.Errorf("Expected wrapped range not to contain %d", h)
		}
	}
}
