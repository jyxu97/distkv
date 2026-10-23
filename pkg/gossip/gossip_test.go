package gossip

import (
	"context"
	"testing"
	"time"
)

// TestNewGossip tests creating a new gossip instance
func TestNewGossip(t *testing.T) {
	g := NewGossip("node1", "localhost:8080", nil)

	if g == nil {
		t.Fatal("NewGossip returned nil")
	}

	if g.localNode.NodeID != "node1" {
		t.Errorf("Expected NodeID 'node1', got '%s'", g.localNode.NodeID)
	}

	if g.localNode.Address != "localhost:8080" {
		t.Errorf("Expected Address 'localhost:8080', got '%s'", g.localNode.Address)
	}

	if g.config == nil {
		t.Error("Config should not be nil")
	}

	if g.started {
		t.Error("Gossip should not be started initially")
	}
}

// TestNewGossipWithCustomConfig tests creating gossip with custom configuration
func TestNewGossipWithCustomConfig(t *testing.T) {
	config := &GossipConfig{
		GossipInterval:    500 * time.Millisecond,
		GossipFanout:      5,
		HeartbeatInterval: 200 * time.Millisecond,
		SuspectTimeout:    2 * time.Second,
		DeadTimeout:       10 * time.Second,
	}

	g := NewGossip("node1", "localhost:8080", config)

	if g.config.GossipFanout != 5 {
		t.Errorf("Expected GossipFanout 5, got %d", g.config.GossipFanout)
	}
}

// TestGossipStartStop tests starting and stopping gossip protocol
func TestGossipStartStop(t *testing.T) {
	g := NewGossip("node1", "localhost:8080", nil)

	// Start gossip
	err := g.Start()
	if err != nil {
		t.Fatalf("Failed to start gossip: %v", err)
	}

	if !g.IsAlive() {
		t.Error("Gossip should be alive after start")
	}

	// Try starting again (should fail)
	err = g.Start()
	if err == nil {
		t.Error("Starting gossip twice should return error")
	}

	// Stop gossip
	err = g.Stop()
	if err != nil {
		t.Fatalf("Failed to stop gossip: %v", err)
	}

	if g.IsAlive() {
		t.Error("Gossip should not be alive after stop")
	}

	// Stopping again should be safe
	err = g.Stop()
	if err != nil {
		t.Errorf("Stopping gossip twice should not error: %v", err)
	}
}

// TestAddNode tests adding nodes to the gossip cluster
func TestAddNode(t *testing.T) {
	g := NewGossip("node1", "localhost:8080", nil)
	err := g.Start()
	if err != nil {
		t.Fatalf("Failed to start gossip: %v", err)
	}
	defer g.Stop()

	// Add a new node
	g.AddNode("node2", "localhost:8081")

	nodes := g.GetNodes()
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(nodes))
	}

	if _, exists := nodes["node2"]; !exists {
		t.Error("node2 should exist in nodes map")
	}

	// Adding same node again should be idempotent
	g.AddNode("node2", "localhost:8081")
	nodes = g.GetNodes()
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes after duplicate add, got %d", len(nodes))
	}
}

// TestRemoveNode tests removing nodes from the gossip cluster
func TestRemoveNode(t *testing.T) {
	g := NewGossip("node1", "localhost:8080", nil)
	err := g.Start()
	if err != nil {
		t.Fatalf("Failed to start gossip: %v", err)
	}
	defer g.Stop()

	g.AddNode("node2", "localhost:8081")
	g.AddNode("node3", "localhost:8082")

	// Remove a node
	g.RemoveNode("node2")

	nodes := g.GetNodes()
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes after removal, got %d", len(nodes))
	}

	if _, exists := nodes["node2"]; exists {
		t.Error("node2 should not exist after removal")
	}

	// Removing non-existent node should be safe
	g.RemoveNode("nonexistent")
	nodes = g.GetNodes()
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes after removing non-existent, got %d", len(nodes))
	}
}

// TestGetAliveNodes tests retrieving only alive nodes
func TestGetAliveNodes(t *testing.T) {
	g := NewGossip("node1", "localhost:8080", nil)
	err := g.Start()
	if err != nil {
		t.Fatalf("Failed to start gossip: %v", err)
	}
	defer g.Stop()

	g.AddNode("node2", "localhost:8081")
	g.AddNode("node3", "localhost:8082")

	// All nodes should be alive initially
	aliveNodes := g.GetAliveNodes()
	if len(aliveNodes) != 3 {
		t.Errorf("Expected 3 alive nodes, got %d", len(aliveNodes))
	}

	// Mark a node as dead
	g.mutex.Lock()
	g.nodes["node2"].SetStatus(NodeDead)
	g.mutex.Unlock()

	aliveNodes = g.GetAliveNodes()
	if len(aliveNodes) != 2 {
		t.Errorf("Expected 2 alive nodes after marking one dead, got %d", len(aliveNodes))
	}
}

// TestProcessGossipMessage tests processing gossip messages
func TestProcessGossipMessage(t *testing.T) {
	g := NewGossip("node1", "localhost:8080", nil)
	err := g.Start()
	if err != nil {
		t.Fatalf("Failed to start gossip: %v", err)
	}
	defer g.Stop()

	// Create a gossip update from node2
	node2Info := NewNodeInfo("node2", "localhost:8081")
	node2Info.HeartbeatCounter = 5

	updates := []*NodeInfo{node2Info}

	// Process the message
	responses := g.ProcessGossipMessage("node2", updates)

	// Should get back information about all our nodes
	if len(responses) < 1 {
		t.Error("Expected at least 1 response")
	}

	// Node2 should now be in our nodes map
	nodes := g.GetNodes()
	if _, exists := nodes["node2"]; !exists {
		t.Error("node2 should be added after processing gossip message")
	}
}

// TestRegisterEventCallback tests registering and receiving node events
func TestRegisterEventCallback(t *testing.T) {
	g := NewGossip("node1", "localhost:8080", nil)
	err := g.Start()
	if err != nil {
		t.Fatalf("Failed to start gossip: %v", err)
	}
	defer g.Stop()

	eventChan := make(chan NodeEvent, 10)

	// Register callback
	g.RegisterEventCallback(func(event NodeEvent) {
		eventChan <- event
	})

	// Add a node, should trigger event
	g.AddNode("node2", "localhost:8081")

	// Wait for event
	select {
	case event := <-eventChan:
		if event.NodeID != "node2" {
			t.Errorf("Expected event for node2, got %s", event.NodeID)
		}
		if event.NewStatus != NodeAlive {
			t.Errorf("Expected status NodeAlive, got %v", event.NewStatus)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for node event")
	}
}

// TestGetLocalNode tests getting local node information
func TestGetLocalNode(t *testing.T) {
	g := NewGossip("node1", "localhost:8080", nil)

	localNode := g.GetLocalNode()
	if localNode.NodeID != "node1" {
		t.Errorf("Expected local node ID 'node1', got '%s'", localNode.NodeID)
	}

	// Modify returned node shouldn't affect internal state
	localNode.NodeID = "modified"

	localNode2 := g.GetLocalNode()
	if localNode2.NodeID != "node1" {
		t.Error("GetLocalNode should return a copy")
	}
}

// TestCreateGossipMessage tests creating gossip messages
func TestCreateGossipMessage(t *testing.T) {
	g := NewGossip("node1", "localhost:8080", nil)
	err := g.Start()
	if err != nil {
		t.Fatalf("Failed to start gossip: %v", err)
	}
	defer g.Stop()

	g.AddNode("node2", "localhost:8081")

	msg := g.createGossipMessage()

	if msg.SenderID != "node1" {
		t.Errorf("Expected sender ID 'node1', got '%s'", msg.SenderID)
	}

	if len(msg.NodeUpdates) != 2 {
		t.Errorf("Expected 2 node updates, got %d", len(msg.NodeUpdates))
	}
}

// TestMarkNodeSuspect tests marking nodes as suspect
func TestMarkNodeSuspect(t *testing.T) {
	g := NewGossip("node1", "localhost:8080", nil)
	err := g.Start()
	if err != nil {
		t.Fatalf("Failed to start gossip: %v", err)
	}
	defer g.Stop()

	g.AddNode("node2", "localhost:8081")

	// Mark node as suspect
	g.markNodeSuspect("node2")

	nodes := g.GetNodes()
	if nodes["node2"].GetStatus() != NodeSuspect {
		t.Errorf("Expected node2 to be suspect, got %v", nodes["node2"].GetStatus())
	}

	// Marking suspect again should not change status
	g.markNodeSuspect("node2")
	nodes = g.GetNodes()
	if nodes["node2"].GetStatus() != NodeSuspect {
		t.Errorf("Expected node2 to remain suspect, got %v", nodes["node2"].GetStatus())
	}
}

// TestConvertStatusToProto tests status conversion to protobuf
func TestConvertStatusToProto(t *testing.T) {
	g := NewGossip("node1", "localhost:8080", nil)

	tests := []struct {
		name     string
		status   NodeStatus
		expected string
	}{
		{"Alive", NodeAlive, "ALIVE"},
		{"Suspect", NodeSuspect, "SUSPECT"},
		{"Dead", NodeDead, "DEAD"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protoStatus := g.convertStatusToProto(tt.status)
			if protoStatus.String() != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, protoStatus.String())
			}
		})
	}
}

// TestConcurrentOperations tests thread safety of gossip operations
func TestConcurrentOperations(t *testing.T) {
	g := NewGossip("node1", "localhost:8080", nil)
	err := g.Start()
	if err != nil {
		t.Fatalf("Failed to start gossip: %v", err)
	}
	defer g.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Spawn multiple goroutines doing concurrent operations
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			for {
				select {
				case <-ctx.Done():
					done <- true
					return
				default:
					g.AddNode("node"+string(rune(id)), "localhost:8080")
					g.GetNodes()
					g.GetAliveNodes()
				}
			}
		}(i)
	}

	// Wait for timeout
	<-ctx.Done()

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should complete without panics
}

// TestNodeInfoCopy tests that GetNodes returns copies
func TestNodeInfoCopy(t *testing.T) {
	g := NewGossip("node1", "localhost:8080", nil)
	err := g.Start()
	if err != nil {
		t.Fatalf("Failed to start gossip: %v", err)
	}
	defer g.Stop()

	g.AddNode("node2", "localhost:8081")

	nodes1 := g.GetNodes()
	nodes1["node2"].SetStatus(NodeDead)

	nodes2 := g.GetNodes()
	if nodes2["node2"].GetStatus() == NodeDead {
		t.Error("Modifying returned nodes should not affect internal state")
	}
}

// TestJoiningFlagPropagation tests that the joining flag travels with gossip
// updates and that clearing it locally reaches peers with the next heartbeat
func TestJoiningFlagPropagation(t *testing.T) {
	joiner := NewGossip("joiner", "localhost:8083", nil)
	joiner.SetLocalJoining(true)

	peer := NewGossip("peer", "localhost:8080", nil)
	peer.AddJoiningNode("joiner", "localhost:8083")
	if !peer.GetNodes()["joiner"].IsJoining() {
		t.Fatal("Expected node added via AddJoiningNode to be joining")
	}

	// Joiner finishes bootstrapping; its next heartbeat carries the cleared flag
	joiner.SetLocalJoining(false)
	joiner.updateLocalHeartbeat()
	peer.ProcessGossipMessage("joiner", []*NodeInfo{joiner.GetLocalNode()})

	if peer.GetNodes()["joiner"].IsJoining() {
		t.Error("Expected peer to see joiner as no longer joining")
	}
}
