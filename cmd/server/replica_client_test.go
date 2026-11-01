// Unit tests for ReplicaClient
package main

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"distkv/pkg/consensus"
)

// TestNewReplicaClient tests ReplicaClient creation
func TestNewReplicaClient(t *testing.T) {
	rc := NewReplicaClient()

	if rc == nil {
		t.Fatal("Expected non-nil ReplicaClient")
	}

	if rc.connections == nil {
		t.Error("Connections map not initialized")
	}

	if rc.nodeAddresses == nil {
		t.Error("NodeAddresses map not initialized")
	}

	if rc.connectionTimeout != 5*time.Second {
		t.Errorf("Expected timeout 5s, got %v", rc.connectionTimeout)
	}

	if len(rc.connections) != 0 {
		t.Error("Expected empty connections map initially")
	}

	if len(rc.nodeAddresses) != 0 {
		t.Error("Expected empty nodeAddresses map initially")
	}
}

// TestUpdateNodeAddress tests updating node addresses
func TestUpdateNodeAddress(t *testing.T) {
	rc := NewReplicaClient()

	// Update address for a node
	rc.UpdateNodeAddress("node1", "localhost:8080")

	if addr, exists := rc.nodeAddresses["node1"]; !exists || addr != "localhost:8080" {
		t.Error("Node address not updated correctly")
	}

	// Update address again (simulating address change)
	rc.UpdateNodeAddress("node1", "localhost:8081")

	if addr, exists := rc.nodeAddresses["node1"]; !exists || addr != "localhost:8081" {
		t.Error("Node address not updated on second call")
	}

	// Add multiple nodes
	rc.UpdateNodeAddress("node2", "localhost:8082")
	rc.UpdateNodeAddress("node3", "localhost:8083")

	if len(rc.nodeAddresses) != 3 {
		t.Errorf("Expected 3 node addresses, got %d", len(rc.nodeAddresses))
	}
}

// TestParseError tests error message parsing
func TestParseError(t *testing.T) {
	tests := []struct {
		name         string
		errorMessage string
		expectNil    bool
		expectMsg    string
	}{
		{"empty string", "", true, ""},
		{"error message", "something failed", false, "something failed"},
		{"complex error", "connection refused: timeout", false, "connection refused: timeout"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parseError(tt.errorMessage)

			if tt.expectNil {
				if err != nil {
					t.Errorf("Expected nil error, got: %v", err)
				}
			} else {
				if err == nil {
					t.Error("Expected non-nil error, got nil")
				} else if err.Error() != tt.expectMsg {
					t.Errorf("Expected error message '%s', got '%s'", tt.expectMsg, err.Error())
				}
			}
		})
	}
}

// TestReplicaClientClose tests closing connections
func TestReplicaClientClose(t *testing.T) {
	rc := NewReplicaClient()

	// Add some node addresses
	rc.UpdateNodeAddress("node1", "localhost:8080")
	rc.UpdateNodeAddress("node2", "localhost:8081")

	// Close should not error even without connections
	err := rc.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	// Maps should be cleared
	if len(rc.connections) != 0 {
		t.Error("Connections map not cleared after Close")
	}

	if len(rc.nodeAddresses) != 0 {
		t.Error("NodeAddresses map not cleared after Close")
	}
}

// TestReplicaClientConcurrentAccess tests concurrent safety
func TestReplicaClientConcurrentAccess(t *testing.T) {
	rc := NewReplicaClient()

	// Concurrent updates
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			nodeID := "node" + string(rune('0'+id))
			address := "localhost:808" + string(rune('0'+id))
			rc.UpdateNodeAddress(nodeID, address)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should have 10 nodes without race conditions
	if len(rc.nodeAddresses) != 10 {
		t.Errorf("Expected 10 nodes, got %d", len(rc.nodeAddresses))
	}
}

// TestWriteReplicaErrorHandling tests error cases
func TestWriteReplicaErrorHandling(t *testing.T) {
	rc := NewReplicaClient()
	ctx := context.Background()

	// Test with unknown node (no address)
	vc := consensus.NewVectorClock()
	resp, err := rc.WriteReplica(ctx, "unknown-node", "key1", []byte("value1"), vc)

	if err == nil {
		t.Error("Expected error for unknown node")
	}

	if resp == nil {
		t.Fatal("Expected non-nil response even on error")
	}

	if resp.Success {
		t.Error("Expected Success to be false for unknown node")
	}

	if resp.NodeID != "unknown-node" {
		t.Errorf("Expected NodeID 'unknown-node', got '%s'", resp.NodeID)
	}

	if resp.Error == nil {
		t.Error("Expected Error field to be set")
	}
}

// TestReadReplicaErrorHandling tests read error cases
func TestReadReplicaErrorHandling(t *testing.T) {
	rc := NewReplicaClient()
	ctx := context.Background()

	// Test with unknown node (no address)
	resp, err := rc.ReadReplica(ctx, "unknown-node", "key1")

	// ReadReplica now returns error when node address is unknown
	if err == nil {
		t.Error("Expected error for unknown node")
	}

	if resp == nil {
		t.Fatal("Expected non-nil response")
	}

	if resp.Success {
		t.Error("Expected Success to be false for unknown node")
	}

	if resp.NodeID != "unknown-node" {
		t.Errorf("Expected NodeID 'unknown-node', got '%s'", resp.NodeID)
	}
}

// TestUpdateNodeAddressClosesOldConnection tests connection cleanup
func TestUpdateNodeAddressClosesOldConnection(t *testing.T) {
	rc := NewReplicaClient()

	// Set initial address
	rc.UpdateNodeAddress("node1", "localhost:8080")

	// Simulate having a connection (we can't create real one without server)
	// Just verify the address is updated
	rc.UpdateNodeAddress("node1", "localhost:8081")

	if addr := rc.nodeAddresses["node1"]; addr != "localhost:8081" {
		t.Errorf("Expected updated address 'localhost:8081', got '%s'", addr)
	}

	// Connection should be removed if it existed
	if _, exists := rc.connections["node1"]; exists {
		t.Error("Old connection should have been removed")
	}
}

// TestReplicaClientMultipleCloses tests calling Close multiple times
func TestReplicaClientMultipleCloses(t *testing.T) {
	rc := NewReplicaClient()

	rc.UpdateNodeAddress("node1", "localhost:8080")

	// First close
	err1 := rc.Close()
	if err1 != nil {
		t.Errorf("First close failed: %v", err1)
	}

	// Second close should also succeed
	err2 := rc.Close()
	if err2 != nil {
		t.Errorf("Second close failed: %v", err2)
	}

	// Maps should still be empty
	if len(rc.connections) != 0 || len(rc.nodeAddresses) != 0 {
		t.Error("Maps not empty after multiple closes")
	}
}

// TestReplicaResponseStructure tests the response structure
func TestReplicaResponseStructure(t *testing.T) {
	rc := NewReplicaClient()
	ctx := context.Background()

	// Test write response structure
	vc := consensus.NewVectorClock()
	writeResp, _ := rc.WriteReplica(ctx, "unknown", "key", []byte("val"), vc)

	// Verify response has all expected fields
	_ = writeResp.NodeID
	_ = writeResp.Success
	_ = writeResp.Error
	_ = writeResp.Value
	_ = writeResp.VectorClock

	// Test read response structure
	readResp, _ := rc.ReadReplica(ctx, "unknown", "key")

	_ = readResp.NodeID
	_ = readResp.Success
	_ = readResp.Error
	_ = readResp.Value
	_ = readResp.VectorClock
}

// TestUpdateNodeAddressKeepsConnectionForSameAddress tests that re-announcing a
// node at the same address does not close its connection, which would fail
// RPCs in flight on it.
func TestUpdateNodeAddressKeepsConnectionForSameAddress(t *testing.T) {
	rc := NewReplicaClient()
	rc.UpdateNodeAddress("node1", "localhost:8080")

	// A non-blocking dial gives a real connection object without a server
	conn, err := grpc.Dial("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()
	rc.connections["node1"] = conn

	rc.UpdateNodeAddress("node1", "localhost:8080")
	if rc.connections["node1"] != conn {
		t.Fatal("Connection was dropped although the address did not change")
	}

	rc.UpdateNodeAddress("node1", "localhost:8081")
	if _, exists := rc.connections["node1"]; exists {
		t.Error("Connection should be dropped when the address changes")
	}
}
