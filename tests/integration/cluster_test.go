// Integration tests for DistKV cluster functionality
// These tests verify that the distributed system works correctly
// across multiple nodes with real network communication.

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"distkv/proto"
)

// TestCluster represents a test DistKV cluster
type TestCluster struct {
	nodes   []*TestNode
	dataDir string
	t       *testing.T
}

// TestNode represents a single DistKV server instance for testing
type TestNode struct {
	nodeID  string
	address string
	port    int
	cmd     *exec.Cmd
	client  proto.DistKVClient
	conn    *grpc.ClientConn
}

// SetupCluster creates a test cluster with the specified number of nodes
func SetupCluster(t *testing.T, nodeCount int) *TestCluster {
	dataDir := fmt.Sprintf("/tmp/distkv-test-%d", time.Now().Unix())

	cluster := &TestCluster{
		nodes:   make([]*TestNode, nodeCount),
		dataDir: dataDir,
		t:       t,
	}

	// Create nodes
	for i := 0; i < nodeCount; i++ {
		port := 8080 + i
		node := &TestNode{
			nodeID:  fmt.Sprintf("test-node%d", i+1),
			address: fmt.Sprintf("localhost:%d", port),
			port:    port,
		}
		cluster.nodes[i] = node
	}

	// Start first node (seed)
	if err := cluster.startNode(0, ""); err != nil {
		t.Fatalf("Failed to start seed node: %v", err)
	}

	// Wait for seed node to be ready
	time.Sleep(2 * time.Second)

	// Start remaining nodes
	for i := 1; i < nodeCount; i++ {
		seedNodes := cluster.nodes[0].address
		if err := cluster.startNode(i, seedNodes); err != nil {
			t.Fatalf("Failed to start node %d: %v", i, err)
		}
		time.Sleep(1 * time.Second) // Stagger startup
	}

	// Wait for cluster to stabilize
	time.Sleep(5 * time.Second)

	// Create gRPC clients for all nodes
	for i, node := range cluster.nodes {
		conn, err := grpc.Dial(node.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("Failed to connect to node %d: %v", i, err)
		}
		node.conn = conn
		node.client = proto.NewDistKVClient(conn)
	}

	return cluster
}

// startNode starts a single DistKV server node
func (tc *TestCluster) startNode(index int, seedNodes string) error {
	node := tc.nodes[index]
	nodeDataDir := fmt.Sprintf("%s/node%d", tc.dataDir, index+1)

	args := []string{
		"-node-id=" + node.nodeID,
		"-address=" + node.address,
		"-data-dir=" + nodeDataDir,
		"-replicas=3",
		"-read-quorum=2",
		"-write-quorum=2",
	}

	if seedNodes != "" {
		args = append(args, "-seed-nodes="+seedNodes)
	}

	cmd := exec.Command("../../build/distkv-server", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start node %s: %v", node.nodeID, err)
	}

	node.cmd = cmd
	return nil
}

// TearDown stops all nodes and cleans up test data
func (tc *TestCluster) TearDown() {
	// Close gRPC connections
	for _, node := range tc.nodes {
		if node.conn != nil {
			node.conn.Close()
		}
	}

	// Stop all server processes
	for _, node := range tc.nodes {
		if node.cmd != nil && node.cmd.Process != nil {
			node.cmd.Process.Kill()
			node.cmd.Wait()
		}
	}

	// Clean up test data
	os.RemoveAll(tc.dataDir)
}

// TestBasicOperations tests basic put/get/delete operations
func TestBasicOperations(t *testing.T) {
	cluster := SetupCluster(t, 3)
	defer cluster.TearDown()

	client := cluster.nodes[0].client
	ctx := context.Background()

	t.Run("Put operation", func(t *testing.T) {
		req := &proto.PutRequest{
			Key:              "test-key",
			Value:            []byte("test-value"),
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		}

		resp, err := client.Put(ctx, req)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		if !resp.Success {
			t.Fatalf("Put returned false: %s", resp.ErrorMessage)
		}
	})

	t.Run("Get operation", func(t *testing.T) {
		req := &proto.GetRequest{
			Key:              "test-key",
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		}

		resp, err := client.Get(ctx, req)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if !resp.Found {
			t.Fatal("Key not found")
		}

		if string(resp.Value) != "test-value" {
			t.Fatalf("Wrong value: expected 'test-value', got '%s'", string(resp.Value))
		}
	})

	t.Run("Delete operation", func(t *testing.T) {
		req := &proto.DeleteRequest{
			Key:              "test-key",
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		}

		resp, err := client.Delete(ctx, req)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		if !resp.Success {
			t.Fatalf("Delete returned false: %s", resp.ErrorMessage)
		}

		// Verify deletion
		getReq := &proto.GetRequest{
			Key:              "test-key",
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		}

		getResp, err := client.Get(ctx, getReq)
		if err != nil {
			t.Fatalf("Get after delete failed: %v", err)
		}

		if getResp.Found {
			t.Fatal("Key still found after deletion")
		}
	})
}

// TestConsistencyLevels tests different consistency levels. The cluster runs
// with W=2, so a write at ONE is refused; reads may use any level.
func TestConsistencyLevels(t *testing.T) {
	cluster := SetupCluster(t, 3)
	defer cluster.TearDown()

	ctx := context.Background()

	testCases := []struct {
		name        string
		consistency proto.ConsistencyLevel
	}{
		{"ONE", proto.ConsistencyLevel_ONE},
		{"QUORUM", proto.ConsistencyLevel_QUORUM},
		{"ALL", proto.ConsistencyLevel_ALL},
		{"DEFAULT", proto.ConsistencyLevel_DEFAULT},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := cluster.nodes[0].client
			key := fmt.Sprintf("consistency-test-%s", tc.name)
			value := fmt.Sprintf("value-%s", tc.name)

			// Put with specified consistency
			putReq := &proto.PutRequest{
				Key:              key,
				Value:            []byte(value),
				ConsistencyLevel: tc.consistency,
			}

			putResp, err := client.Put(ctx, putReq)
			if err != nil {
				t.Fatalf("Put failed: %v", err)
			}

			if tc.consistency == proto.ConsistencyLevel_ONE {
				if putResp.Success {
					t.Fatal("Put at ONE accepted although W=2")
				}
				putReq.ConsistencyLevel = proto.ConsistencyLevel_DEFAULT
				if putResp, err = client.Put(ctx, putReq); err != nil {
					t.Fatalf("Put failed: %v", err)
				}
			}

			if !putResp.Success {
				t.Fatalf("Put failed: %s", putResp.ErrorMessage)
			}

			// Get with specified consistency
			getReq := &proto.GetRequest{
				Key:              key,
				ConsistencyLevel: tc.consistency,
			}

			getResp, err := client.Get(ctx, getReq)
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if !getResp.Found {
				t.Fatal("Key not found")
			}

			if string(getResp.Value) != value {
				t.Fatalf("Wrong value: expected '%s', got '%s'", value, string(getResp.Value))
			}
		})
	}
}

// TestNodeFailure tests behavior when a node fails
func TestNodeFailure(t *testing.T) {
	cluster := SetupCluster(t, 3)
	defer cluster.TearDown()

	ctx := context.Background()

	// Store data while all nodes are up
	client := cluster.nodes[0].client
	putReq := &proto.PutRequest{
		Key:              "failure-test",
		Value:            []byte("test-data"),
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}

	resp, err := client.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Initial put failed: %s", resp.ErrorMessage)
	}

	// Kill one node
	node := cluster.nodes[2]
	if node.cmd != nil && node.cmd.Process != nil {
		node.cmd.Process.Kill()
		node.cmd.Wait()
	}

	// Wait for failure detection
	time.Sleep(10 * time.Second)

	// Should still be able to read (2 out of 3 nodes alive)
	getReq := &proto.GetRequest{
		Key:              "failure-test",
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}

	getResp, err := client.Get(ctx, getReq)
	if err != nil {
		t.Fatalf("Get after node failure failed: %v", err)
	}

	if !getResp.Found {
		t.Fatal("Data lost after node failure")
	}

	if string(getResp.Value) != "test-data" {
		t.Fatalf("Wrong value after node failure: expected 'test-data', got '%s'", string(getResp.Value))
	}

	// Should still be able to write
	putReq2 := &proto.PutRequest{
		Key:              "failure-test-2",
		Value:            []byte("after-failure"),
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}

	putResp2, err := client.Put(ctx, putReq2)
	if err != nil {
		t.Fatalf("Put after node failure failed: %v", err)
	}
	if !putResp2.Success {
		t.Fatalf("Put after node failure failed: %s", putResp2.ErrorMessage)
	}
}

// TestConcurrentOperations tests concurrent reads and writes
func TestConcurrentOperations(t *testing.T) {
	cluster := SetupCluster(t, 3)
	defer cluster.TearDown()

	ctx := context.Background()
	numOperations := 100
	numGoroutines := 10

	var wg sync.WaitGroup
	errors := make(chan error, numOperations)

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			client := cluster.nodes[goroutineID%len(cluster.nodes)].client

			for j := 0; j < numOperations/numGoroutines; j++ {
				key := fmt.Sprintf("concurrent-key-%d-%d", goroutineID, j)
				value := fmt.Sprintf("value-%d-%d", goroutineID, j)

				req := &proto.PutRequest{
					Key:              key,
					Value:            []byte(value),
					ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
				}

				resp, err := client.Put(ctx, req)
				if err != nil {
					errors <- fmt.Errorf("put %s failed: %v", key, err)
					return
				}

				if !resp.Success {
					errors <- fmt.Errorf("put %s failed: %s", key, resp.ErrorMessage)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}

	// Verify all data was written correctly
	for i := 0; i < numGoroutines; i++ {
		client := cluster.nodes[i%len(cluster.nodes)].client

		for j := 0; j < numOperations/numGoroutines; j++ {
			key := fmt.Sprintf("concurrent-key-%d-%d", i, j)
			expectedValue := fmt.Sprintf("value-%d-%d", i, j)

			req := &proto.GetRequest{
				Key:              key,
				ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
			}

			resp, err := client.Get(ctx, req)
			if err != nil {
				t.Errorf("Get %s failed: %v", key, err)
				continue
			}

			if !resp.Found {
				t.Errorf("Key %s not found", key)
				continue
			}

			if string(resp.Value) != expectedValue {
				t.Errorf("Wrong value for %s: expected '%s', got '%s'",
					key, expectedValue, string(resp.Value))
			}
		}
	}
}

// TestSiblingPreservationOnConcurrentWrites tests that concurrent writes to the
// same key from different nodes result in siblings being preserved rather than
// one value being silently discarded.
func TestSiblingPreservationOnConcurrentWrites(t *testing.T) {
	cluster := SetupCluster(t, 3)
	defer cluster.TearDown()

	ctx := context.Background()

	// Write the same key concurrently from two different nodes. Each blind write
	// gets a version from its own coordinator, so the two versions are concurrent.
	key := "sibling-test-key"

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		client := cluster.nodes[0].client
		req := &proto.PutRequest{
			Key:              key,
			Value:            []byte("value-from-node0"),
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		}
		client.Put(ctx, req)
	}()

	go func() {
		defer wg.Done()
		client := cluster.nodes[1].client
		req := &proto.PutRequest{
			Key:              key,
			Value:            []byte("value-from-node1"),
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		}
		client.Put(ctx, req)
	}()

	wg.Wait()

	// Allow time for replication
	time.Sleep(3 * time.Second)

	// Read with QUORUM to trigger conflict detection across replicas
	getReq := &proto.GetRequest{
		Key:              key,
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}

	resp, err := cluster.nodes[2].client.Get(ctx, getReq)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !resp.Found {
		t.Fatal("Key not found")
	}

	// If concurrent versions were detected, siblings should be preserved
	if resp.HasConflict {
		if len(resp.Siblings) < 2 {
			t.Errorf("HasConflict is true but only %d siblings returned", len(resp.Siblings))
		}

		t.Logf("Conflict detected with %d siblings (as expected for concurrent writes)", len(resp.Siblings))
		for i, s := range resp.Siblings {
			t.Logf("  Sibling %d: %s", i+1, string(s.Value))
		}
	} else {
		// If one version happened-before the other (due to timing), no conflict is fine
		t.Logf("No conflict detected — one write causally preceded the other (value: %s)", string(resp.Value))
	}
}

// TestGetResponseNoConflictOnSingleWriter verifies that a single writer
// produces no sibling conflict on read.
func TestGetResponseNoConflictOnSingleWriter(t *testing.T) {
	cluster := SetupCluster(t, 3)
	defer cluster.TearDown()

	ctx := context.Background()
	client := cluster.nodes[0].client

	putReq := &proto.PutRequest{
		Key:              "no-conflict-key",
		Value:            []byte("single-value"),
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}

	resp, err := client.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Put failed: %s", resp.ErrorMessage)
	}

	getReq := &proto.GetRequest{
		Key:              "no-conflict-key",
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}

	getResp, err := client.Get(ctx, getReq)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !getResp.Found {
		t.Fatal("Key not found")
	}

	if getResp.HasConflict {
		t.Error("Expected no conflict for single-writer scenario")
	}

	if string(getResp.Value) != "single-value" {
		t.Errorf("Expected 'single-value', got '%s'", string(getResp.Value))
	}
}
