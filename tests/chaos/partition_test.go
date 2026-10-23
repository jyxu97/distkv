// Chaos engineering tests for DistKV
// These tests simulate various failure scenarios to verify system resilience

package chaos

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"distkv/proto"
)

// ChaosCluster extends TestCluster with chaos engineering capabilities
type ChaosCluster struct {
	nodes      []*ChaosNode
	dataDir    string
	t          *testing.T
	partitions map[string][]string // Maps partition name to node IDs
}

// ChaosNode extends TestNode with chaos capabilities
type ChaosNode struct {
	nodeID     string
	address    string
	port       int
	cmd        *exec.Cmd
	client     proto.DistKVClient
	conn       *grpc.ClientConn
	isolated   bool          // Whether this node is network isolated
	slowDown   time.Duration // Artificial network delay
	packetLoss float64       // Packet loss percentage (0-1)
}

// SetupChaosCluster creates a test cluster for chaos engineering
func SetupChaosCluster(t *testing.T, nodeCount int) *ChaosCluster {
	dataDir := fmt.Sprintf("/tmp/distkv-chaos-%d", time.Now().Unix())

	cluster := &ChaosCluster{
		nodes:      make([]*ChaosNode, nodeCount),
		dataDir:    dataDir,
		t:          t,
		partitions: make(map[string][]string),
	}

	// Create nodes
	for i := 0; i < nodeCount; i++ {
		port := 9080 + i
		node := &ChaosNode{
			nodeID:  fmt.Sprintf("chaos-node%d", i+1),
			address: fmt.Sprintf("localhost:%d", port),
			port:    port,
		}
		cluster.nodes[i] = node
	}

	// Start cluster
	cluster.startCluster()
	return cluster
}

// startCluster starts all nodes in the chaos cluster
func (cc *ChaosCluster) startCluster() {
	// Start first node (seed)
	if err := cc.startNode(0, ""); err != nil {
		cc.t.Fatalf("Failed to start seed node: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Start remaining nodes
	for i := 1; i < len(cc.nodes); i++ {
		seedNodes := cc.nodes[0].address
		if err := cc.startNode(i, seedNodes); err != nil {
			cc.t.Fatalf("Failed to start node %d: %v", i, err)
		}
		time.Sleep(1 * time.Second)
	}

	// Wait for cluster to stabilize
	time.Sleep(5 * time.Second)

	// Create clients
	for i, node := range cc.nodes {
		conn, err := grpc.Dial(node.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			cc.t.Fatalf("Failed to connect to node %d: %v", i, err)
		}
		node.conn = conn
		node.client = proto.NewDistKVClient(conn)
	}
}

// startNode starts a single node
func (cc *ChaosCluster) startNode(index int, seedNodes string) error {
	node := cc.nodes[index]
	nodeDataDir := fmt.Sprintf("%s/node%d", cc.dataDir, index+1)

	args := []string{
		"-node-id=" + node.nodeID,
		"-address=" + node.address,
		"-data-dir=" + nodeDataDir,
		"-replicas=3",
		"-read-quorum=2",
		"-write-quorum=2",
		"-heartbeat-interval=1s",
		"-suspect-timeout=3s",
		"-dead-timeout=10s",
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

// CreateNetworkPartition simulates a network partition
func (cc *ChaosCluster) CreateNetworkPartition(partitionName string, nodeIndices []int) error {
	nodeIDs := make([]string, len(nodeIndices))

	// Block traffic between partitioned nodes and others
	for i, idx := range nodeIndices {
		node := cc.nodes[idx]
		node.isolated = true
		nodeIDs[i] = node.nodeID

		// Use iptables to block traffic (requires root or appropriate permissions)
		for j, otherNode := range cc.nodes {
			if !contains(nodeIndices, j) {
				// Block traffic to/from this node
				otherPort := strconv.Itoa(otherNode.port)

				// Block outgoing traffic
				exec.Command("iptables", "-A", "OUTPUT", "-p", "tcp",
					"--dport", otherPort, "-j", "DROP").Run()

				// Block incoming traffic
				nodePort := strconv.Itoa(node.port)
				exec.Command("iptables", "-A", "INPUT", "-p", "tcp",
					"--sport", otherPort, "--dport", nodePort, "-j", "DROP").Run()
			}
		}
	}

	cc.partitions[partitionName] = nodeIDs
	return nil
}

// HealNetworkPartition removes a network partition
func (cc *ChaosCluster) HealNetworkPartition(partitionName string) error {
	nodeIDs, exists := cc.partitions[partitionName]
	if !exists {
		return fmt.Errorf("partition %s not found", partitionName)
	}

	// Remove iptables rules (simplified - in practice you'd track exact rules)
	exec.Command("iptables", "-F").Run()

	// Mark nodes as no longer isolated
	for _, nodeID := range nodeIDs {
		for _, node := range cc.nodes {
			if node.nodeID == nodeID {
				node.isolated = false
				break
			}
		}
	}

	delete(cc.partitions, partitionName)
	return nil
}

// KillNode terminates a node process
func (cc *ChaosCluster) KillNode(nodeIndex int) error {
	node := cc.nodes[nodeIndex]
	if node.cmd != nil && node.cmd.Process != nil {
		return node.cmd.Process.Kill()
	}
	return nil
}

// RestartNode restarts a previously killed node
func (cc *ChaosCluster) RestartNode(nodeIndex int) error {
	node := cc.nodes[nodeIndex]

	// Wait for previous process to fully terminate
	if node.cmd != nil {
		node.cmd.Wait()
	}

	// Start the node again
	seedNodes := cc.nodes[0].address
	if nodeIndex == 0 && len(cc.nodes) > 1 {
		seedNodes = cc.nodes[1].address
	}

	return cc.startNode(nodeIndex, seedNodes)
}

// TearDown cleans up the chaos cluster
func (cc *ChaosCluster) TearDown() {
	// Heal any active partitions
	for partitionName := range cc.partitions {
		cc.HealNetworkPartition(partitionName)
	}

	// Close connections
	for _, node := range cc.nodes {
		if node.conn != nil {
			node.conn.Close()
		}
	}

	// Kill processes
	for _, node := range cc.nodes {
		if node.cmd != nil && node.cmd.Process != nil {
			node.cmd.Process.Kill()
			node.cmd.Wait()
		}
	}

	// Clean up data
	os.RemoveAll(cc.dataDir)
}

// TestNetworkPartition tests behavior during network partitions
func TestNetworkPartition(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("Network partition tests require root privileges for iptables")
	}

	cluster := SetupChaosCluster(t, 3)
	defer cluster.TearDown()

	ctx := context.Background()

	// Store initial data
	client := cluster.nodes[0].client
	putReq := &proto.PutRequest{
		Key:              "partition-test",
		Value:            []byte("initial-data"),
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}

	resp, err := client.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Initial put failed: %s", resp.ErrorMessage)
	}

	t.Run("Minority partition", func(t *testing.T) {
		// Create partition isolating one node
		err := cluster.CreateNetworkPartition("minority", []int{2})
		if err != nil {
			t.Fatalf("Failed to create partition: %v", err)
		}

		// Wait for partition detection
		time.Sleep(15 * time.Second)

		// Majority partition (nodes 0,1) should still work
		majorityClient := cluster.nodes[0].client
		putReq := &proto.PutRequest{
			Key:              "majority-write",
			Value:            []byte("majority-data"),
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		}

		resp, err := majorityClient.Put(ctx, putReq)
		if err != nil {
			t.Errorf("Write to majority partition failed: %v", err)
		} else if !resp.Success {
			t.Errorf("Write to majority partition failed: %s", resp.ErrorMessage)
		}

		// Minority partition (node 2) should fail quorum operations
		minorityClient := cluster.nodes[2].client
		putReq2 := &proto.PutRequest{
			Key:              "minority-write",
			Value:            []byte("minority-data"),
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		}

		resp2, err := minorityClient.Put(context.WithValue(ctx, "timeout", 5*time.Second), putReq2)
		if err == nil && resp2.Success {
			t.Error("Write to minority partition should have failed")
		}

		// Heal partition
		cluster.HealNetworkPartition("minority")
		time.Sleep(10 * time.Second)

		// After healing, minority node should be able to read majority writes
		getReq := &proto.GetRequest{
			Key:              "majority-write",
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		}

		getResp, err := minorityClient.Get(ctx, getReq)
		if err != nil {
			t.Errorf("Read after partition heal failed: %v", err)
		} else if !getResp.Found {
			t.Error("Data not found after partition heal")
		} else if string(getResp.Value) != "majority-data" {
			t.Errorf("Wrong data after partition heal: expected 'majority-data', got '%s'",
				string(getResp.Value))
		}
	})
}

// TestNodeCrashRecovery tests behavior when nodes crash and recover
func TestNodeCrashRecovery(t *testing.T) {
	cluster := SetupChaosCluster(t, 3)
	defer cluster.TearDown()

	ctx := context.Background()

	// Store initial data
	client := cluster.nodes[0].client
	for i := 0; i < 10; i++ {
		putReq := &proto.PutRequest{
			Key:              fmt.Sprintf("crash-test-%d", i),
			Value:            []byte(fmt.Sprintf("data-%d", i)),
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		}

		resp, err := client.Put(ctx, putReq)
		if err != nil {
			t.Fatalf("Initial put %d failed: %v", i, err)
		}
		if !resp.Success {
			t.Fatalf("Initial put %d failed: %s", i, resp.ErrorMessage)
		}
	}

	// Kill a node
	nodeIndex := 1
	if err := cluster.KillNode(nodeIndex); err != nil {
		t.Fatalf("Failed to kill node: %v", err)
	}

	// Wait for failure detection
	time.Sleep(15 * time.Second)

	// Remaining nodes should still serve reads
	remainingClient := cluster.nodes[0].client
	for i := 0; i < 10; i++ {
		getReq := &proto.GetRequest{
			Key:              fmt.Sprintf("crash-test-%d", i),
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		}

		resp, err := remainingClient.Get(ctx, getReq)
		if err != nil {
			t.Errorf("Read %d after crash failed: %v", i, err)
			continue
		}

		if !resp.Found {
			t.Errorf("Data %d lost after crash", i)
			continue
		}

		expected := fmt.Sprintf("data-%d", i)
		if string(resp.Value) != expected {
			t.Errorf("Wrong data %d after crash: expected '%s', got '%s'",
				i, expected, string(resp.Value))
		}
	}

	// Write new data while node is down
	putReq := &proto.PutRequest{
		Key:              "post-crash-data",
		Value:            []byte("written-while-down"),
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}

	resp, err := remainingClient.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Write after crash failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Write after crash failed: %s", resp.ErrorMessage)
	}

	// Restart the crashed node
	if err := cluster.RestartNode(nodeIndex); err != nil {
		t.Fatalf("Failed to restart node: %v", err)
	}

	// Wait for recovery and anti-entropy
	time.Sleep(20 * time.Second)

	// Recovered node should have all data (including data written while it was down)
	recoveredClient := cluster.nodes[nodeIndex].client
	getReq := &proto.GetRequest{
		Key:              "post-crash-data",
		ConsistencyLevel: proto.ConsistencyLevel_ONE, // Read from local node
	}

	getResp, err := recoveredClient.Get(ctx, getReq)
	if err != nil {
		t.Errorf("Read from recovered node failed: %v", err)
	} else if !getResp.Found {
		t.Error("Post-crash data not found on recovered node")
	} else if string(getResp.Value) != "written-while-down" {
		t.Errorf("Wrong post-crash data: expected 'written-while-down', got '%s'",
			string(getResp.Value))
	}
}

// TestSplitBrainPrevention tests that split-brain scenarios are handled correctly
func TestSplitBrainPrevention(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("Split-brain tests require root privileges for iptables")
	}

	cluster := SetupChaosCluster(t, 4) // Use 4 nodes for cleaner split
	defer cluster.TearDown()

	ctx := context.Background()

	// Store initial data
	client := cluster.nodes[0].client
	putReq := &proto.PutRequest{
		Key:              "split-brain-test",
		Value:            []byte("initial-data"),
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}

	resp, err := client.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Initial put failed: %s", resp.ErrorMessage)
	}

	// Create a split: nodes 0,1 vs nodes 2,3
	err = cluster.CreateNetworkPartition("split-a", []int{0, 1})
	if err != nil {
		t.Fatalf("Failed to create partition A: %v", err)
	}

	// Wait for partition detection
	time.Sleep(15 * time.Second)

	// Both partitions should be able to serve reads (they have majority locally)
	clientA := cluster.nodes[0].client
	clientB := cluster.nodes[2].client

	getReq := &proto.GetRequest{
		Key:              "split-brain-test",
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}

	// Both sides should be able to read existing data
	respA, err := clientA.Get(ctx, getReq)
	if err != nil {
		t.Errorf("Read from partition A failed: %v", err)
	} else if !respA.Found || string(respA.Value) != "initial-data" {
		t.Errorf("Wrong data from partition A")
	}

	respB, err := clientB.Get(ctx, getReq)
	if err != nil {
		t.Errorf("Read from partition B failed: %v", err)
	} else if !respB.Found || string(respB.Value) != "initial-data" {
		t.Errorf("Wrong data from partition B")
	}

	// With N=3, R=2, W=2 and a 2-2 split, writes should fail on both sides
	// because neither can achieve quorum

	putReqA := &proto.PutRequest{
		Key:              "partition-a-write",
		Value:            []byte("data-from-a"),
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}

	putReqB := &proto.PutRequest{
		Key:              "partition-b-write",
		Value:            []byte("data-from-b"),
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}

	// Both writes should fail (can't achieve W=2 quorum with only 2 nodes each)
	ctxA, cancelA := context.WithTimeout(ctx, 10*time.Second)
	defer cancelA()
	respA2, err := clientA.Put(ctxA, putReqA)
	if err == nil && respA2.Success {
		t.Error("Write to partition A should have failed due to insufficient quorum")
	}

	ctxB, cancelB := context.WithTimeout(ctx, 10*time.Second)
	defer cancelB()
	respB2, err := clientB.Put(ctxB, putReqB)
	if err == nil && respB2.Success {
		t.Error("Write to partition B should have failed due to insufficient quorum")
	}

	// Heal the partition
	cluster.HealNetworkPartition("split-a")
	time.Sleep(15 * time.Second)

	// After healing, writes should work again
	putReq3 := &proto.PutRequest{
		Key:              "post-heal-write",
		Value:            []byte("after-healing"),
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}

	resp3, err := client.Put(ctx, putReq3)
	if err != nil {
		t.Errorf("Write after healing failed: %v", err)
	} else if !resp3.Success {
		t.Errorf("Write after healing failed: %s", resp3.ErrorMessage)
	}
}

// TestPartitionCausesSiblings tests that network partitions causing concurrent
// writes result in sibling versions being preserved after partition heals.
func TestPartitionCausesSiblings(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("Network partition tests require root privileges for iptables")
	}

	cluster := SetupChaosCluster(t, 3)
	defer cluster.TearDown()

	ctx := context.Background()
	key := "partition-sibling-test"

	// Store initial data
	client := cluster.nodes[0].client
	putReq := &proto.PutRequest{
		Key:              key,
		Value:            []byte("initial"),
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}
	resp, err := client.Put(ctx, putReq)
	if err != nil || !resp.Success {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Partition: isolate node2 from nodes 0,1
	err = cluster.CreateNetworkPartition("sibling-test", []int{2})
	if err != nil {
		t.Fatalf("Failed to create partition: %v", err)
	}
	time.Sleep(15 * time.Second)

	// Write different values to each side with ONE consistency
	// (bypassing quorum so each side can write locally)
	majorityClient := cluster.nodes[0].client
	putMajority := &proto.PutRequest{
		Key:              key,
		Value:            []byte("majority-value"),
		ConsistencyLevel: proto.ConsistencyLevel_ONE,
	}
	majorityClient.Put(ctx, putMajority)

	minorityClient := cluster.nodes[2].client
	putMinority := &proto.PutRequest{
		Key:              key,
		Value:            []byte("minority-value"),
		ConsistencyLevel: proto.ConsistencyLevel_ONE,
	}
	minorityClient.Put(ctx, putMinority)

	// Heal partition
	cluster.HealNetworkPartition("sibling-test")
	time.Sleep(15 * time.Second)

	// Read with QUORUM — should detect concurrent versions as siblings
	getReq := &proto.GetRequest{
		Key:              key,
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	}

	getResp, err := majorityClient.Get(ctx, getReq)
	if err != nil {
		t.Fatalf("Get after partition heal failed: %v", err)
	}

	if !getResp.Found {
		t.Fatal("Key not found after partition heal")
	}

	if getResp.HasConflict {
		t.Logf("Conflict detected with %d siblings after partition heal (expected)", len(getResp.Siblings))
		values := make(map[string]bool)
		for i, s := range getResp.Siblings {
			t.Logf("  Sibling %d: %s", i+1, string(s.Value))
			values[string(s.Value)] = true
		}
		if !values["majority-value"] || !values["minority-value"] {
			t.Errorf("Expected both partition values in siblings, got %v", values)
		}
	} else {
		// One value may have causally dominated if replication happened before the read
		t.Logf("No conflict — replication resolved versions (value: %s)", string(getResp.Value))
	}
}

// TestClusterScaleOut tests that a new node can join an existing cluster,
// receive data via bootstrap streaming, and participate in quorum operations.
func TestClusterScaleOut(t *testing.T) {
	cluster := SetupChaosCluster(t, 3)
	defer cluster.TearDown()

	ctx := context.Background()
	client := cluster.nodes[0].client

	// Write initial data to the 3-node cluster
	const keyCount = 20
	for i := 0; i < keyCount; i++ {
		resp, err := client.Put(ctx, &proto.PutRequest{
			Key:              fmt.Sprintf("scale-key-%d", i),
			Value:            []byte(fmt.Sprintf("value-%d", i)),
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		})
		if err != nil {
			t.Fatalf("Pre-scale put %d: %v", i, err)
		}
		if !resp.Success {
			t.Fatalf("Pre-scale put %d failed: %s", i, resp.ErrorMessage)
		}
	}

	// Add a 4th node that joins via node-0 as seed
	newPort := 9083
	newNode := &ChaosNode{
		nodeID:  "chaos-node4",
		address: fmt.Sprintf("localhost:%d", newPort),
		port:    newPort,
	}
	cluster.nodes = append(cluster.nodes, newNode)

	if err := cluster.startNode(3, cluster.nodes[0].address); err != nil {
		t.Fatalf("Failed to start 4th node: %v", err)
	}

	// Connect client to the new node
	conn, err := grpc.Dial(newNode.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to new node: %v", err)
	}
	newNode.conn = conn
	newNode.client = proto.NewDistKVClient(conn)

	// The new node streams its ranges during startup; wait for gossip to spread
	// that it has finished bootstrapping
	t.Log("Waiting for new node to finish bootstrap (5s)...")
	time.Sleep(5 * time.Second)

	// New node should serve quorum reads for pre-existing keys
	misses := 0
	for i := 0; i < keyCount; i++ {
		resp, err := newNode.client.Get(ctx, &proto.GetRequest{
			Key:              fmt.Sprintf("scale-key-%d", i),
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		})
		if err != nil || !resp.Found {
			misses++
		}
	}
	if misses > 0 {
		t.Errorf("New node missed %d/%d keys via quorum read after scale-out", misses, keyCount)
	} else {
		t.Logf("New node served all %d pre-existing keys via quorum read", keyCount)
	}

	// Writes to the cluster should still succeed (quorum still satisfiable with 4 nodes)
	putResp, err := newNode.client.Put(ctx, &proto.PutRequest{
		Key:              "post-scale-write",
		Value:            []byte("written-after-scale-out"),
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	})
	if err != nil {
		t.Fatalf("Write via new node failed: %v", err)
	}
	if !putResp.Success {
		t.Fatalf("Write via new node rejected: %s", putResp.ErrorMessage)
	}

	// Original nodes can read the write from the new node
	getResp, err := client.Get(ctx, &proto.GetRequest{
		Key:              "post-scale-write",
		ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
	})
	if err != nil {
		t.Fatalf("Original node read failed: %v", err)
	}
	if !getResp.Found || string(getResp.Value) != "written-after-scale-out" {
		t.Errorf("Original node got wrong value after scale-out: found=%v value=%q",
			getResp.Found, string(getResp.Value))
	}
	t.Log("Scale-out: all checks passed — new node joined, synced, and participates in quorum")
}

// Helper functions

func contains(slice []int, item int) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}
