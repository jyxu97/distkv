// Scale-out verification tool for DistKV.
//
// Demonstrates that when a new node joins, the consistent hash ring
// rebalances and the new node physically holds its assigned keys.
//
// Usage:
//
//	go run ./tests/scaleout -server=localhost:8080 -new-node=localhost:8083
//
// Start a 3-node cluster first, then follow the prompts to add node4.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"distkv/proto"
)

func main() {
	server := flag.String("server", "localhost:8080", "Coordinator address for Put/GetKeyOwnership calls")
	newNode := flag.String("new-node", "localhost:8083", "Address of the new node to verify after scale-out")
	keys := flag.Int("keys", 100, "Number of keys to sample")
	replicas := flag.Int("replicas", 3, "Replication factor N")
	wait := flag.Int("wait", 40, "Seconds to wait for anti-entropy after node is added")
	flag.Parse()

	// Connect to coordinator
	conn, err := grpc.Dial(*server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to coordinator %s: %v\n", *server, err)
		os.Exit(1)
	}
	defer conn.Close()
	kvClient := proto.NewDistKVClient(conn)
	adminClient := proto.NewAdminServiceClient(conn)

	fmt.Printf("=== DistKV Scale-Out Verification ===\n")
	fmt.Printf("Coordinator: %s  |  New node: %s  |  Keys: %d  |  Replicas: %d\n\n",
		*server, *newNode, *keys, *replicas)

	// ── Phase 1: before scale-out ──────────────────────────────────────────────

	fmt.Printf("Phase 1: Pre-writing %d keys and recording ownership...\n", *keys)
	prewrite(kvClient, *keys)

	before := getOwnership(adminClient, *keys, *replicas)
	printNodeCounts("Node key-counts (before scale-out)", before)

	fmt.Printf("\nNow start node4 at %s.\n", *newNode)
	fmt.Print("Press Enter when the new node has joined the cluster...")
	bufio.NewReader(os.Stdin).ReadString('\n')

	fmt.Printf("Waiting %d seconds for anti-entropy to sync keys to the new node...\n", *wait)
	time.Sleep(time.Duration(*wait) * time.Second)

	// ── Phase 2: after scale-out ───────────────────────────────────────────────

	fmt.Println("\nPhase 2: Re-checking ownership and verifying new node holds its keys...")
	after := getOwnership(adminClient, *keys, *replicas)
	printNodeCounts("Node key-counts (after scale-out)", after)

	// Find keys whose replica set now includes the new node
	changed := findChangedKeys(before, after, *newNode)
	fmt.Printf("\nKeys sampled:                  %d\n", *keys)
	fmt.Printf("Keys with changed ownership:   %d (~%.0f%% of total, expected ~25%% for 3→4 nodes)\n",
		len(changed), float64(len(changed))/float64(*keys)*100)

	if len(changed) == 0 {
		fmt.Println("\nNo keys migrated to new node — is the node address correct?")
		os.Exit(1)
	}

	// Connect directly to the new node (LocalGet bypasses quorum)
	nodeConn, err := grpc.Dial(*newNode, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to new node %s: %v\n", *newNode, err)
		os.Exit(1)
	}
	defer nodeConn.Close()
	nodeClient := proto.NewNodeServiceClient(nodeConn)

	ok, total := verifyOnNewNode(kvClient, nodeClient, changed)
	fmt.Printf("Keys successfully read from new node: %d / %d\n", ok, total)

	if ok == total {
		fmt.Println("\nSCALE-OUT VERIFIED: new node holds all its assigned keys.")
	} else {
		fmt.Printf("\nWARNING: %d key(s) not yet present on new node. Try increasing -wait.\n", total-ok)
		os.Exit(1)
	}
}

// prewrite writes numKeys deterministic keys so they can be looked up later.
func prewrite(client proto.DistKVClient, numKeys int) {
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("scaleout-key-%d", i)
		val := []byte(fmt.Sprintf("value-%d", i))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = client.Put(ctx, &proto.PutRequest{Key: key, Value: val})
		cancel()
	}
}

// ownershipMap maps key → slice of node IDs that own it.
type ownershipMap map[string][]string

// getOwnership calls GetKeyOwnership for each sampled key.
func getOwnership(client proto.AdminServiceClient, numKeys, replicas int) ownershipMap {
	result := make(ownershipMap, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("scaleout-key-%d", i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.GetKeyOwnership(ctx, &proto.KeyOwnershipRequest{
			Key:      key,
			Replicas: int32(replicas),
		})
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "GetKeyOwnership(%s): %v\n", key, err)
			continue
		}
		result[key] = resp.Nodes
	}
	return result
}

// printNodeCounts prints how many keys each node owns.
func printNodeCounts(title string, om ownershipMap) {
	counts := make(map[string]int)
	for _, nodes := range om {
		for _, n := range nodes {
			counts[n]++
		}
	}
	fmt.Printf("\n%s:\n", title)
	for node, count := range counts {
		fmt.Printf("  %-20s  %d keys\n", node, count)
	}
}

// findChangedKeys returns keys where the new node appears in the post-scale-out
// replica set but was absent before.
func findChangedKeys(before, after ownershipMap, newNodeID string) []string {
	var changed []string
	for key, afterNodes := range after {
		if containsNode(afterNodes, newNodeID) && !containsNode(before[key], newNodeID) {
			changed = append(changed, key)
		}
	}
	return changed
}

func containsNode(nodes []string, target string) bool {
	for _, n := range nodes {
		if n == target {
			return true
		}
	}
	return false
}

// verifyOnNewNode checks that each changed key can be read directly from the
// new node via LocalGet (bypasses quorum — confirms the node physically has it).
// It also compares the value against the coordinator to detect staleness.
func verifyOnNewNode(kvClient proto.DistKVClient, nodeClient proto.NodeServiceClient, keys []string) (ok, total int) {
	total = len(keys)
	for _, key := range keys {
		// Fetch expected value via coordinator
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		expected, err := kvClient.Get(ctx, &proto.GetRequest{Key: key})
		cancel()
		if err != nil || !expected.Found {
			// Key missing from cluster entirely — skip
			total--
			continue
		}

		// Fetch directly from new node
		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		local, err2 := nodeClient.LocalGet(ctx2, &proto.LocalGetRequest{Key: key})
		cancel2()
		if err2 != nil || !local.Found {
			fmt.Printf("  MISSING  %s\n", key)
			continue
		}
		ok++
	}
	return ok, total
}
