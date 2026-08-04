// Package partition implements consistent hashing for data distribution across nodes.
// Consistent hashing ensures minimal data movement when nodes are added/removed.
package partition

import (
	"crypto/md5"
	"fmt"
	"sort"
	"sync"
)

// ConsistentHash represents a consistent hash ring for distributing data across nodes.
// It uses virtual nodes to ensure uniform distribution even with few physical nodes.
type ConsistentHash struct {
	// virtualNodes: Number of virtual nodes per physical node (default: 150)
	// More virtual nodes = better distribution but more memory usage
	virtualNodes int

	// ring: Maps hash values to physical node IDs
	// Key: hash value, Value: physical node ID
	ring map[uint32]string

	// sortedKeys: Sorted list of hash values for efficient lookup
	// We use binary search to find the correct node for a key
	sortedKeys []uint32

	// nodes: Set of all physical nodes currently in the ring
	nodes map[string]bool

	// mu protects concurrent access to the ring, sortedKeys, and nodes fields
	mu sync.RWMutex
}

// NewConsistentHash creates a new consistent hash ring.
// virtualNodes: number of virtual nodes per physical node (recommended: 150-300)
func NewConsistentHash(virtualNodes int) *ConsistentHash {
	if virtualNodes <= 0 {
		virtualNodes = 150 // Default value from design doc
	}

	return &ConsistentHash{
		virtualNodes: virtualNodes,
		ring:         make(map[uint32]string),
		sortedKeys:   make([]uint32, 0),
		nodes:        make(map[string]bool),
	}
}

// hash computes MD5 hash of input string and returns first 4 bytes as uint32.
// MD5 is fast and provides good distribution for our use case.
func (ch *ConsistentHash) hash(key string) uint32 {
	hasher := md5.New()
	hasher.Write([]byte(key))
	hashBytes := hasher.Sum(nil)

	// Convert first 4 bytes to uint32 for ring position
	return uint32(hashBytes[0])<<24 +
		uint32(hashBytes[1])<<16 +
		uint32(hashBytes[2])<<8 +
		uint32(hashBytes[3])
}

// AddNode adds a physical node to the hash ring.
// It creates multiple virtual nodes (replicas) to ensure uniform distribution.
func (ch *ConsistentHash) AddNode(nodeID string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// Prevent duplicate nodes
	if ch.nodes[nodeID] {
		return
	}

	// Mark node as added
	ch.nodes[nodeID] = true

	// Create virtual nodes for this physical node
	for i := 0; i < ch.virtualNodes; i++ {
		// Create unique virtual node identifier: "nodeID:virtualIndex"
		virtualNodeKey := fmt.Sprintf("%s:%d", nodeID, i)
		hashValue := ch.hash(virtualNodeKey)

		// Add virtual node to ring
		ch.ring[hashValue] = nodeID
		ch.sortedKeys = append(ch.sortedKeys, hashValue)
	}

	// Keep keys sorted for binary search
	sort.Slice(ch.sortedKeys, func(i, j int) bool {
		return ch.sortedKeys[i] < ch.sortedKeys[j]
	})
}

// RemoveNode removes a physical node and all its virtual nodes from the ring.
func (ch *ConsistentHash) RemoveNode(nodeID string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// Check if node exists
	if !ch.nodes[nodeID] {
		return
	}

	// Remove node from set
	delete(ch.nodes, nodeID)

	// Remove all virtual nodes for this physical node
	newSortedKeys := make([]uint32, 0, len(ch.sortedKeys))

	for _, hashValue := range ch.sortedKeys {
		if ch.ring[hashValue] != nodeID {
			// Keep virtual nodes that don't belong to removed node
			newSortedKeys = append(newSortedKeys, hashValue)
		} else {
			// Remove virtual node from ring
			delete(ch.ring, hashValue)
		}
	}

	ch.sortedKeys = newSortedKeys
}

// GetNode returns the node responsible for storing a given key.
// It uses consistent hashing to find the first node clockwise from key's hash.
func (ch *ConsistentHash) GetNode(key string) string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.sortedKeys) == 0 {
		return "" // No nodes available
	}

	// Get hash value for the key
	keyHash := ch.hash(key)

	// Find first node clockwise from key's position using binary search
	idx := sort.Search(len(ch.sortedKeys), func(i int) bool {
		return ch.sortedKeys[i] >= keyHash
	})

	// If no node found (key hash is larger than all nodes), wrap around to first node
	if idx == len(ch.sortedKeys) {
		idx = 0
	}

	// Return the physical node ID
	return ch.ring[ch.sortedKeys[idx]]
}

// GetNodes returns N nodes responsible for a key (for replication).
// It finds the first N unique physical nodes clockwise from the key's position.
func (ch *ConsistentHash) GetNodes(key string, count int) []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.nodes) == 0 {
		return nil
	}

	// Cap count to available nodes
	if count > len(ch.nodes) {
		count = len(ch.nodes)
	}

	keyHash := ch.hash(key)
	result := make([]string, 0, count)
	seen := make(map[string]bool)

	// Find starting position
	startIdx := sort.Search(len(ch.sortedKeys), func(i int) bool {
		return ch.sortedKeys[i] >= keyHash
	})

	// Collect unique nodes starting from the position
	for i := 0; i < len(ch.sortedKeys) && len(result) < count; i++ {
		// Wrap around the ring
		idx := (startIdx + i) % len(ch.sortedKeys)
		nodeID := ch.ring[ch.sortedKeys[idx]]

		// Add only unique physical nodes
		if !seen[nodeID] {
			seen[nodeID] = true
			result = append(result, nodeID)
		}
	}

	return result
}

// GetAllNodes returns all nodes currently in the hash ring.
func (ch *ConsistentHash) GetAllNodes() []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	nodes := make([]string, 0, len(ch.nodes))
	for nodeID := range ch.nodes {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// Size returns the number of physical nodes in the ring.
func (ch *ConsistentHash) Size() int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	return len(ch.nodes)
}

// GetNodeDistribution returns how many virtual nodes each physical node has.
// Useful for debugging and ensuring balanced distribution.
func (ch *ConsistentHash) GetNodeDistribution() map[string]int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	distribution := make(map[string]int)
	for _, nodeID := range ch.ring {
		distribution[nodeID]++
	}
	return distribution
}
