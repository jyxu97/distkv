// Package gossip implements failure detection using gossip protocol
// The gossip protocol helps nodes discover and monitor each other's health
// in a distributed system without requiring centralized coordination.
package gossip

import (
	"fmt"
	"sync"
	"time"
)

// NodeStatus represents the current state of a node in the cluster.
type NodeStatus int

const (
	// NodeAlive means the node is healthy and responsive
	NodeAlive NodeStatus = iota
	// NodeSuspect means we haven't heard from the node recently (might be down)
	NodeSuspect
	// NodeDead means the node is confirmed to be down
	NodeDead
)

// String returns human-readable representation of node status.
func (s NodeStatus) String() string {
	switch s {
	case NodeAlive:
		return "ALIVE"
	case NodeSuspect:
		return "SUSPECT"
	case NodeDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// NodeInfo contains information about a node in the cluster.
// This information is shared between nodes via gossip messages.
type NodeInfo struct {
	// NodeID is the unique identifier for this node
	NodeID string

	// Address is the network address (IP:port) where this node can be reached
	Address string

	// HeartbeatCounter increments each time the node sends a heartbeat
	// This helps detect if we're receiving stale information
	HeartbeatCounter uint64

	// LastSeen is when we last heard from this node (Unix timestamp)
	LastSeen int64

	// Status indicates if the node is alive, suspect, or dead
	Status NodeStatus

	// Version tracks the version of this node info for conflict resolution
	Version uint64

	// mutex protects concurrent updates to this node info
	mutex sync.RWMutex
}

// NewNodeInfo creates a new NodeInfo for a node.
func NewNodeInfo(nodeID, address string) *NodeInfo {
	now := time.Now().Unix()

	return &NodeInfo{
		NodeID:           nodeID,
		Address:          address,
		HeartbeatCounter: 0,
		LastSeen:         now,
		Status:           NodeAlive,
		Version:          1,
	}
}

// UpdateHeartbeat increments the heartbeat counter and updates last seen time.
// This is called when the local node sends a heartbeat.
func (ni *NodeInfo) UpdateHeartbeat() {
	ni.mutex.Lock()
	defer ni.mutex.Unlock()

	ni.HeartbeatCounter++
	ni.LastSeen = time.Now().Unix()
	ni.Version++
}

// UpdateLastSeen updates when we last heard from this node.
// This is called when receiving gossip messages from other nodes.
func (ni *NodeInfo) UpdateLastSeen() {
	ni.mutex.Lock()
	defer ni.mutex.Unlock()

	ni.LastSeen = time.Now().Unix()
	ni.Version++
}

// SetStatus changes the node's status (alive, suspect, dead).
func (ni *NodeInfo) SetStatus(status NodeStatus) {
	ni.mutex.Lock()
	defer ni.mutex.Unlock()

	if ni.Status != status {
		ni.Status = status
		ni.Version++
	}
}

// GetStatus returns the current status of the node.
func (ni *NodeInfo) GetStatus() NodeStatus {
	ni.mutex.RLock()
	defer ni.mutex.RUnlock()

	return ni.Status
}

// GetHeartbeatCounter returns the current heartbeat counter.
func (ni *NodeInfo) GetHeartbeatCounter() uint64 {
	ni.mutex.RLock()
	defer ni.mutex.RUnlock()

	return ni.HeartbeatCounter
}

// GetLastSeen returns when we last heard from this node.
func (ni *NodeInfo) GetLastSeen() int64 {
	ni.mutex.RLock()
	defer ni.mutex.RUnlock()

	return ni.LastSeen
}

// GetVersion returns the current version of this node info.
func (ni *NodeInfo) GetVersion() uint64 {
	ni.mutex.RLock()
	defer ni.mutex.RUnlock()

	return ni.Version
}

// IsStale checks if this node info is older than another version.
// Used to determine which node info is more recent during gossip.
func (ni *NodeInfo) IsStale(otherVersion uint64, otherHeartbeat uint64) bool {
	ni.mutex.RLock()
	defer ni.mutex.RUnlock()

	// If other version is higher, our info is stale
	if otherVersion > ni.Version {
		return true
	}

	// If versions are equal, compare heartbeat counters
	if otherVersion == ni.Version && otherHeartbeat > ni.HeartbeatCounter {
		return true
	}

	return false
}

// MergeFrom updates this node info with more recent information from another source.
// This implements the "merge" operation in gossip protocols.
func (ni *NodeInfo) MergeFrom(other *NodeInfo) bool {
	if ni.NodeID != other.NodeID {
		return false // Can't merge info from different nodes
	}

	ni.mutex.Lock()
	defer ni.mutex.Unlock()

	updated := false

	// Use HeartbeatCounter as the primary ordering mechanism. Version is NOT
	// transmitted across gossip hops (the proto omits it, so receivers always
	// reconstruct it as 1), which means Version-first comparisons break after any
	// local SetStatus/UpdateLastSeen call increments Version above 1.
	// HeartbeatCounter is the only reliably end-to-end monotone signal.
	if other.HeartbeatCounter > ni.HeartbeatCounter ||
		(other.HeartbeatCounter == ni.HeartbeatCounter && other.Version > ni.Version) {

		ni.HeartbeatCounter = other.HeartbeatCounter
		// Record when we received this update, not when the remote node generated it.
		// LastSeen tracks "when did I last get news about this node", so stale
		// timestamps must not propagate across gossip hops.
		ni.LastSeen = time.Now().Unix()
		ni.Status = other.Status
		ni.Version = other.Version
		updated = true
	}

	return updated
}

// Copy creates a thread-safe copy of the node info.
// This is used when sending gossip messages to avoid data races.
func (ni *NodeInfo) Copy() *NodeInfo {
	ni.mutex.RLock()
	defer ni.mutex.RUnlock()

	return &NodeInfo{
		NodeID:           ni.NodeID,
		Address:          ni.Address,
		HeartbeatCounter: ni.HeartbeatCounter,
		LastSeen:         ni.LastSeen,
		Status:           ni.Status,
		Version:          ni.Version,
	}
}

// IsExpired checks if this node should be considered dead based on timeout.
// suspectTimeout: how long to wait before marking a node as suspect
// deadTimeout: how long to wait before marking a node as dead
func (ni *NodeInfo) IsExpired(suspectTimeout, deadTimeout time.Duration) NodeStatus {
	ni.mutex.RLock()
	defer ni.mutex.RUnlock()

	now := time.Now().Unix()
	timeSinceLastSeen := time.Duration(now-ni.LastSeen) * time.Second

	if timeSinceLastSeen > deadTimeout {
		return NodeDead
	} else if timeSinceLastSeen > suspectTimeout {
		return NodeSuspect
	}

	return NodeAlive
}

// String returns a human-readable representation of the node info.
func (ni *NodeInfo) String() string {
	ni.mutex.RLock()
	defer ni.mutex.RUnlock()

	return fmt.Sprintf("NodeInfo{ID:%s, Addr:%s, HB:%d, Status:%s, Ver:%d}",
		ni.NodeID, ni.Address, ni.HeartbeatCounter, ni.Status, ni.Version)
}

// GossipConfig holds configuration for the gossip protocol.
type GossipConfig struct {
	// HeartbeatInterval is how often to send heartbeats
	HeartbeatInterval time.Duration

	// SuspectTimeout is how long to wait before marking a node as suspect
	SuspectTimeout time.Duration

	// DeadTimeout is how long to wait before marking a node as dead
	DeadTimeout time.Duration

	// GossipInterval is how often to gossip with other nodes
	GossipInterval time.Duration

	// GossipFanout is how many nodes to gossip with each round
	GossipFanout int

	// MaxGossipPacketSize limits the size of gossip messages
	MaxGossipPacketSize int
}

// DefaultGossipConfig returns reasonable default configuration values.
func DefaultGossipConfig() *GossipConfig {
	return &GossipConfig{
		HeartbeatInterval:   1 * time.Second,  // Send heartbeat every second
		SuspectTimeout:      5 * time.Second,  // Mark suspect after 5 seconds
		DeadTimeout:         30 * time.Second, // Mark dead after 30 seconds
		GossipInterval:      1 * time.Second,  // Gossip every second
		GossipFanout:        3,                // Gossip to 3 random nodes
		MaxGossipPacketSize: 64 * 1024,        // 64KB max packet size
	}
}
