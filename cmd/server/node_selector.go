// NodeSelector implementation that integrates consistent hashing with gossip protocol
// This bridges the partitioning system with the failure detection system.

package main

import (
	"time"

	"distkv/pkg/gossip"
	"distkv/pkg/partition"
	"distkv/pkg/replication"
)

// NodeSelector implements the replication.NodeSelector interface
// It uses consistent hashing to determine which nodes should store data
// and gossip protocol to know which nodes are currently alive.
type NodeSelector struct {
	// consistentHash provides the partitioning logic
	consistentHash *partition.ConsistentHash

	// gossipManager provides information about node health
	gossipManager *gossip.Gossip
}

// NewNodeSelector creates a new NodeSelector that bridges consistent hashing with gossip
func NewNodeSelector(consistentHash *partition.ConsistentHash, gossipManager *gossip.Gossip) *NodeSelector {
	return &NodeSelector{
		consistentHash: consistentHash,
		gossipManager:  gossipManager,
	}
}

// GetReplicas returns N nodes that should store replicas of the key
// Uses consistent hashing to determine the nodes, regardless of their current status.
// Joining nodes are skipped as if they were not on the ring yet, so these are the
// replicas that already hold the key's data and can serve reads.
func (ns *NodeSelector) GetReplicas(key string, count int) []replication.ReplicaInfo {
	allNodes := ns.gossipManager.GetNodes()
	isJoining := func(nodeID string) bool {
		nodeInfo, exists := allNodes[nodeID]
		return exists && nodeInfo.IsJoining()
	}

	// Get nodes from consistent hashing (this gives us the ideal placement)
	nodeIDs := ns.consistentHash.GetNodesExcluding(key, count, isJoining)
	return ns.toReplicaInfos(nodeIDs, allNodes)
}

// GetPendingReplicas returns the alive joining nodes that will become replicas
// of the key once they finish bootstrapping. They receive writes so they do not
// miss updates made while they stream data, but are never read from.
func (ns *NodeSelector) GetPendingReplicas(key string, count int) []replication.ReplicaInfo {
	allNodes := ns.gossipManager.GetNodes()

	var pendingIDs []string
	for _, nodeID := range ns.consistentHash.GetNodes(key, count) {
		if nodeInfo, exists := allNodes[nodeID]; exists && nodeInfo.IsJoining() {
			pendingIDs = append(pendingIDs, nodeID)
		}
	}

	pending := make([]replication.ReplicaInfo, 0, len(pendingIDs))
	for _, replica := range ns.toReplicaInfos(pendingIDs, allNodes) {
		if replica.IsAlive {
			pending = append(pending, replica)
		}
	}
	return pending
}

// toReplicaInfos converts node IDs to ReplicaInfo using the gossip view.
func (ns *NodeSelector) toReplicaInfos(nodeIDs []string, allNodes map[string]*gossip.NodeInfo) []replication.ReplicaInfo {
	replicas := make([]replication.ReplicaInfo, 0, len(nodeIDs))

	for _, nodeID := range nodeIDs {
		if nodeInfo, exists := allNodes[nodeID]; exists {
			replica := replication.ReplicaInfo{
				NodeID:   nodeID,
				Address:  nodeInfo.Address,
				IsAlive:  nodeInfo.GetStatus() == gossip.NodeAlive,
				LastSeen: convertUnixToTime(nodeInfo.GetLastSeen()),
			}
			replicas = append(replicas, replica)
		} else {
			// Node doesn't exist in gossip (shouldn't happen in normal operation)
			// Create a dead replica entry
			replica := replication.ReplicaInfo{
				NodeID:   nodeID,
				Address:  "unknown",
				IsAlive:  false,
				LastSeen: convertUnixToTime(0),
			}
			replicas = append(replicas, replica)
		}
	}

	return replicas
}

// GetAliveReplicas returns only the alive nodes from GetReplicas
// This is used when we need to ensure we only contact responsive nodes
func (ns *NodeSelector) GetAliveReplicas(key string, count int) []replication.ReplicaInfo {
	// Get all potential replicas
	allReplicas := ns.GetReplicas(key, count)

	// Filter to only alive replicas
	aliveReplicas := make([]replication.ReplicaInfo, 0, len(allReplicas))

	for _, replica := range allReplicas {
		if replica.IsAlive {
			aliveReplicas = append(aliveReplicas, replica)
		}
	}

	return aliveReplicas
}

// Helper function to convert Unix timestamp to time.Time
func convertUnixToTime(unixTime int64) time.Time {
	return time.Unix(unixTime, 0)
}
