// Service implementations for DistKV server
// This file contains the gRPC service implementations that handle client requests
// and inter-node communication.

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"

	"distkv/pkg/consensus"
	"distkv/pkg/gossip"
	"distkv/pkg/partition"
	"distkv/pkg/replication"
	"distkv/pkg/storage"
	"distkv/proto"
)

// DistKVServiceImpl implements the main client-facing API
type DistKVServiceImpl struct {
	proto.UnimplementedDistKVServer
	server *DistKVServer
}

// Put stores a key-value pair using quorum replication
func (s *DistKVServiceImpl) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	writeResp, err := s.coordinateWrite(ctx, req.Key, req.Value, req.VectorClock)
	if err != nil {
		return &proto.PutResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &proto.PutResponse{
		Success:      writeResp.Success,
		VectorClock:  convertVectorClockToProto(writeResp.VectorClock),
		ErrorMessage: "", // Success
	}, nil
}

// coordinateWrite generates the version for one logical write and replicates
// it. The version is created exactly once, here, before replication: the
// client's causal context (empty for a blind write) with this node's entry
// advanced to a value it has never issued before, even across restarts.
// Replicas store that clock unchanged, and it is what the client gets back.
// A nil value writes a tombstone.
func (s *DistKVServiceImpl) coordinateWrite(ctx context.Context, key string, value []byte, causalContext *proto.VectorClock) (*replication.WriteResponse, error) {
	var contextVC *consensus.VectorClock
	if causalContext != nil {
		contextVC = convertVectorClockFromProto(causalContext)
	}

	version, err := consensus.NewVersion(contextVC, s.server.config.NodeID, s.server.dotCounter)
	if err != nil {
		return nil, fmt.Errorf("failed to generate version: %v", err)
	}

	return s.server.quorumManager.Write(&replication.WriteRequest{
		Key:         key,
		Value:       value,
		VectorClock: version,
		Context:     ctx,
	})
}

// Get retrieves a value by key using quorum reads
func (s *DistKVServiceImpl) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	// Create read request for quorum manager
	readReq := &replication.ReadRequest{
		Key:     req.Key,
		Context: ctx,
	}

	// Perform quorum read
	readResp, err := s.server.quorumManager.Read(readReq)
	if err != nil {
		return &proto.GetResponse{
			Found:        false,
			ErrorMessage: err.Error(),
		}, nil
	}

	// Convert siblings to proto format
	var protoSiblings []*proto.SiblingVersion
	for _, s := range readResp.Siblings {
		protoSiblings = append(protoSiblings, &proto.SiblingVersion{
			Value:       s.Value,
			VectorClock: convertVectorClockToProto(s.VectorClock),
		})
	}

	// With siblings, the context returned to the client must cover all of
	// them, so a write that resolves the conflict supersedes every sibling
	// instead of just the one shown as the primary value.
	causalContext := readResp.VectorClock
	if readResp.HasConflict {
		causalContext = consensus.NewVectorClock()
		for _, sibling := range readResp.Siblings {
			if sibling.VectorClock != nil {
				causalContext.Update(sibling.VectorClock)
			}
		}
	}

	return &proto.GetResponse{
		Value:        readResp.Value,
		Found:        readResp.Found,
		VectorClock:  convertVectorClockToProto(causalContext),
		ErrorMessage: "",
		Siblings:     protoSiblings,
		HasConflict:  readResp.HasConflict,
	}, nil
}

// Delete removes a key-value pair using tombstone markers. Like Put, the
// tombstone's version is the client's causal context (if any) advanced once by
// this coordinator.
func (s *DistKVServiceImpl) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	// A nil value indicates deletion
	writeResp, err := s.coordinateWrite(ctx, req.Key, nil, req.VectorClock)
	if err != nil {
		return &proto.DeleteResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &proto.DeleteResponse{
		Success:      writeResp.Success,
		ErrorMessage: "",
	}, nil
}

// BatchPut performs multiple put operations
func (s *DistKVServiceImpl) BatchPut(ctx context.Context, req *proto.BatchPutRequest) (*proto.BatchPutResponse, error) {
	// For simplicity, we'll perform each put sequentially
	// A production implementation might optimize this with parallel writes

	var failedKeys []string

	for key, value := range req.Items {
		// Each item is its own blind write; Put generates a distinct version
		putReq := &proto.PutRequest{
			Key:              key,
			Value:            value,
			ConsistencyLevel: req.ConsistencyLevel,
		}

		// Perform the put
		putResp, err := s.Put(ctx, putReq)
		if err != nil || !putResp.Success {
			failedKeys = append(failedKeys, key)
		}
	}

	success := len(failedKeys) == 0
	errorMessage := ""
	if !success {
		errorMessage = fmt.Sprintf("Failed to put %d out of %d keys", len(failedKeys), len(req.Items))
	}

	return &proto.BatchPutResponse{
		Success:      success,
		ErrorMessage: errorMessage,
		FailedKeys:   failedKeys,
	}, nil
}

// NodeServiceImpl implements inter-node communication
type NodeServiceImpl struct {
	proto.UnimplementedNodeServiceServer
	server *DistKVServer
}

// LocalGet performs a local-only read operation (bypasses quorum)
// This is used for inter-node replication reads to avoid infinite recursion
func (s *NodeServiceImpl) LocalGet(ctx context.Context, req *proto.LocalGetRequest) (*proto.LocalGetResponse, error) {
	// Read directly from local storage engine (bypass quorum manager). Use
	// GetEntry so a tombstone's clock is reported: without it a delete would
	// look clockless and lose to any stale copy of the value.
	entry, err := s.server.storageEngine.GetEntry(req.Key)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return &proto.LocalGetResponse{
				Found:        false,
				ErrorMessage: "",
			}, nil
		}
		return &proto.LocalGetResponse{
			Found:        false,
			ErrorMessage: err.Error(),
		}, nil
	}

	// Handle tombstone entries
	if entry != nil && entry.Deleted {
		return &proto.LocalGetResponse{
			Found:        false,
			VectorClock:  convertVectorClockToProto(entry.VectorClock),
			ErrorMessage: "",
		}, nil
	}

	return &proto.LocalGetResponse{
		Value:       entry.Value,
		Found:       true,
		VectorClock: convertVectorClockToProto(entry.VectorClock),
	}, nil
}

// Replicate handles replication requests from other nodes. The replica stores
// the version exactly as received — it never advances the clock itself — and
// echoes that clock in its ACK. It refuses a version that its local entry
// supersedes, so a delayed or duplicate message cannot roll the key back.
func (s *NodeServiceImpl) Replicate(ctx context.Context, req *proto.ReplicateRequest) (*proto.ReplicateResponse, error) {
	vectorClock := convertVectorClockFromProto(req.VectorClock)

	var entry *storage.Entry
	if req.IsDelete {
		entry = storage.NewDeleteEntry(req.Key, vectorClock)
	} else {
		entry = storage.NewEntry(req.Key, req.Value, vectorClock)
	}

	applied, current, err := s.server.storageEngine.WriteIf(entry, func(current *storage.Entry) bool {
		return !replication.Supersedes(current, vectorClock, req.IsDelete)
	})
	if err != nil {
		return &proto.ReplicateResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	if !applied {
		return &proto.ReplicateResponse{
			Success: false,
			ErrorMessage: fmt.Sprintf("stale version %s: local version %s supersedes it",
				vectorClock, current.VectorClock),
			VectorClock: convertVectorClockToProto(current.VectorClock),
		}, nil
	}

	return &proto.ReplicateResponse{
		Success:     true,
		VectorClock: convertVectorClockToProto(vectorClock),
	}, nil
}

// AntiEntropy handles anti-entropy repair requests.
// The caller sends its local data hash; if our hash differs we return all our
// non-deleted entries so the caller can apply anything it's missing or behind on.
func (s *NodeServiceImpl) AntiEntropy(ctx context.Context, req *proto.AntiEntropyRequest) (*proto.AntiEntropyResponse, error) {
	iter, err := s.server.storageEngine.Iterator()
	if err != nil {
		log.Printf("AntiEntropy: failed to open iterator: %v", err)
		return &proto.AntiEntropyResponse{}, nil
	}
	defer iter.Close()

	h := sha256.New()
	var entries []*proto.KeyValue
	for iter.Valid() {
		entry := iter.Value()
		if entry != nil && !entry.Deleted {
			fmt.Fprintf(h, "%s:%x\n", entry.Key, entry.Value)
			kv := &proto.KeyValue{
				Key:   entry.Key,
				Value: entry.Value,
			}
			if entry.VectorClock != nil {
				kv.VectorClock = convertVectorClockToProto(entry.VectorClock)
			}
			entries = append(entries, kv)
		}
		iter.Next()
	}

	localHash := hex.EncodeToString(h.Sum(nil))

	// If the caller's hash matches ours, data is in sync — return nothing.
	if req.MerkleTree != nil && req.MerkleTree.Hash == localHash {
		return &proto.AntiEntropyResponse{}, nil
	}

	return &proto.AntiEntropyResponse{MissingKeys: entries}, nil
}

// StreamRange returns every live entry whose ring hash falls in one of the
// requested token ranges. A joining node calls this on each peer to bootstrap
// the ranges it is taking over. Tombstones are skipped, as in AntiEntropy.
func (s *NodeServiceImpl) StreamRange(ctx context.Context, req *proto.StreamRangeRequest) (*proto.StreamRangeResponse, error) {
	ranges := make([]partition.TokenRange, len(req.Ranges))
	for i, r := range req.Ranges {
		ranges[i] = partition.TokenRange{Start: r.Start, End: r.End}
	}

	iter, err := s.server.storageEngine.Iterator()
	if err != nil {
		return nil, fmt.Errorf("failed to open storage iterator: %v", err)
	}
	defer iter.Close()

	var entries []*proto.KeyValue
	for ; iter.Valid(); iter.Next() {
		entry := iter.Value()
		if entry == nil || entry.Deleted {
			continue
		}

		h := s.server.consistentHash.HashKey(entry.Key)
		for _, r := range ranges {
			if r.Contains(h) {
				entries = append(entries, &proto.KeyValue{
					Key:         entry.Key,
					Value:       entry.Value,
					VectorClock: convertVectorClockToProto(entry.VectorClock),
				})
				break
			}
		}
	}

	return &proto.StreamRangeResponse{Entries: entries}, nil
}

// Gossip handles gossip protocol messages
func (s *NodeServiceImpl) Gossip(ctx context.Context, req *proto.GossipMessage) (*proto.GossipResponse, error) {
	// Convert proto node info to internal format
	nodeUpdates := make([]*gossip.NodeInfo, len(req.NodeUpdates))
	for i, protoNodeInfo := range req.NodeUpdates {
		nodeUpdates[i] = convertNodeInfoFromProto(protoNodeInfo)
	}

	// Process gossip message
	responses := s.server.gossipManager.ProcessGossipMessage(req.SenderId, nodeUpdates)

	// Convert responses back to proto format
	protoResponses := make([]*proto.NodeInfo, len(responses))
	for i, nodeInfo := range responses {
		protoResponses[i] = convertNodeInfoToProto(nodeInfo)
	}

	return &proto.GossipResponse{
		NodeUpdates: protoResponses,
	}, nil
}

// Handoff handles hinted handoff requests (placeholder)
func (s *NodeServiceImpl) Handoff(ctx context.Context, req *proto.HandoffRequest) (*proto.HandoffResponse, error) {
	// This is a placeholder for hinted handoff
	// A full implementation would apply the hinted writes to the local storage
	log.Printf("Handoff request received with %d writes", len(req.Writes))

	return &proto.HandoffResponse{
		Success: true,
	}, nil
}

// AdminServiceImpl implements cluster management operations
type AdminServiceImpl struct {
	proto.UnimplementedAdminServiceServer
	server *DistKVServer
}

// AddNode adds a new node to the cluster
func (s *AdminServiceImpl) AddNode(ctx context.Context, req *proto.AddNodeRequest) (*proto.AddNodeResponse, error) {
	// Use the node ID provided by the joining node
	nodeID := req.NodeId

	// Add to gossip manager. A bootstrapping node is registered as joining
	// before it enters the ring, so it never serves reads for its new ranges.
	if req.Joining {
		s.server.gossipManager.AddJoiningNode(nodeID, req.NodeAddress)
	} else {
		s.server.gossipManager.AddNode(nodeID, req.NodeAddress)
	}

	// Update replica client with new node address
	s.server.replicaClient.UpdateNodeAddress(nodeID, req.NodeAddress)

	// Add to consistent hash ring. This happens before we reply, so once the
	// joining node's announce returns, our writes already include it.
	s.server.consistentHash.AddNode(nodeID)

	log.Printf("Added node %s at %s to cluster", nodeID, req.NodeAddress)

	return &proto.AddNodeResponse{
		Success: true,
	}, nil
}

// RemoveNode removes a node from the cluster
func (s *AdminServiceImpl) RemoveNode(ctx context.Context, req *proto.RemoveNodeRequest) (*proto.RemoveNodeResponse, error) {
	// Generate node ID from address
	nodeID := fmt.Sprintf("node-%s", req.NodeAddress)

	// Remove from gossip manager
	s.server.gossipManager.RemoveNode(nodeID)

	// Remove from consistent hash ring
	s.server.consistentHash.RemoveNode(nodeID)

	log.Printf("Removed node %s at %s from cluster", nodeID, req.NodeAddress)

	return &proto.RemoveNodeResponse{
		Success: true,
	}, nil
}

// Rebalance triggers data rebalancing (placeholder)
func (s *AdminServiceImpl) Rebalance(ctx context.Context, req *proto.RebalanceRequest) (*proto.RebalanceResponse, error) {
	// This is a placeholder for rebalancing
	// A full implementation would move data between nodes to balance the load
	log.Printf("Rebalance requested (force=%v)", req.Force)

	return &proto.RebalanceResponse{
		Success:   true,
		KeysMoved: 0,
	}, nil
}

// GetClusterStatus returns the current status of the cluster
func (s *AdminServiceImpl) GetClusterStatus(ctx context.Context, req *proto.ClusterStatusRequest) (*proto.ClusterStatusResponse, error) {
	// Get all nodes from gossip manager
	nodes := s.server.gossipManager.GetNodes()

	// Convert to proto format and count statuses
	var protoNodes []*proto.NodeStatusInfo
	var aliveCount, deadCount int

	for nodeID, nodeInfo := range nodes {
		status := convertNodeStatusToProto(nodeInfo.GetStatus())
		protoNode := &proto.NodeStatusInfo{
			NodeId:   nodeID,
			Address:  nodeInfo.Address,
			Status:   status,
			LastSeen: nodeInfo.GetLastSeen(),
		}
		protoNodes = append(protoNodes, protoNode)

		if nodeInfo.GetStatus() == gossip.NodeAlive {
			aliveCount++
		} else if nodeInfo.GetStatus() == gossip.NodeDead {
			deadCount++
		}
	}

	totalNodes := len(nodes)
	availabilityPercentage := 0.0
	if totalNodes > 0 {
		availabilityPercentage = float64(aliveCount) / float64(totalNodes) * 100.0
	}

	// Get storage statistics
	stats := s.server.storageEngine.Stats()

	return &proto.ClusterStatusResponse{
		Nodes: protoNodes,
		Health: &proto.ClusterHealth{
			TotalNodes:             int32(totalNodes),
			AliveNodes:             int32(aliveCount),
			DeadNodes:              int32(deadCount),
			AvailabilityPercentage: availabilityPercentage,
		},
		Metrics: &proto.ClusterMetrics{
			TotalKeys:     0, // Placeholder - would need to aggregate across nodes
			TotalRequests: stats.ReadCount + stats.WriteCount,
			AvgLatencyMs:  float64(stats.AvgReadLatency.Milliseconds()),
			Qps:           0, // Placeholder - would need to calculate QPS
		},
	}, nil
}

func (s *AdminServiceImpl) GetKeyOwnership(ctx context.Context, req *proto.KeyOwnershipRequest) (*proto.KeyOwnershipResponse, error) {
	count := int(req.Replicas)
	if count <= 0 {
		count = 3
	}
	nodes := s.server.consistentHash.GetNodes(req.Key, count)
	return &proto.KeyOwnershipResponse{
		Key:   req.Key,
		Nodes: nodes,
	}, nil
}

// Helper functions for proto conversion

func convertVectorClockFromProto(protoVC *proto.VectorClock) *consensus.VectorClock {
	if protoVC == nil {
		return consensus.NewVectorClock()
	}

	return consensus.NewVectorClockFromMap(protoVC.Clocks)
}

func convertVectorClockToProto(vc *consensus.VectorClock) *proto.VectorClock {
	if vc == nil {
		return &proto.VectorClock{Clocks: make(map[string]uint64)}
	}

	return &proto.VectorClock{
		Clocks: vc.GetClocks(),
	}
}

func convertNodeInfoFromProto(protoNodeInfo *proto.NodeInfo) *gossip.NodeInfo {
	return &gossip.NodeInfo{
		NodeID:           protoNodeInfo.NodeId,
		Address:          protoNodeInfo.Address,
		HeartbeatCounter: protoNodeInfo.HeartbeatCounter,
		LastSeen:         protoNodeInfo.LastSeen,
		Status:           convertNodeStatusFromProto(protoNodeInfo.Status),
		Joining:          protoNodeInfo.Joining,
		Version:          1,
	}
}

func convertNodeStatusFromProto(status proto.NodeStatus) gossip.NodeStatus {
	switch status {
	case proto.NodeStatus_ALIVE:
		return gossip.NodeAlive
	case proto.NodeStatus_SUSPECT:
		return gossip.NodeSuspect
	case proto.NodeStatus_DEAD:
		return gossip.NodeDead
	default:
		return gossip.NodeAlive
	}
}

func convertNodeInfoToProto(nodeInfo *gossip.NodeInfo) *proto.NodeInfo {
	return &proto.NodeInfo{
		NodeId:           nodeInfo.NodeID,
		Address:          nodeInfo.Address,
		HeartbeatCounter: nodeInfo.GetHeartbeatCounter(),
		LastSeen:         nodeInfo.GetLastSeen(),
		Status:           convertNodeStatusToProto(nodeInfo.GetStatus()),
		Joining:          nodeInfo.IsJoining(),
	}
}

func convertNodeStatusToProto(status gossip.NodeStatus) proto.NodeStatus {
	switch status {
	case gossip.NodeAlive:
		return proto.NodeStatus_ALIVE
	case gossip.NodeSuspect:
		return proto.NodeStatus_SUSPECT
	case gossip.NodeDead:
		return proto.NodeStatus_DEAD
	default:
		return proto.NodeStatus_DEAD
	}
}
