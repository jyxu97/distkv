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
	// Convert request to internal format
	vectorClock := convertVectorClockFromProto(req.VectorClock)
	if vectorClock == nil {
		// Create new vector clock if none provided
		vectorClock = consensus.NewVectorClock()
		vectorClock.Increment(s.server.config.NodeID)
	}

	// Create write request for quorum manager
	writeReq := &replication.WriteRequest{
		Key:         req.Key,
		Value:       req.Value,
		VectorClock: vectorClock,
		Context:     ctx,
	}

	// Perform quorum write
	writeResp, err := s.server.quorumManager.Write(writeReq)
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

	return &proto.GetResponse{
		Value:        readResp.Value,
		Found:        readResp.Found,
		VectorClock:  convertVectorClockToProto(readResp.VectorClock),
		ErrorMessage: "",
		Siblings:     protoSiblings,
		HasConflict:  readResp.HasConflict,
	}, nil
}

// Delete removes a key-value pair using tombstone markers
func (s *DistKVServiceImpl) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	// Create vector clock for this delete operation
	vectorClock := consensus.NewVectorClock()
	vectorClock.Increment(s.server.config.NodeID)

	// Create write request with nil value (tombstone)
	writeReq := &replication.WriteRequest{
		Key:         req.Key,
		Value:       nil, // nil value indicates deletion
		VectorClock: vectorClock,
		Context:     ctx,
	}

	// Perform quorum write
	writeResp, err := s.server.quorumManager.Write(writeReq)
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
		// Create vector clock for this operation
		vectorClock := consensus.NewVectorClock()
		vectorClock.Increment(s.server.config.NodeID)

		// Create individual put request
		putReq := &proto.PutRequest{
			Key:              key,
			Value:            value,
			ConsistencyLevel: req.ConsistencyLevel,
			VectorClock:      convertVectorClockToProto(vectorClock),
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
	// Read directly from local storage engine (bypass quorum manager)
	entry, err := s.server.storageEngine.Get(req.Key)
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

// Replicate handles replication requests from other nodes
func (s *NodeServiceImpl) Replicate(ctx context.Context, req *proto.ReplicateRequest) (*proto.ReplicateResponse, error) {
	// Convert vector clock
	vectorClock := convertVectorClockFromProto(req.VectorClock)

	// Perform local storage operation
	var err error
	if req.IsDelete {
		err = s.server.storageEngine.Delete(req.Key, vectorClock)
	} else {
		err = s.server.storageEngine.Put(req.Key, req.Value, vectorClock)
	}

	if err != nil {
		return &proto.ReplicateResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	// Update vector clock for this node
	vectorClock.Increment(s.server.config.NodeID)

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

	// Add to gossip manager
	s.server.gossipManager.AddNode(nodeID, req.NodeAddress)

	// Add to consistent hash ring
	s.server.consistentHash.AddNode(nodeID)

	// Update replica client with new node address
	s.server.replicaClient.UpdateNodeAddress(nodeID, req.NodeAddress)

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
	nodeInfo := gossip.NewNodeInfo(protoNodeInfo.NodeId, protoNodeInfo.Address)
	// Note: In a real implementation, you'd properly set all fields
	// For now, we'll just set the basic ones
	return nodeInfo
}

func convertNodeInfoToProto(nodeInfo *gossip.NodeInfo) *proto.NodeInfo {
	return &proto.NodeInfo{
		NodeId:           nodeInfo.NodeID,
		Address:          nodeInfo.Address,
		HeartbeatCounter: nodeInfo.GetHeartbeatCounter(),
		LastSeen:         nodeInfo.GetLastSeen(),
		Status:           convertNodeStatusToProto(nodeInfo.GetStatus()),
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
