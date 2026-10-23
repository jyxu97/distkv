// ReplicaClient implementation for inter-node communication
// This handles gRPC communication between nodes for replication operations.

package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"distkv/pkg/consensus"
	"distkv/pkg/partition"
	"distkv/pkg/replication"
	"distkv/pkg/tls"
	"distkv/proto"
)

// ReplicaClient implements the replication.ReplicaClient interface
// It manages gRPC connections to other nodes and handles replica operations
type ReplicaClient struct {
	// connections is a pool of gRPC connections to other nodes
	// Key: nodeID, Value: gRPC connection
	connections map[string]*grpc.ClientConn

	// nodeAddresses maps node IDs to their network addresses
	// Key: nodeID, Value: address (e.g., "192.168.1.10:8080")
	nodeAddresses map[string]string

	// mutex protects concurrent access to the connection maps
	mutex sync.RWMutex

	// connectionTimeout is how long to wait when establishing connections
	connectionTimeout time.Duration

	// tlsConfig holds TLS configuration for inter-node communication
	tlsConfig *tls.Config
}

// NewReplicaClient creates a new ReplicaClient for inter-node communication
func NewReplicaClient() *ReplicaClient {
	return &ReplicaClient{
		connections:       make(map[string]*grpc.ClientConn),
		nodeAddresses:     make(map[string]string),
		connectionTimeout: 5 * time.Second,
		tlsConfig:         nil, // Will be set via SetTLSConfig if TLS is enabled
	}
}

// SetTLSConfig sets the TLS configuration for inter-node communication
func (rc *ReplicaClient) SetTLSConfig(config *tls.Config) {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	rc.tlsConfig = config

	// Close all existing connections so they get recreated with TLS
	for nodeID, conn := range rc.connections {
		go func(nid string, c *grpc.ClientConn) {
			c.Close()
		}(nodeID, conn)
		delete(rc.connections, nodeID)
	}
}

// WriteReplica writes a value to a specific replica node
// This is called during quorum write operations
func (rc *ReplicaClient) WriteReplica(ctx context.Context, nodeID string, key string, value []byte, vectorClock *consensus.VectorClock) (*replication.ReplicaResponse, error) {
	// Get gRPC client for the target node
	client, err := rc.getNodeServiceClient(nodeID)
	if err != nil {
		return &replication.ReplicaResponse{
			NodeID:  nodeID,
			Success: false,
			Error:   fmt.Errorf("failed to connect to node %s: %v", nodeID, err),
		}, err
	}

	// Convert vector clock to proto format
	protoVectorClock := convertVectorClockToProto(vectorClock)

	// Create replication request
	req := &proto.ReplicateRequest{
		Key:         key,
		Value:       value,
		VectorClock: protoVectorClock,
		IsDelete:    value == nil, // nil value means deletion
	}

	// Make the gRPC call with timeout
	resp, err := client.Replicate(ctx, req)
	if err != nil {
		return &replication.ReplicaResponse{
			NodeID:  nodeID,
			Success: false,
			Error:   fmt.Errorf("replication failed to node %s: %v", nodeID, err),
		}, err
	}

	// Convert response
	var responseVectorClock *consensus.VectorClock
	if resp.VectorClock != nil {
		responseVectorClock = convertVectorClockFromProto(resp.VectorClock)
	}

	return &replication.ReplicaResponse{
		NodeID:      nodeID,
		Value:       nil, // Write operations don't return values
		VectorClock: responseVectorClock,
		Success:     resp.Success,
		Error:       parseError(resp.ErrorMessage),
	}, nil
}

// ReadReplica reads a value from a specific replica node
// This is called during quorum read operations
func (rc *ReplicaClient) ReadReplica(ctx context.Context, nodeID string, key string) (*replication.ReplicaResponse, error) {
	log.Printf("ReplicaClient.ReadReplica: attempting to read key '%s' from node %s", key, nodeID)

	// Get NodeService client for the target node (for inter-node communication)
	client, err := rc.getNodeServiceClient(nodeID)
	if err != nil {
		log.Printf("ReplicaClient.ReadReplica: failed to get client for node %s: %v", nodeID, err)
		return &replication.ReplicaResponse{
			NodeID:  nodeID,
			Success: false,
			Error:   fmt.Errorf("failed to connect to node %s: %v", nodeID, err),
		}, err
	}

	// Create local get request (bypasses quorum)
	req := &proto.LocalGetRequest{
		Key: key,
	}

	// Make the gRPC call to LocalGet (avoids infinite recursion)
	log.Printf("ReplicaClient.ReadReplica: making LocalGet gRPC call to node %s", nodeID)
	resp, err := client.LocalGet(ctx, req)
	if err != nil {
		log.Printf("ReplicaClient.ReadReplica: gRPC call to node %s failed: %v", nodeID, err)
		// Return failed response but don't return error - let quorum manager handle it
		return &replication.ReplicaResponse{
			NodeID:  nodeID,
			Success: false,
			Error:   fmt.Errorf("read failed from node %s: %v", nodeID, err),
		}, nil // Return nil error so quorum manager continues with other nodes
	}

	// Convert response
	var responseVectorClock *consensus.VectorClock
	if resp.VectorClock != nil {
		responseVectorClock = convertVectorClockFromProto(resp.VectorClock)
	}

	var value []byte
	if resp.Found {
		value = resp.Value
	}

	return &replication.ReplicaResponse{
		NodeID:      nodeID,
		Value:       value,
		VectorClock: responseVectorClock,
		Success:     resp.Found || resp.ErrorMessage == "", // Success if found or no error
		Error:       parseError(resp.ErrorMessage),
	}, nil
}

// SyncRange implements replication.AntiEntropyPeerClient.
// It sends localHash to the peer via the AntiEntropy RPC and returns
// entries the peer has that may differ from our local state.
func (rc *ReplicaClient) SyncRange(ctx context.Context, nodeID string, localHash string) ([]replication.AntiEntropyEntry, error) {
	client, err := rc.getNodeServiceClient(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to node %s: %v", nodeID, err)
	}

	resp, err := client.AntiEntropy(ctx, &proto.AntiEntropyRequest{
		MerkleTree: &proto.MerkleNode{Hash: localHash},
	})
	if err != nil {
		return nil, fmt.Errorf("AntiEntropy RPC to %s failed: %v", nodeID, err)
	}

	entries := make([]replication.AntiEntropyEntry, 0, len(resp.MissingKeys))
	for _, kv := range resp.MissingKeys {
		entries = append(entries, replication.AntiEntropyEntry{
			Key:         kv.Key,
			Value:       kv.Value,
			VectorClock: convertVectorClockFromProto(kv.VectorClock),
			IsDeleted:   kv.IsDeleted,
		})
	}
	return entries, nil
}

// StreamRange fetches every live entry the peer holds in the given token ranges.
// A joining node uses it to bootstrap the ranges it is taking over.
func (rc *ReplicaClient) StreamRange(ctx context.Context, nodeID string, ranges []partition.TokenRange) ([]replication.AntiEntropyEntry, error) {
	client, err := rc.getNodeServiceClient(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to node %s: %v", nodeID, err)
	}

	protoRanges := make([]*proto.TokenRange, len(ranges))
	for i, r := range ranges {
		protoRanges[i] = &proto.TokenRange{Start: r.Start, End: r.End}
	}

	resp, err := client.StreamRange(ctx, &proto.StreamRangeRequest{Ranges: protoRanges})
	if err != nil {
		return nil, fmt.Errorf("StreamRange RPC to %s failed: %v", nodeID, err)
	}

	entries := make([]replication.AntiEntropyEntry, 0, len(resp.Entries))
	for _, kv := range resp.Entries {
		entries = append(entries, replication.AntiEntropyEntry{
			Key:         kv.Key,
			Value:       kv.Value,
			VectorClock: convertVectorClockFromProto(kv.VectorClock),
		})
	}
	return entries, nil
}

// UpdateNodeAddress updates the network address for a node
// This is called when we discover nodes through gossip or configuration
func (rc *ReplicaClient) UpdateNodeAddress(nodeID, address string) {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	// Update the address mapping
	rc.nodeAddresses[nodeID] = address

	// If we have an existing connection to this node and the address changed,
	// close the old connection so it gets recreated with the new address
	if conn, exists := rc.connections[nodeID]; exists {
		go func() {
			// Close in background to avoid blocking
			conn.Close()
		}()
		delete(rc.connections, nodeID)
	}
}

// Close closes all gRPC connections
func (rc *ReplicaClient) Close() error {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	// Close all connections
	for nodeID, conn := range rc.connections {
		if err := conn.Close(); err != nil {
			// Log error but continue closing other connections
			fmt.Printf("Error closing connection to node %s: %v\n", nodeID, err)
		}
	}

	// Clear the maps
	rc.connections = make(map[string]*grpc.ClientConn)
	rc.nodeAddresses = make(map[string]string)

	return nil
}

// getNodeServiceClient gets a NodeService gRPC client for inter-node operations
func (rc *ReplicaClient) getNodeServiceClient(nodeID string) (proto.NodeServiceClient, error) {
	conn, err := rc.getConnection(nodeID)
	if err != nil {
		return nil, err
	}

	return proto.NewNodeServiceClient(conn), nil
}

// getConnection gets or creates a gRPC connection to a node
func (rc *ReplicaClient) getConnection(nodeID string) (*grpc.ClientConn, error) {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	// Check if we already have a connection
	if conn, exists := rc.connections[nodeID]; exists {
		// Verify the connection is still good
		if conn.GetState().String() != "SHUTDOWN" {
			return conn, nil
		}
		// Connection is shut down, remove it
		delete(rc.connections, nodeID)
	}

	// Get the address for this node
	address, exists := rc.nodeAddresses[nodeID]
	if !exists {
		return nil, fmt.Errorf("no address known for node %s", nodeID)
	}

	// Create new connection
	ctx, cancel := context.WithTimeout(context.Background(), rc.connectionTimeout)
	defer cancel()

	// Determine dial options based on TLS configuration
	var dialOpts []grpc.DialOption
	if rc.tlsConfig != nil && rc.tlsConfig.Enabled {
		// Create client TLS config for inter-node communication
		creds, err := tls.LoadClientCredentials(rc.tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %v", err)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	dialOpts = append(dialOpts, grpc.WithBlock()) // Wait for connection to be established

	conn, err := grpc.DialContext(ctx, address, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s at %s: %v", nodeID, address, err)
	}

	// Cache the connection
	rc.connections[nodeID] = conn

	return conn, nil
}

// parseError converts an error message string to an error (or nil if empty)
func parseError(errorMessage string) error {
	if errorMessage == "" {
		return nil
	}
	return fmt.Errorf("%s", errorMessage)
}

// Note: convertVectorClockToProto and convertVectorClockFromProto functions
// are defined in services.go to avoid duplication
