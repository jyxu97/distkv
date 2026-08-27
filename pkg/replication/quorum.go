// Package replication implements data replication and quorum consensus
// for maintaining consistency across multiple nodes in the distributed system.
package replication

import (
	"context"
	"distkv/pkg/consensus"
	"distkv/pkg/errors"
	"distkv/pkg/logging"
	"distkv/pkg/metrics"
	"distkv/pkg/storage"
	"fmt"
	"sync"
	"time"
)

// QuorumConfig defines the replication parameters for the system.
// These settings control the consistency vs availability trade-offs.
type QuorumConfig struct {
	// N is the total number of replicas for each key
	N int

	// R is the number of replicas that must respond for a read operation
	R int

	// W is the number of replicas that must acknowledge a write operation
	W int

	// RequestTimeout is the maximum time to wait for replica responses
	RequestTimeout time.Duration

	// RetryAttempts is how many times to retry failed operations
	RetryAttempts int

	// RetryDelay is the delay between retry attempts
	RetryDelay time.Duration
}

// DefaultQuorumConfig returns the default configuration from the design document.
// N=3, W=2, R=2 provides strong consistency when W + R > N
func DefaultQuorumConfig() *QuorumConfig {
	return &QuorumConfig{
		N:              3,               // Store 3 copies of each key
		R:              2,               // Read from 2 replicas (majority)
		W:              2,               // Write to 2 replicas (majority)
		RequestTimeout: 5 * time.Second, // 5 second timeout for operations
		RetryAttempts:  3,               // Retry failed operations 3 times
		RetryDelay:     100 * time.Millisecond,
	}
}

// Validate checks if the quorum configuration is valid.
func (qc *QuorumConfig) Validate() error {
	if qc.N <= 0 {
		return fmt.Errorf("N must be positive, got %d", qc.N)
	}

	if qc.R <= 0 || qc.R > qc.N {
		return fmt.Errorf("R must be between 1 and N, got R=%d, N=%d", qc.R, qc.N)
	}

	if qc.W <= 0 || qc.W > qc.N {
		return fmt.Errorf("W must be between 1 and N, got W=%d, N=%d", qc.W, qc.N)
	}

	return nil
}

// IsStrongConsistency returns true if the configuration guarantees strong consistency.
// This happens when W + R > N, ensuring read and write quorums overlap.
func (qc *QuorumConfig) IsStrongConsistency() bool {
	return qc.W+qc.R > qc.N
}

// ReplicaInfo contains information about a replica node.
type ReplicaInfo struct {
	NodeID   string    // Unique identifier for the node
	Address  string    // Network address of the node
	IsAlive  bool      // Whether the node is currently reachable
	LastSeen time.Time // When we last heard from this node
}

// WriteRequest represents a request to write data to replicas.
type WriteRequest struct {
	Key         string                 // The key to write
	Value       []byte                 // The value to store
	VectorClock *consensus.VectorClock // Version information
	Context     context.Context        // Request context for timeouts
}

// WriteResponse represents the response from a write operation.
type WriteResponse struct {
	Success         bool                   // Whether the write succeeded
	VectorClock     *consensus.VectorClock // Updated vector clock
	ReplicasWritten int                    // Number of replicas that acknowledged
	Errors          []error                // Any errors that occurred
}

// ReadRequest represents a request to read data from replicas.
type ReadRequest struct {
	Key     string          // The key to read
	Context context.Context // Request context for timeouts
}

// Sibling represents one concurrent version of a value.
type Sibling struct {
	Value       []byte
	VectorClock *consensus.VectorClock
	NodeID      string
}

// ReadResponse represents the response from a read operation.
type ReadResponse struct {
	Value        []byte                 // The value (nil if not found)
	VectorClock  *consensus.VectorClock // Version information
	Found        bool                   // Whether the key was found
	ReplicasRead int                    // Number of replicas that responded
	Errors       []error                // Any errors that occurred
	Siblings     []Sibling              // All concurrent versions (len > 1 means conflict)
	HasConflict  bool                   // True when multiple concurrent versions exist
}

// ReplicaResponse represents a response from a single replica.
type ReplicaResponse struct {
	NodeID      string                 // Which node responded
	Value       []byte                 // The value from this replica
	VectorClock *consensus.VectorClock // Vector clock from this replica
	Success     bool                   // Whether the operation succeeded
	Error       error                  // Any error that occurred
}

// QuorumManager handles quorum-based operations across replicas.
type QuorumManager struct {
	config        *QuorumConfig
	nodeSelector  NodeSelector  // Selects which nodes to use for a key
	client        ReplicaClient // Communicates with replica nodes
	storageEngine StorageEngine // Real LSM-tree storage engine
	mutex         sync.RWMutex
	logger        *logging.Logger
	metrics       *metrics.MetricsCollector
}

// NodeSelector interface for selecting replica nodes for a given key.
// This will be implemented using consistent hashing.
type NodeSelector interface {
	// GetReplicas returns N nodes that should store replicas of the key
	GetReplicas(key string, count int) []ReplicaInfo

	// GetAliveReplicas returns only the alive nodes from GetReplicas
	GetAliveReplicas(key string, count int) []ReplicaInfo
}

// ReplicaClient interface for communicating with replica nodes.
// This will be implemented using gRPC calls.
type ReplicaClient interface {
	// WriteReplica writes a value to a specific replica node
	WriteReplica(ctx context.Context, nodeID string, key string, value []byte,
		vectorClock *consensus.VectorClock) (*ReplicaResponse, error)

	// ReadReplica reads a value from a specific replica node
	ReadReplica(ctx context.Context, nodeID string, key string) (*ReplicaResponse, error)
}

// StorageEngine interface for the storage layer
type StorageEngine interface {
	Put(key string, value []byte, vectorClock *consensus.VectorClock) error
	Get(key string) (*storage.Entry, error)
	Delete(key string, vectorClock *consensus.VectorClock) error
	Iterator() (storage.Iterator, error)
}

// NewQuorumManager creates a new quorum manager.
func NewQuorumManager(config *QuorumConfig, nodeSelector NodeSelector, client ReplicaClient, storageEngine StorageEngine) (*QuorumManager, error) {
	logger := logging.WithComponent("replication.quorum")

	if config == nil {
		config = DefaultQuorumConfig()
		logger.Info("Using default quorum configuration")
	}

	if err := config.Validate(); err != nil {
		logger.WithError(err).Error("Invalid quorum configuration")
		return nil, errors.Wrap(err, errors.ErrCodeInvalidConfig, "invalid quorum config")
	}

	logger.WithFields(map[string]interface{}{
		"N": config.N,
		"R": config.R,
		"W": config.W,
	}).Info("Creating new quorum manager")

	return &QuorumManager{
		config:        config,
		nodeSelector:  nodeSelector,
		client:        client,
		storageEngine: storageEngine,
		logger:        logger,
		metrics:       metrics.GetGlobalMetrics(),
	}, nil
}

// Write performs a quorum write operation.
// It writes to W replicas and returns success when enough replicas acknowledge.
func (qm *QuorumManager) Write(req *WriteRequest) (*WriteResponse, error) {
	tracker := metrics.NewLatencyTracker()
	defer func() {
		qm.metrics.Replication().QuorumWriteOps.Add(1)
		qm.metrics.Replication().QuorumWriteLatencyNs.Store(tracker.Finish())
	}()

	// Validate input
	if req.Key == "" {
		return nil, errors.New(errors.ErrCodeInvalidKey, "key cannot be empty")
	}

	qm.logger.WithField("key", req.Key).Debug("Starting quorum write")

	// Get replica nodes for this key
	replicas := qm.nodeSelector.GetAliveReplicas(req.Key, qm.config.N)

	qm.logger.WithFields(map[string]interface{}{
		"key":           req.Key,
		"replicaCount":  len(replicas),
		"requiredCount": qm.config.W,
	}).Debug("Found replicas for write operation")

	if len(replicas) < qm.config.W {
		return nil, fmt.Errorf("insufficient alive replicas: need %d, have %d",
			qm.config.W, len(replicas))
	}

	// Create context with timeout
	ctx := req.Context
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), qm.config.RequestTimeout)
		defer cancel()
	}

	// Send write requests to all available replicas concurrently
	responseChan := make(chan *ReplicaResponse, len(replicas))

	for _, replica := range replicas {
		go func(r ReplicaInfo) {
			response, err := qm.client.WriteReplica(ctx, r.NodeID, req.Key, req.Value, req.VectorClock)
			if err != nil {
				responseChan <- &ReplicaResponse{
					NodeID:  r.NodeID,
					Success: false,
					Error:   err,
				}
			} else {
				responseChan <- response
			}
		}(replica)
	}

	// Collect responses until we have enough successful writes
	var successfulWrites int
	var errs []error
	var latestVectorClock *consensus.VectorClock

	for i := 0; i < len(replicas) && successfulWrites < qm.config.W; i++ {
		select {
		case response := <-responseChan:
			if response.Success {
				successfulWrites++
				// Keep track of the most recent vector clock
				if latestVectorClock == nil ||
					(response.VectorClock != nil && response.VectorClock.IsAfter(latestVectorClock)) {
					latestVectorClock = response.VectorClock
				}
			} else {
				errs = append(errs, fmt.Errorf("node %s: %v", response.NodeID, response.Error))
			}
		case <-ctx.Done():
			return &WriteResponse{
				Success:         false,
				ReplicasWritten: successfulWrites,
				Errors:          append(errs, ctx.Err()),
			}, ctx.Err()
		}
	}

	// Return success if we got enough acknowledgments
	if successfulWrites >= qm.config.W {
		qm.metrics.Replication().QuorumWriteSuccess.Add(1)
		qm.logger.WithFields(map[string]interface{}{
			"key":              req.Key,
			"successfulWrites": successfulWrites,
			"required":         qm.config.W,
		}).Debug("Quorum write succeeded")
		return &WriteResponse{
			Success:         true,
			VectorClock:     latestVectorClock,
			ReplicasWritten: successfulWrites,
			Errors:          errs,
		}, nil
	}

	qm.metrics.Replication().QuorumWriteFailed.Add(1)
	qm.logger.WithFields(map[string]interface{}{
		"key":              req.Key,
		"successfulWrites": successfulWrites,
		"required":         qm.config.W,
	}).Warn("Quorum write failed")
	return &WriteResponse{
		Success:         false,
		ReplicasWritten: successfulWrites,
		Errors:          errs,
	}, errors.NewQuorumFailedError(qm.config.W, successfulWrites)
}

// Read performs a quorum read operation.
// It reads from R replicas and resolves conflicts using vector clocks.
func (qm *QuorumManager) Read(req *ReadRequest) (*ReadResponse, error) {
	tracker := metrics.NewLatencyTracker()
	defer func() {
		qm.metrics.Replication().QuorumReadOps.Add(1)
		qm.metrics.Replication().QuorumReadLatencyNs.Store(tracker.Finish())
	}()

	// Validate input
	if req.Key == "" {
		return nil, errors.New(errors.ErrCodeInvalidKey, "key cannot be empty")
	}

	qm.logger.WithField("key", req.Key).Debug("Starting quorum read")

	// Get replica nodes for this key
	replicas := qm.nodeSelector.GetAliveReplicas(req.Key, qm.config.N)

	qm.logger.WithFields(map[string]interface{}{
		"key":           req.Key,
		"replicaCount":  len(replicas),
		"requiredCount": qm.config.R,
	}).Debug("Found replicas for read operation")

	if len(replicas) < qm.config.R {
		return nil, fmt.Errorf("insufficient alive replicas: need %d, have %d",
			qm.config.R, len(replicas))
	}

	// Create context with timeout
	ctx := req.Context
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), qm.config.RequestTimeout)
		defer cancel()
	}

	// Send read requests to all available replicas concurrently; return as soon
	// as R succeed. This matches Dynamo's approach: we don't know in advance
	// which replicas will respond, so we fan out to all and take the first R.
	responseChan := make(chan *ReplicaResponse, len(replicas))

	for _, r := range replicas {
		go func(r ReplicaInfo) {
			response, err := qm.client.ReadReplica(ctx, r.NodeID, req.Key)
			if err != nil {
				responseChan <- &ReplicaResponse{
					NodeID:  r.NodeID,
					Success: false,
					Error:   err,
				}
			} else {
				responseChan <- response
			}
		}(r)
	}

	// Collect until R successes or all replicas have responded.
	var responses []*ReplicaResponse
	var errs []error

	for i := 0; i < len(replicas); i++ {
		select {
		case response := <-responseChan:
			if response.Success {
				responses = append(responses, response)
				if len(responses) >= qm.config.R {
					goto quorumMet
				}
			} else {
				errs = append(errs, fmt.Errorf("node %s: %v", response.NodeID, response.Error))
			}
		case <-ctx.Done():
			return &ReadResponse{
				Found:        false,
				ReplicasRead: len(responses),
				Errors:       append(errs, ctx.Err()),
			}, ctx.Err()
		}
	}
quorumMet:

	// Check if we got enough responses
	if len(responses) < qm.config.R {
		qm.metrics.Replication().QuorumReadFailed.Add(1)
		qm.logger.WithFields(map[string]interface{}{
			"key":       req.Key,
			"responses": len(responses),
			"required":  qm.config.R,
		}).Warn("Quorum read failed")
		return &ReadResponse{
			Found:        false,
			ReplicasRead: len(responses),
			Errors:       errs,
		}, errors.NewQuorumFailedError(qm.config.R, len(responses))
	}

	qm.metrics.Replication().QuorumReadSuccess.Add(1)
	qm.logger.WithFields(map[string]interface{}{
		"key":       req.Key,
		"responses": len(responses),
		"required":  qm.config.R,
	}).Debug("Quorum read succeeded")

	// Resolve conflicts and find the most recent version
	result := qm.resolveReadConflicts(responses, errs)

	// Trigger async read repair for any stale replicas when there's a clear winner.
	// If versions are concurrent (siblings), there's no single correct value to push.
	if !result.HasConflict && result.Found && result.VectorClock != nil {
		go qm.repairStaleReplicas(req.Key, result.Value, result.VectorClock, responses)
	}

	return result, nil
}

// resolveReadConflicts resolves conflicts among replica responses using vector clocks.
// When concurrent versions are detected (no causal ordering), all versions are preserved
// as siblings (Dynamo-style) and returned to the client for application-level resolution.
func (qm *QuorumManager) resolveReadConflicts(responses []*ReplicaResponse, errors []error) *ReadResponse {
	if len(responses) == 0 {
		return &ReadResponse{
			Found:        false,
			ReplicasRead: 0,
			Errors:       errors,
		}
	}

	// Build list of winning versions by filtering out causally dominated ones.
	// Start with all responses as candidates, then remove any that are strictly
	// preceded by another.
	type candidate struct {
		resp *ReplicaResponse
	}
	candidates := make([]candidate, 0, len(responses))

	for _, response := range responses {
		dominated := false
		newCandidates := candidates[:0]

		for _, c := range candidates {
			// Check if existing candidate dominates the new response
			if c.resp.VectorClock != nil && response.VectorClock != nil {
				if c.resp.VectorClock.IsAfter(response.VectorClock) {
					dominated = true
					newCandidates = append(newCandidates, c)
					continue
				}
				if response.VectorClock.IsAfter(c.resp.VectorClock) {
					// New response dominates existing candidate — drop the candidate
					continue
				}
				if c.resp.VectorClock.Equal(response.VectorClock) {
					if response.Value == nil && c.resp.Value != nil {
						// New response is a tombstone at the same VC — tombstone supersedes
						// the value (a delete always wins over an equal-version write).
						// Drop the existing value candidate; response will be added below.
						continue
					}
					// Same version on multiple replicas — not a conflict, just replication
					dominated = true
					newCandidates = append(newCandidates, c)
					continue
				}
			} else if response.VectorClock != nil && c.resp.VectorClock == nil {
				// New response has a clock, old doesn't — new dominates
				continue
			} else if response.VectorClock == nil && c.resp.VectorClock != nil {
				// Old has a clock, new doesn't — old dominates
				dominated = true
				newCandidates = append(newCandidates, c)
				continue
			}
			// Genuinely concurrent — keep both as siblings
			newCandidates = append(newCandidates, c)
		}

		candidates = newCandidates
		if !dominated {
			candidates = append(candidates, candidate{resp: response})
		}
	}

	// Build siblings from surviving candidates
	siblings := make([]Sibling, 0, len(candidates))
	for _, c := range candidates {
		siblings = append(siblings, Sibling{
			Value:       c.resp.Value,
			VectorClock: c.resp.VectorClock,
			NodeID:      c.resp.NodeID,
		})
	}

	hasConflict := len(siblings) > 1
	first := candidates[0].resp

	return &ReadResponse{
		Value:        first.Value,
		VectorClock:  first.VectorClock,
		Found:        first.Value != nil,
		ReplicasRead: len(responses),
		Errors:       errors,
		Siblings:     siblings,
		HasConflict:  hasConflict,
	}
}

// repairStaleReplicas asynchronously pushes the winning version to any replicas
// that returned a causally older version during a quorum read (read repair).
func (qm *QuorumManager) repairStaleReplicas(key string, winningValue []byte, winningVC *consensus.VectorClock, responses []*ReplicaResponse) {
	ctx, cancel := context.WithTimeout(context.Background(), qm.config.RequestTimeout)
	defer cancel()

	for _, resp := range responses {
		if resp.VectorClock == nil || !winningVC.IsAfter(resp.VectorClock) {
			continue // Already up to date or ordering is indeterminate
		}
		qm.logger.WithFields(map[string]interface{}{
			"key":    key,
			"nodeID": resp.NodeID,
		}).Debug("Read repair: pushing newer version to stale replica")

		if _, err := qm.client.WriteReplica(ctx, resp.NodeID, key, winningValue, winningVC); err != nil {
			qm.logger.WithFields(map[string]interface{}{
				"key":    key,
				"nodeID": resp.NodeID,
			}).WithError(err).Warn("Read repair write failed")
		}
	}
}

// GetConfig returns the current quorum configuration.
func (qm *QuorumManager) GetConfig() *QuorumConfig {
	qm.mutex.RLock()
	defer qm.mutex.RUnlock()

	configCopy := *qm.config
	return &configCopy
}
