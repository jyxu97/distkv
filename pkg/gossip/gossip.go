// Package gossip - Main gossip protocol implementation
// This implements the gossip-based failure detection system where nodes
// periodically exchange information about cluster membership and health.
package gossip

import (
	"context"
	"distkv/pkg/errors"
	"distkv/pkg/logging"
	"distkv/pkg/metrics"
	"math/rand"
	"sync"
	"time"

	pb "distkv/proto"
	"google.golang.org/grpc"
)

// Gossip manages the gossip protocol for failure detection in the cluster.
// It maintains a view of all nodes and their health status.
type Gossip struct {
	// localNode is information about this node
	localNode *NodeInfo

	// nodes maps node IDs to their information
	nodes map[string]*NodeInfo

	// config holds gossip protocol configuration
	config *GossipConfig

	// mutex protects concurrent access to the node map
	mutex sync.RWMutex

	// stopChan signals shutdown
	stopChan chan struct{}

	// wg tracks background goroutines
	wg sync.WaitGroup

	// eventCallbacks are called when node status changes
	eventCallbacks []NodeEventCallback

	// started indicates if the gossip protocol is running
	started bool

	// connectionPool manages reusable gRPC connections with health checking
	connectionPool *ConnectionPool

	// logger is the component logger
	logger *logging.Logger

	// metrics collector
	metrics *metrics.MetricsCollector
}

// NodeEvent represents a change in node status.
type NodeEvent struct {
	NodeID    string
	Address   string
	OldStatus NodeStatus
	NewStatus NodeStatus
	Timestamp time.Time
}

// NodeEventCallback is called when a node's status changes.
type NodeEventCallback func(event NodeEvent)

// NewGossip creates a new gossip instance for a node.
func NewGossip(nodeID, address string, config *GossipConfig) *Gossip {
	logger := logging.WithComponent("gossip").
		WithFields(map[string]interface{}{
			"nodeID":  nodeID,
			"address": address,
		})

	if config == nil {
		config = DefaultGossipConfig()
		logger.Info("Using default gossip configuration")
	}

	localNode := NewNodeInfo(nodeID, address)
	logger.Info("Creating new gossip instance")

	// Create connection pool with configuration
	poolConfig := &ConnectionPoolConfig{
		MaxConnections:    100,
		MaxIdleTime:       5 * time.Minute,
		HealthCheckPeriod: 30 * time.Second,
		DialTimeout:       5 * time.Second,
	}

	return &Gossip{
		localNode:      localNode,
		nodes:          make(map[string]*NodeInfo),
		config:         config,
		stopChan:       make(chan struct{}),
		eventCallbacks: make([]NodeEventCallback, 0),
		started:        false,
		connectionPool: NewConnectionPool(poolConfig, logger),
		logger:         logger,
		metrics:        metrics.GetGlobalMetrics(),
	}
}

// Start begins the gossip protocol background processes.
func (g *Gossip) Start() error {
	g.logger.Info("Starting gossip protocol")

	g.mutex.Lock()
	defer g.mutex.Unlock()

	if g.started {
		g.logger.Warn("Gossip protocol already started")
		return errors.New(errors.ErrCodeInternal, "gossip already started")
	}

	// Add local node to the cluster view
	g.nodes[g.localNode.NodeID] = g.localNode.Copy()
	g.metrics.Gossip().TotalNodes.Store(int64(len(g.nodes)))
	g.metrics.Gossip().AliveNodes.Store(1)

	// Start background workers
	g.startBackgroundWorkers()

	g.started = true
	g.logger.Info("Gossip protocol started successfully")
	return nil
}

// Stop shuts down the gossip protocol gracefully.
func (g *Gossip) Stop() error {
	g.logger.Info("Stopping gossip protocol")

	g.mutex.Lock()
	if !g.started {
		g.mutex.Unlock()
		g.logger.Warn("Gossip protocol not started")
		return nil
	}
	g.started = false
	g.mutex.Unlock()

	// Signal shutdown
	g.logger.Debug("Signaling background workers to stop")
	close(g.stopChan)

	// Wait for background workers with timeout
	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		g.logger.Debug("Background workers stopped successfully")
	case <-time.After(10 * time.Second):
		g.logger.Warn("Timeout waiting for background workers to stop")
	}

	// Close connection pool
	g.logger.Debug("Closing connection pool")
	if err := g.connectionPool.Close(); err != nil {
		g.logger.WithError(err).Warn("Error closing connection pool")
	}

	g.logger.Info("Gossip protocol stopped successfully")
	return nil
}

// AddNode adds a new node to the cluster view.
// This is typically called when a node joins the cluster.
func (g *Gossip) AddNode(nodeID, address string) {
	g.addNode(nodeID, address, false)
}

// AddJoiningNode adds a node that is still bootstrapping. It is inserted with
// Joining already set so the node is never briefly visible as readable.
func (g *Gossip) AddJoiningNode(nodeID, address string) {
	g.addNode(nodeID, address, true)
}

func (g *Gossip) addNode(nodeID, address string, joining bool) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if _, exists := g.nodes[nodeID]; !exists {
		nodeInfo := NewNodeInfo(nodeID, address)
		nodeInfo.Joining = joining
		g.nodes[nodeID] = nodeInfo

		// Notify callbacks about new node
		g.notifyNodeEvent(NodeEvent{
			NodeID:    nodeID,
			Address:   address,
			OldStatus: NodeDead, // Conceptually, it didn't exist before
			NewStatus: NodeAlive,
			Timestamp: time.Now(),
		})
	}
}

// SetLocalJoining marks the local node as bootstrapping (or done). Peers pick
// up the change with the next heartbeat, since merges are ordered by heartbeat.
func (g *Gossip) SetLocalJoining(joining bool) {
	g.localNode.SetJoining(joining)

	g.mutex.Lock()
	defer g.mutex.Unlock()
	if localNodeCopy, exists := g.nodes[g.localNode.NodeID]; exists {
		localNodeCopy.SetJoining(joining)
	}
}

// RemoveNode removes a node from the cluster view.
// This is typically called when a node is permanently removed.
func (g *Gossip) RemoveNode(nodeID string) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if nodeInfo, exists := g.nodes[nodeID]; exists {
		oldStatus := nodeInfo.GetStatus()
		delete(g.nodes, nodeID)

		// Notify callbacks about node removal
		g.notifyNodeEvent(NodeEvent{
			NodeID:    nodeID,
			Address:   nodeInfo.Address,
			OldStatus: oldStatus,
			NewStatus: NodeDead,
			Timestamp: time.Now(),
		})
	}
}

// GetNodes returns a snapshot of all known nodes.
func (g *Gossip) GetNodes() map[string]*NodeInfo {
	g.mutex.RLock()
	defer g.mutex.RUnlock()

	nodes := make(map[string]*NodeInfo)
	for nodeID, nodeInfo := range g.nodes {
		nodes[nodeID] = nodeInfo.Copy()
	}

	return nodes
}

// GetAliveNodes returns only the nodes that are currently alive.
func (g *Gossip) GetAliveNodes() []*NodeInfo {
	g.mutex.RLock()
	defer g.mutex.RUnlock()

	aliveNodes := make([]*NodeInfo, 0)
	for _, nodeInfo := range g.nodes {
		if nodeInfo.GetStatus() == NodeAlive {
			aliveNodes = append(aliveNodes, nodeInfo.Copy())
		}
	}

	return aliveNodes
}

// RegisterEventCallback registers a callback for node status changes.
func (g *Gossip) RegisterEventCallback(callback NodeEventCallback) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	g.eventCallbacks = append(g.eventCallbacks, callback)
}

// ProcessGossipMessage processes an incoming gossip message from another node.
// This is where the actual gossip information merging happens.
func (g *Gossip) ProcessGossipMessage(senderID string, nodeUpdates []*NodeInfo) []*NodeInfo {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	responses := make([]*NodeInfo, 0)

	// Process each node update in the message
	for _, update := range nodeUpdates {
		nodeID := update.NodeID

		if existingNode, exists := g.nodes[nodeID]; exists {
			// Node exists, merge information
			oldStatus := existingNode.GetStatus()

			if existingNode.MergeFrom(update) {
				// Information was updated, check for status change
				newStatus := existingNode.GetStatus()

				if oldStatus != newStatus {
					g.notifyNodeEvent(NodeEvent{
						NodeID:    nodeID,
						Address:   existingNode.Address,
						OldStatus: oldStatus,
						NewStatus: newStatus,
						Timestamp: time.Now(),
					})
				}
			}

			// Always include our version in response
			responses = append(responses, existingNode.Copy())
		} else {
			// New node, add it to our view
			g.nodes[nodeID] = update.Copy()

			g.notifyNodeEvent(NodeEvent{
				NodeID:    nodeID,
				Address:   update.Address,
				OldStatus: NodeDead, // Conceptually new
				NewStatus: update.Status,
				Timestamp: time.Now(),
			})

			responses = append(responses, update.Copy())
		}
	}

	// Also send back information about nodes the sender didn't mention
	for nodeID, nodeInfo := range g.nodes {
		found := false
		for _, update := range nodeUpdates {
			if update.NodeID == nodeID {
				found = true
				break
			}
		}

		if !found {
			responses = append(responses, nodeInfo.Copy())
		}
	}

	return responses
}

// startBackgroundWorkers starts the gossip protocol background tasks.
func (g *Gossip) startBackgroundWorkers() {
	// Heartbeat worker - updates local node's heartbeat
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		ticker := time.NewTicker(g.config.HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				g.updateLocalHeartbeat()
			case <-g.stopChan:
				return
			}
		}
	}()

	// Failure detection worker - checks for dead/suspect nodes
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		ticker := time.NewTicker(g.config.SuspectTimeout / 2) // Check twice per timeout period
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				g.checkNodeHealth()
			case <-g.stopChan:
				return
			}
		}
	}()

	// Gossip worker - sends gossip messages to random nodes
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		ticker := time.NewTicker(g.config.GossipInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				g.sendGossipMessages()
			case <-g.stopChan:
				return
			}
		}
	}()
}

// updateLocalHeartbeat increments the local node's heartbeat counter.
func (g *Gossip) updateLocalHeartbeat() {
	g.localNode.UpdateHeartbeat()

	// Update the local node in our cluster view
	g.mutex.Lock()
	if localNodeCopy, exists := g.nodes[g.localNode.NodeID]; exists {
		localNodeCopy.HeartbeatCounter = g.localNode.GetHeartbeatCounter()
		localNodeCopy.LastSeen = g.localNode.GetLastSeen()
		localNodeCopy.Version = g.localNode.GetVersion()
		localNodeCopy.Joining = g.localNode.IsJoining()
	}
	g.mutex.Unlock()
}

// checkNodeHealth examines all nodes and updates their status based on timeouts.
func (g *Gossip) checkNodeHealth() {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	for nodeID, nodeInfo := range g.nodes {
		if nodeID == g.localNode.NodeID {
			continue // Skip local node
		}

		oldStatus := nodeInfo.GetStatus()
		expectedStatus := nodeInfo.IsExpired(g.config.SuspectTimeout, g.config.DeadTimeout)

		if oldStatus != expectedStatus {
			nodeInfo.SetStatus(expectedStatus)

			g.notifyNodeEvent(NodeEvent{
				NodeID:    nodeID,
				Address:   nodeInfo.Address,
				OldStatus: oldStatus,
				NewStatus: expectedStatus,
				Timestamp: time.Now(),
			})
		}
	}
}

// sendGossipMessages sends gossip messages to random nodes.
func (g *Gossip) sendGossipMessages() {
	aliveNodes := g.GetAliveNodes()
	if len(aliveNodes) <= 1 {
		return // Only us or no one else alive
	}

	// Filter out local node
	targetNodes := make([]*NodeInfo, 0)
	for _, node := range aliveNodes {
		if node.NodeID != g.localNode.NodeID {
			targetNodes = append(targetNodes, node)
		}
	}

	if len(targetNodes) == 0 {
		return
	}

	// Select random nodes to gossip with (fanout)
	fanout := g.config.GossipFanout
	if fanout > len(targetNodes) {
		fanout = len(targetNodes)
	}

	// Randomly select nodes to gossip with
	rand.Shuffle(len(targetNodes), func(i, j int) {
		targetNodes[i], targetNodes[j] = targetNodes[j], targetNodes[i]
	})

	selectedNodes := targetNodes[:fanout]

	// Create gossip message with current node information
	gossipMessage := g.createGossipMessage()

	// Send gossip messages concurrently
	for _, targetNode := range selectedNodes {
		go g.sendGossipToNode(targetNode, gossipMessage)
	}
}

// notifyNodeEvent sends node status change events to registered callbacks.
func (g *Gossip) notifyNodeEvent(event NodeEvent) {
	for _, callback := range g.eventCallbacks {
		// Run callback in goroutine to avoid blocking
		go callback(event)
	}
}

// GetLocalNode returns information about the local node.
func (g *Gossip) GetLocalNode() *NodeInfo {
	return g.localNode.Copy()
}

// IsAlive returns true if the gossip protocol is running.
func (g *Gossip) IsAlive() bool {
	g.mutex.RLock()
	defer g.mutex.RUnlock()

	return g.started
}

// createGossipMessage creates a gossip message containing information about all known nodes.
func (g *Gossip) createGossipMessage() *GossipMessageData {
	g.mutex.RLock()
	defer g.mutex.RUnlock()

	nodeUpdates := make([]*NodeInfo, 0, len(g.nodes))
	for _, nodeInfo := range g.nodes {
		nodeUpdates = append(nodeUpdates, nodeInfo.Copy())
	}

	return &GossipMessageData{
		SenderID:    g.localNode.NodeID,
		NodeUpdates: nodeUpdates,
	}
}

// sendGossipToNode sends a gossip message to a specific node and processes the response.
func (g *Gossip) sendGossipToNode(targetNode *NodeInfo, message *GossipMessageData) {
	conn, err := g.getConnection(targetNode.Address)
	if err != nil {
		g.logger.WithError(err).WithFields(map[string]interface{}{
			"targetNode": targetNode.NodeID,
			"address":    targetNode.Address,
		}).Debug("Failed to get connection to node")
		g.metrics.Gossip().MessageErrors.Add(1)
		g.markNodeSuspect(targetNode.NodeID)
		return
	}

	client := pb.NewNodeServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Convert our internal message to protobuf format
	protoMessage := g.convertToProtoMessage(message)

	response, err := client.Gossip(ctx, protoMessage)
	if err != nil {
		g.logger.WithError(err).WithFields(map[string]interface{}{
			"targetNode": targetNode.NodeID,
			"address":    targetNode.Address,
		}).Debug("Failed to send gossip message")
		g.metrics.Gossip().MessageErrors.Add(1)
		g.markNodeSuspect(targetNode.NodeID)
		return
	}

	g.metrics.Gossip().MessagesSent.Add(1)
	g.metrics.Gossip().MessagesReceived.Add(1)

	// Process the response and update our node information
	g.processGossipResponse(targetNode.NodeID, response)
}

// getConnection gets a gRPC connection from the pool.
func (g *Gossip) getConnection(address string) (*grpc.ClientConn, error) {
	return g.connectionPool.GetConnection(address)
}

// markNodeSuspect marks a node as suspect when we can't communicate with it.
func (g *Gossip) markNodeSuspect(nodeID string) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if nodeInfo, exists := g.nodes[nodeID]; exists {
		oldStatus := nodeInfo.GetStatus()
		if oldStatus != NodeSuspect && oldStatus != NodeDead {
			nodeInfo.SetStatus(NodeSuspect)

			g.notifyNodeEvent(NodeEvent{
				NodeID:    nodeID,
				Address:   nodeInfo.Address,
				OldStatus: oldStatus,
				NewStatus: NodeSuspect,
				Timestamp: time.Now(),
			})
		}
	}
}

// GossipMessageData represents the internal format of gossip messages.
type GossipMessageData struct {
	SenderID    string
	NodeUpdates []*NodeInfo
}

// convertToProtoMessage converts internal gossip message to protobuf format.
func (g *Gossip) convertToProtoMessage(message *GossipMessageData) *pb.GossipMessage {
	protoNodeUpdates := make([]*pb.NodeInfo, 0, len(message.NodeUpdates))

	for _, nodeInfo := range message.NodeUpdates {
		protoNodeInfo := &pb.NodeInfo{
			NodeId:           nodeInfo.NodeID,
			Address:          nodeInfo.Address,
			HeartbeatCounter: nodeInfo.GetHeartbeatCounter(),
			LastSeen:         nodeInfo.GetLastSeen(),
			Status:           g.convertStatusToProto(nodeInfo.GetStatus()),
			Joining:          nodeInfo.IsJoining(),
		}
		protoNodeUpdates = append(protoNodeUpdates, protoNodeInfo)
	}

	return &pb.GossipMessage{
		SenderId:    message.SenderID,
		NodeUpdates: protoNodeUpdates,
	}
}

// convertStatusToProto converts internal node status to protobuf enum.
func (g *Gossip) convertStatusToProto(status NodeStatus) pb.NodeStatus {
	switch status {
	case NodeAlive:
		return pb.NodeStatus_ALIVE
	case NodeSuspect:
		return pb.NodeStatus_SUSPECT
	case NodeDead:
		return pb.NodeStatus_DEAD
	default:
		return pb.NodeStatus_ALIVE
	}
}

// processGossipResponse processes a gossip response and updates our node view.
func (g *Gossip) processGossipResponse(senderID string, response *pb.GossipResponse) {
	// Convert protobuf response back to internal format
	nodeUpdates := make([]*NodeInfo, 0, len(response.NodeUpdates))

	for _, protoNodeInfo := range response.NodeUpdates {
		internalStatus := g.convertStatusFromProto(protoNodeInfo.Status)
		nodeInfo := &NodeInfo{
			NodeID:           protoNodeInfo.NodeId,
			Address:          protoNodeInfo.Address,
			HeartbeatCounter: protoNodeInfo.HeartbeatCounter,
			LastSeen:         protoNodeInfo.LastSeen,
			Status:           internalStatus,
			Joining:          protoNodeInfo.Joining,
			Version:          1, // We don't have version in proto, so set to 1
		}
		nodeUpdates = append(nodeUpdates, nodeInfo)
	}

	// Process the updates using existing logic
	g.ProcessGossipMessage(senderID, nodeUpdates)
}

// convertStatusFromProto converts protobuf node status to internal enum.
func (g *Gossip) convertStatusFromProto(status pb.NodeStatus) NodeStatus {
	switch status {
	case pb.NodeStatus_ALIVE:
		return NodeAlive
	case pb.NodeStatus_SUSPECT:
		return NodeSuspect
	case pb.NodeStatus_DEAD:
		return NodeDead
	default:
		return NodeAlive
	}
}
