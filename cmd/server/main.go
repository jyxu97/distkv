// DistKV Server - Main entry point for the distributed key-value store server
// This starts a DistKV node that can participate in the distributed cluster.
//
// The server provides both client-facing API and internal node communication.
// It integrates all the components: storage engine, replication, gossip, etc.

package main

import (
	"context"
	cryptotls "crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"distkv/pkg/consensus"
	"distkv/pkg/gossip"
	"distkv/pkg/logging"
	"distkv/pkg/metrics"
	"distkv/pkg/partition"
	"distkv/pkg/replication"
	"distkv/pkg/storage"
	"distkv/pkg/tls"
	"distkv/proto"
)

// ServerConfig holds all configuration for the DistKV server
type ServerConfig struct {
	// Server identification
	NodeID           string // Unique identifier for this node
	Address          string // Bind address (e.g., "0.0.0.0:8080")
	AdvertiseAddress string // Address advertised to peers; defaults to Address
	DataDir          string // Directory to store data files

	// Cluster configuration
	SeedNodes    []string // List of seed nodes to join the cluster
	VirtualNodes int      // Number of virtual nodes for consistent hashing

	// Storage configuration
	StorageConfig *storage.StorageConfig

	// Replication configuration
	QuorumConfig *replication.QuorumConfig

	// Gossip configuration
	GossipConfig *gossip.GossipConfig

	// TLS configuration
	TLSConfig *tls.Config
}

// DistKVServer implements the main server logic
type DistKVServer struct {
	// Core configuration
	config *ServerConfig

	// Storage layer
	storageEngine *storage.Engine

	// Partitioning and replication
	consistentHash *partition.ConsistentHash
	quorumManager  *replication.QuorumManager

	// Failure detection
	gossipManager *gossip.Gossip

	// gRPC server
	grpcServer *grpc.Server

	// Node management
	nodeSelector  *NodeSelector
	replicaClient *ReplicaClient

	// Background repair
	antiEntropy *replication.AntiEntropyManager

	// dotCounter issues this node's entry in the vector clocks of writes it
	// coordinates; persisted so values are never reused across restarts
	dotCounter *consensus.DotCounter

	// Bootstrap of a node joining an existing cluster. rangeStreamer is the
	// replica client; tests replace it to inject streaming faults.
	rangeStreamer   rangeStreamer
	bootstrapTiming bootstrapTiming
	bootstrapDone   chan struct{} // Closed once the node serves reads

	stopCh   chan struct{} // Closed by Stop to end background work
	stopOnce sync.Once
}

// rangeStreamer fetches the entries a peer holds in the given token ranges.
type rangeStreamer interface {
	StreamRange(ctx context.Context, nodeID string, ranges []partition.TokenRange) ([]replication.AntiEntropyEntry, error)
}

// bootstrapTiming bounds each bootstrap attempt and the backoff between attempts.
type bootstrapTiming struct {
	PeerTimeout time.Duration // Limit for streaming from one peer
	RetryBase   time.Duration // Wait after the first failed attempt
	RetryMax    time.Duration // Cap on the exponential backoff
}

// defaultBootstrapTiming is used outside tests.
var defaultBootstrapTiming = bootstrapTiming{
	PeerTimeout: 60 * time.Second,
	RetryBase:   1 * time.Second,
	RetryMax:    30 * time.Second,
}

// main is the entry point for the DistKV server
func main() {
	// Initialize logging system
	logging.InitGlobalLogger(&logging.LogConfig{
		Level:      logging.INFO,
		Component:  "distkv.server",
		Output:     os.Stdout,
		TimeFormat: "2006-01-02 15:04:05.000",
	})
	logger := logging.GetGlobalLogger()

	// Initialize metrics
	_ = metrics.GetGlobalMetrics()

	logger.Info("DistKV server starting")

	// Parse command line flags
	config, err := parseFlags(flag.CommandLine, os.Args[1:])
	if err != nil {
		logger.WithError(err).Fatal("Invalid command line flags")
	}

	// Validate configuration
	if err := validateConfig(config); err != nil {
		logger.WithError(err).Fatal("Invalid configuration")
	}

	// Create and start the server
	server, err := NewDistKVServer(config)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create server")
	}

	// Start the server
	if err := server.Start(); err != nil {
		logger.WithError(err).Fatal("Failed to start server")
	}

	// Wait for shutdown signal
	waitForShutdown()

	// Graceful shutdown with timeout
	logger.Info("Initiating graceful shutdown")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer shutdownCancel()

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- server.Stop()
	}()

	select {
	case err := <-shutdownDone:
		if err != nil {
			logger.WithError(err).Error("Error during shutdown")
		} else {
			logger.Info("DistKV server shut down successfully")
		}
	case <-shutdownCtx.Done():
		logger.Error("Shutdown timeout exceeded, forcing exit")
	}
}

// parseFlags parses command line arguments and returns server configuration.
// The flag set is a parameter so tests can parse without touching flag.CommandLine.
func parseFlags(fs *flag.FlagSet, args []string) (*ServerConfig, error) {
	var (
		nodeID           = fs.String("node-id", "", "Unique node identifier (required)")
		address          = fs.String("address", "localhost:8080", "Server listen address")
		advertiseAddress = fs.String("advertise-address", "", "Address advertised to cluster peers (defaults to --address)")
		dataDir          = fs.String("data-dir", "./data", "Directory for data storage")
		seedNodes        = fs.String("seed-nodes", "", "Comma-separated list of seed nodes")
		virtualNodes     = fs.Int("virtual-nodes", 150, "Number of virtual nodes for consistent hashing")

		// Storage flags
		memTableSize     = fs.Int("mem-table-size", 1024, "MemTable size in bytes")
		ssTableSize      = fs.Int64("sstable-size", 256*1024*1024, "SSTable size in bytes")
		bloomFilterBits  = fs.Int("bloom-filter-bits", 10, "Bloom filter bits per key")
		compactionThresh = fs.Int("compaction-threshold", 4, "Number of SSTables to trigger compaction")
		walEnabled       = fs.Bool("wal-enabled", true, "Write a write-ahead log before each MemTable insert")
		walSync          = fs.Bool("wal-sync", true, "fsync the WAL before acknowledging each write (false risks losing recent writes on power loss)")

		// Replication flags
		replicas    = fs.Int("replicas", 3, "Number of replicas (N)")
		readQuorum  = fs.Int("read-quorum", 2, "Read quorum size (R)")
		writeQuorum = fs.Int("write-quorum", 2, "Write quorum size (W)")

		// Gossip flags
		heartbeatInterval = fs.Duration("heartbeat-interval", 1*time.Second, "Heartbeat interval")
		suspectTimeout    = fs.Duration("suspect-timeout", 30*time.Second, "Suspect timeout")
		deadTimeout       = fs.Duration("dead-timeout", 120*time.Second, "Dead timeout")
		gossipInterval    = fs.Duration("gossip-interval", 1*time.Second, "Gossip interval")
		gossipFanout      = fs.Int("gossip-fanout", 3, "Gossip fanout")

		// TLS flags
		tlsEnabled    = fs.Bool("tls-enabled", false, "Enable TLS for secure communication")
		tlsCertFile   = fs.String("tls-cert-file", "", "Path to TLS certificate file")
		tlsKeyFile    = fs.String("tls-key-file", "", "Path to TLS private key file")
		tlsCAFile     = fs.String("tls-ca-file", "", "Path to TLS CA certificate file for client verification")
		tlsClientAuth = fs.String("tls-client-auth", "NoClientCert", "Client authentication policy: NoClientCert, RequestClientCert, RequireAnyClientCert, VerifyClientCertIfGiven, RequireAndVerifyClientCert")
	)

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	// Generate node ID if not provided
	if *nodeID == "" {
		*nodeID = generateNodeID(*address)
	}

	// Default advertise address to bind address
	if *advertiseAddress == "" {
		*advertiseAddress = *address
	}

	// Parse seed nodes
	var seedNodesList []string
	if *seedNodes != "" {
		seedNodesList = strings.Split(*seedNodes, ",")
		for i, node := range seedNodesList {
			seedNodesList[i] = strings.TrimSpace(node)
		}
	}

	// Parse TLS client auth type
	var clientAuthType cryptotls.ClientAuthType
	switch *tlsClientAuth {
	case "NoClientCert":
		clientAuthType = cryptotls.NoClientCert
	case "RequestClientCert":
		clientAuthType = cryptotls.RequestClientCert
	case "RequireAnyClientCert":
		clientAuthType = cryptotls.RequireAnyClientCert
	case "VerifyClientCertIfGiven":
		clientAuthType = cryptotls.VerifyClientCertIfGiven
	case "RequireAndVerifyClientCert":
		clientAuthType = cryptotls.RequireAndVerifyClientCert
	default:
		clientAuthType = cryptotls.NoClientCert
	}

	config := &ServerConfig{
		NodeID:           *nodeID,
		Address:          *address,
		AdvertiseAddress: *advertiseAddress,
		DataDir:          *dataDir,
		SeedNodes:        seedNodesList,
		VirtualNodes:     *virtualNodes,

		StorageConfig: &storage.StorageConfig{
			MemTableMaxSize:     *memTableSize,
			MaxMemTables:        2,
			SSTableMaxSize:      *ssTableSize,
			BloomFilterBits:     *bloomFilterBits,
			CompressionEnabled:  true,
			CompactionThreshold: *compactionThresh,
			MaxCompactionSize:   1024 * 1024 * 1024, // 1GB
			TombstoneTTL:        3 * time.Hour,
			GCInterval:          1 * time.Hour,
			WriteBufferSize:     4 * 1024 * 1024,
			CacheSize:           128 * 1024 * 1024,
			MaxOpenFiles:        1000,
			WALEnabled:          *walEnabled,
			WALSyncOnPut:        *walSync,
		},

		QuorumConfig: &replication.QuorumConfig{
			N:              *replicas,
			R:              *readQuorum,
			W:              *writeQuorum,
			RequestTimeout: 5 * time.Second,
			RetryAttempts:  3,
			RetryDelay:     100 * time.Millisecond,
		},

		GossipConfig: &gossip.GossipConfig{
			HeartbeatInterval:   *heartbeatInterval,
			SuspectTimeout:      *suspectTimeout,
			DeadTimeout:         *deadTimeout,
			GossipInterval:      *gossipInterval,
			GossipFanout:        *gossipFanout,
			MaxGossipPacketSize: 64 * 1024,
		},

		TLSConfig: &tls.Config{
			Enabled:    *tlsEnabled,
			CertFile:   *tlsCertFile,
			KeyFile:    *tlsKeyFile,
			CAFile:     *tlsCAFile,
			ClientAuth: clientAuthType,
		},
	}
	return config, nil
}

// validateConfig validates the server configuration
func validateConfig(config *ServerConfig) error {
	if config.NodeID == "" {
		return fmt.Errorf("node-id is required")
	}

	if config.Address == "" {
		return fmt.Errorf("address is required")
	}

	if config.DataDir == "" {
		return fmt.Errorf("data-dir is required")
	}

	// Validate quorum configuration
	if err := config.QuorumConfig.Validate(); err != nil {
		return fmt.Errorf("invalid quorum config: %v", err)
	}

	// Validate TLS configuration
	if err := tls.ValidateConfig(config.TLSConfig); err != nil {
		return fmt.Errorf("invalid TLS config: %v", err)
	}

	return nil
}

// generateNodeID generates a node ID based on the address if not provided
func generateNodeID(address string) string {
	// Use address as base and add timestamp for uniqueness
	timestamp := time.Now().Unix()
	return fmt.Sprintf("node-%s-%d", strings.ReplaceAll(address, ":", "-"), timestamp)
}

// NewDistKVServer creates a new DistKV server instance
func NewDistKVServer(config *ServerConfig) (*DistKVServer, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	// Initialize storage engine
	storageDir := filepath.Join(config.DataDir, "storage")
	storageEngine, err := storage.NewEngine(storageDir, config.StorageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage engine: %v", err)
	}

	// Open the version counter used to stamp writes this node coordinates
	dotCounter, err := consensus.OpenDotCounter(filepath.Join(config.DataDir, "version_counter"), consensus.DefaultDotReservation)
	if err != nil {
		return nil, fmt.Errorf("failed to open version counter: %v", err)
	}

	// Initialize consistent hashing
	consistentHash := partition.NewConsistentHash(config.VirtualNodes)

	// Initialize gossip manager (use AdvertiseAddress so peers can reach us)
	gossipManager := gossip.NewGossip(config.NodeID, config.AdvertiseAddress, config.GossipConfig)

	// Create node selector and replica client
	nodeSelector := NewNodeSelector(consistentHash, gossipManager)
	replicaClient := NewReplicaClient()

	// Set TLS configuration on replica client if enabled
	if config.TLSConfig.Enabled {
		replicaClient.SetTLSConfig(config.TLSConfig)
	}

	// Initialize quorum manager with storage engine
	quorumManager, err := replication.NewQuorumManager(config.QuorumConfig, nodeSelector, replicaClient, storageEngine)
	if err != nil {
		return nil, fmt.Errorf("failed to create quorum manager: %v", err)
	}

	// Initialize anti-entropy manager (30-second sync interval)
	antiEntropy := replication.NewAntiEntropyManager(
		config.NodeID,
		storageEngine,
		&gossipPeerProvider{gossipManager},
		replicaClient,
		30*time.Second,
	)

	return &DistKVServer{
		config:          config,
		storageEngine:   storageEngine,
		consistentHash:  consistentHash,
		quorumManager:   quorumManager,
		gossipManager:   gossipManager,
		nodeSelector:    nodeSelector,
		replicaClient:   replicaClient,
		antiEntropy:     antiEntropy,
		dotCounter:      dotCounter,
		rangeStreamer:   replicaClient,
		bootstrapTiming: defaultBootstrapTiming,
		bootstrapDone:   make(chan struct{}),
		stopCh:          make(chan struct{}),
	}, nil
}

// gossipPeerProvider adapts gossip.Gossip to replication.PeerProvider.
type gossipPeerProvider struct {
	g *gossip.Gossip
}

func (gp *gossipPeerProvider) GetAlivePeers() []replication.PeerInfo {
	nodes := gp.g.GetAliveNodes()
	peers := make([]replication.PeerInfo, len(nodes))
	for i, n := range nodes {
		peers[i] = replication.PeerInfo{NodeID: n.NodeID}
	}
	return peers
}

// Start starts the DistKV server
func (s *DistKVServer) Start() error {
	logger := logging.WithComponent("server.start").
		WithFields(map[string]interface{}{
			"nodeID":  s.config.NodeID,
			"address": s.config.Address,
		})
	logger.Info("Starting DistKV server")

	// Keep the hash ring and replica client in sync with gossip membership.
	// OldStatus == NodeDead is the sentinel meaning "this node was not known before"
	// (used by both AddNode and ProcessGossipMessage when a node is first discovered).
	// ConsistentHash.AddNode and replicaClient.UpdateNodeAddress are both idempotent,
	// so firing this for the local node (which joinCluster also handles) is harmless.
	s.gossipManager.RegisterEventCallback(func(event gossip.NodeEvent) {
		if event.OldStatus == gossip.NodeDead && event.NodeID != s.config.NodeID {
			s.consistentHash.AddNode(event.NodeID)
			s.replicaClient.UpdateNodeAddress(event.NodeID, event.Address)
			logger.WithFields(map[string]interface{}{
				"nodeID":  event.NodeID,
				"address": event.Address,
			}).Info("Gossip: new node discovered, added to hash ring")
		}
	})

	// A node joining an existing cluster must not serve reads for its new key
	// ranges until it has streamed their data. Mark it joining before gossip
	// starts so peers never see it as readable.
	joining := len(s.config.SeedNodes) > 0
	if joining {
		s.gossipManager.SetLocalJoining(true)
	}

	// Start gossip protocol
	if err := s.gossipManager.Start(); err != nil {
		logger.WithError(err).Error("Failed to start gossip manager")
		return fmt.Errorf("failed to start gossip manager: %v", err)
	}

	// Add self to consistent hash ring
	s.consistentHash.AddNode(s.config.NodeID)

	// Add self to gossip manager with the advertise address
	s.gossipManager.AddNode(s.config.NodeID, s.config.AdvertiseAddress)

	// Start gRPC server before announcing to the cluster so that peers can
	// immediately reach us via gossip once we announce ourselves.
	if err := s.startGRPCServer(); err != nil {
		logger.WithError(err).Error("Failed to start gRPC server")
		return fmt.Errorf("failed to start gRPC server: %v", err)
	}

	if joining {
		// Join and bootstrap in the background. The node accepts requests as a
		// coordinator meanwhile, but stays excluded from reads until bootstrap
		// succeeds; if it never does, the node stays joining rather than
		// serving reads from incomplete data.
		go s.runBootstrap()
	} else {
		if err := s.joinCluster(); err != nil {
			return fmt.Errorf("failed to initialize single-node cluster: %v", err)
		}
		close(s.bootstrapDone)
	}

	// Start anti-entropy background repair
	s.antiEntropy.Start()

	logger.Info("DistKV server started successfully")
	return nil
}

// Stop stops the DistKV server gracefully
func (s *DistKVServer) Stop() error {
	logger := logging.WithComponent("server.shutdown").
		WithField("nodeID", s.config.NodeID)
	logger.Info("Initiating graceful shutdown")

	var shutdownErrors []error

	// End the bootstrap loop if it is still running
	s.stopOnce.Do(func() { close(s.stopCh) })

	// Stop anti-entropy background repair
	if s.antiEntropy != nil {
		s.antiEntropy.Stop()
	}

	// Stop accepting new requests - stop gRPC server
	if s.grpcServer != nil {
		logger.Info("Stopping gRPC server")
		done := make(chan struct{})
		go func() {
			s.grpcServer.GracefulStop()
			close(done)
		}()

		select {
		case <-done:
			logger.Info("gRPC server stopped successfully")
		case <-time.After(30 * time.Second):
			logger.Warn("gRPC graceful stop timeout, forcing shutdown")
			s.grpcServer.Stop()
		}
	}

	// Stop gossip manager
	if s.gossipManager != nil {
		logger.Info("Stopping gossip manager")
		if err := s.gossipManager.Stop(); err != nil {
			logger.WithError(err).Error("Error stopping gossip manager")
			shutdownErrors = append(shutdownErrors, err)
		}
	}

	// Close replica client connections
	if s.replicaClient != nil {
		logger.Info("Closing replica client connections")
		// Add Close method call if needed
	}

	// Close storage engine (flush remaining data)
	if s.storageEngine != nil {
		logger.Info("Closing storage engine")
		if err := s.storageEngine.Close(); err != nil {
			logger.WithError(err).Error("Error closing storage engine")
			shutdownErrors = append(shutdownErrors, err)
		}
	}

	// Print final metrics snapshot
	metricsSnapshot := metrics.GetGlobalMetrics().Snapshot()
	logger.WithFields(map[string]interface{}{
		"uptimeSeconds":   metricsSnapshot.System.UptimeSeconds,
		"totalReads":      metricsSnapshot.Storage.ReadOps,
		"totalWrites":     metricsSnapshot.Storage.WriteOps,
		"sstableCount":    metricsSnapshot.Storage.SSTableCount,
		"compactionCount": metricsSnapshot.Storage.CompactionCount,
	}).Info("Final metrics snapshot")

	if len(shutdownErrors) > 0 {
		logger.WithField("errorCount", len(shutdownErrors)).
			Warn("Server shutdown completed with errors")
		return fmt.Errorf("shutdown completed with %d errors", len(shutdownErrors))
	}

	logger.Info("Server shutdown completed successfully")
	return nil
}

// joinCluster attempts to join the cluster by contacting seed nodes. It fails
// if no seed is reachable or if any node a seed reports alive could not be
// told about us: such a node would keep coordinating writes without sending
// them to us, so data we stream from its replicas could miss them.
func (s *DistKVServer) joinCluster() error {
	logger := logging.WithComponent("server.cluster")

	if len(s.config.SeedNodes) == 0 {
		logger.Info("No seed nodes specified, starting as single-node cluster")
		// Still add self to hash ring for single-node operation
		s.replicaClient.UpdateNodeAddress(s.config.NodeID, s.config.Address)
		return nil
	}

	logger.WithField("seedNodes", s.config.SeedNodes).
		Info("Attempting to join cluster via seed nodes")

	// Try to contact each seed node to get cluster membership
	var members []*proto.NodeStatusInfo
	var lastErr error
	for _, seedAddress := range s.config.SeedNodes {
		nodes, err := s.contactSeedNode(seedAddress)
		if err != nil {
			lastErr = err
			logger.WithError(err).WithField("seedAddress", seedAddress).
				Warn("Failed to contact seed node")
			continue
		}
		members = append(members, nodes...)
	}
	if len(members) == 0 {
		return fmt.Errorf("no seed node reachable: %v", lastErr)
	}

	// Add discovered nodes to our systems
	for _, member := range members {
		if member.NodeId == s.config.NodeID {
			continue
		}

		// Add to gossip manager
		s.gossipManager.AddNode(member.NodeId, member.Address)

		// Add to consistent hash ring
		s.consistentHash.AddNode(member.NodeId)

		// Update replica client with node address
		s.replicaClient.UpdateNodeAddress(member.NodeId, member.Address)

		logger.WithFields(map[string]interface{}{
			"nodeID":  member.NodeId,
			"address": member.Address,
		}).Debug("Discovered and added node")
	}

	// Also add self to replica client for local operations
	s.replicaClient.UpdateNodeAddress(s.config.NodeID, s.config.AdvertiseAddress)

	// Announce ourselves to every live member, not just the seeds. Each peer
	// adds us to its ring before replying, so once every announce succeeds,
	// their writes already include us and we can snapshot their data.
	announced := make(map[string]bool)
	var unannounced []string
	for _, member := range members {
		if member.NodeId == s.config.NodeID || member.Status != proto.NodeStatus_ALIVE || announced[member.Address] {
			continue
		}
		announced[member.Address] = true
		if err := s.announceSelfToNode(member.Address); err != nil {
			unannounced = append(unannounced, member.NodeId)
			logger.WithError(err).WithField("nodeID", member.NodeId).
				Warn("Failed to announce to node")
		}
	}
	// Seed addresses may be aliases of members already announced; best effort
	for _, seedAddress := range s.config.SeedNodes {
		if !announced[seedAddress] {
			announced[seedAddress] = true
			_ = s.announceSelfToNode(seedAddress)
		}
	}
	if len(unannounced) > 0 {
		return fmt.Errorf("could not announce to live nodes %v", unannounced)
	}

	logger.WithField("nodeCount", len(announced)+1).
		Info("Successfully joined cluster")
	return nil
}

// runBootstrap joins the cluster and streams this node's ranges, retrying
// with capped exponential backoff until an attempt fully succeeds or the
// server stops. Only a successful attempt clears the joining flag; until then
// the node receives writes for its ranges but serves no reads.
func (s *DistKVServer) runBootstrap() {
	logger := logging.WithComponent("server.bootstrap")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-s.stopCh
		cancel()
	}()

	backoff := s.bootstrapTiming.RetryBase
	for attempt := 1; ; attempt++ {
		err := s.joinCluster()
		if err == nil {
			err = s.bootstrap(ctx)
		}
		if err == nil {
			s.gossipManager.SetLocalJoining(false)
			close(s.bootstrapDone)
			logger.WithField("attempt", attempt).Info("Bootstrap complete, serving reads")
			return
		}

		logger.WithError(err).WithFields(map[string]interface{}{
			"attempt": attempt,
			"retryIn": backoff.String(),
		}).Warn("Bootstrap attempt failed; node stays joining and serves no reads")

		select {
		case <-ctx.Done():
			logger.Info("Bootstrap stopped before completing; node never served reads")
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > s.bootstrapTiming.RetryMax {
			backoff = s.bootstrapTiming.RetryMax
		}
	}
}

// bootstrap streams the token ranges this node now owns from their previous
// replicas and reports whether the transfer is complete enough to serve reads.
//
// A write acknowledged before we joined is on at least W of a range's previous
// replicas. Streaming from any (previous replicas - W + 1) of them therefore
// intersects every such write quorum, so each range needs that many successful
// sources. A source counts only if its whole stream was received and every
// entry applied. Writes made after we joined reach us directly as a pending
// replica, and merging by vector clock never lets an older streamed version
// overwrite them.
func (s *DistKVServer) bootstrap(ctx context.Context) error {
	logger := logging.WithComponent("server.bootstrap")

	gossipView := s.gossipManager.GetNodes()
	isJoining := func(nodeID string) bool {
		nodeInfo, exists := gossipView[nodeID]
		return exists && nodeInfo.IsJoining()
	}

	n, w := s.config.QuorumConfig.N, s.config.QuorumConfig.W
	plan := s.consistentHash.BootstrapSources(s.config.NodeID, n, isJoining)
	ranges := s.consistentHash.TokenRangesFor(s.config.NodeID, n)
	if len(plan) == 0 || len(ranges) == 0 {
		return fmt.Errorf("node %s owns no ranges on the ring", s.config.NodeID)
	}

	var peers []string
	seen := make(map[string]bool)
	for _, segment := range plan {
		if len(segment.Sources) == 0 {
			return fmt.Errorf("range %v has no readable previous replica to stream from", segment.Range)
		}
		for _, id := range segment.Sources {
			if !seen[id] {
				seen[id] = true
				peers = append(peers, id)
			}
		}
	}
	sort.Strings(peers)

	complete := make(map[string]bool)
	var streamed int
	for _, peer := range peers {
		count, err := s.streamFromPeer(ctx, peer, ranges)
		if err != nil {
			logger.WithError(err).WithField("peer", peer).Warn("Bootstrap: incomplete stream from peer")
			continue
		}
		complete[peer] = true
		streamed += count
	}

	var uncovered int
	for _, segment := range plan {
		need := len(segment.Sources) - w + 1
		if need < 1 {
			need = 1
		}
		have := 0
		for _, id := range segment.Sources {
			if complete[id] {
				have++
			}
		}
		if have < need {
			uncovered++
		}
	}

	var failed []string
	for _, peer := range peers {
		if !complete[peer] {
			failed = append(failed, peer)
		}
	}
	if uncovered > 0 {
		return fmt.Errorf("%d of %d ranges lack a complete stream from enough previous replicas (failed peers: %v)",
			uncovered, len(plan), failed)
	}

	logger.WithFields(map[string]interface{}{
		"ranges":      len(ranges),
		"entries":     streamed,
		"peers":       len(complete),
		"failedPeers": failed,
	}).Info("Bootstrap: every range covered by enough previous replicas")
	return nil
}

// streamFromPeer streams our ranges from one peer and applies every entry,
// bounded by the bootstrap peer timeout. It returns an error unless the whole
// stream arrived and every entry was applied.
func (s *DistKVServer) streamFromPeer(ctx context.Context, peer string, ranges []partition.TokenRange) (int, error) {
	peerCtx, cancel := context.WithTimeout(ctx, s.bootstrapTiming.PeerTimeout)
	defer cancel()

	entries, err := s.rangeStreamer.StreamRange(peerCtx, peer, ranges)
	if err != nil {
		return 0, err
	}
	for _, entry := range entries {
		if err := replication.MergeEntry(s.storageEngine, entry); err != nil {
			return 0, fmt.Errorf("failed to apply key %q: %v", entry.Key, err)
		}
	}
	return len(entries), nil
}

// contactSeedNode contacts a seed node to get current cluster membership
func (s *DistKVServer) contactSeedNode(seedAddress string) ([]*proto.NodeStatusInfo, error) {
	// Create gRPC connection to seed node
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Determine dial options based on TLS configuration
	var dialOpts []grpc.DialOption
	if s.config.TLSConfig.Enabled {
		// Create client TLS config for inter-node communication
		clientTLSConfig := &tls.Config{
			Enabled:    true,
			CAFile:     s.config.TLSConfig.CAFile,
			CertFile:   s.config.TLSConfig.CertFile,
			KeyFile:    s.config.TLSConfig.KeyFile,
			ServerName: "localhost", // For self-signed certs
		}
		creds, err := tls.LoadClientCredentials(clientTLSConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %v", err)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.DialContext(ctx, seedAddress, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to seed node: %v", err)
	}
	defer conn.Close()

	// Get admin client
	adminClient := proto.NewAdminServiceClient(conn)

	// Request cluster status
	resp, err := adminClient.GetClusterStatus(ctx, &proto.ClusterStatusRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster status: %v", err)
	}

	return resp.Nodes, nil
}

// announceSelfToNode announces this node to an existing cluster node
func (s *DistKVServer) announceSelfToNode(targetAddress string) error {
	// Create gRPC connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Determine dial options based on TLS configuration
	var dialOpts []grpc.DialOption
	if s.config.TLSConfig.Enabled {
		// Create client TLS config for inter-node communication
		clientTLSConfig := &tls.Config{
			Enabled:    true,
			CAFile:     s.config.TLSConfig.CAFile,
			CertFile:   s.config.TLSConfig.CertFile,
			KeyFile:    s.config.TLSConfig.KeyFile,
			ServerName: "localhost", // For self-signed certs
		}
		creds, err := tls.LoadClientCredentials(clientTLSConfig)
		if err != nil {
			return fmt.Errorf("failed to load TLS credentials: %v", err)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.DialContext(ctx, targetAddress, dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Get admin service client
	adminClient := proto.NewAdminServiceClient(conn)

	// Announce ourselves using AddNode (with advertise address so peers can reach us)
	req := &proto.AddNodeRequest{
		NodeId:       s.config.NodeID,
		NodeAddress:  s.config.AdvertiseAddress,
		VirtualNodes: int32(s.config.VirtualNodes),
		Joining:      s.gossipManager.GetLocalNode().IsJoining(),
	}

	_, err = adminClient.AddNode(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to announce: %v", err)
	}

	logging.WithComponent("server.cluster").
		WithField("targetAddress", targetAddress).
		Debug("Successfully announced self to node")
	return nil
}

// startGRPCServer starts the gRPC server
func (s *DistKVServer) startGRPCServer() error {
	logger := logging.WithComponent("server.grpc")

	// Create listener
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", s.config.Address, err)
	}

	// Create gRPC server with TLS if enabled
	var grpcServer *grpc.Server
	if s.config.TLSConfig.Enabled {
		// Load TLS credentials
		creds, err := tls.LoadServerCredentials(s.config.TLSConfig)
		if err != nil {
			return fmt.Errorf("failed to load TLS credentials: %v", err)
		}

		grpcServer = grpc.NewServer(grpc.Creds(creds))
		logger.WithFields(map[string]interface{}{
			"address":  s.config.Address,
			"certFile": s.config.TLSConfig.CertFile,
		}).Info("gRPC server starting with TLS enabled")
	} else {
		grpcServer = grpc.NewServer()
		logger.WithField("address", s.config.Address).
			Warn("gRPC server starting WITHOUT TLS (insecure)")
	}

	s.grpcServer = grpcServer

	// Register services
	distkvService := &DistKVServiceImpl{server: s}
	proto.RegisterDistKVServer(s.grpcServer, distkvService)

	nodeService := &NodeServiceImpl{server: s}
	proto.RegisterNodeServiceServer(s.grpcServer, nodeService)

	adminService := &AdminServiceImpl{server: s}
	proto.RegisterAdminServiceServer(s.grpcServer, adminService)

	// Enable reflection for debugging
	reflection.Register(s.grpcServer)

	// Start server in background
	go func() {
		logger.Info("gRPC server listening")
		if err := s.grpcServer.Serve(listener); err != nil {
			logger.WithError(err).Error("gRPC server error")
		}
	}()

	return nil
}

// waitForShutdown waits for OS signals to shutdown gracefully
func waitForShutdown() {
	logger := logging.WithComponent("server.signals")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	sig := <-sigChan
	logger.WithField("signal", sig.String()).Info("Received shutdown signal")
}
