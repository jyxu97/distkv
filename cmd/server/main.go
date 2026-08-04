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
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

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
	config := parseFlags()

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

// parseFlags parses command line arguments and returns server configuration
func parseFlags() *ServerConfig {
	var (
		nodeID           = flag.String("node-id", "", "Unique node identifier (required)")
		address          = flag.String("address", "localhost:8080", "Server listen address")
		advertiseAddress = flag.String("advertise-address", "", "Address advertised to cluster peers (defaults to --address)")
		dataDir          = flag.String("data-dir", "./data", "Directory for data storage")
		seedNodes        = flag.String("seed-nodes", "", "Comma-separated list of seed nodes")
		virtualNodes     = flag.Int("virtual-nodes", 150, "Number of virtual nodes for consistent hashing")

		// Storage flags
		memTableSize     = flag.Int("mem-table-size", 1024, "MemTable size in bytes")
		ssTableSize      = flag.Int64("sstable-size", 256*1024*1024, "SSTable size in bytes")
		bloomFilterBits  = flag.Int("bloom-filter-bits", 10, "Bloom filter bits per key")
		compactionThresh = flag.Int("compaction-threshold", 4, "Number of SSTables to trigger compaction")

		// Replication flags
		replicas    = flag.Int("replicas", 3, "Number of replicas (N)")
		readQuorum  = flag.Int("read-quorum", 2, "Read quorum size (R)")
		writeQuorum = flag.Int("write-quorum", 2, "Write quorum size (W)")

		// Gossip flags
		heartbeatInterval = flag.Duration("heartbeat-interval", 1*time.Second, "Heartbeat interval")
		suspectTimeout    = flag.Duration("suspect-timeout", 30*time.Second, "Suspect timeout")
		deadTimeout       = flag.Duration("dead-timeout", 120*time.Second, "Dead timeout")
		gossipInterval    = flag.Duration("gossip-interval", 1*time.Second, "Gossip interval")
		gossipFanout      = flag.Int("gossip-fanout", 3, "Gossip fanout")

		// TLS flags
		tlsEnabled    = flag.Bool("tls-enabled", false, "Enable TLS for secure communication")
		tlsCertFile   = flag.String("tls-cert-file", "", "Path to TLS certificate file")
		tlsKeyFile    = flag.String("tls-key-file", "", "Path to TLS private key file")
		tlsCAFile     = flag.String("tls-ca-file", "", "Path to TLS CA certificate file for client verification")
		tlsClientAuth = flag.String("tls-client-auth", "NoClientCert", "Client authentication policy: NoClientCert, RequestClientCert, RequireAnyClientCert, VerifyClientCertIfGiven, RequireAndVerifyClientCert")
	)

	flag.Parse()

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

	return &ServerConfig{
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
		config:         config,
		storageEngine:  storageEngine,
		consistentHash: consistentHash,
		quorumManager:  quorumManager,
		gossipManager:  gossipManager,
		nodeSelector:   nodeSelector,
		replicaClient:  replicaClient,
		antiEntropy:    antiEntropy,
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

	// Join cluster by connecting to seed nodes
	if err := s.joinCluster(); err != nil {
		logger.WithError(err).Warn("Failed to join cluster, operating as single-node")
		// Continue anyway - we can operate as a single-node cluster
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

// joinCluster attempts to join the cluster by contacting seed nodes
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
	var discoveredNodes []string
	for _, seedAddress := range s.config.SeedNodes {
		nodes, err := s.contactSeedNode(seedAddress)
		if err != nil {
			logger.WithError(err).WithField("seedAddress", seedAddress).
				Warn("Failed to contact seed node")
			continue
		}
		discoveredNodes = append(discoveredNodes, nodes...)
	}

	// Add discovered nodes to our systems
	for _, nodeInfo := range discoveredNodes {
		// Parse nodeInfo format: "nodeID@address"
		parts := strings.Split(nodeInfo, "@")
		if len(parts) != 2 {
			continue
		}
		nodeID, address := parts[0], parts[1]

		// Add to gossip manager
		s.gossipManager.AddNode(nodeID, address)

		// Add to consistent hash ring
		s.consistentHash.AddNode(nodeID)

		// Update replica client with node address
		s.replicaClient.UpdateNodeAddress(nodeID, address)

		logger.WithFields(map[string]interface{}{
			"nodeID":  nodeID,
			"address": address,
		}).Debug("Discovered and added node")
	}

	// Also add self to replica client for local operations
	s.replicaClient.UpdateNodeAddress(s.config.NodeID, s.config.AdvertiseAddress)

	// Announce ourselves to the cluster
	for _, seedAddress := range s.config.SeedNodes {
		if err := s.announceSelfToNode(seedAddress); err != nil {
			logger.WithError(err).WithField("seedAddress", seedAddress).
				Warn("Failed to announce to seed node")
		}
	}

	logger.WithField("nodeCount", len(discoveredNodes)+1).
		Info("Successfully joined cluster")
	return nil
}

// contactSeedNode contacts a seed node to get current cluster membership
func (s *DistKVServer) contactSeedNode(seedAddress string) ([]string, error) {
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

	// Convert response to node list
	var nodes []string
	for _, node := range resp.Nodes {
		nodeInfo := fmt.Sprintf("%s@%s", node.NodeId, node.Address)
		nodes = append(nodes, nodeInfo)
	}

	return nodes, nil
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
