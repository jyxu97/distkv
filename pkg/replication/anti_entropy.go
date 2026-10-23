// Package replication - Anti-entropy background repair
//
// AntiEntropyManager periodically compares data with peer nodes using a
// Merkle-style hash. When hashes differ, the peer returns its entries and
// the local node applies any that are missing or causally newer.
package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"distkv/pkg/consensus"
	"distkv/pkg/logging"
)

// PeerInfo identifies a peer node for anti-entropy communication.
type PeerInfo struct {
	NodeID string
}

// PeerProvider returns the set of currently alive peer nodes.
type PeerProvider interface {
	GetAlivePeers() []PeerInfo
}

// AntiEntropyEntry is a versioned key-value pair received from a peer.
type AntiEntropyEntry struct {
	Key         string
	Value       []byte
	VectorClock *consensus.VectorClock
	IsDeleted   bool
}

// AntiEntropyPeerClient sends anti-entropy requests to peers.
// SyncRange sends the local hash to the peer; the peer responds with
// any entries that differ from that hash.
type AntiEntropyPeerClient interface {
	SyncRange(ctx context.Context, nodeID string, localHash string) ([]AntiEntropyEntry, error)
}

// AntiEntropyManager runs periodic background sync with peer nodes.
type AntiEntropyManager struct {
	nodeID        string
	storageEngine StorageEngine
	peerProvider  PeerProvider
	peerClient    AntiEntropyPeerClient
	interval      time.Duration
	logger        *logging.Logger
	stopCh        chan struct{}
}

// NewAntiEntropyManager creates a new anti-entropy manager.
func NewAntiEntropyManager(
	nodeID string,
	storageEngine StorageEngine,
	peerProvider PeerProvider,
	peerClient AntiEntropyPeerClient,
	interval time.Duration,
) *AntiEntropyManager {
	return &AntiEntropyManager{
		nodeID:        nodeID,
		storageEngine: storageEngine,
		peerProvider:  peerProvider,
		peerClient:    peerClient,
		interval:      interval,
		logger:        logging.WithComponent("replication.anti-entropy"),
		stopCh:        make(chan struct{}),
	}
}

// Start launches the background anti-entropy goroutine.
func (am *AntiEntropyManager) Start() {
	go am.run()
	am.logger.WithField("interval", am.interval).Info("Anti-entropy started")
}

// Stop halts the background goroutine.
func (am *AntiEntropyManager) Stop() {
	close(am.stopCh)
}

func (am *AntiEntropyManager) run() {
	ticker := time.NewTicker(am.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			am.runRound()
		case <-am.stopCh:
			return
		}
	}
}

// runRound performs one anti-entropy round: build local hash, then sync with
// every alive peer.
func (am *AntiEntropyManager) runRound() {
	peers := am.peerProvider.GetAlivePeers()
	if len(peers) == 0 {
		return
	}

	localHash, err := am.buildHash()
	if err != nil {
		am.logger.WithError(err).Warn("Anti-entropy: failed to build local hash")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, peer := range peers {
		if peer.NodeID == am.nodeID {
			continue
		}
		am.syncWithPeer(ctx, peer, localHash)
	}
}

// syncWithPeer sends the local hash to a peer. If the peer has different data
// it returns those entries; we apply any that are missing or causally newer
// than what we have locally.
func (am *AntiEntropyManager) syncWithPeer(ctx context.Context, peer PeerInfo, localHash string) {
	diffEntries, err := am.peerClient.SyncRange(ctx, peer.NodeID, localHash)
	if err != nil {
		am.logger.WithField("peer", peer.NodeID).WithError(err).Warn("Anti-entropy: sync failed")
		return
	}

	if len(diffEntries) == 0 {
		am.logger.WithField("peer", peer.NodeID).Debug("Anti-entropy: in sync")
		return
	}

	am.logger.WithFields(map[string]interface{}{
		"peer": peer.NodeID,
		"keys": len(diffEntries),
	}).Info("Anti-entropy: applying divergent entries")

	for _, entry := range diffEntries {
		if err := MergeEntry(am.storageEngine, entry); err != nil {
			am.logger.WithError(err).WithField("key", entry.Key).Warn("Anti-entropy: failed to apply entry")
		}
	}
}

// MergeEntry applies an entry received from a peer if the key is missing
// locally or the entry is causally newer. Concurrent versions (siblings) are
// skipped — read repair and application merge handle those.
func MergeEntry(storageEngine StorageEngine, entry AntiEntropyEntry) error {
	existing, _ := storageEngine.Get(entry.Key)
	if existing != nil {
		if entry.VectorClock == nil || existing.VectorClock == nil || !entry.VectorClock.IsAfter(existing.VectorClock) {
			return nil
		}
	}
	return storageEngine.Put(entry.Key, entry.Value, entry.VectorClock)
}

// buildHash computes a SHA-256 over all non-deleted local entries.
// Two nodes with identical data will produce the same hash.
func (am *AntiEntropyManager) buildHash() (string, error) {
	iter, err := am.storageEngine.Iterator()
	if err != nil {
		return "", fmt.Errorf("failed to open storage iterator: %w", err)
	}
	defer iter.Close()

	h := sha256.New()
	for iter.Valid() {
		entry := iter.Value()
		if entry != nil && !entry.Deleted {
			fmt.Fprintf(h, "%s:%x\n", entry.Key, entry.Value)
		}
		iter.Next()
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
