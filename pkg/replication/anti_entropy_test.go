package replication

import (
	"context"
	"testing"
	"time"

	"distkv/pkg/consensus"
	"distkv/pkg/storage"
)

// mockAntiEntropyClient implements AntiEntropyPeerClient for testing.
type mockAntiEntropyClient struct {
	responses map[string][]AntiEntropyEntry // nodeID → entries to return
	calls     []string                      // nodeIDs that were called
}

func (m *mockAntiEntropyClient) SyncRange(_ context.Context, nodeID string, _ string) ([]AntiEntropyEntry, error) {
	m.calls = append(m.calls, nodeID)
	return m.responses[nodeID], nil
}

// mockPeerProvider implements PeerProvider for testing.
type mockPeerProvider struct {
	peers []PeerInfo
}

func (m *mockPeerProvider) GetAlivePeers() []PeerInfo { return m.peers }

// TestAntiEntropyAppliesMissingKey verifies that when a peer returns an entry
// the local node doesn't have, anti-entropy writes it to local storage.
func TestAntiEntropyAppliesMissingKey(t *testing.T) {
	local := NewMockStorageEngine()

	peerVC := consensus.NewVectorClock()
	peerVC.Increment("node2")

	peerClient := &mockAntiEntropyClient{
		responses: map[string][]AntiEntropyEntry{
			"node2": {{Key: "missing-key", Value: []byte("peer-value"), VectorClock: peerVC}},
		},
	}

	mgr := NewAntiEntropyManager(
		"node1",
		local,
		&mockPeerProvider{peers: []PeerInfo{{NodeID: "node2"}}},
		peerClient,
		1*time.Hour, // long interval — we'll trigger manually
	)

	// Trigger one round directly
	mgr.runRound()

	// Verify the missing key was applied
	entry, err := local.Get("missing-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if entry == nil {
		t.Fatal("Expected missing-key to be applied, but it wasn't found")
	}
	if string(entry.Value) != "peer-value" {
		t.Errorf("Expected 'peer-value', got '%s'", string(entry.Value))
	}
}

// TestAntiEntropySkipsUpToDateKey verifies that when local already has a
// causally equal or newer version, anti-entropy does not overwrite it.
func TestAntiEntropySkipsUpToDateKey(t *testing.T) {
	local := NewMockStorageEngine()

	localVC := consensus.NewVectorClock()
	localVC.Increment("node1")
	localVC.Increment("node1") // counter = 2
	local.Put("key", []byte("local-value"), localVC)

	olderVC := consensus.NewVectorClock()
	olderVC.Increment("node1") // counter = 1, strictly older

	peerClient := &mockAntiEntropyClient{
		responses: map[string][]AntiEntropyEntry{
			"node2": {{Key: "key", Value: []byte("old-value"), VectorClock: olderVC}},
		},
	}

	mgr := NewAntiEntropyManager(
		"node1",
		local,
		&mockPeerProvider{peers: []PeerInfo{{NodeID: "node2"}}},
		peerClient,
		1*time.Hour,
	)

	mgr.runRound()

	entry, _ := local.Get("key")
	if string(entry.Value) != "local-value" {
		t.Errorf("Anti-entropy should not overwrite newer local value, got '%s'", string(entry.Value))
	}
}

// TestAntiEntropySkipsSelf verifies the manager never calls SyncRange on its own nodeID.
func TestAntiEntropySkipsSelf(t *testing.T) {
	peerClient := &mockAntiEntropyClient{responses: map[string][]AntiEntropyEntry{}}

	mgr := NewAntiEntropyManager(
		"node1",
		NewMockStorageEngine(),
		&mockPeerProvider{peers: []PeerInfo{{NodeID: "node1"}}},
		peerClient,
		1*time.Hour,
	)

	mgr.runRound()

	if len(peerClient.calls) != 0 {
		t.Errorf("Expected no calls to self, got %v", peerClient.calls)
	}
}

// Ensure MockStorageEngine satisfies StorageEngine (compile-time check).
var _ StorageEngine = (*MockStorageEngine)(nil)

// Ensure mockIterator satisfies storage.Iterator.
var _ storage.Iterator = (*mockIterator)(nil)

// TestAntiEntropySkipsKeysNotOwned verifies that anti-entropy only pulls keys
// this node replicates, so a key dropped after a handoff is not re-acquired.
func TestAntiEntropySkipsKeysNotOwned(t *testing.T) {
	local := NewMockStorageEngine()

	peerVC := consensus.NewVectorClock()
	peerVC.Increment("node2")
	peerClient := &mockAntiEntropyClient{
		responses: map[string][]AntiEntropyEntry{
			"node2": {
				{Key: "owned", Value: []byte("v"), VectorClock: peerVC},
				{Key: "handed-off", Value: []byte("v"), VectorClock: peerVC},
			},
		},
	}

	mgr := NewAntiEntropyManager("node1", local,
		&mockPeerProvider{peers: []PeerInfo{{NodeID: "node2"}}}, peerClient, 1*time.Hour)
	mgr.SetOwnershipFilter(func(key string) bool { return key == "owned" })
	mgr.runRound()

	if entry, _ := local.Get("owned"); entry == nil {
		t.Error("Expected the owned key to be pulled")
	}
	if entry, _ := local.Get("handed-off"); entry != nil {
		t.Error("Anti-entropy pulled a key this node does not replicate")
	}

	// Stop is safe to call twice
	mgr.Stop()
	mgr.Stop()
}
