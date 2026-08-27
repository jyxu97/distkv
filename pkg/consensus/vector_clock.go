// Package consensus implements vector clocks for detecting concurrent updates
// and resolving conflicts in a distributed system without global coordination.
package consensus

import (
	"encoding/json"
	"fmt"
	"strings"
)

// VectorClock tracks logical time across distributed nodes to detect causality.
// Each node maintains a counter, and we can determine if events happened
// concurrently (conflict) or in causal order (one happened before another).
type VectorClock struct {
	// clocks maps node_id -> logical_clock_value
	// Example: {"node1": 5, "node2": 3, "node3": 7}
	// This means node1 has seen 5 events, node2 has seen 3, etc.
	clocks map[string]uint64
}

// NewVectorClock creates a new vector clock with no events.
func NewVectorClock() *VectorClock {
	return &VectorClock{
		clocks: make(map[string]uint64),
	}
}

// NewVectorClockFromMap creates a vector clock from existing clock values.
// Used when receiving vector clocks from other nodes or storage.
func NewVectorClockFromMap(clocks map[string]uint64) *VectorClock {
	clocksCopy := make(map[string]uint64)
	for nodeID, clock := range clocks {
		clocksCopy[nodeID] = clock
	}

	return &VectorClock{
		clocks: clocksCopy,
	}
}

// MarshalJSON serializes the vector clock as a JSON object mapping node IDs to
// their logical clock values, e.g. {"node1":5,"node2":3}.
//
// This is required because the clocks map is unexported: without a custom
// marshaler the default encoding/json output is an empty object ({}), which
// silently drops all causality information when an Entry is persisted to an
// SSTable or the write-ahead log. Every on-disk vector clock depends on this.
func (vc *VectorClock) MarshalJSON() ([]byte, error) {
	if vc == nil || vc.clocks == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(vc.clocks)
}

// UnmarshalJSON restores a vector clock from the JSON object produced by
// MarshalJSON. A null or empty object yields an initialized, empty clock so
// callers never have to nil-check the internal map.
func (vc *VectorClock) UnmarshalJSON(data []byte) error {
	clocks := make(map[string]uint64)
	if len(data) > 0 && string(data) != "null" {
		if err := json.Unmarshal(data, &clocks); err != nil {
			return err
		}
	}
	vc.clocks = clocks
	return nil
}

// Increment increases the logical clock for a specific node.
// Call this when the node performs a local operation (read/write).
func (vc *VectorClock) Increment(nodeID string) {
	vc.clocks[nodeID] = vc.clocks[nodeID] + 1
}

// Update merges another vector clock with this one, taking the maximum
// of each node's clock. This happens when receiving data from another node.
func (vc *VectorClock) Update(other *VectorClock) {
	// Take maximum of each node's clock value
	for nodeID, otherClock := range other.clocks {
		currentClock := vc.clocks[nodeID]
		if otherClock > currentClock {
			vc.clocks[nodeID] = otherClock
		}
	}

	// Also include any nodes that exist in our clock but not in other
	// (no change needed as we already have higher or equal values)
}

// Copy creates a deep copy of the vector clock.
// Important for concurrent operations to avoid race conditions.
func (vc *VectorClock) Copy() *VectorClock {
	clocksCopy := make(map[string]uint64)
	for nodeID, clock := range vc.clocks {
		clocksCopy[nodeID] = clock
	}

	return &VectorClock{
		clocks: clocksCopy,
	}
}

// IsAfter determines if this vector clock happened after another.
// Returns true if this clock is strictly greater (happened after other).
func (vc *VectorClock) IsAfter(other *VectorClock) bool {
	// Must be greater than or equal in all positions
	// AND strictly greater in at least one position
	hasStrictlyGreater := false

	// Check all nodes in the other clock
	for nodeID, otherClock := range other.clocks {
		ourClock := vc.clocks[nodeID]

		if ourClock < otherClock {
			// We're behind in at least one node, so we're not after
			return false
		}

		if ourClock > otherClock {
			hasStrictlyGreater = true
		}
	}

	// Check if we have nodes that the other doesn't (those count as strictly greater)
	for nodeID := range vc.clocks {
		if _, exists := other.clocks[nodeID]; !exists {
			if vc.clocks[nodeID] > 0 {
				hasStrictlyGreater = true
			}
		}
	}

	return hasStrictlyGreater
}

// IsBefore determines if this vector clock happened before another.
// Returns true if other clock happened after this one.
func (vc *VectorClock) IsBefore(other *VectorClock) bool {
	return other.IsAfter(vc)
}

// IsConcurrent determines if two vector clocks represent concurrent events.
// Concurrent means neither happened before the other - they're in conflict!
func (vc *VectorClock) IsConcurrent(other *VectorClock) bool {
	return !vc.IsAfter(other) && !vc.IsBefore(other) && !vc.Equal(other)
}

// Equal checks if two vector clocks are identical.
func (vc *VectorClock) Equal(other *VectorClock) bool {
	// Must have same nodes
	if len(vc.clocks) != len(other.clocks) {
		// But handle case where missing nodes have implicit 0 values
		allNodes := make(map[string]bool)
		for nodeID := range vc.clocks {
			allNodes[nodeID] = true
		}
		for nodeID := range other.clocks {
			allNodes[nodeID] = true
		}

		for nodeID := range allNodes {
			ourClock := vc.clocks[nodeID]      // 0 if not present
			otherClock := other.clocks[nodeID] // 0 if not present

			if ourClock != otherClock {
				return false
			}
		}
		return true
	}

	// Same length, check each value
	for nodeID, clock := range vc.clocks {
		if other.clocks[nodeID] != clock {
			return false
		}
	}

	return true
}

// GetClock returns the clock value for a specific node.
// Returns 0 if the node doesn't exist in the vector clock.
func (vc *VectorClock) GetClock(nodeID string) uint64 {
	return vc.clocks[nodeID]
}

// GetClocks returns a copy of all clock values.
// Useful for serialization and debugging.
func (vc *VectorClock) GetClocks() map[string]uint64 {
	clocks := make(map[string]uint64)
	for nodeID, clock := range vc.clocks {
		clocks[nodeID] = clock
	}
	return clocks
}

// Merge creates a new vector clock that represents the maximum of two clocks.
// This is used in read repair and anti-entropy operations.
func (vc *VectorClock) Merge(other *VectorClock) *VectorClock {
	merged := vc.Copy()
	merged.Update(other)
	return merged
}

// String returns a human-readable representation of the vector clock.
// Format: "node1:5,node2:3,node3:7"
func (vc *VectorClock) String() string {
	if len(vc.clocks) == 0 {
		return "empty"
	}

	parts := make([]string, 0, len(vc.clocks))
	for nodeID, clock := range vc.clocks {
		parts = append(parts, fmt.Sprintf("%s:%d", nodeID, clock))
	}

	return strings.Join(parts, ",")
}

// Size returns the number of nodes tracked in this vector clock.
func (vc *VectorClock) Size() int {
	return len(vc.clocks)
}

// CompareResult represents the relationship between two vector clocks.
type CompareResult int

const (
	// Before means this vector clock happened before the other
	Before CompareResult = iota
	// After means this vector clock happened after the other
	After
	// Concurrent means the vector clocks are concurrent (conflict!)
	Concurrent
	// Equal means the vector clocks are identical
	Equal
)

// Compare returns the relationship between two vector clocks.
// This is the main function used for conflict detection.
func (vc *VectorClock) Compare(other *VectorClock) CompareResult {
	if vc.Equal(other) {
		return Equal
	}

	if vc.IsAfter(other) {
		return After
	}

	if vc.IsBefore(other) {
		return Before
	}

	return Concurrent // Neither before nor after = concurrent
}
