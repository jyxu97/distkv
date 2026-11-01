// Local cleanup of keys a node no longer replicates after a range handoff.
//
// When a node joins, some existing nodes stop being replicas of some keys but
// keep their copies. Cleanup removes such a copy only when all of these hold:
//
//  1. Ownership has converged: this node has finished bootstrapping, no node
//     on its ring is joining, and every ring member confirms (over RPC) the
//     same membership with nobody joining. Otherwise some coordinator may
//     still read from or count on this node for the key.
//  2. This node is not one of the key's N replicas on that converged ring.
//  3. Every one of the key's N replicas currently holds a version equal to or
//     causally after the local one (tombstones included), so dropping the
//     local copy loses no version and no delete.
//  4. The removal is atomic and conditional: it happens only if the local
//     entry is still exactly the version that was verified, so a write that
//     lands here after verification is kept.
//
// Removal writes a local purge marker through the WAL; it is never replicated
// and carries no version, so it cannot shadow data on any other node.
package main

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"distkv/pkg/consensus"
	"distkv/pkg/replication"
	"distkv/pkg/storage"
	"distkv/proto"
)

// Reasons a key that this node no longer replicates is kept anyway.
const (
	skipReplicaUnreachable = "replica_unreachable" // Could not read a replica
	skipReplicaMissing     = "replica_missing"     // A replica does not have the key
	skipReplicaOlder       = "replica_older"       // A replica has an older version
	skipReplicaConcurrent  = "replica_concurrent"  // A replica has a concurrent version
	skipChangedLocally     = "changed_locally"     // The local entry changed after verification
)

// cleanupCandidate is a local key verified as safe to remove, with the exact
// local entry that was verified.
type cleanupCandidate struct {
	key   string
	entry *storage.Entry
}

// cleanupReport is the outcome of a cleanup run.
type cleanupReport struct {
	scanned  int
	owned    int
	eligible []cleanupCandidate
	removed  int
	skipped  map[string]int
}

// cleanupRPCTimeout bounds each RPC cleanup makes to a peer.
const cleanupRPCTimeout = 5 * time.Second

// cleanup runs one cleanup pass. With dryRun it only reports which keys are
// eligible. It returns an error, and removes nothing, if ownership has not
// converged.
func (s *DistKVServer) cleanup(ctx context.Context, dryRun bool) (*cleanupReport, error) {
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()

	ring, err := s.confirmRingConverged(ctx)
	if err != nil {
		return nil, err
	}

	report, err := s.findCleanupCandidates(ctx)
	if err != nil {
		return nil, err
	}
	if dryRun {
		return report, nil
	}
	return report, s.removeCandidates(report, ring)
}

// localRingView returns this node's sorted ring members and the members it
// sees as joining.
func (s *DistKVServer) localRingView() (members, joining []string) {
	members = s.consistentHash.GetAllNodes()
	sort.Strings(members)
	gossipView := s.gossipManager.GetNodes()
	for _, id := range members {
		if info, ok := gossipView[id]; ok && info.IsJoining() {
			joining = append(joining, id)
		}
	}
	return members, joining
}

// isReady reports whether this node has finished bootstrapping.
func (s *DistKVServer) isReady() bool {
	select {
	case <-s.bootstrapDone:
		return !s.gossipManager.GetLocalNode().IsJoining()
	default:
		return false
	}
}

// confirmRingConverged checks condition 1 and returns the ring membership it
// confirmed, as a comparable string.
func (s *DistKVServer) confirmRingConverged(ctx context.Context) (string, error) {
	if !s.isReady() {
		return "", fmt.Errorf("this node has not finished bootstrapping")
	}

	members, joining := s.localRingView()
	if len(joining) > 0 {
		return "", fmt.Errorf("nodes still joining: %v", joining)
	}
	ring := strings.Join(members, ",")

	for _, id := range members {
		if id == s.config.NodeID {
			continue
		}
		rpcCtx, cancel := context.WithTimeout(ctx, cleanupRPCTimeout)
		view, err := s.replicaClient.RingView(rpcCtx, id)
		cancel()
		if err != nil {
			return "", fmt.Errorf("cannot confirm ring view of %s: %v", id, err)
		}
		if !view.Ready || len(view.Joining) > 0 {
			return "", fmt.Errorf("%s is not settled (ready=%v, joining=%v)", id, view.Ready, view.Joining)
		}
		if peerRing := strings.Join(view.Members, ","); peerRing != ring {
			return "", fmt.Errorf("%s has a different ring view [%s], ours is [%s]", id, peerRing, ring)
		}
	}
	return ring, nil
}

// findCleanupCandidates checks conditions 2 and 3 for every local key.
func (s *DistKVServer) findCleanupCandidates(ctx context.Context) (*cleanupReport, error) {
	// Snapshot local entries first so no storage iterator stays open across RPCs
	var local []cleanupCandidate
	iter, err := s.storageEngine.Iterator()
	if err != nil {
		return nil, fmt.Errorf("failed to open storage iterator: %v", err)
	}
	for ; iter.Valid(); iter.Next() {
		if entry := iter.Value(); entry != nil {
			local = append(local, cleanupCandidate{key: entry.Key, entry: copyEntry(entry)})
		}
	}
	iter.Close()

	report := &cleanupReport{scanned: len(local), skipped: map[string]int{}}
	n := s.config.QuorumConfig.N
	for _, candidate := range local {
		replicas := s.consistentHash.GetNodes(candidate.key, n)
		if containsNode(replicas, s.config.NodeID) {
			report.owned++
			continue
		}

		reason := ""
		for _, id := range replicas {
			rpcCtx, cancel := context.WithTimeout(ctx, cleanupRPCTimeout)
			resp, err := s.replicaClient.ReadReplica(rpcCtx, id, candidate.key)
			cancel()
			if err != nil || resp == nil || !resp.Success {
				reason = skipReplicaUnreachable
			} else {
				reason = replicaCovers(candidate.entry, resp)
			}
			if reason != "" {
				break
			}
		}

		if reason != "" {
			report.skipped[reason]++
			continue
		}
		report.eligible = append(report.eligible, candidate)
	}
	return report, nil
}

// replicaCovers returns "" if a replica's version makes the local entry
// redundant, or the reason it does not.
func replicaCovers(local *storage.Entry, remote *replication.ReplicaResponse) string {
	if remote.VectorClock == nil {
		return skipReplicaMissing // The replica has no version of the key at all
	}
	localVC := local.VectorClock
	if localVC == nil {
		localVC = consensus.NewVectorClock()
	}
	remoteDeleted := remote.Value == nil

	switch remote.VectorClock.Compare(localVC) {
	case consensus.After:
		return ""
	case consensus.Equal:
		switch {
		case local.Deleted:
			// The replica must hold the delete too, not a value at the same clock
			if remoteDeleted {
				return ""
			}
			return skipReplicaOlder
		case remoteDeleted:
			return "" // A tombstone at the same clock supersedes the value
		case bytes.Equal(remote.Value, local.Value):
			return ""
		default:
			return skipReplicaConcurrent // Different values at the same clock
		}
	case consensus.Before:
		return skipReplicaOlder
	default:
		return skipReplicaConcurrent
	}
}

// removeCandidates applies condition 4: each key is removed only if its local
// entry is still the verified version and this node's ring is unchanged.
func (s *DistKVServer) removeCandidates(report *cleanupReport, ring string) error {
	for _, candidate := range report.eligible {
		members, joining := s.localRingView()
		if len(joining) > 0 || strings.Join(members, ",") != ring {
			return fmt.Errorf("ring changed during cleanup after removing %d keys", report.removed)
		}

		verified := candidate.entry
		removed, _, err := s.storageEngine.WriteIf(storage.NewPurgeEntry(candidate.key), func(current *storage.Entry) bool {
			return current != nil && sameEntryVersion(current, verified)
		})
		if err != nil {
			return fmt.Errorf("failed to remove %q after removing %d keys: %v", candidate.key, report.removed, err)
		}
		if removed {
			report.removed++
		} else {
			report.skipped[skipChangedLocally]++
		}
	}
	return nil
}

// copyEntry deep-copies an entry. Iterator entries share their value and
// clock with storage; copying keeps the verified version fixed even if a
// storage implementation reuses them, so a later write cannot look verified.
func copyEntry(entry *storage.Entry) *storage.Entry {
	copied := *entry
	copied.Value = append([]byte(nil), entry.Value...)
	if entry.VectorClock != nil {
		copied.VectorClock = entry.VectorClock.Copy()
	}
	return &copied
}

// sameEntryVersion reports whether two local entries are the same version.
func sameEntryVersion(a, b *storage.Entry) bool {
	if a.Deleted != b.Deleted || !bytes.Equal(a.Value, b.Value) {
		return false
	}
	if a.VectorClock == nil || b.VectorClock == nil {
		return a.VectorClock == nil && b.VectorClock == nil
	}
	return a.VectorClock.Equal(b.VectorClock)
}

func containsNode(nodes []string, id string) bool {
	for _, n := range nodes {
		if n == id {
			return true
		}
	}
	return false
}

// cleanupResponse converts a cleanup report to its RPC form.
func cleanupResponse(report *cleanupReport, err error) *proto.CleanupResponse {
	resp := &proto.CleanupResponse{Success: err == nil, Skipped: map[string]int64{}}
	if err != nil {
		resp.ErrorMessage = err.Error()
	}
	if report == nil {
		return resp
	}
	resp.KeysScanned = int64(report.scanned)
	resp.KeysOwned = int64(report.owned)
	resp.KeysEligible = int64(len(report.eligible))
	resp.KeysRemoved = int64(report.removed)
	for i, candidate := range report.eligible {
		if i == 100 {
			break
		}
		resp.SampleEligibleKeys = append(resp.SampleEligibleKeys, candidate.key)
	}
	for reason, count := range report.skipped {
		resp.Skipped[reason] = int64(count)
	}
	return resp
}
