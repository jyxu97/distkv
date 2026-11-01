// Integration tests for per-request consistency levels.
package main

import (
	"context"
	"strings"
	"testing"

	"distkv/proto"
)

// TestConsistencyLevels covers how the server applies a request's consistency
// level with N=3, R=2, W=2: reads may ask for any count from 1 to N, writes
// for W or more, and an unset level uses the configured R and W.
func TestConsistencyLevels(t *testing.T) {
	a, _, c := startVersionCluster(t, 2, 2)
	svc := &DistKVServiceImpl{server: a}
	ctx := context.Background()

	putAt := func(key string, level proto.ConsistencyLevel) *proto.PutResponse {
		resp, err := svc.Put(ctx, &proto.PutRequest{Key: key, Value: []byte("v"), ConsistencyLevel: level})
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		return resp
	}
	getAt := func(key string, level proto.ConsistencyLevel) *proto.GetResponse {
		resp, err := svc.Get(ctx, &proto.GetRequest{Key: key, ConsistencyLevel: level})
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		return resp
	}

	// A write below W is refused before reaching any replica
	if resp := putAt("weak", proto.ConsistencyLevel_ONE); resp.Success || !strings.Contains(resp.ErrorMessage, "W=2") {
		t.Fatalf("Put at ONE with W=2: success=%v %q", resp.Success, resp.ErrorMessage)
	}
	del, err := svc.Delete(ctx, &proto.DeleteRequest{Key: "weak", ConsistencyLevel: proto.ConsistencyLevel_ONE})
	if err != nil || del.Success {
		t.Fatalf("Delete at ONE with W=2 accepted: %v", err)
	}
	for _, node := range []*DistKVServer{a, c} {
		if _, err := node.storageEngine.GetEntry("weak"); err == nil {
			t.Fatalf("refused write reached %s", node.config.NodeID)
		}
	}

	for _, level := range []proto.ConsistencyLevel{proto.ConsistencyLevel_DEFAULT,
		proto.ConsistencyLevel_QUORUM, proto.ConsistencyLevel_ALL} {
		if resp := putAt("key-"+level.String(), level); !resp.Success {
			t.Fatalf("Put at %s failed: %s", level, resp.ErrorMessage)
		}
	}

	// With one replica down, ALL fails in both directions while weaker levels work
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if resp := putAt("key-down", proto.ConsistencyLevel_ALL); resp.Success {
		t.Fatal("Put at ALL succeeded with a replica down")
	}
	if resp := putAt("key-down", proto.ConsistencyLevel_QUORUM); !resp.Success {
		t.Fatalf("Put at QUORUM failed with one replica down: %s", resp.ErrorMessage)
	}
	if resp := getAt("key-down", proto.ConsistencyLevel_ALL); resp.ErrorMessage == "" {
		t.Fatal("Get at ALL succeeded with a replica down")
	}
	for _, level := range []proto.ConsistencyLevel{proto.ConsistencyLevel_ONE,
		proto.ConsistencyLevel_QUORUM, proto.ConsistencyLevel_DEFAULT} {
		if resp := getAt("key-down", level); resp.ErrorMessage != "" || !resp.Found {
			t.Fatalf("Get at %s with one replica down: found=%v %q", level, resp.Found, resp.ErrorMessage)
		}
	}
}
