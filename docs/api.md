# DistKV API Documentation

This document describes the DistKV client API and internal service interfaces.

## Client API

### Connection

```go
import "distkv/client"

// Create client
config := &ClientConfig{
    Servers: []string{"node1:8080", "node2:8081", "node3:8082"},
    Timeout: 5 * time.Second,
    ConsistencyLevel: QUORUM,
}
client, err := NewClient(config)
```

### Basic Operations

#### PUT - Store Key-Value Pair

```bash
# CLI
distkv-client put <key> <value> [options]

# Examples
distkv-client put "user:123" "John Doe"
distkv-client -consistency=all put "critical-key" "important data"
```

**Parameters:**
- `key` (string): The key to store
- `value` (string): The value to store
- `consistency` (optional): one, quorum, all (default: quorum)
- `vector_clock` (optional, gRPC only): Causal context — the `vector_clock` from a previous GET or PUT of this key. The new version supersedes everything that context covers. Omit it for a blind write; the CLI always writes blind.

**Response:**
- `success` (bool): Whether operation succeeded
- `error` (string): Error message if failed
- `vector_clock` (object, gRPC only): The version the replicas stored for this write

**Versioning:** the coordinator creates the write's version once, before replicating it: the supplied context with the coordinator's own entry advanced to a value it has never issued before (persisted in `version_counter` in the data directory, so restarts do not reuse it). Every replica stores that exact version.

#### GET - Retrieve Value

```bash
# CLI
distkv-client get <key> [options]

# Examples  
distkv-client get "user:123"
distkv-client -consistency=one get "cache-key"
```

**Parameters:**
- `key` (string): The key to retrieve
- `consistency` (optional): one, quorum, all (default: quorum)

**Response:**
- `found` (bool): Whether key exists
- `value` (string): The stored value (primary or first sibling)
- `vector_clock` (object): Causal context for the next write. Without a conflict it is the value's version; with siblings it is the merge of all sibling clocks, so a PUT carrying it supersedes every sibling
- `has_conflict` (bool): True when concurrent versions exist (Dynamo-style siblings)
- `siblings` (array of SiblingVersion): All concurrent versions when `has_conflict` is true

**SiblingVersion:**
- `value` (bytes): The value of this version
- `vector_clock` (object): Vector clock for this version

**Conflict Handling:**

When multiple nodes accept concurrent writes to the same key (no causal ordering between their vector clocks), the GET response preserves all versions as siblings instead of silently picking one. The client is responsible for resolving the conflict and writing back the merged value with the GET's `vector_clock` as context.

```bash
# Example: conflict detected
$ distkv-client get "user:123"
Key: user:123
CONFLICT: 2 concurrent versions detected!
  Version 1: Alice (vector clock: map[node1:1])
  Version 2: Bob (vector clock: map[node2:1])
Please resolve the conflict by writing the correct value with PUT.
```

#### DELETE - Remove Key

```bash
# CLI
distkv-client delete <key> [options]

# Examples
distkv-client delete "user:123"
distkv-client -consistency=all delete "sensitive-key"
```

**Parameters:**
- `key` (string): The key to delete
- `consistency` (optional): one, quorum, all (default: quorum)
- `vector_clock` (optional, gRPC only): Causal context, as for PUT. A delete with the context from a GET supersedes the versions that GET returned

**Response:**
- `success` (bool): Whether operation succeeded

#### BATCH - Multiple Operations

```bash
# CLI  
distkv-client batch <key1> <value1> <key2> <value2> ...

# Example
distkv-client batch "user:1" "Alice" "user:2" "Bob" "user:3" "Charlie"
```

**Parameters:**
- Multiple key-value pairs

**Response:**
- `success` (bool): Whether all operations succeeded
- `failed_keys` ([]string): Keys that failed to store

### Administrative Operations

#### STATUS - Cluster Health

```bash
# CLI
distkv-client status

# Example output
=== Cluster Status ===
Health: 3 total nodes, 3 alive, 0 dead (100.0% availability)

=== Nodes ===
  node1 (localhost:8080) - ALIVE - Last seen: 2024-01-15T10:30:45Z
  node2 (localhost:8081) - ALIVE - Last seen: 2024-01-15T10:30:44Z  
  node3 (localhost:8082) - ALIVE - Last seen: 2024-01-15T10:30:43Z

=== Metrics ===
Total requests: 1543
Average latency: 2.3 ms
```

## Internal Service APIs

### NodeService (Inter-node Communication)

#### Replicate
Replicates data to a specific node.

```protobuf
service NodeService {
  rpc Replicate(ReplicateRequest) returns (ReplicateResponse);
}

message ReplicateRequest {
  string key = 1;
  bytes value = 2;
  VectorClock vector_clock = 3;
  bool is_delete = 4;
}
```

#### Gossip
Exchanges node information for failure detection.

```protobuf
rpc Gossip(GossipMessage) returns (GossipResponse);

message GossipMessage {
  string sender_id = 1;
  repeated NodeInfo node_updates = 2;
}
```

### AdminService (Cluster Management)

#### AddNode
Adds a new node to the cluster.

```protobuf
service AdminService {
  rpc AddNode(AddNodeRequest) returns (AddNodeResponse);
}

message AddNodeRequest {
  string node_address = 1;
  int32 virtual_nodes = 2;
}
```

#### GetClusterStatus
Returns current cluster health and metrics.

```protobuf
rpc GetClusterStatus(ClusterStatusRequest) returns (ClusterStatusResponse);

message ClusterStatusResponse {
  repeated NodeStatus nodes = 1;
  ClusterHealth health = 2;
  ClusterMetrics metrics = 3;
}
```

## Consistency Levels

### ONE
- **Write**: Only one replica needs to acknowledge
- **Read**: Read from one replica only
- **Guarantees**: High availability, eventual consistency
- **Use case**: Caching, logs, non-critical data

### QUORUM (Default)
- **Write**: Majority of replicas must acknowledge (W=2 out of N=3)
- **Read**: Read from majority of replicas (R=2 out of N=3)
- **Guarantees**: Strong consistency (W + R > N)
- **Use case**: Most applications requiring consistency

### ALL  
- **Write**: All replicas must acknowledge (W=N)
- **Read**: Read from one replica (R=1)
- **Guarantees**: Strongest consistency, lower availability
- **Use case**: Critical data requiring perfect consistency

## Error Handling

### Common Errors

- `KeyNotFound`: Requested key doesn't exist
- `QuorumFailure`: Not enough replicas responded
- `NetworkTimeout`: Request timed out
- `NodeUnavailable`: Target node is down
- `InvalidRequest`: Malformed request

### Error Response Format

```json
{
  "success": false,
  "error": "QuorumFailure: got 1 acknowledgments, needed 2",
  "code": "QUORUM_FAILURE"
}
```

## Performance Characteristics

### Throughput
- **Single node**: 10,000+ QPS
- **3-node cluster**: 30,000+ QPS (linear scaling)

### Latency (99th percentile)
- **Read**: < 5ms
- **Write**: < 10ms
- **Network latency**: Adds ~1-2ms per hop

### Storage
- **Compression**: 3-4x reduction with Snappy
- **Compaction**: Background process, minimal impact
- **Memory usage**: ~64MB MemTable per node

## Best Practices

### Key Design
- Use meaningful prefixes: `user:123`, `session:abc`
- Avoid hotspots: Don't use sequential keys
- Keep keys short: < 250 characters

### Value Design  
- Limit value size: < 1MB recommended
- Use compression for large values
- Consider JSON for structured data

### Consistency
- Use QUORUM for most operations
- Use ONE for caches and logs
- Use ALL only for critical operations

### Monitoring
- Monitor cluster status regularly
- Set up alerts for node failures
- Track request latency and error rates