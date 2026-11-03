# DistKV

Distributed key-value store built from scratch in Go, inspired by Amazon's Dynamo. It demonstrates core distributed systems concepts including consistent hashing, quorum consensus, vector clocks, and gossip protocols.

## Features

- **Fault Tolerance**: Quorum replication (N=3, R=2, W=2) sustains reads and writes through single-node failures
- **Tunable Consistency**: N/R/W quorum parameters set per server at startup, and a per-request consistency level (ONE/QUORUM/ALL) for reads, or for writes that need more acks than W
- **Persistent Storage**: LSM-tree storage engine with MemTables, SSTables, Bloom filters, and compaction
- **Durability**: Each write is appended to a write-ahead log and fsynced before it is acknowledged; SSTables are fsynced before the WAL segment covering them is deleted, a flush that fails is retried and keeps its WAL segment meanwhile, recovery writes leftover segments to SSTables in their original order so older writes never shadow newer ones, and a compaction interrupted by a crash never lets its old input tables shadow newer output
- **Gossip Protocol**: Network-based gossip for cluster membership and failure detection
- **Consistent Hashing**: Virtual node-based partitioning; new nodes propagate to all peers' hash rings within one gossip interval via event callbacks
- **Vector Clocks**: Conflict detection and causality tracking with Dynamo-style sibling preservation
- **Read Repair**: Stale replicas are automatically updated during quorum reads
- **Anti-Entropy**: Background 30-second sync repairs divergent data for the keys each node replicates
- **Safe Scale-Out**: A joining node serves no reads until it has streamed its new ranges; previous owners can then reclaim space with a verified `cleanup`
- **TLS Security**: TLS 1.2+ encryption for all client-server and inter-node communication
- **Kubernetes**: StatefulSet deployment with headless service discovery

## System Requirements

- Go 1.24 or later
- Protocol Buffers compiler (protoc)
- Make (optional, for build automation)

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/yvie97/DistKV.git
cd DistKV
```

### 2. Install Prerequisites

**Linux/Mac:**
```bash
# Option 1: Automated installation
./scripts/install-prerequisites.sh && make all

# Option 2: Manual
# - Go 1.24+: https://golang.org/dl/
# - protoc: https://github.com/protocolbuffers/protobuf/releases
```

**Windows:**
```cmd
scripts\build.bat
```

### 3. Build the Project

```bash
make all
```

> **Note**: The protobuf files (`proto/distkv.pb.go` and `proto/distkv_grpc.pb.go`) are auto-generated during build and not committed to version control.

### 4. Start a Single Node

```bash
./build/distkv-server -node-id=node1 -address=localhost:8080 -data-dir=./data

# In another terminal
./build/distkv-client put user:123 "John Doe"
./build/distkv-client get user:123
./build/distkv-client status
```

### 5. Start a 3-Node Development Cluster

```bash
# Start the cluster (runs in background)
make dev-cluster

# Stop the cluster
make stop-cluster
```

**Manual testing:**
```bash
./build/distkv-client --server=localhost:8080 put key1 "value1"
./build/distkv-client --server=localhost:8081 get key1
./build/distkv-client --server=localhost:8082 get key1
```

## Architecture

### System Components

[![DistKV runtime architecture](docs/distkv-architecture.svg)](https://jyxu97.github.io/distkv/)

[Open the interactive architecture diagram](https://jyxu97.github.io/distkv/)

### Key Technologies

- **Storage Engine**: LSM-tree with a write-ahead log, 64 MB MemTables, SSTables, and Bloom filters. A full MemTable is sealed (with a WAL rotation) and flushed by a background worker, so writes continue in a new MemTable during the flush; only when one sealed MemTable is still waiting and the new one fills up too do writers wait (`MaxMemTables`, default 2), and they fail instead of waiting while flushing keeps failing. Level-based compaction by default: 4 level-0 SSTables trigger a merge into level 1, and each deeper level holds 10x more tables (7 levels). Expired tombstones are dropped only in the bottom level, so they cannot uncover older data below them. `-compaction-strategy=simple` instead merges level 0 into one table
- **Partitioning**: Consistent hashing with 150 virtual nodes
- **Replication**: Quorum-based consensus (default: N=3, R=2, W=2)
- **Conflict Resolution**: Vector clocks with Dynamo-style sibling preservation
- **Read Repair**: Coordinator detects stale replicas during quorum reads and pushes updates asynchronously
- **Scale-Out**: A joining node is flagged `joining` in gossip and bootstraps before serving reads: peers send it writes for its new token ranges as a pending replica (required acks become W + pending), while it streams those ranges, tombstones included, from their previous replicas. It becomes readable only once every range has a complete stream from N − W + 1 previous replicas (enough to intersect every write quorum); otherwise it stays joining and retries with backoff. Previous owners keep their copies until an operator runs `cleanup` (see [operations guide](docs/operations.md#cleaning-up-after-adding-a-node)), which removes only keys every current replica already holds
- **Anti-Entropy**: 30-second background sync; nodes exchange a hash of their local entries and apply missing or causally newer versions of keys they replicate
- **Failure Detection**: Gossip protocol with heartbeat monitoring
- **Communication**: gRPC with Protocol Buffers

## Configuration

### Server

```bash
distkv-server [options]

Options:
  -node-id string          Unique node identifier (required)
  -address string          Server listen address (default: localhost:8080)
  -advertise-address string Address advertised to cluster peers (defaults to --address)
  -data-dir string         Directory for data storage (default: ./data)
  -seed-nodes string       Comma-separated list of seed nodes for cluster joining
  -replicas int            Number of replicas N (default: 3)
  -read-quorum int         Read quorum size R (default: 2)
  -write-quorum int        Write quorum size W (default: 2)
  -virtual-nodes int       Virtual nodes for consistent hashing (default: 150)
  -mem-table-size int      MemTable size in bytes before a flush (default: 67108864, 64 MB)
  -compaction-strategy     level or simple (default: level)
  -compaction-threshold    Level-0 SSTables that trigger compaction (default: 4)

Durability Options:
  -wal-enabled             Write a write-ahead log before each MemTable insert (default: true)
  -wal-sync                fsync the WAL before acknowledging each write (default: true)

TLS Options:
  -tls-enabled             Enable TLS (default: false)
  -tls-cert-file string    Path to TLS certificate file
  -tls-key-file string     Path to TLS private key file
  -tls-ca-file string      Path to TLS CA certificate file
  -tls-client-auth string  Client auth policy (default: NoClientCert)
```

### Client

```bash
distkv-client [options] <command> [args...]

Options:
  -server string          Server address (default: localhost:8080)
  -timeout duration       Request timeout (default: 5s)
  -consistency string     one, quorum or all (default: the server's R and W; see Consistency Models)

TLS Options:
  -tls-enabled                  Enable TLS (default: false)
  -tls-ca-file string           Path to CA certificate
  -tls-cert-file string         Path to client certificate (for mTLS)
  -tls-key-file string          Path to client key (for mTLS)
  -tls-server-name string       Expected server name (default: localhost)
  -tls-insecure-skip-verify     Skip cert verification (testing only)

Commands:
  put <key> <value>       Store a key-value pair
  get <key>               Retrieve value for a key
  delete <key>            Delete a key-value pair
  batch <k1> <v1> ...     Store multiple key-value pairs
  local-get <key>         Read a key from the connected server only (bypasses quorum)
  status                  Show cluster status
  cleanup [apply]         List (or with apply, remove) keys the server no longer replicates
```

The CLI writes without a causal context (no vector clock), so each `put` and `delete` is a blind write. Causal updates need the gRPC API: pass the `vector_clock` returned by a Get or Put back with the next Put or Delete.

## Testing

### Unit Tests

```bash
make test

# Run specific packages
go test ./pkg/consensus/...   # Vector clocks and version counter
go test ./pkg/storage/...     # Storage engine, WAL, crash recovery, compaction
go test ./pkg/partition/...   # Consistent hashing
go test ./pkg/gossip/...      # Gossip protocol
go test ./pkg/replication/... # Quorum reads/writes, read repair, anti-entropy
go test ./cmd/server/...      # In-process clusters: bootstrap, versioning, cleanup, consistency levels
```

### Integration Tests

Integration tests start their own cluster from the built server binary, so build it first; no need to run `make dev-cluster`. They use ports 8080–8082, which must be free.

```bash
make build
go test -v -timeout=120s ./tests/integration/...
```

Covers: basic put/get/delete, consistency levels (ONE/QUORUM/ALL, including a ONE write refused with W=2), node failure, concurrent writes, sibling preservation.

### Chaos Tests

```bash
make chaos-test
```

Covers: network partition and recovery, sibling preservation under partition, cluster scale-out (3→4 nodes: hash-ring convergence within one gossip interval, data streamed to the new node during bootstrap).

### Benchmark

```bash
make dev-cluster
make benchmark

# Read-only, 90 seconds per concurrency level
go run ./tests/benchmark -server=localhost:8080 -mode=read -duration=90s
```

Sweeps concurrency levels (default 10→200, set with `-concurrency=10,50,100`) for read-only and mixed (50/50 read/write) workloads, reporting QPS, p50/p95/p99 latency, and error rate. Requests go through the single server given by `-server`.

On one 8-core laptop running all three nodes and the client (N=3, R=2, W=2, 1,000 keys of 64 bytes, read-only, 90 s per level, server logs discarded, measured before the server's defaults changed to a 64 MB MemTable and level-based compaction), throughput reached about 25.5K QPS with p99 under 10 ms at concurrency 100 and leveled off near 27K QPS. Treat this as a single-machine prototype figure, not cluster capacity.

## Consistency Models

N, R and W are set per server with `-replicas`, `-read-quorum` and `-write-quorum`. A request without a consistency level uses R (reads) or W (writes). A request can also name a level, which the coordinator turns into a replica count: ONE = 1, QUORUM = N/2 + 1, ALL = N.

- **Reads** accept any level. ONE answers from the first replica and may return a stale value; ALL fails if any replica is unreachable.
- **Writes** accept a level only if it needs at least W acks; with the default W=2, a write at ONE is refused. Bootstrap depends on every acknowledged write being on at least W replicas: that is how a joining node knows that streaming from any `sources − W + 1` previous replicas covers every acknowledged write. A write still goes to every replica either way; the level only changes how many acks the coordinator waits for.

The `ConsistencyLevel` enum's zero value is `DEFAULT` (ONE = 1, QUORUM = 2, ALL = 3), so a request that leaves the level unset gets the server's R or W rather than ONE. Clients built against an older `distkv.proto`, where ONE was 0, must be regenerated and rebuilt.

### Overlapping Quorums (R + W > N)
```bash
# Default: N=3, R=2, W=2
./build/distkv-server -node-id=node1 -replicas=3 -read-quorum=2 -write-quorum=2
```
With a stable replica set, every read quorum overlaps the replicas that acknowledged the latest successful write, so a read sees that write or a newer version. This is not linearizability: read repair is asynchronous, a write that fails can still have reached some replicas, and concurrent writes surface as siblings.

### Eventual Consistency (R + W ≤ N)
```bash
# N=3, R=1, W=1: highest availability, reads may be stale until repair
./build/distkv-server -node-id=node1 -replicas=3 -read-quorum=1 -write-quorum=1
```

### Conflict Resolution

When concurrent writes occur with no causal ordering (detected via vector clocks), DistKV preserves all concurrent versions as siblings rather than silently discarding any.

```
$ ./build/distkv-client get user:123
Key: user:123
CONFLICT: 2 concurrent versions detected!
  Version 1: Alice (vector clock: map[node1:1])
  Version 2: Bob (vector clock: map[node2:1])
Please resolve the conflict by writing the correct value with PUT.
```

To resolve a conflict, write the merged value through the gRPC API with the Get response's `vector_clock` as the causal context: it covers every sibling, so the new version supersedes them all. A `put` from the CLI carries no context, so it is concurrent with the siblings rather than descended from them.

## Kubernetes Deployment

```bash
# Deploy 3-node StatefulSet with headless service
kubectl apply -f deploy/k8s/distkv-cluster.yaml

# Validate quorum replication, pod-failure recovery, and scale-out
./scripts/k8s-validate.sh
```

The StatefulSet uses `--advertise-address=$(POD_IP):8080` so each pod advertises its real IP to gossip peers rather than `0.0.0.0`.

## Docker

```bash
# Build image
make docker-build

# 3-node cluster
docker-compose up -d
docker-compose exec distkv-node1 ./distkv-client status
docker-compose down

# With monitoring (Prometheus + Grafana)
docker-compose --profile with-monitoring up -d
# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000 (admin/admin)
```

For production Docker configuration see `deploy/docker/`.

## Cluster Status

```bash
./build/distkv-client status
```

```
=== Cluster Status ===
Health: 3 total nodes, 3 alive, 0 dead (100.0% availability)

=== Nodes ===
  node1 (localhost:8080) - ALIVE - Last seen: 2025-09-05T22:45:29-07:00
  node2 (localhost:8081) - ALIVE - Last seen: 2025-09-05T22:45:28-07:00
  node3 (localhost:8082) - ALIVE - Last seen: 2025-09-05T22:45:27-07:00

=== Metrics ===
Total requests: 5234
Average latency: 0.00 ms
```

## TLS Security

```bash
# Generate development certificates
./scripts/generate-certs.sh

# Start server with TLS
./build/distkv-server \
  -node-id=node1 -address=localhost:8080 \
  -tls-enabled=true \
  -tls-cert-file=./certs/server-cert.pem \
  -tls-key-file=./certs/server-key.pem \
  -tls-ca-file=./certs/ca-cert.pem

# Connect client with TLS
./build/distkv-client \
  -tls-enabled=true \
  -tls-ca-file=./certs/ca-cert.pem \
  put mykey "secure value"
```

> **Warning**: Certificates generated by `generate-certs.sh` are for development only. Never use self-signed certificates or commit private keys in production.

For detailed TLS configuration see [docs/TLS_SETUP.md](docs/TLS_SETUP.md).

## Project Structure

```
DistKV/
├── cmd/
│   ├── server/                  # Server entry point, gRPC services, bootstrap, cleanup
│   └── client/                  # CLI client
├── pkg/
│   ├── consensus/               # Vector clocks and per-node version counter
│   ├── errors/                  # Structured errors with codes and context
│   ├── gossip/                  # Gossip protocol, failure detection, connection pool
│   ├── logging/                 # Component-based structured logging
│   ├── metrics/                 # Storage, replication, gossip, network metrics
│   ├── partition/               # Consistent hashing with virtual nodes
│   ├── replication/             # Quorum read/write, anti-entropy, read repair
│   ├── storage/                 # LSM-tree engine: WAL, MemTable, SSTable, Bloom filter, compaction
│   └── tls/                     # TLS credential loading
├── proto/                       # Protobuf definitions (generated files not committed)
├── tests/
│   ├── integration/             # End-to-end cluster tests
│   ├── chaos/                   # Fault injection: partition, recovery, scale-out
│   └── benchmark/               # QPS and latency benchmark tool
├── deploy/
│   ├── k8s/                     # Kubernetes StatefulSet manifests
│   └── docker/                  # Docker Compose configurations
├── scripts/                     # Build, cluster management, cert generation, K8s validation
└── docs/                        # TLS setup, API reference, operations guide
```

## Troubleshooting

**`protoc: command not found`**
```bash
# macOS
brew install protobuf
# Ubuntu
sudo apt install protobuf-compiler
```

**Missing `.pb.go` files**
```bash
./scripts/generate-proto.sh
```

**`bind: address already in use`**
```bash
lsof -i :8080
make stop-cluster
```

**`connection refused` from client**
```bash
# Ensure server is running
./build/distkv-server --node-id=node1 --address=localhost:8080 --data-dir=./data
```

## Learning Resources

This project demonstrates:
- **CAP Theorem**: Configurable quorum parameters for consistency vs availability
- **Consistent Hashing**: Minimize data movement during scaling with virtual nodes
- **Vector Clocks**: Track causality without global coordination
- **Quorum Consensus**: Balance consistency and availability with N/R/W
- **Gossip Protocols**: Decentralized failure detection and cluster coordination
- **LSM-trees**: Write-optimized storage with MemTables, SSTables, and compaction

## Acknowledgments

- Inspired by the [Amazon Dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- Storage engine design influenced by Cassandra and LevelDB
