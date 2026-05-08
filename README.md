# DistKV

Distributed key-value store built from scratch in Go, inspired by Amazon's Dynamo. It demonstrates core distributed systems concepts including consistent hashing, quorum consensus, vector clocks, and gossip protocols.

## Features

- **Fault Tolerance**: Quorum replication (N=3, R=2, W=2) sustains reads and writes through single-node failures
- **Tunable Consistency**: Configurable N/R/W quorum parameters for consistency vs availability trade-offs
- **Persistent Storage**: LSM-tree storage engine with MemTables, SSTables, Bloom filters, and level-based compaction
- **Gossip Protocol**: Network-based gossip for cluster membership and failure detection
- **Consistent Hashing**: Virtual node-based partitioning with minimal data movement when scaling
- **Vector Clocks**: Conflict detection and causality tracking with Dynamo-style sibling preservation
- **Read Repair**: Stale replicas are automatically updated during quorum reads
- **Anti-Entropy**: Background 30-second sync detects and repairs divergent data across nodes
- **TLS Security**: TLS 1.2+ encryption for all client-server and inter-node communication
- **Kubernetes**: StatefulSet deployment with headless service discovery

## System Requirements

- Go 1.19 or later
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
# - Go 1.19+: https://golang.org/dl/
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

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Client    │     │   Client    │     │   Client    │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┴───────────────────┘
                           │
                    ┌──────▼──────┐
                    │ Coordinator │
                    │   Nodes     │
                    └──────┬──────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   ┌────▼────┐       ┌────▼────┐       ┌────▼────┐
   │Storage  │       │Storage  │       │Storage  │
   │Node A   │◄─────►│Node B   │◄─────►│Node C   │
   └─────────┘       └─────────┘       └─────────┘
```

### Key Technologies

- **Storage Engine**: LSM-tree with MemTables, SSTables, Bloom filters, and level-based compaction (7 levels, 10x growth per level)
- **Partitioning**: Consistent hashing with 150 virtual nodes
- **Replication**: Quorum-based consensus (default: N=3, R=2, W=2)
- **Conflict Resolution**: Vector clocks with Dynamo-style sibling preservation
- **Read Repair**: Coordinator detects stale replicas during quorum reads and pushes updates asynchronously
- **Anti-Entropy**: 30-second background sync; nodes exchange a hash of all local entries and apply missing or causally newer keys
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
  -consistency string     Consistency level: one, quorum, all (default: quorum)

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
  status                  Show cluster status
```

## Testing

### Unit Tests

```bash
make test

# Run specific packages
go test ./pkg/consensus/...   # Vector clock tests (17 tests)
go test ./pkg/storage/...     # Storage engine tests (28+ tests)
go test ./pkg/partition/...   # Consistent hashing tests (22 tests)
go test ./pkg/gossip/...      # Gossip protocol tests
go test ./pkg/replication/... # Replication tests
```

### Integration Tests

Integration tests start their own cluster automatically — no need to run `make dev-cluster` first.

```bash
go test -v -timeout=120s ./tests/integration/...
```

Covers: basic put/get/delete, consistency levels (ONE/QUORUM/ALL), node failure, concurrent writes, sibling preservation.

### Chaos Tests

```bash
make chaos-test
```

Covers: network partition and recovery, sibling preservation under partition, cluster scale-out (3→4 nodes with anti-entropy sync).

### Benchmark

```bash
make dev-cluster
make benchmark
```

Sweeps concurrency levels (10→200) for read-only and mixed (50/50 read/write) workloads, reporting QPS, p50/p95/p99 latency, and error rate.

## Consistency Models

### Strong Consistency (W + R > N)
```bash
# Default: N=3, W=2, R=2 — reads always return the latest write
./build/distkv-client -consistency=quorum put key value
```

### Eventual Consistency (W + R ≤ N)
```bash
# N=3, W=1, R=1 — high availability, eventual consistency
./build/distkv-client -consistency=one put key value
```

### All Replicas (W=N, R=1)
```bash
# All replicas must acknowledge writes — strongest consistency, lower availability
./build/distkv-client -consistency=all put key value
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

$ ./build/distkv-client put user:123 "Alice and Bob"
```

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
│   ├── server/                  # Server entry point, gRPC services, node routing
│   └── client/                  # CLI client
├── pkg/
│   ├── consensus/               # Vector clocks (17 tests)
│   ├── errors/                  # Structured errors with codes and context (18 tests)
│   ├── gossip/                  # Gossip protocol, failure detection, connection pool
│   ├── logging/                 # Component-based structured logging (15 tests)
│   ├── metrics/                 # Storage, replication, gossip, network metrics (11 tests)
│   ├── partition/               # Consistent hashing with virtual nodes (22 tests)
│   ├── replication/             # Quorum read/write, anti-entropy, read repair
│   ├── storage/                 # LSM-tree engine: MemTable, SSTable, Bloom filter, compaction
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
