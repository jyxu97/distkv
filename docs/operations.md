# DistKV Operations Guide

This guide covers operational aspects of running DistKV in production.

## Cluster Operations

### Starting a New Cluster

1. **Start the first node (seed node)**:
```bash
./distkv-server -node-id=node1 -address=10.0.1.10:8080 -data-dir=/var/lib/distkv
```

2. **Start additional nodes**:
```bash
./distkv-server -node-id=node2 -address=10.0.1.11:8080 -data-dir=/var/lib/distkv -seed-nodes=10.0.1.10:8080
./distkv-server -node-id=node3 -address=10.0.1.12:8080 -data-dir=/var/lib/distkv -seed-nodes=10.0.1.10:8080
```

3. **Verify cluster formation**:
```bash
./distkv-client -server=10.0.1.10:8080 status
```

### Adding Nodes

1. **Prepare the new server**:
   - Install DistKV binary
   - Create data directory
   - Configure network access

2. **Start new node with seed nodes**:
```bash
./distkv-server -node-id=node4 -address=10.0.1.13:8080 -data-dir=/var/lib/distkv -seed-nodes=10.0.1.10:8080,10.0.1.11:8080
```

   The node joins in a `joining` state: it receives writes for its new token
   ranges but serves no reads until it has streamed those ranges from its
   peers. Startup logs `Bootstrap complete, serving reads` when done.

3. **Verify node joined**:
```bash
./distkv-client status
# Should show 4 nodes
```

### Removing Nodes

1. **Graceful shutdown**:
```bash
# Send SIGTERM to the process
kill -TERM <pid>
# Or use systemctl if running as service
systemctl stop distkv
```

2. **Remove from cluster** (via admin API):
```bash
curl -X POST http://10.0.1.10:8080/admin/remove-node \
  -d '{"node_address": "10.0.1.13:8080"}'
```

3. **Clean up data** (after ensuring data is replicated):
```bash
rm -rf /var/lib/distkv
```

## Monitoring and Alerting

### Key Metrics to Monitor

#### Cluster Health
- **Node availability**: Percentage of nodes that are alive
- **Replica health**: Number of under-replicated keys
- **Network partitions**: Nodes that can't communicate

#### Performance Metrics
- **Request latency**: p50, p95, p99 for reads and writes  
- **Request rate**: QPS per operation type
- **Error rate**: Percentage of failed requests
- **Connection count**: Number of active client connections

#### Storage Metrics
- **Disk usage**: Per node and total cluster
- **MemTable size**: Current memory usage
- **SSTable count**: Number of files per node
- **Compaction rate**: Background compaction activity

### Health Checks

#### Node Health Check
```bash
#!/bin/bash
# health-check.sh
RESPONSE=$(./distkv-client -server=localhost:8080 -timeout=3s status 2>/dev/null)
if [ $? -eq 0 ] && echo "$RESPONSE" | grep -q "ALIVE"; then
    echo "healthy"
    exit 0
else
    echo "unhealthy"  
    exit 1
fi
```

#### Cluster Health Check
```bash
#!/bin/bash
# cluster-health.sh
STATUS=$(./distkv-client status -server=localhost:8080)
ALIVE_NODES=$(echo "$STATUS" | grep -o '[0-9]* alive' | cut -d' ' -f1)
TOTAL_NODES=$(echo "$STATUS" | grep -o '[0-9]* total' | cut -d' ' -f1)

if [ "$ALIVE_NODES" -ge 2 ] && [ "$ALIVE_NODES" -gt "$((TOTAL_NODES / 2))" ]; then
    echo "cluster healthy: $ALIVE_NODES/$TOTAL_NODES nodes alive"
    exit 0
else
    echo "cluster unhealthy: $ALIVE_NODES/$TOTAL_NODES nodes alive"
    exit 1
fi
```

### Alerting Rules

#### Critical Alerts
- Node down for > 5 minutes
- Cluster availability < 66% (less than majority)
- Request error rate > 5%
- Disk usage > 85%

#### Warning Alerts
- Node suspected (missed heartbeats)
- Request latency p99 > 50ms
- Compaction behind by > 2 hours
- Memory usage > 80%
- Sibling conflict rate > 1% of reads (see Conflict Monitoring below)

## Conflict Monitoring

### Understanding Siblings

DistKV uses Dynamo-style sibling preservation for concurrent writes. When two nodes accept writes to the same key without coordinating (e.g., during a network partition or with `ONE` consistency), the system preserves **all concurrent versions as siblings** rather than silently discarding any. The client is responsible for resolving conflicts.

### Detecting Conflicts

When a `GET` response has `has_conflict = true`, the `siblings` field contains all concurrent versions. Monitor the rate of conflicted reads:

```bash
# Check logs for conflict events
grep "CONFLICT" /var/log/distkv/distkv.log | wc -l

# Monitor conflict rate via metrics
# The replication metrics track conflict resolution stats
./distkv-client status | grep -i conflict
```

### Common Causes of Siblings

| Cause | Solution |
|-------|----------|
| Network partition healed with divergent writes | Application-level merge on read |
| High write concurrency with `ONE` consistency | Use `QUORUM` consistency for writes |
| Clock skew between nodes | Ensure NTP synchronization |
| Node rejoining after prolonged downtime | Wait for anti-entropy repair to complete |

### Resolving Siblings

When the client detects siblings, it should:

1. Read all sibling values and their vector clocks
2. Apply application-specific merge logic (e.g., last-writer-wins, union, custom merge)
3. Write back the resolved value with a `PUT`, which creates a new vector clock that supersedes all siblings

```bash
# Example: client detects conflict
$ ./distkv-client get user:123
Key: user:123
CONFLICT: 2 concurrent versions detected!
  Version 1: Alice (vector clock: map[node1:1])
  Version 2: Bob (vector clock: map[node2:1])
Please resolve the conflict by writing the correct value with PUT.

# Resolve by writing the merged value
$ ./distkv-client put user:123 "Alice and Bob"
```

### Reducing Conflict Frequency

- **Use `QUORUM` or `ALL` consistency** for writes to ensure coordination
- **Minimize partition duration** — monitor and alert on network issues
- **Avoid writing the same key from multiple nodes simultaneously** when possible
- **Keep NTP synchronized** to reduce the window for concurrent operations

## Backup and Recovery

### Data Backup

#### Snapshot Backup
```bash
#!/bin/bash
# backup.sh
NODE_DATA_DIR="/var/lib/distkv"
BACKUP_DIR="/backup/distkv/$(date +%Y%m%d_%H%M%S)"

# Stop writes (optional for consistency)
curl -X POST http://localhost:8080/admin/pause-writes

# Create snapshot
mkdir -p "$BACKUP_DIR"
cp -r "$NODE_DATA_DIR"/* "$BACKUP_DIR/"

# Resume writes
curl -X POST http://localhost:8080/admin/resume-writes

echo "Backup created: $BACKUP_DIR"
```

#### Incremental Backup
```bash
#!/bin/bash
# incremental-backup.sh  
LAST_BACKUP_TIME=$(cat /var/lib/distkv/.last_backup 2>/dev/null || echo "0")
CURRENT_TIME=$(date +%s)

# Find files modified since last backup
find /var/lib/distkv -type f -newer "/var/lib/distkv/.last_backup" 2>/dev/null |
while read file; do
    cp --parents "$file" "/backup/distkv/incremental/$(date +%Y%m%d)"
done

echo "$CURRENT_TIME" > /var/lib/distkv/.last_backup
```

### Disaster Recovery

#### Single Node Recovery
1. **Stop the failed node**
2. **Replace hardware if needed**
3. **Restore from backup**:
```bash
cp -r /backup/distkv/latest/* /var/lib/distkv/
chown -R distkv:distkv /var/lib/distkv
```
4. **Start the node**:
```bash
./distkv-server -node-id=node1 -address=10.0.1.10:8080 -data-dir=/var/lib/distkv -seed-nodes=10.0.1.11:8080
```
5. **Wait for anti-entropy to sync data**

#### Cluster Recovery
1. **If majority of nodes are still alive**:
   - Add new nodes to replace failed ones
   - Data will automatically replicate

2. **If majority of nodes failed**:
   - Restore from backup on new hardware
   - Start cluster from backup
   - May lose recent writes (RPO = backup frequency)

## Performance Tuning

### Storage Tuning

#### MemTable Configuration
```bash
# Increase MemTable size for write-heavy workloads
./distkv-server -mem-table-size=128000000  # 128MB

# Decrease for memory-constrained environments
./distkv-server -mem-table-size=32000000   # 32MB
```

#### Compaction Tuning
```bash
# More aggressive compaction for write-heavy workloads
./distkv-server -compaction-threshold=2

# Less aggressive for read-heavy workloads  
./distkv-server -compaction-threshold=8
```

### Network Tuning

#### Connection Limits
```bash
# Increase for high-concurrency applications
ulimit -n 65536

# Configure in systemd service
echo "LimitNOFILE=65536" >> /etc/systemd/system/distkv.service
```

#### TCP Optimization
```bash
# /etc/sysctl.conf
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216  
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
```

### JVM Tuning (if using Go with C libraries)
```bash
# Memory allocation
export GOGC=100  # Default garbage collection target
export GOMAXPROCS=8  # Match CPU cores
```

## Troubleshooting

### Common Issues

#### Split Brain Detection
**Symptoms**: Conflicting data, unable to achieve quorum
```bash
# Check node connectivity
for node in node1 node2 node3; do
    echo "=== $node ==="
    ./distkv-client -server=$node:8080 status
done
```
**Solution**: Ensure network connectivity, restart minority partition

#### High Write Latency  
**Symptoms**: Write requests timing out, high p99 latency
```bash
# Check compaction status
grep "compaction" /var/log/distkv/distkv.log
```
**Solution**: Tune compaction settings, add more nodes

#### Memory Issues
**Symptoms**: OOM kills, high memory usage
```bash
# Check MemTable sizes
./distkv-client status | grep -i memory
```
**Solution**: Reduce MemTable size, add more RAM

### Debug Commands

#### Network Connectivity
```bash
# Test gRPC connectivity
grpc_cli call 10.0.1.10:8080 distkv.NodeService/Gossip \
  "sender_id:'debug' node_updates:[]"
```

#### Storage Inspection
```bash
# Check SSTable files
ls -la /var/lib/distkv/storage/*.db
ls -la /var/lib/distkv/storage/*.meta
```

#### Log Analysis
```bash
# Recent errors
tail -f /var/log/distkv/distkv.log | grep ERROR

# Slow queries  
grep "latency" /var/log/distkv/distkv.log | awk '{if($5>100)print}'
```

## Security Considerations

### Network Security
- Use TLS for client connections
- Implement firewall rules
- VPN for inter-datacenter communication

### Authentication
- Implement API key authentication
- Use mutual TLS for node-to-node communication
- Regular key rotation

### Data Security  
- Encrypt data at rest
- Encrypt data in transit
- Implement audit logging

### Access Control
- Least privilege principle
- Role-based access control
- Regular security audits