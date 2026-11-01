// Benchmark tool for DistKV — measures QPS and latency percentiles.
//
// Usage:
//
//	go run ./tests/benchmark -server=localhost:8080
//
// Start a local cluster first:
//
//	make dev-cluster
//
// Availability test (manual):
//
//	While the benchmark is running, kill one node to verify requests keep succeeding:
//	  pkill -f "distkv-server.*node2"
//	Requests should continue succeeding (quorum still reachable with 2 of 3 nodes).
//	Restart the node and confirm it rejoins:
//	  ./build/distkv-server --node-id=node2 --address=localhost:8081 --data-dir=data2 --seed-nodes=localhost:8080 &
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"distkv/proto"
)

const (
	numKeys   = 1_000
	valueSize = 64
)

// benchDuration is how long each concurrency level runs (-duration).
var benchDuration = 5 * time.Second

func main() {
	server := flag.String("server", "localhost:8080", "DistKV server address")
	clusterSize := flag.Int("nodes", 3, "Number of nodes in the cluster")
	replicas := flag.Int("replicas", 3, "Replication factor (N)")
	readQuorum := flag.Int("read-quorum", 2, "Read quorum (R)")
	writeQuorum := flag.Int("write-quorum", 2, "Write quorum (W)")
	duration := flag.Duration("duration", benchDuration, "How long to run each concurrency level")
	mode := flag.String("mode", "all", "Phases to run: all, read (read-only), or mixed (50/50)")
	levelsFlag := flag.String("concurrency", "10,25,50,100,150,200", "Comma-separated concurrency levels to run")
	flag.Parse()
	var levels []int
	for _, field := range strings.Split(*levelsFlag, ",") {
		level, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || level <= 0 {
			fmt.Fprintf(os.Stderr, "invalid -concurrency %q\n", *levelsFlag)
			os.Exit(1)
		}
		levels = append(levels, level)
	}
	benchDuration = *duration
	if *mode != "all" && *mode != "read" && *mode != "mixed" {
		fmt.Fprintf(os.Stderr, "invalid -mode %q: want all, read or mixed\n", *mode)
		os.Exit(1)
	}

	conn, err := grpc.Dial(*server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to %s: %v\n", *server, err)
		os.Exit(1)
	}
	defer conn.Close()
	client := proto.NewDistKVClient(conn)

	fmt.Printf("Cluster: %d nodes  |  N=%d R=%d W=%d  |  %s per level\n\n",
		*clusterSize, *replicas, *readQuorum, *writeQuorum, benchDuration)

	// Pre-load keys
	fmt.Printf("Pre-loading %d keys into %s...\n", numKeys, *server)
	preload(client, numKeys)
	fmt.Println("Pre-load done.")

	header := fmt.Sprintf("%-12s %-12s %-10s %-10s %-10s %-10s",
		"Concurrency", "QPS", "p50(ms)", "p95(ms)", "p99(ms)", "Errors%")
	divider := fmt.Sprintf("%-12s %-12s %-10s %-10s %-10s %-10s",
		"───────────", "───────────", "────────", "────────", "────────", "────────")

	if *mode != "mixed" {
		// Phase 1: read-only
		fmt.Println("\n=== Phase 1: Read-only ===")
		fmt.Println(header)
		fmt.Println(divider)
		readMaxQPS := 0.0
		for _, c := range levels {
			qps, p50, p95, p99, errRate := runBenchmark(client, c, 1.0)
			marker := ""
			if p99 > 10 {
				marker = "  ← p99>10ms"
			} else {
				readMaxQPS = qps
			}
			fmt.Printf("%-12d %-12.0f %-10.2f %-10.2f %-10.2f %-10.2f%s\n",
				c, qps, p50, p95, p99, errRate, marker)
		}
		fmt.Printf("\nPeak read QPS (p99<10ms): %.0f\n", readMaxQPS)
	}

	if *mode != "read" {
		// Phase 2: mixed 50/50
		fmt.Println("\n=== Phase 2: Mixed read/write (50/50) ===")
		fmt.Println(header)
		fmt.Println(divider)
		mixedMaxQPS := 0.0
		for _, c := range levels {
			qps, p50, p95, p99, errRate := runBenchmark(client, c, 0.5)
			marker := ""
			if p99 > 10 {
				marker = "  ← p99>10ms"
			} else {
				mixedMaxQPS = qps
			}
			fmt.Printf("%-12d %-12.0f %-10.2f %-10.2f %-10.2f %-10.2f%s\n",
				c, qps, p50, p95, p99, errRate, marker)
		}
		fmt.Printf("\nPeak mixed QPS (p99<10ms): %.0f\n", mixedMaxQPS)
	}
}

// preload writes numKeys random key-value pairs into the cluster.
func preload(client proto.DistKVClient, n int) {
	value := make([]byte, valueSize)
	rand.Read(value)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = client.Put(ctx, &proto.PutRequest{Key: key, Value: value})
		cancel()
		if (i+1)%1000 == 0 {
			fmt.Printf("  %d / %d\n", i+1, n)
		}
	}
}

// runBenchmark runs a benchmark for benchDuration with the given concurrency
// and read ratio, returning (QPS, p50_ms, p95_ms, p99_ms, errorRate%).
func runBenchmark(client proto.DistKVClient, concurrency int, readRatio float64) (qps, p50, p95, p99, errRate float64) {
	var (
		mu        sync.Mutex
		latencies []time.Duration
		totalOps  int64
		totalErrs int64
		stop      = make(chan struct{})
		wg        sync.WaitGroup
	)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value := make([]byte, valueSize)
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("bench-key-%d", rand.Intn(numKeys))
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				var opErr bool
				if rand.Float64() < readRatio {
					resp, err := client.Get(ctx, &proto.GetRequest{Key: key})
					opErr = err != nil || (resp != nil && !resp.Found && resp.ErrorMessage != "")
				} else {
					rand.Read(value)
					resp, err := client.Put(ctx, &proto.PutRequest{Key: key, Value: value})
					opErr = err != nil || (resp != nil && !resp.Success)
				}
				cancel()
				lat := time.Since(start)

				atomic.AddInt64(&totalOps, 1)
				if opErr {
					atomic.AddInt64(&totalErrs, 1)
				}
				mu.Lock()
				latencies = append(latencies, lat)
				mu.Unlock()
			}
		}()
	}

	time.Sleep(benchDuration)
	close(stop)
	wg.Wait()

	ops := atomic.LoadInt64(&totalOps)
	errs := atomic.LoadInt64(&totalErrs)
	qps = float64(ops) / benchDuration.Seconds()
	if ops > 0 {
		errRate = float64(errs) / float64(ops) * 100
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	n := len(latencies)
	p50 = percentileMs(latencies, n, 0.50)
	p95 = percentileMs(latencies, n, 0.95)
	p99 = percentileMs(latencies, n, 0.99)
	return
}

func percentileMs(sorted []time.Duration, n int, pct float64) float64 {
	if n == 0 {
		return 0
	}
	idx := int(float64(n)*pct) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return float64(sorted[idx]) / float64(time.Millisecond)
}
