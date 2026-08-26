# Distributed Key-Value Store

A distributed, replicated key-value store built from scratch in Python (FastAPI)
to demonstrate core distributed-systems concepts: consistent hashing, synchronous
replication, quorum writes, and failure handling. Built as a portfolio project,
not a production system — scope is deliberately narrow (see "Out of Scope" below).

## What It Does

- Runs as 5 identical FastAPI nodes in Docker containers, each exposing
  `PUT /key/{k}`, `GET /key/{k}`, `DELETE /key/{k}`.
- Keys are assigned to nodes via **consistent hashing with virtual nodes**
  (a static ring built once at startup — no live rebalancing).
- Every write is **synchronously replicated** to a key's primary + 2 replicas
  (3 nodes total per key, out of 5 in the cluster).
- Writes succeed as long as a **write quorum** (2 of 3) acknowledges — a single
  down node doesn't block reads or writes for keys it holds.
- Reads **fail over** across the preference list: if the first node to try is
  down, the coordinator tries the next one automatically.
- Every request logs structured (JSON-lines) data: latency, running throughput,
  and (for writes) replication timing — designed to be parsed into real
  percentile numbers, not just eyeballed.
- A standalone load-test script (`load_test.py`) generates real throughput/
  latency numbers against a running cluster.

## Architecture

                 ┌─────────────┐
    client ───▶  │ any node    │  (acts as "coordinator" for this request)
                 └──────┬──────┘
                        │  1. hash(key) -> preference list via consistent hash ring
                        │     e.g. ["node-1", "node-4", "node-2"]
                        ▼
    ┌───────────────────┼───────────────────┐
    ▼                   ▼                   ▼
┌─────────┐ ┌─────────┐ ┌─────────┐
│ node-1 │ │ node-4 │ │ node-2 │ <- preference list
│(primary)│ │(replica)│ │(replica)│ for this key
└─────────┘ └─────────┘ └─────────┘

PUT: coordinator writes to all 3, waits for quorum (2/3) before ack
GET: coordinator tries node-1 first; if down, falls through to node-4, then node-2

Cluster has 5 nodes total (node-1..node-5). Each key lives on only
3 of the 5 — the other 2 simply don't have it (by design).



### Key Design Decisions

| Concept | Choice made | Why |
|---|---|---|
| Key placement | Consistent hashing + virtual nodes (150/node) | Avoids full remap of keys when a node fails (vs. naive `hash % N`); virtual nodes smooth key distribution across few physical nodes |
| Replication | Synchronous | Client only sees success once quorum is durable — simpler consistency model than async, at the cost of write latency |
| Write success threshold | Quorum (2 of 3), not strict (3 of 3) | Keeps writes available during a single node failure ("sloppy quorum") — tradeoff: a node that was down during a write is silently stale until it receives a future write for that key (no hinted handoff/read-repair implemented) |
| Read routing | Try preference list in order, first success wins | Transparent failover — client never needs to know a node was down |
| Ring rebalancing | Static, built once at startup | Explicitly out of scope — real systems (Cassandra, DynamoDB) support live ring changes; this project demonstrates the hashing/replication concepts, not cluster membership protocols |

## How to Run

Requires Docker Desktop running.

```bash
docker compose up --build
```

This starts 5 nodes (`node-1` through `node-5`), reachable at:
- `localhost:8001` … `localhost:8005`

Basic usage:

```bash
# Write a key (works from any node - it'll route internally)
curl -X PUT localhost:8001/key/foo -H "Content-Type: application/json" -d '{"value": "bar"}'

# Read it back (from any node)
curl localhost:8003/key/foo

# Delete it
curl -X DELETE localhost:8002/key/foo

# See which nodes actually hold a key's data
curl localhost:8001/debug/keys
```

### Demoing Failure Handling

```bash
# 1. Write a key, note its preference_list in the response
curl -X PUT localhost:8001/key/demo -H "Content-Type: application/json" -d '{"value": "test"}'

# 2. Find the real container name for one node in that list
docker ps

# 3. Kill it
docker stop distributed-kv-store-node-X-1

# 4. Reads/writes for that key still succeed (failover + quorum)
curl localhost:8003/key/demo
curl -X PUT localhost:8003/key/demo -H "Content-Type: application/json" -d '{"value": "test2"}'

# 5. Bring it back — it will be stale until a future write touches this key
docker start distributed-kv-store-node-X-1
curl localhost:8004/debug/keys
```

### Running the Load Test

```bash
python load_test.py --requests 500 --read-ratio 0.5
```

Streams progress and prints a summary with throughput and p50/p95/p99 latency.
Logs from the cluster itself (structured JSON) can be captured via:

```bash
docker compose logs -f > cluster_logs.jsonl
```

## Out of Scope (Deliberately)

- No transactions, query language, or UI
- No dynamic ring rebalancing on node join/leave (static ring only)
- No async/eventual replication (synchronous only)
- No authentication/authorization
- No hinted handoff or read-repair (a node down during a write stays stale
  until directly written to again)
- Load test is sequential, not truly concurrent (no asyncio/thread pool) —
  numbers reflect one client's sequential latency, not peak cluster throughput

## My Test Results

Total requests attempted: 300
Successful responses: 300
Errors (connection-level): 0
Status code breakdown: {200: 300}
Total wall time: 8.16s
Observed throughput: 36.78 ops/sec
Latency p50: 16.00 ms
Latency p95: 63.00 ms
Latency p99: 63.00 ms
Latency mean: 27.19 ms
Latency max: 63.00 


Total requests attempted: 1000
Successful responses:     1000
Errors (connection-level): 0
Status code breakdown:    {200: 1000}
Total wall time:          26.92s
Observed throughput:      37.14 ops/sec
Latency p50:              16.00 ms
Latency p95:              63.00 ms
Latency p99:              63.00 ms
Latency mean:             26.92 ms
Latency max:              63.00 ms


Environment: 5 nodes, replication factor 3, write quorum 2, Docker Desktop on Windows.

Note: p95/p99/max are identical, suggesting a discrete slow path (likely replication/forwarding across container network) rather than gradual latency spread — worth deeper profiling as a next step.