# Official YCSB Redis benchmark

This document records a run of the official [YCSB](https://github.com/brianfrankcooper/YCSB)
Redis binding against a single cache-cat node. It is separate from the custom
`benchmark/` harness in this repository.

## Environment

- YCSB `0.17.0`, Redis binding `0.17.0` (Jedis `2.9.0`)
- cache-cat release binary, single node
- Raft: `127.0.0.1:5001`; Redis protocol: `127.0.0.1:6380`
- Host: 22 vCPU, 30 GiB RAM
- Data: 1,000,000 records, about 1 KB each (10 fields x 100 B)
- Request distribution: zipfian
- Threads: 64
- Workloads: A (50/50 read/update), B (95/5), C (100% read)

Note: YCSB's Redis binding stores each record as a Redis hash and uses
`HMSET`/`HGETALL` (not string `SET`/`GET`). These results therefore measure
hash-heavy workloads as produced by the stock YCSB binding.

YCSB's `bin/ycsb` is a Python 2 script. This machine has no Python 2, so the
equivalent `bin/ycsb.sh` launcher was used.

## Reproduction

Start the server:

```bash
./target/release/cache_cat \
  --conf cache_cat/conf/node1.toml \
  --log-path /tmp/cc-ycsb/log \
  --redis-port 6380 \
  --address 127.0.0.1:5001
```

Load the data and run the workloads:

```bash
export YCSB_HOME=/path/to/ycsb-0.17.0

"$YCSB_HOME/bin/ycsb.sh" load redis -s -P "$YCSB_HOME/workloads/workloada" \
  -p redis.host=127.0.0.1 -p redis.port=6380 \
  -p recordcount=1000000 -p operationcount=0 -p threadcount=64

for w in a b c; do
  "$YCSB_HOME/bin/ycsb.sh" run redis -s -P "$YCSB_HOME/workloads/workload$w" \
    -p redis.host=127.0.0.1 -p redis.port=6380 \
    -p recordcount=1000000 -p operationcount=1000000 -p threadcount=64
done
```

## Results

Load phase:

| Operation | Count | Runtime | Throughput | Avg | P95 | P99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| INSERT | 1,000,000 | 21.105 s | 47,382 ops/s | 1,342 µs | 2,433 µs | 3,309 µs |

Run phase:

| Workload | Ops | Runtime | Throughput | Read avg / P99 | Update avg / P99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| A (50/50) | 1,000,000 | 8.345 s | **119,832 ops/s** | 191 µs / 691 µs | 866 µs / 1,986 µs |
| B (95/5) | 1,000,000 | 2.937 s | **340,483 ops/s** | 176 µs / 569 µs | 324 µs / 1,183 µs |
| C (100% read) | 1,000,000 | 2.502 s | **399,680 ops/s** | 156 µs / 522 µs | — |

All operations returned `OK`; no errors were reported.

## Interpretation

- Updates are the write bottleneck: Raft append/apply makes update latency roughly
  4-5x the read latency in workload A.
- Read-heavy workloads reach roughly 340k-400k ops/s with 64 threads on this host.
- Loading 1M records is a cold, write-only phase and is much slower than read
  throughput because every insert is a replicated write.
- These numbers are for a single local node with no network or TLS overhead, 1 KB
  values and a 1M-key space. They are not a cluster benchmark and should not be
  treated as universal capacity numbers.

## Profiling notes

A `perf record -e task-clock -F 199 -g --call-graph dwarf` run during workload A
(50/50) and workload C (100% read) showed:

- allocator work (`mi_free`, `_mi_page_malloc_zero`, page collection) is a large
  user-space cost;
- `Bytes` clone/drop and RESP/hash encoding are visible on the read path;
- Tokio scheduling and blocking-pool task polling are visible;
- `raft_engine::Engine::write` appears on the write path, but no `memcpy`,
  `sendfile` or `copy_file_range` appears near the top;
- roughly 40% of samples are unattributed kernel frames because kernel symbols are
  masked on this host, so the profile cannot separate network/syscall/fsync costs
  further.

Implication: for these 1 KB hash workloads, zero-copy would not address the top
user-space costs, and io_uring is not an obvious first-order win. The more useful
next steps are reducing allocation/clone work on HGETALL/encoding, batching Raft log
writes, and streaming snapshots. Kernel/syscall profiling with resolved symbols is
required before treating io_uring as a priority.

For memory implications and host sizing, see `capacity_planning_en.md`.
