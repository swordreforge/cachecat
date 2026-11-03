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

For memory implications and host sizing, see `capacity_planning_en.md`.
