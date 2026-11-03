# Capacity planning and memory model

cache-cat is an in-memory, Raft-replicated state machine, not an eviction-based
cache. This document explains where memory goes, how to size a node, and why adding
nodes does not automatically add data capacity.

## 1. What a node stores

Every Raft member applies the full log to its own state machine. There is no
built-in sharding/partitioning, so each node stores **the entire data set**, not
`data_size / node_count`.

The process therefore needs memory for:

- the live keys and values;
- per-entry metadata (version, expiration, container nodes/buckets);
- hash/tree/skiplist indexes and allocator fragmentation;
- Raft apply buffers, in-flight requests and client connection buffers;
- snapshot construction and the incremental snapshot queue.

Raft logs and snapshots also consume disk; those are separate from the in-memory
state size but still belong in host capacity planning.

## 2. No eviction and no maxmemory

There is no LRU/LFU eviction and no `maxmemory` fallback. If the data set no longer
fits in the process's memory budget, the process is killed by the OOM killer or
fails allocation. It does not silently drop keys.

Use Redis/Valkey instead when data may be discarded and reloaded from the source.
Use cache-cat when a successful write must remain part of the cluster's consistent
state even after a single-node failure.

## 3. Sizing formula

Treat the following as a starting point, not a precise allocator model:

```text
per-node memory
  >= live data size
   * overhead factor (roughly 1.5x - 3x; higher for many small keys)
   + snapshot headroom
   + Raft/connection/allocator reserves
   + 20% - 30% OS headroom
```

The overhead factor depends on key/value sizes, data type (streams, sorted sets,
hashes, PELs), expiration metadata and fragmentation. Small keys have a much higher
relative overhead than large values.

Snapshot construction serializes the data set and can temporarily add a large
allocation on top of the live state; see `snapshot_en.md`. Plan the container or
host limit for the snapshot peak, not only the steady-state live set.

`MEMORY USAGE` reports an estimate of the logical container memory, not the entire
process RSS. Use `INFO memory` (`used_memory_rss`, `used_memory_peak_rss`) together
with real load tests for host sizing.

## 4. Scaling and deployment

- **Adding nodes increases fault tolerance and replication, not data capacity.**
  Every node still holds the full data set.
- To scale data beyond one node's memory, an external sharding layer or multiple
  Raft groups are required. That is not built into this repository today.
- Treat cache-cat nodes as dedicated, memory-optimized data nodes. Avoid
  co-locating them with other memory-heavy services.
- Spread replicas across hosts/failure domains (anti-affinity) so losing one host
  does not remove all copies of the data.
- Set container memory limits above the worst-case snapshot peak and leave OS
  headroom; otherwise the OOM killer will terminate the process.

## 5. Measured example

The official YCSB run in `benchmark_ycsb_en.md` loaded 1,000,000 records of about
1 KB each on a 30 GiB host. The raw payload is about 1 GB, while the server process
RSS was roughly 3 GB after the run. That factor is consistent with allocation,
metadata, indexes, Raft buffers and allocator retention; it is not a universal
multiplier.

At larger scales the same rule applies: 100 million 1 KB records would require
roughly 100 GB of raw payload **per replica**, before overhead and snapshot
headroom. That requires large-memory hosts or sharding, not simply more nodes.

## 6. Practical checklist

- Estimate raw data size and the expected overhead factor from a realistic load.
- Add snapshot peak memory and 20% - 30% OS headroom.
- Monitor `INFO memory` RSS and host memory pressure.
- Test with the real key/value sizes and data types, not only tiny strings.
- If you need elastic capacity with eviction, choose Redis/Valkey.
- If you need strong consistency with full replicas, size every node for the full
  data set.
