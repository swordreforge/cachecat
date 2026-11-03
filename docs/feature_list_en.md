# Feature List

## Features to be Supported in the Future

- Support for io_uring
- Visual interface (displaying the current running status of cache-cat)
- Compressed snapshots (when string length exceeds a threshold, e.g., 20 bytes, compress individual strings during snapshot writing)
- Support for importing Redis snapshot files
- MTLS
- Horizontal sharding / multiple Raft groups (currently every node stores the full data set, so adding nodes does not add capacity)

## Unsupported Features

**Eviction policies**: In any scenario that requires algorithms such as LRU or LFU, you should use Redis or Valkey directly instead of Cache-cat.

The reason is straightforward: Cache-cat uses a consensus algorithm, which, unlike Redis, ensures that data is not lost due to a single-node failure.

If your application semantics allow data to be discarded at any time and reloaded from the source when needed, then what you need is a cache such as Redis or Valkey.

Cache-cat is intended for scenarios where, once a write succeeds, the data must remain part of the cluster’s consistent state even in the event of a single-node failure. See `capacity_planning_en.md` for the capacity and replica memory model.