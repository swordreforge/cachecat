# Fork notes

This repository is a fork of
[nasuiyile/cache-cat](https://github.com/nasuiyile/cache-cat).
Upstream history is retained; the last upstream commit included here is
`5ed250c` ("Removed some useless code"). All fork work is in the commits after it:

- Compare: <https://github.com/swordreforge/cachecat/compare/5ed250c...master>
- Local view: `git log --oneline 5ed250c..HEAD`

## What changed

### Redis Stream command surface

Added the missing consumer-group, metadata and Redis 8.2 commands:

- `XLEN`, `XRANGE`, `XREVRANGE`, `XINFO`
- `XPENDING`, `XACK`, `XACKDEL`, `XCLAIM`, `XAUTOCLAIM`
- `XREADGROUP` (including blocking reads)
- `XDEL`, `XDELEX`, `XTRIM`, `XSETID`

All are registered in the server and Lua command factories, so they work in
`MULTI/EXEC` and `EVAL`.

### Blocking reads and wake-ups

- Replaced the untyped blocking context with a single `StreamWaiter` registry for
  XREAD and XREADGROUP.
- Stream writes publish one generic `changed_streams` event; XREAD is evaluated only
  on appends, while group waiters re-check their group state.
- New entries wake at most one waiter per `(key, group)`; chained delivery continues
  while entries remain.
- Destroyed/recreated groups answer `NOGROUP` directly, without a wasted Raft write.
- `DEL`/`UNLINK` of a stream and `SET`/`MSET` overwriting a stream now wake blocked
  group readers (`NOGROUP`/`WRONGTYPE`) while plain XREAD stays blocked, matching
  Redis' `unblock_on_nokey`.
- An empty blocking XREADGROUP submits the replicated read only when its consumer is
  newly created; an existing consumer is deferred until first delivery.
- Removed the unused local `SharedStream`/`Notify` blocking stack; the Raft-aware
  registry is the only wake-up mechanism.

### Replication and determinism

- Added clock-parameterised core methods (`xreadgroup_at`, `xclaim_at`,
  `xpending_range_at`, `xinfo_consumers_at`, `xinfo_stream_full_at`,
  `xautoclaim_at`) so PEL timestamps and consumer times come from the replicated
  write clock.
- Added a replicated consumer-group generation and stream snapshot version 4, closing
  the destroy+recreate race for blocked XREADGROUP clients.
- Added snapshot replay and bincode round-trip coverage for the new operations.

### Correctness fixes

- Snapshot file `VERSION` was still 1 while the documented metadata format is 2.
- Variants declared after `RedisOperation::RedisEvalSha` were encoded with an
  out-of-range serde index because that variant uses `#[serde(skip)]`.

### Docs and tests

- Added English/Chinese stream design documents, capacity planning and an official
  YCSB benchmark note under `docs/`.
- Current test suite: `cargo test -p cache_cat --lib -- --skip test::tests::test_add`
  (606 passing).

## Compatibility

This is an alpha fork and does not promise state/log compatibility with upstream:

- The Stream snapshot version is now 4.
- Raft log/snapshot payloads include the new stream operations and clock fields.
- Existing upstream snapshot files should be treated as incompatible unless migrated.

## License

The upstream project and this fork are licensed under the Apache License 2.0. See
`LICENSE`. The upstream project also credits code derived from
[lichuang/coredb](https://github.com/lichuang/coredb) and
[lichuang/rockraft](https://github.com/lichuang/rockraft).
