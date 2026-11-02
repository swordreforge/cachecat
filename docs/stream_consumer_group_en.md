# Stream consumer groups and blocking wake-ups

This document summarises the work that added the Redis Stream consumer-group commands to
cache-cat and rebuilt the blocking wake-up path. Everything described here is on `master`
(commits at the end) and was verified with unit/integration tests and a real node.

## 1. Scope

Before this work the protocol layer only exposed `XADD`, `XGROUP` and `XREAD`. Consumer-group
reads, the pending-entries list and claiming had no entry point, and blocking reads were only
woken by `XADD` with a single untyped waiter context, so a group read paid for a useless Raft
write on every wake-up and a destroyed group left clients blocked.

The work has two parts:

1. add `XLEN`, `XRANGE`, `XACK`, `XREADGROUP`, `XPENDING` and `XCLAIM`;
2. turn blocking wake-ups into "any stream write publishes a generic change event and each
   waiter re-checks its own state".

A follow-up batch then added the read-only `XREVRANGE` and `XINFO` (section 2.2), a
second batch added `XDEL` and `XTRIM` (section 2.3), and a third added `XAUTOCLAIM`
(section 2.4).

## 2. Commands

| Command | trait | replicated payload | notes |
| --- | --- | --- | --- |
| `XLEN` | `ReadCommand` | `ReadOperation::XLen` | entry count |
| `XRANGE` | `ReadCommand` | `ReadOperation::XRange` | interval plus `COUNT` |
| `XPENDING` | `ReadCommand` | `ReadOperation::XPending` | summary and extended forms |
| `XACK` | `ComputeCommand` | `BaseOperation::XAck` | clears PEL entries |
| `XCLAIM` | `ComputeCommand` | `BaseOperation::XClaim` | moves PEL ownership |
| `XREADGROUP` | multi-key write | `RedisOperation::RedisXReadGroup` + per-key `BaseOperation::XReadGroup` | atomic across keys, snapshots record single-key deltas |
| `XREVRANGE` | `ReadCommand` | `ReadOperation::XRevRange` | reverse interval plus `COUNT` |
| `XINFO` | `ReadCommand` | `ReadOperation::XInfo` | `STREAM` / `GROUPS` / `CONSUMERS` / `HELP` |
| `XDEL` | `ComputeCommand` | `BaseOperation::XDel` | removes entries |
| `XTRIM` | `ComputeCommand` | `BaseOperation::XTrim` | trims with a reference policy |
| `XAUTOCLAIM` | `ComputeCommand` | `BaseOperation::XAutoClaim` | scans the PEL and claims idle entries |

All eleven are registered in both the server factory and the Lua factory, so they work inside
`MULTI/EXEC` and `EVAL`; the last five arrived in the follow-up read-only, delete/trim and
auto-claim batches.

### 2.1 Redis-compatible details

Checked against `redis/8.0` `src/t_stream.c`:

- **XRANGE**: `-`, `+`, `(id` exclusive bounds and bare milliseconds; exclusive bounds are
  shifted inwards and overflow reports `ERR invalid start/end ID for the interval`; `(-` and
  `(+` are invalid; `COUNT 0` (and negatives) return a null array only when the key exists,
  a missing key returns an empty array; the last repeated `COUNT` wins.
- **XACK**: key, type and group are checked before any ID is parsed, so a missing key or
  group answers `0` even for a malformed ID; when a valid group exists an invalid ID is an
  error. A zero acknowledgement records no snapshot delta.
- **XREADGROUP**: `>` for new entries, explicit IDs for consumer history, `$`/`+` rejected
  with Redis' message; `COUNT`, `BLOCK` and `NOACK` supported; `NOGROUP` includes the
  `in XREADGROUP with GROUP option` suffix.
- **XPENDING**: summary `[total, min, max, [[consumer, count], ...]]`; extended
  `[id, consumer, idle, deliveries]` with `IDLE`, range, `count` and `consumer`; negative
  `IDLE` and `count` are clamped to zero.
- **XCLAIM**: `min-idle-time`, `IDLE`, `TIME`, `RETRYCOUNT`, `FORCE`, `JUSTID`, `LASTID`;
  IDs and options may be interleaved; `JUSTID` returns ids only.

### 2.2 Read-only follow-up

- **XREVRANGE**: the wire order is `end start`; the parser normalizes it into the same
  inclusive low/high bounds as XRANGE before replication. `COUNT` follows the XRANGE rules
  exactly (last wins, negatives clamp to zero, `COUNT 0` is a null array when the key exists,
  a missing key is an empty array). The COUNT parser is shared by both commands.
- **XINFO**: `STREAM [FULL [COUNT n]]`, `GROUPS`, `CONSUMERS` and `HELP`. A missing key is
  `ERR no such key` (not NOGROUP); a missing group in `CONSUMERS` is
  `NOGROUP No such consumer group '<group>' for key name '<key>'`. RESP2 uses Redis' flat
  field/value arrays and RESP3 uses maps.
- **Deterministic times**: `idle`/`inactive` and the FULL form's `seen-time`/`active-time`
  use the read's logical clock through new `xinfo_consumers_at` and `xinfo_stream_full_at`
  variants, so replicas agree. `FULL` defaults to `COUNT 10`, `COUNT 0` is unlimited and a
  negative count falls back to 10, as in Redis.
- **Field choice**: cache-cat stores entries in a B-tree rather than a radix tree, so the
  internal `radix-tree-keys`/`radix-tree-nodes` fields are omitted; every other field is
  reported with Redis semantics.

### 2.3 Delete and trim follow-up

- **XDEL**: Redis checks the key, then its type, and only then parses IDs, so a missing key
  answers `0` even for a malformed ID. The replicated request keeps raw IDs and validates
  them inside `mutate` after the type check. Deleting removes payloads only: PEL records,
  group cursors and the last-generated ID survive, `max-deleted-entry-id` advances only for
  IDs actually removed, and duplicate IDs count once.
- **XTRIM**: supports `MAXLEN`/`MINID`, optional `~`/`=`, `LIMIT n`, and the Redis 8.2
  `KEEPREF`/`DELREF`/`ACKED` reference policies. Options are parsed before the key lookup,
  so an invalid option is an error even for a missing key; a missing key itself answers `0`
  and is not created.
  - `LIMIT` requires `~`, `0` means unlimited, negative `MAXLEN`/`LIMIT` report Redis'
    dedicated messages, and `MAXLEN` and `MINID` are mutually exclusive. Without an explicit
    `LIMIT`, `~` uses the core's 10,000-entry examination cap.
  - `KEEPREF` (default) leaves the PEL alone, `DELREF` clears PEL references for trimmed
    entries, and `ACKED` trims only entries no group still references.
  - Unlike XDEL, XTRIM does not advance `max-deleted-entry-id`.
- **Snapshots and wake-ups**: `TrimOptions` is serializable, so snapshot deltas and Raft
  logs replay the exact mode, limit and policy. Neither command appends entries, so the
  `changed_streams` append flag stays false and blocked XREAD/XREADGROUP clients sleep until
  the next XADD.

### 2.4 Auto-claim follow-up

- **XAUTOCLAIM**: supports `min-idle-time`, `start`, `COUNT` (default 100, must be positive)
  and `JUSTID`. `start` accepts `-`, `+`, bare milliseconds and the exclusive `(id` form; an
  exclusive MAX overflow reports `ERR invalid start ID for the interval`.
- **Error precedence**: Redis parses idle/start/options before looking up the key and group,
  so malformed arguments win over a missing key; only valid arguments produce
  `NOGROUP No such key '<key>' or consumer group '<group>'`.
- **Scan semantics**: scans the PEL from `start`, examines at most `10 * COUNT` records, and
  counts both claimed and cleaned-up deleted IDs against `COUNT`. `next-start` is the next
  unexamined PEL ID, or `0-0` at the end. PEL records whose payload was deleted are removed
  and returned as the third reply element.
- **Deterministic time**: `xautoclaim_at(..., now)` takes delivery timestamps and consumer
  activity from the replicated write clock; `JUSTID` does not increment delivery attempts.
- The reply is `[next-start, claimed, deleted-ids]`, where `claimed` is `[id, fields]` pairs
  or an ID array under `JUSTID`. A successful call always creates/touches the consumer, so it
  records a stream snapshot delta.

## 3. Replication and snapshots

- Reads go through `ReadOperation`; single-key writes through `BaseOperation` and
  `execute_compute` (which also serves CAS snapshot replay); multi-key `XREADGROUP` is one
  `RedisOperation` log entry whose per-key work records single-key operations for snapshots.
- Delivery timestamps, consumer activity and XPENDING idle times are replicated state, so the
  core exposes clock-parameterised variants (`xreadgroup_at`, `xclaim_at`, `xpending_range_at`,
  `xinfo_consumers_at`, `xinfo_stream_full_at`, `xautoclaim_at`) fed by `update.write_clock`.
  A replay test confirms identical PEL state.
- **Group generations** (stream snapshot version 4): `Group::generation` is assigned from a
  replicated monotonic counter on `XGROUP CREATE`, persisted in the stream snapshot, captured
  by a blocked `XREADGROUP` before its first submit and validated in the apply path. A client
  can therefore never start consuming a group that was destroyed and recreated while waiting.

## 4. Blocking wake-ups

- `StreamWaiter::Read(XReadParams)` and `StreamWaiter::Group { group, identities }` share one
  registry, so group waiters no longer run XREAD computations.
- `ApplyEffects::changed_streams` is a `HashMap<(db, key), bool>`: `execute_compute` publishes
  the event for every successful stream write (no per-command bookkeeping) and the boolean says
  whether entries were appended, derived by comparing `entries_added` around the mutation.
  XREAD waiters are only evaluated on appends; group waiters still see every change.
- New entries wake at most one waiter per `(key, group)`; that delivery write advances the
  cursor and its own apply wakes the next waiter while entries remain, so delivery stays
  exactly-once and writes match non-empty reads. A destroyed or recreated group wakes every
  waiter of that group and is answered directly with `NOGROUP`, without an extra Raft write.
- Registration and readiness checks happen under the global writer, so no wake-up can be lost.
  Group state is cached once per `(key, group)`, `wake_ready` takes the waiter queue out of the
  map instead of cloning it, and wake-ups use `execute_multi_read_ref` to avoid cloning
  `XReadParams`.

## 5. Two existing defects fixed along the way

- **Snapshot file version**: the dynamic-metadata format is documented as version 2 but the
  constant was still 1, so the version byte could not distinguish the old inline-metadata
  layout. The "old version" corruption test had been failing since the format was introduced.
  `VERSION` is now 2 and the header assertion expects `MCDC\x02`.
- **`#[serde(skip)]` variant indices**: serde counts a skipped variant when encoding but not
  when decoding, so variants declared *after* `RedisOperation::RedisEvalSha` are written with
  an out-of-range index and break Raft log apply. `RedisXReadGroup` is declared before it and a
  bincode round-trip test guards the layout.

## 6. Verification

- `cargo test -p cache_cat --lib -- --skip test::tests::test_add` → **585 passed / 0 failed**
  (`test_add` needs a live node on port 5001).
- Coverage includes argument/boundary/error text for all eleven commands, PEL equality after
  snapshot replay, generation round-trips and rejection, one wake per group, per-group
  independent wakes, `NOGROUP` after destroy, no XREAD scans on non-append changes,
  XREVRANGE's end/start reversal, XINFO's RESP2/RESP3 shapes and clock-derived times,
  XDEL/XTRIM snapshot replay and reference policies, and XAUTOCLAIM's cursor, COUNT, JUSTID,
  PEL cleanup and deterministic write clock.
- Real single node: `BLOCK 0` plus `XGROUP DESTROY` answers `NOGROUP` immediately; one append
  serves one of three blocked consumers and the next append serves the next; destroying another
  group leaves the waiter asleep; destroy+recreate plus `XADD` answers `NOGROUP` with an empty
  new PEL; two entries written in one `EVAL` are delivered through chained wake-ups; an `XACK`
  no longer wakes a blocked `XREAD`; XREVRANGE and XINFO (RESP2/RESP3, HELP, NOGROUP,
  MULTI/EXEC and EVAL) behave correctly; XDEL/XTRIM handle deletion, trimming, LIMIT and all
  reference policies through MULTI/EXEC and EVAL, and blocked readers only wake on an append;
  XAUTOCLAIM pages with its cursor, honours COUNT/JUSTID/min-idle-time, cleans deleted PEL
  records and reports argument errors before NOGROUP; snapshots are written as `MCDC\x02` and
  restore data and PEL after a restart.

## 7. Known limits and next steps

- Remaining stream commands are not exposed yet: `XACKDEL`/`XDELEX` and `XSETID`. Both exist
  in the core; only protocol plumbing remains.
- `DEL key` does not wake blocked clients, matching Redis (DEL produces no stream readiness).
- The first submit of a blocking read is still a Raft write; an empty read still creates or
  touches the consumer, as Redis does.
- The server keeps using `Arc<RwLock<RedisStream>>`; the `SharedStream` notification stack is
  unused and cannot become the authority while group reads must go through Raft.

## 8. Commits

```
36d809c add XAUTOCLAIM stream command
e4c159d add XDEL and XTRIM stream commands
91e8886 add XREVRANGE and XINFO stream commands
db95a9f trim allocations on the stream wake path
c4ecf53 skip useless XREAD scans on non-append stream changes
efce599 avoid needless Raft writes on stream wakeups
70608ad fix stream blocking wakeups
1efeea2 add stream consumer group commands
c4f6d2e fix snapshot format version
```
