//! Stream commands must behave the same through the Raft read/write paths.

use crate::error::ProtocolError;
use crate::protocol::raft_command::RaftCommandFactory;
use crate::protocol::stream::xreadgroup::XReadGroupReq;
use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::{base_request, do_request, read_request};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::GroupRead;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::{AtomicRequest, Operation, RedisOperation, Request};
use bytes::Bytes;

const DB: u16 = 1;
const CLOCK: u64 = 1_000;

fn try_request(args: &[&str]) -> Result<Operation, ProtocolError> {
    let items = args
        .iter()
        .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
        .collect::<Vec<_>>();
    RaftCommandFactory::init_lua().parse_request(&items)
}

fn request(args: &[&str]) -> Operation {
    try_request(args).unwrap_or_else(|error| panic!("{}: {error}", args[0]))
}

fn apply(cache: &MyCache, operation: Operation) -> Value {
    let mut update_type = UpdateType::None;
    let mut update = Update {
        effects: None,
        waiters: None,
        db_number: DB,
        write_clock: cache.set_write_clock(CLOCK),
        update_type: &mut update_type,
    };
    do_request(cache, operation, &mut update, true)
}

fn read(cache: &MyCache, operation: Operation) -> Value {
    let Operation::Read(read_operation) = operation else {
        panic!("expected a replicated read");
    };
    let read_clock = cache.get_and_update_read_clock();
    read_request(cache, read_operation, DB, read_clock)
}

fn entry_ids(reply: Value) -> Vec<Bytes> {
    let Value::Array(Some(entries)) = reply else {
        panic!("expected an array of entries, got {reply:?}");
    };
    entries
        .into_iter()
        .map(|entry| {
            let Value::Array(Some(mut parts)) = entry else {
                panic!("expected an [id, fields] entry");
            };
            parts.remove(0).string_bytes_clone().unwrap()
        })
        .collect()
}

#[test]
fn stream_replies_match_between_the_protocol_and_raft_paths() {
    let cache = MyCache::new(2).unwrap();
    apply(&cache, request(&["XADD", "s", "1-0", "field", "value"]));
    apply(&cache, request(&["XADD", "s", "2-0", "field", "value"]));

    assert!(matches!(
        read(&cache, request(&["XLEN", "s"])),
        Value::Integer(2)
    ));
    assert!(matches!(
        read(&cache, request(&["XLEN", "missing"])),
        Value::Integer(0)
    ));

    assert_eq!(
        entry_ids(read(&cache, request(&["XRANGE", "s", "-", "+"]))),
        [Bytes::from_static(b"1-0"), Bytes::from_static(b"2-0")]
    );
    assert_eq!(
        entry_ids(read(
            &cache,
            request(&["XRANGE", "s", "(1-0", "+", "COUNT", "1"])
        )),
        [Bytes::from_static(b"2-0")]
    );
}

#[test]
fn xack_only_touches_the_consumer_group_pel() {
    let cache = MyCache::new(2).unwrap();
    apply(&cache, request(&["XADD", "s", "1-0", "field", "value"]));
    apply(&cache, request(&["XGROUP", "CREATE", "s", "g", "0"]));

    // Nothing is pending yet, so the reply is zero and the stream is untouched.
    assert!(matches!(
        apply(&cache, request(&["XACK", "s", "g", "1-0"])),
        Value::Integer(0)
    ));
    assert!(matches!(
        read(&cache, request(&["XLEN", "s"])),
        Value::Integer(1)
    ));

    // Redis parses IDs only once the group exists.
    assert!(matches!(
        apply(&cache, request(&["XACK", "s", "g", "bogus"])),
        Value::Error(ref message) if message.starts_with("ERR Invalid stream ID")
    ));
    // A missing key answers zero without creating it, even with a bad ID.
    assert!(matches!(
        apply(&cache, request(&["XACK", "missing", "g", "bogus"])),
        Value::Integer(0)
    ));
    assert!(matches!(
        read(&cache, request(&["XLEN", "missing"])),
        Value::Integer(0)
    ));
}

fn apply_snapshot(
    cache: &MyCache,
    args: &[&str],
    revision: &mut u64,
) -> (Value, Vec<AtomicRequest>) {
    let mut queue = Vec::new();
    let mut update_type = UpdateType::Snapshot {
        queue: &mut queue,
        revision,
    };
    let mut update = Update {
        effects: None,
        waiters: None,
        db_number: DB,
        write_clock: cache.set_write_clock(CLOCK),
        update_type: &mut update_type,
    };
    let reply = do_request(cache, request(args), &mut update, true);
    (reply, queue)
}

/// Replay a recorded snapshot delta through its persisted representation.
fn replay(cache: &MyCache, queue: &[AtomicRequest]) {
    let bytes = bincode2::serialize(queue).unwrap();
    let queue: Vec<AtomicRequest> = bincode2::deserialize(&bytes).unwrap();
    for atomic in queue {
        let mut update_type = UpdateType::CAS {
            expected_revision: atomic.expected_revision,
            revision: atomic.version,
        };
        let mut update = Update {
            effects: None,
            waiters: None,
            db_number: atomic.db_number,
            write_clock: cache.set_write_clock(atomic.write_clock),
            update_type: &mut update_type,
        };
        base_request(cache, atomic.request, &mut update);
    }
}

fn seed_group(cache: &MyCache) {
    apply(cache, request(&["XADD", "s", "1-0", "field", "one"]));
    apply(cache, request(&["XADD", "s", "2-0", "field", "two"]));
    apply(cache, request(&["XGROUP", "CREATE", "s", "g", "0"]));
}

#[test]
fn replicated_xreadgroup_request_roundtrips_through_bincode() {
    // Regression guard: a variant declared after a `#[serde(skip)]` variant is
    // encoded with an index the decoder rejects, which breaks Raft log apply.
    let operation = request(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"]);
    let encoded = bincode2::serialize(&Request::new(CLOCK, DB, operation)).unwrap();
    let decoded: Request = bincode2::deserialize(&encoded).unwrap();
    assert!(matches!(
        decoded.operation,
        Operation::Redis(crate::raft::types::entry::request::RedisOperation::RedisXReadGroup(_))
    ));
}

#[test]
fn xreadgroup_assigns_pending_and_replays_deterministically() {
    let cache = MyCache::new(2).unwrap();
    seed_group(&cache);

    let mut revision = 0;
    let (reply, queue) = apply_snapshot(
        &cache,
        &[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "COUNT",
            "10",
            "STREAMS",
            "s",
            ">",
        ],
        &mut revision,
    );
    let Value::MapWithResp2 { entries, .. } = reply else {
        panic!("expected a stream map, got {reply:?}");
    };
    assert_eq!(entries.len(), 1);
    let Value::Array(Some(stream_entries)) = &entries[0].1 else {
        panic!("expected an entries array");
    };
    assert_eq!(stream_entries.len(), 2);
    // One single-key operation per stream is what a snapshot records.
    assert_eq!(queue.len(), 1);
    assert!(matches!(&queue[0].request, BaseOperation::XReadGroup(_)));
    assert_eq!(queue[0].write_clock, CLOCK);

    // Nothing new is available on a second `>` read.
    assert!(matches!(
        apply(
            &cache,
            request(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"])
        ),
        Value::Array(None)
    ));

    // The PEL is part of the snapshot delta: replay must reproduce it exactly.
    let before = read(&cache, request(&["XPENDING", "s", "g"]));
    let restored = MyCache::new(2).unwrap();
    seed_group(&restored);
    replay(&restored, &queue);
    let after = read(&restored, request(&["XPENDING", "s", "g"]));
    assert_eq!(before.encode_proto(2), after.encode_proto(2));
    assert_eq!(before.encode_proto(3), after.encode_proto(3));

    // Claiming moves ownership without changing the pending total.
    let claimed = apply(&cache, request(&["XCLAIM", "s", "g", "c2", "0", "1-0"]));
    assert!(matches!(claimed, Value::Array(Some(ref items)) if items.len() == 1));
    let Value::Array(Some(summary)) = read(&cache, request(&["XPENDING", "s", "g"])) else {
        panic!("expected a summary");
    };
    assert!(matches!(summary[0], Value::Integer(2)));
    let Value::Array(Some(consumers)) = &summary[3] else {
        panic!("expected consumers");
    };
    assert_eq!(consumers.len(), 2, "c and c2 both own one entry");

    // Acknowledging removes the entry from the PEL but not from the stream.
    assert!(matches!(
        apply(&cache, request(&["XACK", "s", "g", "1-0"])),
        Value::Integer(1)
    ));
    assert!(matches!(
        read(&cache, request(&["XLEN", "s"])),
        Value::Integer(2)
    ));

    // The consumer history still reports its remaining pending entry.
    let history = apply(
        &cache,
        request(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "0"]),
    );
    let Value::MapWithResp2 { entries, .. } = history else {
        panic!("expected history entries, got {history:?}");
    };
    let Value::Array(Some(stream_entries)) = &entries[0].1 else {
        panic!("expected an entries array");
    };
    assert_eq!(stream_entries.len(), 1);
    let Value::Array(Some(entry)) = &stream_entries[0] else {
        panic!("expected [id, fields]");
    };
    assert_eq!(
        entry[0].string_bytes_clone().as_deref(),
        Some(b"2-0".as_slice())
    );

    // A group that does not exist is a NOGROUP error, not an empty reply.
    assert!(matches!(
        apply(
            &cache,
            request(&["XREADGROUP", "GROUP", "nope", "c", "STREAMS", "s", ">"])
        ),
        Value::Error(ref message) if message.starts_with("NOGROUP")
    ));
}

fn group_generation(cache: &MyCache) -> u64 {
    let entry = cache.databases[DB as usize]
        .mocha
        .get_entry(b"s".as_slice())
        .expect("stream exists");
    let ValueObject::Stream(stream) = &entry.value.data else {
        panic!("expected a stream");
    };
    stream.read().group_generation(b"g").expect("group exists")
}

fn group_request(expected: u64, consumer: &str) -> Operation {
    Operation::Redis(RedisOperation::RedisXReadGroup(XReadGroupReq {
        keys: vec![Bytes::from_static(b"s")],
        group: Bytes::from_static(b"g"),
        consumer: Bytes::copy_from_slice(consumer.as_bytes()),
        modes: vec![GroupRead::New],
        count: None,
        no_ack: false,
        expected_generations: vec![Some(expected)],
    }))
}

#[test]
fn xreadgroup_rejects_a_stale_group_generation() {
    let cache = MyCache::new(2).unwrap();
    seed_group(&cache);
    let generation = group_generation(&cache);

    // A waiter that captured the group before a recreate must not silently
    // start reading the replacement group.
    assert!(matches!(
        apply(&cache, group_request(generation + 1, "stale")),
        Value::Error(ref message) if message.starts_with("NOGROUP")
    ));

    // Destroy + recreate allocates a new generation.
    apply(&cache, request(&["XGROUP", "DESTROY", "s", "g"]));
    apply(&cache, request(&["XGROUP", "CREATE", "s", "g", "0"]));
    let recreated = group_generation(&cache);
    assert!(recreated > generation);
    assert!(matches!(
        apply(&cache, group_request(generation, "stale")),
        Value::Error(ref message) if message.starts_with("NOGROUP")
    ));

    // The current generation still delivers normally.
    let reply = apply(&cache, group_request(recreated, "fresh"));
    assert!(matches!(reply, Value::MapWithResp2 { .. }));
    assert!(matches!(
        apply(&cache, group_request(recreated, "fresh")),
        Value::Array(None)
    ));
}

#[test]
fn read_only_stream_metadata_commands_use_the_replicated_read_path() {
    let cache = MyCache::new(2).unwrap();
    seed_group(&cache);
    apply(
        &cache,
        request(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"]),
    );

    // XREVRANGE keeps the wire order (end first) but returns descending IDs.
    assert_eq!(
        entry_ids(read(&cache, request(&["XREVRANGE", "s", "+", "-"]))),
        [Bytes::from_static(b"2-0"), Bytes::from_static(b"1-0")]
    );
    assert!(matches!(
        read(&cache, request(&["XREVRANGE", "missing", "+", "-"])),
        Value::Array(Some(ref entries)) if entries.is_empty()
    ));

    let Value::Map(stream_info) = read(&cache, request(&["XINFO", "STREAM", "s"])) else {
        panic!("expected an XINFO STREAM map");
    };
    assert!(
        stream_info
            .iter()
            .any(
                |(name, value)| name.string_bytes_clone().as_deref() == Some(b"length")
                    && matches!(value, Value::Integer(2))
            )
    );

    let Value::Array(Some(groups)) = read(&cache, request(&["XINFO", "GROUPS", "s"])) else {
        panic!("expected an XINFO GROUPS array");
    };
    assert_eq!(groups.len(), 1);

    let Value::Array(Some(consumers)) = read(&cache, request(&["XINFO", "CONSUMERS", "s", "g"]))
    else {
        panic!("expected an XINFO CONSUMERS array");
    };
    assert_eq!(consumers.len(), 1);

    assert!(matches!(
        read(&cache, request(&["XINFO", "CONSUMERS", "s", "nope"])),
        Value::Error(ref message) if message.starts_with("NOGROUP")
    ));
    assert!(matches!(
        read(&cache, request(&["XINFO", "HELP"])),
        Value::Array(Some(ref lines)) if !lines.is_empty()
    ));
}

#[test]
fn xinfo_read_operations_roundtrip_through_bincode() {
    let operation = request(&["XINFO", "STREAM", "s", "FULL", "COUNT", "2"]);
    let encoded = bincode2::serialize(&Request::new(CLOCK, DB, operation)).unwrap();
    let decoded: Request = bincode2::deserialize(&encoded).unwrap();
    assert!(matches!(
        decoded.operation,
        Operation::Read(crate::raft::types::entry::read_operation::ReadOperation::XInfo(_))
    ));
}

#[test]
fn xdel_and_xtrim_replay_from_snapshot_deltas() {
    let cache = MyCache::new(2).unwrap();
    let restored = MyCache::new(2).unwrap();
    for id in ["1-0", "2-0", "3-0"] {
        apply(&cache, request(&["XADD", "s", id, "field", "value"]));
        apply(&restored, request(&["XADD", "s", id, "field", "value"]));
    }

    let mut revision = 0;
    let (deleted, mut queue) = apply_snapshot(&cache, &["XDEL", "s", "2-0"], &mut revision);
    assert!(matches!(deleted, Value::Integer(1)));
    let (trimmed, trim_queue) =
        apply_snapshot(&cache, &["XTRIM", "s", "MAXLEN", "1"], &mut revision);
    assert!(matches!(trimmed, Value::Integer(1)));
    queue.extend(trim_queue);

    replay(&restored, &queue);
    assert!(matches!(
        read(&restored, request(&["XLEN", "s"])),
        Value::Integer(1)
    ));
    // The delta must preserve XDEL's tombstone even though replay ran XTRIM.
    let Value::Map(stream_info) = read(&restored, request(&["XINFO", "STREAM", "s"])) else {
        panic!("expected an XINFO STREAM map");
    };
    let max_deleted = stream_info
        .iter()
        .find(|(name, _)| name.string_bytes_clone().as_deref() == Some(b"max-deleted-entry-id"))
        .map(|(_, value)| value.string_bytes_clone().unwrap())
        .expect("max-deleted-entry-id must be reported");
    assert_eq!(max_deleted.as_ref(), b"2-0");
}

#[test]
fn approximate_xtrim_options_survive_snapshot_replay() {
    let cache = MyCache::new(2).unwrap();
    let restored = MyCache::new(2).unwrap();
    for index in 1..=5 {
        let id = format!("{index}-0");
        apply(&cache, request(&["XADD", "s", &id, "field", "value"]));
        apply(&restored, request(&["XADD", "s", &id, "field", "value"]));
    }

    let mut revision = 0;
    let (trimmed, queue) = apply_snapshot(
        &cache,
        &["XTRIM", "s", "MAXLEN", "~", "0", "LIMIT", "2"],
        &mut revision,
    );
    assert!(matches!(trimmed, Value::Integer(2)));
    replay(&restored, &queue);
    assert!(matches!(
        read(&restored, request(&["XLEN", "s"])),
        Value::Integer(3)
    ));
}

#[test]
fn xdel_and_xtrim_base_operations_roundtrip_through_bincode() {
    let cases: [&[&str]; 2] = [
        &["XDEL", "s", "1-0"],
        &["XTRIM", "s", "MAXLEN", "~", "5", "LIMIT", "2"],
    ];
    for args in cases {
        let operation = request(args);
        let encoded = bincode2::serialize(&Request::new(CLOCK, DB, operation)).unwrap();
        let decoded: Request = bincode2::deserialize(&encoded).unwrap();
        assert!(matches!(
            decoded.operation,
            Operation::Base(BaseOperation::XDel(_) | BaseOperation::XTrim(_))
        ));
    }
}

fn seed_autoclaim(cache: &MyCache) {
    apply(cache, request(&["XADD", "s", "1-0", "field", "one"]));
    apply(cache, request(&["XADD", "s", "2-0", "field", "two"]));
    apply(cache, request(&["XGROUP", "CREATE", "s", "g", "0"]));
    apply(
        cache,
        request(&["XREADGROUP", "GROUP", "g", "old", "STREAMS", "s", ">"]),
    );
}

#[test]
fn xautoclaim_claims_pending_entries_and_replays_deterministically() {
    let cache = MyCache::new(2).unwrap();
    let restored = MyCache::new(2).unwrap();
    seed_autoclaim(&cache);
    seed_autoclaim(&restored);

    let mut revision = 0;
    let (reply, queue) = apply_snapshot(
        &cache,
        &["XAUTOCLAIM", "s", "g", "new", "0", "-", "COUNT", "1"],
        &mut revision,
    );
    let Value::Array(Some(parts)) = reply else {
        panic!("expected [next, claimed, deleted]");
    };
    assert_eq!(
        parts[0].string_bytes_clone().as_deref(),
        Some(b"2-0".as_slice())
    );
    assert!(matches!(&parts[1], Value::Array(Some(claimed)) if claimed.len() == 1));
    assert!(matches!(&parts[2], Value::Array(Some(deleted)) if deleted.is_empty()));

    replay(&restored, &queue);
    let before = read(&cache, request(&["XPENDING", "s", "g"]));
    let after = read(&restored, request(&["XPENDING", "s", "g"]));
    assert_eq!(before.encode_proto(2), after.encode_proto(2));
    assert_eq!(before.encode_proto(3), after.encode_proto(3));
}

#[test]
fn xautoclaim_cleans_deleted_payloads_from_the_pel() {
    let cache = MyCache::new(2).unwrap();
    seed_autoclaim(&cache);
    assert!(matches!(
        apply(&cache, request(&["XDEL", "s", "1-0"])),
        Value::Integer(1)
    ));

    let reply = apply(
        &cache,
        request(&["XAUTOCLAIM", "s", "g", "new", "0", "-", "JUSTID"]),
    );
    let Value::Array(Some(parts)) = reply else {
        panic!("expected [next, claimed, deleted]");
    };
    let Value::Array(Some(deleted)) = &parts[2] else {
        panic!("expected deleted ids");
    };
    assert_eq!(
        deleted[0].string_bytes_clone().as_deref(),
        Some(b"1-0".as_slice())
    );
    let Value::Array(Some(summary)) = read(&cache, request(&["XPENDING", "s", "g"])) else {
        panic!("expected an XPENDING summary");
    };
    assert!(matches!(summary[0], Value::Integer(1)));
}

#[test]
fn xautoclaim_base_operation_roundtrips_through_bincode() {
    let operation = request(&[
        "XAUTOCLAIM",
        "s",
        "g",
        "c",
        "500",
        "(1-0",
        "COUNT",
        "5",
        "JUSTID",
    ]);
    let encoded = bincode2::serialize(&Request::new(CLOCK, DB, operation)).unwrap();
    let decoded: Request = bincode2::deserialize(&encoded).unwrap();
    assert!(matches!(
        decoded.operation,
        Operation::Base(BaseOperation::XAutoClaim(_))
    ));
}

fn integer_items(reply: &Value) -> Vec<i64> {
    let Value::Array(Some(items)) = reply else {
        panic!("expected an array, got {reply:?}");
    };
    items
        .iter()
        .map(|value| {
            let Value::Integer(item) = value else {
                panic!("expected an integer, got {value:?}");
            };
            *item
        })
        .collect()
}

#[test]
fn xsetid_updates_metadata_and_replays_deterministically() {
    let cache = MyCache::new(2).unwrap();
    let restored = MyCache::new(2).unwrap();
    for id in ["1-0", "2-0"] {
        apply(&cache, request(&["XADD", "s", id, "field", "value"]));
        apply(&restored, request(&["XADD", "s", id, "field", "value"]));
    }

    let mut revision = 0;
    let (reply, queue) = apply_snapshot(
        &cache,
        &[
            "XSETID",
            "s",
            "10-0",
            "ENTRIESADDED",
            "20",
            "MAXDELETEDID",
            "3-0",
        ],
        &mut revision,
    );
    assert!(matches!(reply, Value::SimpleString(ref status) if status == "OK"));
    replay(&restored, &queue);

    let before = read(&cache, request(&["XINFO", "STREAM", "s"]));
    let after = read(&restored, request(&["XINFO", "STREAM", "s"]));
    assert_eq!(before.encode_proto(2), after.encode_proto(2));
    assert_eq!(before.encode_proto(3), after.encode_proto(3));

    // Redis reports a missing key only after the arguments are valid.
    assert!(matches!(
        apply(&cache, request(&["XSETID", "missing", "1-0"])),
        Value::Error(ref message) if message == "ERR no such key"
    ));
    assert!(matches!(
        try_request(&["XSETID", "missing", "bogus"]),
        Err(ProtocolError::Response(ref message))
            if message.starts_with("ERR Invalid stream ID")
    ));
    assert!(matches!(
        apply(&cache, request(&["XSETID", "s", "1-0"])),
        Value::Error(ref message) if message.contains("current max_deleted_entry_id")
    ));
}

#[test]
fn xackdel_acknowledges_and_deletes_through_the_raft_path() {
    let cache = MyCache::new(2).unwrap();
    seed_group(&cache);
    apply(
        &cache,
        request(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"]),
    );

    let reply = apply(
        &cache,
        request(&["XACKDEL", "s", "g", "IDS", "2", "1-0", "2-0"]),
    );
    assert_eq!(integer_items(&reply), [1, 1]);
    assert!(matches!(
        read(&cache, request(&["XLEN", "s"])),
        Value::Integer(0)
    ));
    let Value::Array(Some(summary)) = read(&cache, request(&["XPENDING", "s", "g"])) else {
        panic!("expected an XPENDING summary");
    };
    assert!(matches!(summary[0], Value::Integer(0)));

    // A missing group reports one -1 per ID and leaves the PEL untouched.
    apply(&cache, request(&["XADD", "s", "3-0", "field", "three"]));
    apply(
        &cache,
        request(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"]),
    );
    let reply = apply(
        &cache,
        request(&["XACKDEL", "s", "nope", "IDS", "1", "3-0"]),
    );
    assert_eq!(integer_items(&reply), [-1]);
    let Value::Array(Some(summary)) = read(&cache, request(&["XPENDING", "s", "g"])) else {
        panic!("expected an XPENDING summary");
    };
    assert!(matches!(summary[0], Value::Integer(1)));
}

#[test]
fn xdelex_deletes_with_the_reference_policy() {
    let cache = MyCache::new(2).unwrap();
    seed_group(&cache);
    apply(
        &cache,
        request(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"]),
    );

    // ACKED keeps entries still referenced by the group.
    let reply = apply(
        &cache,
        request(&["XDELEX", "s", "ACKED", "IDS", "2", "1-0", "2-0"]),
    );
    assert_eq!(integer_items(&reply), [2, 2]);
    assert!(matches!(
        read(&cache, request(&["XLEN", "s"])),
        Value::Integer(2)
    ));

    // DELREF removes the payloads and their PEL references.
    let reply = apply(
        &cache,
        request(&["XDELEX", "s", "DELREF", "IDS", "2", "1-0", "2-0"]),
    );
    assert_eq!(integer_items(&reply), [1, 1]);
    assert!(matches!(
        read(&cache, request(&["XLEN", "s"])),
        Value::Integer(0)
    ));
    let Value::Array(Some(summary)) = read(&cache, request(&["XPENDING", "s", "g"])) else {
        panic!("expected an XPENDING summary");
    };
    assert!(matches!(summary[0], Value::Integer(0)));

    let reply = apply(&cache, request(&["XDELEX", "missing", "IDS", "1", "1-0"]));
    assert_eq!(integer_items(&reply), [-1]);
}

#[test]
fn final_stream_base_operations_roundtrip_through_bincode() {
    let cases: [&[&str]; 3] = [
        &[
            "XSETID",
            "s",
            "10-0",
            "ENTRIESADDED",
            "20",
            "MAXDELETEDID",
            "3-0",
        ],
        &["XACKDEL", "s", "g", "ACKED", "IDS", "1", "1-0"],
        &["XDELEX", "s", "DELREF", "IDS", "1", "1-0"],
    ];
    for args in cases {
        let operation = request(args);
        let encoded = bincode2::serialize(&Request::new(CLOCK, DB, operation)).unwrap();
        let decoded: Request = bincode2::deserialize(&encoded).unwrap();
        assert!(matches!(
            decoded.operation,
            Operation::Base(
                BaseOperation::XSetId(_) | BaseOperation::XAckDel(_) | BaseOperation::XDelex(_)
            )
        ));
    }
}
