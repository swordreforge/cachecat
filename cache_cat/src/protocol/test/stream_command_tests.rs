//! Stream commands must behave the same through the Raft read/write paths.

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

fn request(args: &[&str]) -> Operation {
    let items = args
        .iter()
        .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
        .collect::<Vec<_>>();
    RaftCommandFactory::init_lua()
        .parse_request(&items)
        .unwrap_or_else(|error| panic!("{}: {error}", args[0]))
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
