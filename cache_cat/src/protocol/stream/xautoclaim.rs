//! XAUTOCLAIM command implementation.
//!
//! `XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]`
//!
//! Scans the group PEL from `start`, transfers up to `count` idle entries to
//! `consumer`, and removes PEL records whose stream payload was deleted. The
//! reply is `[next-start, claimed, deleted-ids]`. The first two elements share
//! XCLAIM's shape: entries (or ids with JUSTID), then cleaned-up ids.
//!
//! Redis parses the idle time, start and options before it reports NOGROUP, so
//! malformed arguments win over a missing key or group. Delivery timestamps
//! come from the replicated write clock through `xautoclaim_at`.

use super::xclaim::claim_reply;
use super::{no_group, parse_interval_start};
use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::option_eq;
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::{
    AutoClaimOptions, AutoClaimReply, StreamError, StreamId,
};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Replicated XAUTOCLAIM request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XAutoClaimReq {
    pub key: Bytes,
    pub group: Bytes,
    pub consumer: Bytes,
    pub options: AutoClaimOptions,
}

impl fmt::Display for XAutoClaimReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "XAUTOCLAIM {} {} {} idle={} start={} count={} justid={}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.group),
            String::from_utf8_lossy(&self.consumer),
            self.options.min_idle_ms,
            self.options.start,
            self.options.count,
            self.options.just_id
        )
    }
}

fn invalid_min_idle() -> ProtocolError {
    ProtocolError::response("ERR Invalid min-idle-time argument for XAUTOCLAIM")
}

fn invalid_count() -> ProtocolError {
    ProtocolError::response("ERR COUNT must be > 0")
}

impl XAutoClaimReq {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 6 {
            return Err(ProtocolError::WrongArgCount("xautoclaim"));
        }
        // Match Redis' order: idle time, start, options, then the keyspace.
        let min_idle = items[4]
            .try_parse_canonical_i64()
            .map_err(|_| invalid_min_idle())?;
        let start = parse_interval_start(&items[5])?;
        let mut options = AutoClaimOptions {
            // Redis clamps a negative min-idle-time to zero.
            min_idle_ms: if min_idle < 0 { 0 } else { min_idle as u64 },
            start,
            ..AutoClaimOptions::default()
        };
        let max_count = (isize::MAX as i64) / 16;
        let mut index = 6;
        while index < items.len() {
            let option = items[index]
                .string_bytes_clone()
                .ok_or(ProtocolError::SyntaxError)?;
            if option_eq(&option, b"COUNT") && index + 1 < items.len() {
                let parsed = items[index + 1].try_parse_canonical_i64()?;
                if !(1..=max_count).contains(&parsed) {
                    return Err(invalid_count());
                }
                options.count = usize::try_from(parsed).map_err(|_| invalid_count())?;
                index += 2;
            } else if option_eq(&option, b"JUSTID") {
                options.just_id = true;
                index += 1;
            } else {
                return Err(ProtocolError::SyntaxError);
            }
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let group = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("group"))?;
        let consumer = items[3]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("consumer"))?;
        Ok(Self {
            key,
            group,
            consumer,
            options,
        })
    }
}

fn id_reply(id: StreamId) -> Value {
    Value::BulkString(Some(id.to_string().into()))
}

fn auto_claim_reply(reply: AutoClaimReply) -> Value {
    Value::Array(Some(vec![
        id_reply(reply.next_start),
        claim_reply(reply.claimed),
        Value::Array(Some(reply.deleted_ids.into_iter().map(id_reply).collect())),
    ]))
}

impl ComputeCommand for XAutoClaimReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::XAutoClaim(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expire = entry.get_expire_policy();
        let version = entry.value.version;
        let ValueObject::Stream(stream) = entry.value.data else {
            return (MochaOperation::Abort, ProtocolError::WrongType.into());
        };
        let result =
            stream
                .write()
                .xautoclaim_at(&self.group, &self.consumer, self.options, write_clock);
        match result {
            // XAUTOCLAIM always creates/touches the consumer, so the stream is
            // rewritten even when no entry was claimed.
            Ok(reply) => (
                MochaOperation::Insert {
                    value: MyValue {
                        version,
                        data: ValueObject::Stream(stream),
                    },
                    expire,
                },
                auto_claim_reply(reply),
            ),
            Err(StreamError::NoGroup) => (MochaOperation::Abort, no_group(&self.key, &self.group)),
            Err(error) => (
                MochaOperation::Abort,
                ProtocolError::response(format!("ERR {error}")).into(),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (MochaOperation::Abort, no_group(&self.key, &self.group))
    }
}

/// XAUTOCLAIM command handler.
pub struct XAutoClaimCommand;

impl RaftCommand for XAutoClaimCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(BaseOperation::XAutoClaim(
            XAutoClaimReq::parse(items)?,
        )))
    }
}

#[async_trait]
impl Command for XAutoClaimCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let operation = self.raft_request(items)?;
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(operation);
            return Ok(Value::queued());
        }
        server.app.write(operation, client.db_number).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::structure::stream::{
        AddId, GroupRead, GroupStart, IdRange, PendingQuery, ReadGroupOptions, RedisStream,
    };
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn arguments(args: &[&str]) -> Vec<Value> {
        args.iter()
            .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
            .collect()
    }

    fn parse(args: &[&str]) -> Result<XAutoClaimReq, ProtocolError> {
        XAutoClaimReq::parse(&arguments(args))
    }

    fn request(args: &[&str]) -> XAutoClaimReq {
        parse(args).unwrap()
    }

    fn snapshot(stream: RedisStream) -> EntrySnapshot<MyValue> {
        EntrySnapshot {
            value: MyValue::new(ValueObject::Stream(Arc::new(RwLock::new(stream)))),
            expire_at: None,
        }
    }

    fn stream_with_pending() -> RedisStream {
        let mut stream = RedisStream::new();
        for id in [
            StreamId::new(1, 0),
            StreamId::new(2, 0),
            StreamId::new(3, 0),
        ] {
            stream
                .xadd(
                    AddId::Explicit(id),
                    vec![(b"field".to_vec(), b"value".to_vec())],
                )
                .unwrap();
        }
        stream
            .xgroup_create(b"g", GroupStart::Id(StreamId::ZERO), None)
            .unwrap();
        stream
            .xreadgroup_at(
                b"g",
                b"old",
                GroupRead::New,
                ReadGroupOptions::default(),
                500,
            )
            .unwrap();
        stream
    }

    fn pending_deliveries(stream: &RedisStream) -> Vec<u64> {
        stream
            .xpending_range(
                b"g",
                PendingQuery {
                    range: IdRange::all(),
                    count: 100,
                    consumer: None,
                    min_idle_ms: None,
                },
            )
            .unwrap()
            .into_iter()
            .map(|entry| entry.deliveries)
            .collect()
    }

    #[test]
    fn parses_defaults_and_options() {
        let req = request(&["XAUTOCLAIM", "s", "g", "c", "0", "-"]);
        assert_eq!(req.options.count, 100);
        assert!(!req.options.just_id);
        assert_eq!(req.options.start, StreamId::ZERO);

        let req = request(&[
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
        assert_eq!(req.options.min_idle_ms, 500);
        assert_eq!(req.options.start, StreamId::new(1, 1));
        assert_eq!(req.options.count, 5);
        assert!(req.options.just_id);

        // Negative idle is clamped and JUSTID may repeat.
        let req = request(&["XAUTOCLAIM", "s", "g", "c", "-5", "+", "JUSTID", "JUSTID"]);
        assert_eq!(req.options.min_idle_ms, 0);
        assert_eq!(req.options.start, StreamId::MAX);
    }

    #[test]
    fn rejects_bad_arguments_like_redis() {
        assert!(matches!(
            parse(&["XAUTOCLAIM", "s", "g", "c", "0"]),
            Err(ProtocolError::WrongArgCount("xautoclaim"))
        ));
        assert!(matches!(
            parse(&["XAUTOCLAIM", "s", "g", "c", "abc", "-"]),
            Err(ProtocolError::Response(ref message))
                if message == "ERR Invalid min-idle-time argument for XAUTOCLAIM"
        ));
        assert!(matches!(
            parse(&["XAUTOCLAIM", "s", "g", "c", "0", "bogus"]),
            Err(ProtocolError::Response(ref message))
                if message.starts_with("ERR Invalid stream ID")
        ));
        assert!(matches!(
            parse(&["XAUTOCLAIM", "s", "g", "c", "0", "(18446744073709551615-18446744073709551615"]),
            Err(ProtocolError::Response(ref message))
                if message.contains("invalid start ID")
        ));
        assert!(matches!(
            parse(&["XAUTOCLAIM", "s", "g", "c", "0", "-", "COUNT", "0"]),
            Err(ProtocolError::Response(ref message)) if message == "ERR COUNT must be > 0"
        ));
        assert!(matches!(
            parse(&["XAUTOCLAIM", "s", "g", "c", "0", "-", "COUNT", "-1"]),
            Err(ProtocolError::Response(ref message)) if message == "ERR COUNT must be > 0"
        ));
        assert!(matches!(
            parse(&["XAUTOCLAIM", "s", "g", "c", "0", "-", "COUNT", "abc"]),
            Err(ProtocolError::NotAnInteger)
        ));
        assert!(matches!(
            parse(&["XAUTOCLAIM", "s", "g", "c", "0", "-", "COUNT"]),
            Err(ProtocolError::SyntaxError)
        ));
        assert!(matches!(
            parse(&["XAUTOCLAIM", "s", "g", "c", "0", "-", "BOGUS"]),
            Err(ProtocolError::SyntaxError)
        ));
    }

    #[test]
    fn missing_key_or_group_is_nogroup() {
        let (operation, reply) = request(&["XAUTOCLAIM", "s", "g", "c", "0", "-"]).init();
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message)
                if message == "NOGROUP No such key 's' or consumer group 'g'"
        ));
    }

    #[test]
    fn wrong_type_is_an_error() {
        let wrong_type = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        let (operation, reply) =
            request(&["XAUTOCLAIM", "s", "g", "c", "0", "-"]).mutate(wrong_type, 600);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
    }

    #[test]
    fn claims_entries_and_moves_pel_ownership() {
        let (operation, reply) = request(&["XAUTOCLAIM", "s", "g", "new", "0", "-"])
            .mutate(snapshot(stream_with_pending()), 600);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("auto-claiming must rewrite the stream");
        };
        let Value::Array(Some(parts)) = reply else {
            panic!("expected [next, claimed, deleted]");
        };
        assert_eq!(parts.len(), 3);
        assert_eq!(
            parts[0].string_bytes_clone().as_deref(),
            Some(b"0-0".as_slice())
        );
        let Value::Array(Some(claimed)) = &parts[1] else {
            panic!("expected claimed entries");
        };
        assert_eq!(claimed.len(), 3);
        assert!(matches!(&parts[2], Value::Array(Some(deleted)) if deleted.is_empty()));

        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        let summary = stream.read().xpending(b"g").unwrap();
        assert_eq!(summary.total, 3);
        assert_eq!(summary.consumers.len(), 1);
        assert_eq!(summary.consumers[0].0, b"new".to_vec());
        // Delivery attempts were incremented for all three.
        assert_eq!(pending_deliveries(&stream.read()), vec![2, 2, 2]);
    }

    #[test]
    fn justid_does_not_increment_delivery_attempts() {
        let (operation, reply) = request(&["XAUTOCLAIM", "s", "g", "new", "0", "-", "JUSTID"])
            .mutate(snapshot(stream_with_pending()), 600);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("auto-claiming must rewrite the stream");
        };
        let Value::Array(Some(parts)) = reply else {
            panic!("expected [next, claimed, deleted]");
        };
        let Value::Array(Some(ids)) = &parts[1] else {
            panic!("expected ids");
        };
        assert_eq!(ids.len(), 3);
        assert_eq!(
            ids[0].string_bytes_clone().as_deref(),
            Some(b"1-0".as_slice())
        );
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        // JUSTID transfers ownership but keeps the delivery counters intact.
        assert_eq!(pending_deliveries(&stream.read()), vec![1, 1, 1]);
    }

    #[test]
    fn count_limits_the_claim_and_returns_a_cursor() {
        let (_, reply) = request(&["XAUTOCLAIM", "s", "g", "new", "0", "-", "COUNT", "2"])
            .mutate(snapshot(stream_with_pending()), 600);
        let Value::Array(Some(parts)) = reply else {
            panic!("expected [next, claimed, deleted]");
        };
        assert_eq!(
            parts[0].string_bytes_clone().as_deref(),
            Some(b"3-0".as_slice())
        );
        let Value::Array(Some(claimed)) = &parts[1] else {
            panic!("expected claimed entries");
        };
        assert_eq!(claimed.len(), 2);
    }

    #[test]
    fn min_idle_filters_entries() {
        let (_, reply) = request(&["XAUTOCLAIM", "s", "g", "new", "200", "-"])
            .mutate(snapshot(stream_with_pending()), 600);
        let Value::Array(Some(parts)) = reply else {
            panic!("expected [next, claimed, deleted]");
        };
        assert!(matches!(&parts[1], Value::Array(Some(claimed)) if claimed.is_empty()));
        assert!(matches!(&parts[2], Value::Array(Some(deleted)) if deleted.is_empty()));
    }

    #[test]
    fn deleted_payloads_are_removed_from_the_pel() {
        let mut stream = stream_with_pending();
        stream.xdel(&[StreamId::new(1, 0)]);
        let (_, reply) =
            request(&["XAUTOCLAIM", "s", "g", "new", "0", "-"]).mutate(snapshot(stream), 600);
        let Value::Array(Some(parts)) = reply else {
            panic!("expected [next, claimed, deleted]");
        };
        let Value::Array(Some(deleted)) = &parts[2] else {
            panic!("expected deleted ids");
        };
        assert_eq!(deleted.len(), 1);
        assert_eq!(
            deleted[0].string_bytes_clone().as_deref(),
            Some(b"1-0".as_slice())
        );
    }

    #[test]
    fn claims_use_the_supplied_write_clock() {
        let (operation, _) = request(&["XAUTOCLAIM", "s", "g", "new", "0", "-"])
            .mutate(snapshot(stream_with_pending()), 600);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("auto-claiming must rewrite the stream");
        };
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        let entries = stream
            .read()
            .xpending_range(
                b"g",
                PendingQuery {
                    range: IdRange::all(),
                    count: 100,
                    consumer: None,
                    min_idle_ms: None,
                },
            )
            .unwrap();
        assert!(
            entries.iter().all(|entry| entry.last_delivery_ms == 600),
            "every claimed entry must record the replicated clock"
        );
    }

    #[test]
    fn raft_request_uses_the_xautoclaim_base_operation() {
        let operation = XAutoClaimCommand
            .raft_request(&arguments(&["XAUTOCLAIM", "s", "g", "c", "0", "-"]))
            .unwrap();
        assert!(matches!(
            operation,
            Operation::Base(BaseOperation::XAutoClaim(_))
        ));
    }
}
