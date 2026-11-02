//! XDELEX command implementation.
//!
//! `XDELEX key [KEEPREF|DELREF|ACKED] IDS numids id [id ...]`
//!
//! Deletes payloads without requiring a PEL entry. The reference policy decides
//! whether pending references block the deletion (`ACKED`) or are removed
//! together with the payload (`DELREF`). Redis validates the key type before
//! the options and the IDs, so the raw option block is kept in the replicated
//! request and parsed during execution.

use super::{delete_status_reply, not_found_statuses, parse_delete_args, parse_strict_ids};
use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::{DeleteStatus, ReferencePolicy};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Replicated XDELEX request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XDelexReq {
    pub key: Bytes,
    /// `[KEEPREF|DELREF|ACKED] IDS numids id ...`, validated at apply time.
    pub args: Vec<Bytes>,
}

impl fmt::Display for XDelexReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "XDELEX {} ({} args)",
            String::from_utf8_lossy(&self.key),
            self.args.len()
        )
    }
}

impl XDelexReq {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // XDELEX key IDS numids id
        if items.len() < 5 {
            return Err(ProtocolError::WrongArgCount("xdelex"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let args = items[2..]
            .iter()
            .map(|value| {
                value
                    .string_bytes_clone()
                    .ok_or(ProtocolError::InvalidArgument("argument"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { key, args })
    }
}

impl ComputeCommand for XDelexReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::XDelex(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expire = entry.get_expire_policy();
        let version = entry.value.version;
        let ValueObject::Stream(stream) = entry.value.data else {
            return (MochaOperation::Abort, ProtocolError::WrongType.into());
        };
        let parsed = match parse_delete_args(&self.args) {
            Ok(parsed) => parsed,
            Err(error) => return (MochaOperation::Abort, error.into()),
        };
        let ids = match parse_strict_ids(&parsed.ids) {
            Ok(ids) => ids,
            Err(error) => return (MochaOperation::Abort, error.into()),
        };
        let statuses = stream.write().xdelex(&ids, parsed.references);
        // DELREF can clear PEL references even when the payload is already
        // gone, in which case every status is NotFound but state did change.
        // Acked's StillReferenced status means nothing was modified.
        let changed = parsed.references == ReferencePolicy::DelRef
            || statuses
                .iter()
                .any(|status| *status == DeleteStatus::Deleted);
        let reply = delete_status_reply(statuses);
        if !changed {
            return (MochaOperation::Abort, reply);
        }
        (
            MochaOperation::Insert {
                value: MyValue {
                    version,
                    data: ValueObject::Stream(stream),
                },
                expire,
            },
            reply,
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        // Options are parsed even when the key is missing, but IDs are not.
        match parse_delete_args(&self.args) {
            Ok(parsed) => (MochaOperation::Abort, not_found_statuses(parsed.ids.len())),
            Err(error) => (MochaOperation::Abort, error.into()),
        }
    }
}

/// XDELEX command handler.
pub struct XDelexCommand;

impl RaftCommand for XDelexCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(BaseOperation::XDelex(XDelexReq::parse(
            items,
        )?)))
    }
}

#[async_trait]
impl Command for XDelexCommand {
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
        AddId, GroupRead, GroupStart, ReadGroupOptions, RedisStream, StreamId,
    };
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn arguments(args: &[&str]) -> Vec<Value> {
        args.iter()
            .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
            .collect()
    }

    fn parse(args: &[&str]) -> Result<XDelexReq, ProtocolError> {
        XDelexReq::parse(&arguments(args))
    }

    fn request(args: &[&str]) -> XDelexReq {
        parse(args).unwrap()
    }

    fn snapshot(stream: RedisStream) -> EntrySnapshot<MyValue> {
        EntrySnapshot {
            value: MyValue::new(ValueObject::Stream(Arc::new(RwLock::new(stream)))),
            expire_at: None,
        }
    }

    fn stream_with_entries(count: u64) -> RedisStream {
        let mut stream = RedisStream::new();
        for index in 1..=count {
            stream
                .xadd(
                    AddId::Explicit(StreamId::new(index, 0)),
                    vec![(b"field".to_vec(), b"value".to_vec())],
                )
                .unwrap();
        }
        stream
    }

    fn stream_with_pending() -> RedisStream {
        let mut stream = stream_with_entries(2);
        stream
            .xgroup_create(b"g", GroupStart::Id(StreamId::ZERO), None)
            .unwrap();
        stream
            .xreadgroup(
                b"g",
                b"c",
                GroupRead::New,
                ReadGroupOptions {
                    count: None,
                    no_ack: false,
                },
            )
            .unwrap();
        stream
    }

    fn statuses(reply: &Value) -> Vec<i64> {
        let Value::Array(Some(items)) = reply else {
            panic!("expected a status array");
        };
        items
            .iter()
            .map(|value| {
                let Value::Integer(status) = value else {
                    panic!("expected an integer status");
                };
                *status
            })
            .collect()
    }

    #[test]
    fn rejects_bad_arity() {
        assert!(matches!(
            parse(&["XDELEX", "s", "IDS", "1"]),
            Err(ProtocolError::WrongArgCount("xdelex"))
        ));
        assert_eq!(
            parse(&["XDELEX", "s", "IDS", "1", "1-0"]).unwrap().args,
            vec![
                Bytes::from_static(b"IDS"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"1-0")
            ]
        );
    }

    #[test]
    fn missing_key_reports_one_minus_one_per_id() {
        let (operation, reply) = request(&["XDELEX", "s", "IDS", "2", "1-0", "2-0"]).init();
        assert!(matches!(operation, MochaOperation::Abort));
        assert_eq!(statuses(&reply), [-1, -1]);
    }

    #[test]
    fn wrong_type_is_checked_before_options() {
        let wrong_type = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        let (operation, reply) =
            request(&["XDELEX", "s", "BOGUS", "IDS", "1", "1-0"]).mutate(wrong_type, 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
    }

    #[test]
    fn validates_options_and_ids() {
        assert!(matches!(
            request(&["XDELEX", "s", "IDS", "2", "1-0"]).init().1,
            Value::Error(ref message)
                if message.contains("`numids` parameter must match")
        ));
        assert!(matches!(
            request(&["XDELEX", "s", "IDS", "1", "bogus"]).mutate(
                snapshot(stream_with_entries(1)),
                0
            ),
            (MochaOperation::Abort, Value::Error(ref message))
                if message.starts_with("ERR Invalid stream ID")
        ));
    }

    #[test]
    fn deletes_payloads_and_advances_the_tombstone() {
        let (operation, reply) = request(&["XDELEX", "s", "IDS", "2", "1-0", "9-0"])
            .mutate(snapshot(stream_with_entries(2)), 0);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("XDELEX must rewrite the stream");
        };
        assert_eq!(statuses(&reply), [1, -1]);
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        assert_eq!(stream.read().xlen(), 1);
        assert_eq!(stream.read().max_deleted_entry_id(), StreamId::new(1, 0));
    }

    #[test]
    fn acked_keeps_referenced_entries() {
        let (operation, reply) = request(&["XDELEX", "s", "ACKED", "IDS", "2", "1-0", "2-0"])
            .mutate(snapshot(stream_with_pending()), 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert_eq!(statuses(&reply), [2, 2]);
    }

    #[test]
    fn delref_clears_group_references() {
        let (operation, reply) = request(&["XDELEX", "s", "DELREF", "IDS", "2", "1-0", "2-0"])
            .mutate(snapshot(stream_with_pending()), 0);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("XDELEX must rewrite the stream");
        };
        assert_eq!(statuses(&reply), [1, 1]);
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        assert_eq!(stream.read().xlen(), 0);
        assert_eq!(stream.read().xpending(b"g").unwrap().total, 0);
    }

    #[test]
    fn unknown_ids_report_not_found_without_rewriting() {
        let (operation, reply) = request(&["XDELEX", "s", "IDS", "1", "9-0"])
            .mutate(snapshot(stream_with_entries(1)), 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert_eq!(statuses(&reply), [-1]);
    }

    #[test]
    fn raft_request_uses_the_xdelex_base_operation() {
        let operation = XDelexCommand
            .raft_request(&arguments(&["XDELEX", "s", "IDS", "1", "1-0"]))
            .unwrap();
        assert!(matches!(
            operation,
            Operation::Base(BaseOperation::XDelex(_))
        ));
    }
}
