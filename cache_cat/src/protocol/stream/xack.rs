//! XACK command implementation.
//!
//! `XACK key group id [id ...]` removes delivered entries from the pending
//! entries list of a stream consumer group.
//!
//! Redis checks the key, its type and the consumer group before parsing IDs:
//! a missing key or group answers 0 without touching the keyspace, while an
//! existing group with an invalid ID is an error.

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::StreamId;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Replicated XACK request.
///
/// IDs stay as raw bytes so the protocol layer can preserve Redis' validation
/// order: key, type and group are inspected before any ID is parsed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XAckReq {
    pub key: Bytes,
    pub group: Bytes,
    pub ids: Vec<Bytes>,
}

impl fmt::Display for XAckReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "XACK {} {}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.group)
        )?;
        for id in &self.ids {
            write!(f, " {}", String::from_utf8_lossy(id))?;
        }
        Ok(())
    }
}

fn invalid_stream_id() -> ProtocolError {
    ProtocolError::response("ERR Invalid stream ID specified as stream command argument")
}

impl XAckReq {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("xack"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let group = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("group"))?;
        let ids = items[3..]
            .iter()
            .map(|value| {
                value
                    .string_bytes_clone()
                    .ok_or(ProtocolError::InvalidArgument("id"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { key, group, ids })
    }
}

impl ComputeCommand for XAckReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::XAck(self)
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
        let mut state = stream.write();
        // A missing group is a zero acknowledgement count, and Redis never
        // parses the IDs in that case.
        if !state.has_group(&self.group) {
            return (MochaOperation::Abort, Value::Integer(0));
        }
        let mut ids = Vec::with_capacity(self.ids.len());
        for raw in &self.ids {
            let id = std::str::from_utf8(raw)
                .ok()
                .and_then(|text| StreamId::from_str(text).ok());
            let Some(id) = id else {
                return (MochaOperation::Abort, invalid_stream_id().into());
            };
            ids.push(id);
        }
        let acked = state.xack(&self.group, &ids);
        drop(state);
        if acked == 0 {
            // Nothing left the PEL, so the snapshot must not record a delta.
            return (MochaOperation::Abort, Value::Integer(0));
        }
        (
            MochaOperation::Insert {
                value: MyValue {
                    version,
                    data: ValueObject::Stream(stream),
                },
                expire,
            },
            Value::Integer(i64::try_from(acked).unwrap_or(i64::MAX)),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        // A missing key answers 0 and must not be created.
        (MochaOperation::Abort, Value::Integer(0))
    }
}

/// XACK command handler.
pub struct XAckCommand;

impl RaftCommand for XAckCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(BaseOperation::XAck(XAckReq::parse(items)?)))
    }
}

#[async_trait]
impl Command for XAckCommand {
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
        AddId, GroupRead, GroupStart, ReadGroupOptions, RedisStream,
    };
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn arguments(args: &[&str]) -> Vec<Value> {
        args.iter()
            .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
            .collect()
    }

    fn parse(args: &[&str]) -> Result<XAckReq, ProtocolError> {
        XAckReq::parse(&arguments(args))
    }

    fn snapshot_with_pending() -> EntrySnapshot<MyValue> {
        let mut stream = RedisStream::new();
        stream
            .xadd(
                AddId::Explicit(StreamId::new(1, 0)),
                vec![(b"field".to_vec(), b"value".to_vec())],
            )
            .unwrap();
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
        EntrySnapshot {
            value: MyValue::new(ValueObject::Stream(Arc::new(RwLock::new(stream)))),
            expire_at: None,
        }
    }

    fn request(key: &str, group: &str, ids: &[&str]) -> XAckReq {
        XAckReq {
            key: Bytes::copy_from_slice(key.as_bytes()),
            group: Bytes::copy_from_slice(group.as_bytes()),
            ids: ids
                .iter()
                .map(|id| Bytes::copy_from_slice(id.as_bytes()))
                .collect(),
        }
    }

    #[test]
    fn rejects_bad_arity_and_bad_arguments() {
        assert!(matches!(
            parse(&["XACK", "s", "g"]),
            Err(ProtocolError::WrongArgCount("xack"))
        ));
        assert_eq!(
            parse(&["XACK", "s", "g", "1-0"]).unwrap().ids,
            vec![Bytes::from_static(b"1-0")]
        );
    }

    #[test]
    fn missing_key_is_zero_without_creating_it() {
        let (operation, reply) = request("missing", "g", &["1-0"]).init();
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(reply, Value::Integer(0)));
    }

    #[test]
    fn missing_key_beats_an_invalid_id() {
        // Redis returns 0 before it parses any ID.
        let (operation, reply) = request("missing", "g", &["not-an-id"]).init();
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(reply, Value::Integer(0)));
    }

    #[test]
    fn missing_group_is_zero() {
        let mut stream = RedisStream::new();
        stream
            .xadd(
                AddId::Explicit(StreamId::new(1, 0)),
                vec![(b"field".to_vec(), b"value".to_vec())],
            )
            .unwrap();
        let snapshot = EntrySnapshot {
            value: MyValue::new(ValueObject::Stream(Arc::new(RwLock::new(stream)))),
            expire_at: None,
        };
        let (operation, reply) = request("s", "nope", &["not-an-id"]).mutate(snapshot, 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(reply, Value::Integer(0)));
    }

    #[test]
    fn wrong_type_is_an_error() {
        let snapshot = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        let (operation, reply) = request("s", "g", &["1-0"]).mutate(snapshot, 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
    }

    #[test]
    fn invalid_id_after_the_group_is_known_is_an_error() {
        let (operation, reply) =
            request("s", "g", &["1-0", "bogus"]).mutate(snapshot_with_pending(), 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message) if message.starts_with("ERR Invalid stream ID")
        ));
    }

    #[test]
    fn acknowledges_pending_entries_once() {
        let (operation, reply) = request("s", "g", &["1-0"]).mutate(snapshot_with_pending(), 0);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("acknowledging a pending entry must rewrite the stream");
        };
        assert!(matches!(reply, Value::Integer(1)));
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        assert_eq!(stream.read().xpending(b"g").unwrap().total, 0);
        // The entry itself is retained; XACK only clears the PEL.
        assert_eq!(stream.read().xlen(), 1);

        // A second acknowledgement finds nothing and records no delta.
        let entry = EntrySnapshot {
            value,
            expire_at: None,
        };
        let (operation, reply) = request("s", "g", &["1-0"]).mutate(entry, 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(reply, Value::Integer(0)));
    }

    #[test]
    fn unknown_ids_acknowledge_nothing() {
        let (operation, reply) = request("s", "g", &["9-0"]).mutate(snapshot_with_pending(), 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(reply, Value::Integer(0)));
    }

    #[test]
    fn raft_request_uses_the_xack_base_operation() {
        let operation = XAckCommand
            .raft_request(&arguments(&["XACK", "s", "g", "1-0"]))
            .unwrap();
        assert!(matches!(operation, Operation::Base(BaseOperation::XAck(_))));
    }
}
