//! XDEL command implementation.
//!
//! `XDEL key id [id ...]` removes entries from a stream.
//!
//! Redis checks the key and its type before parsing any ID: a missing key
//! answers 0 even when an ID is malformed. IDs are therefore kept as raw bytes
//! in the replicated request and validated inside `mutate`, after the type
//! check. Deleting a payload does not touch the PEL, group cursors or the
//! last-generated ID; it only advances `max_deleted_entry_id`.

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

/// Replicated XDEL request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XDelReq {
    pub key: Bytes,
    /// Raw IDs, validated only once the key is known to hold a stream.
    pub ids: Vec<Bytes>,
}

impl fmt::Display for XDelReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "XDEL {}", String::from_utf8_lossy(&self.key))?;
        for id in &self.ids {
            write!(f, " {}", String::from_utf8_lossy(id))?;
        }
        Ok(())
    }
}

fn invalid_stream_id() -> ProtocolError {
    ProtocolError::response("ERR Invalid stream ID specified as stream command argument")
}

impl XDelReq {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("xdel"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let ids = items[2..]
            .iter()
            .map(|value| {
                value
                    .string_bytes_clone()
                    .ok_or(ProtocolError::InvalidArgument("id"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { key, ids })
    }
}

impl ComputeCommand for XDelReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::XDel(self)
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
        let deleted = stream.write().xdel(&ids);
        if deleted == 0 {
            // Nothing was removed, so the snapshot must not record a delta.
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
            Value::Integer(i64::try_from(deleted).unwrap_or(i64::MAX)),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        // A missing key answers 0 and must not be created.
        (MochaOperation::Abort, Value::Integer(0))
    }
}

/// XDEL command handler.
pub struct XDelCommand;

impl RaftCommand for XDelCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(BaseOperation::XDel(XDelReq::parse(items)?)))
    }
}

#[async_trait]
impl Command for XDelCommand {
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

    fn parse(args: &[&str]) -> Result<XDelReq, ProtocolError> {
        XDelReq::parse(&arguments(args))
    }

    fn stream_with_entries() -> RedisStream {
        let mut stream = RedisStream::new();
        for id in [StreamId::new(1, 0), StreamId::new(2, 0)] {
            stream
                .xadd(
                    AddId::Explicit(id),
                    vec![(b"field".to_vec(), b"value".to_vec())],
                )
                .unwrap();
        }
        stream
    }

    fn snapshot(stream: RedisStream) -> EntrySnapshot<MyValue> {
        EntrySnapshot {
            value: MyValue::new(ValueObject::Stream(Arc::new(RwLock::new(stream)))),
            expire_at: None,
        }
    }

    fn request(key: &str, ids: &[&str]) -> XDelReq {
        XDelReq {
            key: Bytes::copy_from_slice(key.as_bytes()),
            ids: ids
                .iter()
                .map(|id| Bytes::copy_from_slice(id.as_bytes()))
                .collect(),
        }
    }

    #[test]
    fn rejects_bad_arity() {
        assert!(matches!(
            parse(&["XDEL", "s"]),
            Err(ProtocolError::WrongArgCount("xdel"))
        ));
        assert_eq!(
            parse(&["XDEL", "s", "1-0", "2-0"]).unwrap().ids,
            vec![Bytes::from_static(b"1-0"), Bytes::from_static(b"2-0")]
        );
    }

    #[test]
    fn missing_key_is_zero_without_parsing_ids() {
        let (operation, reply) = request("missing", &["not-an-id"]).init();
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(reply, Value::Integer(0)));
    }

    #[test]
    fn wrong_type_is_an_error() {
        let snapshot = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        let (operation, reply) = request("s", &["1-0"]).mutate(snapshot, 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
    }

    #[test]
    fn invalid_id_on_a_stream_is_an_error_and_deletes_nothing() {
        let (operation, reply) =
            request("s", &["1-0", "bogus"]).mutate(snapshot(stream_with_entries()), 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message) if message.starts_with("ERR Invalid stream ID")
        ));
    }

    #[test]
    fn deletes_payloads_once_and_advances_the_tombstone() {
        let (operation, reply) =
            request("s", &["1-0", "1-0", "9-0"]).mutate(snapshot(stream_with_entries()), 0);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("deleting an entry must rewrite the stream");
        };
        assert!(matches!(reply, Value::Integer(1)));
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        let stream = stream.read();
        assert_eq!(stream.xlen(), 1);
        assert_eq!(stream.last_generated_id(), StreamId::new(2, 0));
        assert_eq!(
            stream.xinfo_stream().max_deleted_entry_id,
            StreamId::new(1, 0)
        );
        drop(stream);

        // Deleting an ID that is already gone records no snapshot delta.
        let entry = EntrySnapshot {
            value,
            expire_at: None,
        };
        let (operation, reply) = request("s", &["1-0"]).mutate(entry, 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(reply, Value::Integer(0)));
    }

    #[test]
    fn deleting_does_not_clear_the_group_pel() {
        let mut stream = stream_with_entries();
        stream
            .xgroup_create(b"g", GroupStart::Id(StreamId::ZERO), None)
            .unwrap();
        stream
            .xreadgroup(
                b"g",
                b"c",
                GroupRead::New,
                ReadGroupOptions {
                    count: Some(1),
                    no_ack: false,
                },
            )
            .unwrap();
        assert_eq!(stream.xpending(b"g").unwrap().total, 1);

        let (operation, reply) = request("s", &["1-0"]).mutate(snapshot(stream), 0);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("expected a stream rewrite");
        };
        assert!(matches!(reply, Value::Integer(1)));
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        // XDEL removes the payload but leaves the pending reference intact.
        assert_eq!(stream.read().xlen(), 1);
        assert_eq!(stream.read().xpending(b"g").unwrap().total, 1);
    }

    #[test]
    fn raft_request_uses_the_xdel_base_operation() {
        let operation = XDelCommand
            .raft_request(&arguments(&["XDEL", "s", "1-0"]))
            .unwrap();
        assert!(matches!(operation, Operation::Base(BaseOperation::XDel(_))));
    }
}
