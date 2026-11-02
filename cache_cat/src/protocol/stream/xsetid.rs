//! XSETID command implementation.
//!
//! `XSETID key last-id [ENTRIESADDED n] [MAXDELETEDID id]`
//!
//! Administrative metadata operation for replicating stream values. The last ID
//! must not move behind the current tombstone or the top live entry, and
//! `ENTRIESADDED` cannot be smaller than the current length.

use super::parse_stream_id;
use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::option_eq;
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::{SetIdOptions, StreamId};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Replicated XSETID request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XSetIdReq {
    pub key: Bytes,
    pub id: StreamId,
    pub options: SetIdOptions,
}

impl fmt::Display for XSetIdReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "XSETID {} {} entries_added={:?} max_deleted={:?}",
            String::from_utf8_lossy(&self.key),
            self.id,
            self.options.entries_added,
            self.options.max_deleted_id
        )
    }
}

impl XSetIdReq {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("xsetid"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let id = parse_stream_id(&items[2])?;
        let mut options = SetIdOptions::default();
        let mut index = 3;
        while index < items.len() {
            let option = items[index]
                .string_bytes_clone()
                .ok_or(ProtocolError::SyntaxError)?;
            if option_eq(&option, b"ENTRIESADDED") && index + 1 < items.len() {
                let value = items[index + 1].try_parse_canonical_i64()?;
                if value < 0 {
                    return Err(ProtocolError::response(
                        "ERR entries_added must be positive",
                    ));
                }
                options.entries_added =
                    Some(u64::try_from(value).map_err(|_| ProtocolError::NotAnInteger)?);
                index += 2;
            } else if option_eq(&option, b"MAXDELETEDID") && index + 1 < items.len() {
                let max_deleted = parse_stream_id(&items[index + 1])?;
                if id < max_deleted {
                    return Err(ProtocolError::response(
                        "ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id",
                    ));
                }
                options.max_deleted_id = Some(max_deleted);
                index += 2;
            } else {
                return Err(ProtocolError::SyntaxError);
            }
        }
        Ok(Self { key, id, options })
    }
}

impl ComputeCommand for XSetIdReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::XSetId(self)
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
        let result = {
            let mut state = stream.write();
            if self.id < state.max_deleted_entry_id() {
                return (
                    MochaOperation::Abort,
                    ProtocolError::response(
                        "ERR The ID specified in XSETID is smaller than current max_deleted_entry_id",
                    )
                    .into(),
                );
            }
            if state.last_entry_id().is_some_and(|last| last > self.id) {
                return (
                    MochaOperation::Abort,
                    ProtocolError::response(
                        "ERR The ID specified in XSETID is smaller than the target stream top item",
                    )
                    .into(),
                );
            }
            if self
                .options
                .entries_added
                .is_some_and(|added| added < state.xlen() as u64)
            {
                return (
                    MochaOperation::Abort,
                    ProtocolError::response(
                        "ERR The entries_added specified in XSETID is smaller than the target stream length",
                    )
                    .into(),
                );
            }
            state.xsetid(self.id, self.options)
        };
        match result {
            Ok(()) => (
                MochaOperation::Insert {
                    value: MyValue {
                        version,
                        data: ValueObject::Stream(stream),
                    },
                    expire,
                },
                Value::ok(),
            ),
            // All public error paths are checked above; keep a defensive reply
            // if the core ever adds another invariant.
            Err(error) => (
                MochaOperation::Abort,
                ProtocolError::response(format!("ERR {error}")).into(),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (
            MochaOperation::Abort,
            ProtocolError::response("ERR no such key").into(),
        )
    }
}

/// XSETID command handler.
pub struct XSetIdCommand;

impl RaftCommand for XSetIdCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(BaseOperation::XSetId(XSetIdReq::parse(
            items,
        )?)))
    }
}

#[async_trait]
impl Command for XSetIdCommand {
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
    use crate::raft::types::core::structure::stream::{AddId, RedisStream};
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn arguments(args: &[&str]) -> Vec<Value> {
        args.iter()
            .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
            .collect()
    }

    fn parse(args: &[&str]) -> Result<XSetIdReq, ProtocolError> {
        XSetIdReq::parse(&arguments(args))
    }

    fn request(args: &[&str]) -> XSetIdReq {
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

    #[test]
    fn parses_ids_and_options() {
        let req = request(&[
            "XSETID",
            "s",
            "10-0",
            "ENTRIESADDED",
            "20",
            "MAXDELETEDID",
            "9-0",
        ]);
        assert_eq!(req.id, StreamId::new(10, 0));
        assert_eq!(req.options.entries_added, Some(20));
        assert_eq!(req.options.max_deleted_id, Some(StreamId::new(9, 0)));

        // Repeated options keep the last value.
        let req = request(&[
            "XSETID",
            "s",
            "10-0",
            "ENTRIESADDED",
            "20",
            "ENTRIESADDED",
            "30",
        ]);
        assert_eq!(req.options.entries_added, Some(30));
    }

    #[test]
    fn rejects_invalid_arguments_like_redis() {
        assert!(matches!(
            parse(&["XSETID", "s"]),
            Err(ProtocolError::WrongArgCount("xsetid"))
        ));
        assert!(matches!(
            parse(&["XSETID", "s", "bogus"]),
            Err(ProtocolError::Response(ref message))
                if message.starts_with("ERR Invalid stream ID")
        ));
        assert!(matches!(
            parse(&["XSETID", "s", "-"]),
            Err(ProtocolError::Response(ref message))
                if message.starts_with("ERR Invalid stream ID")
        ));
        assert!(matches!(
            parse(&["XSETID", "s", "1-0", "ENTRIESADDED", "-1"]),
            Err(ProtocolError::Response(ref message))
                if message == "ERR entries_added must be positive"
        ));
        assert!(matches!(
            parse(&["XSETID", "s", "1-0", "ENTRIESADDED", "abc"]),
            Err(ProtocolError::NotAnInteger)
        ));
        assert!(matches!(
            parse(&["XSETID", "s", "1-0", "MAXDELETEDID", "2-0"]),
            Err(ProtocolError::Response(ref message))
                if message.contains("provided max_deleted_entry_id")
        ));
        assert!(matches!(
            parse(&["XSETID", "s", "1-0", "MAXDELETEDID", "bogus"]),
            Err(ProtocolError::Response(ref message))
                if message.starts_with("ERR Invalid stream ID")
        ));
        assert!(matches!(
            parse(&["XSETID", "s", "1-0", "BOGUS"]),
            Err(ProtocolError::SyntaxError)
        ));
        assert!(matches!(
            parse(&["XSETID", "s", "1-0", "ENTRIESADDED"]),
            Err(ProtocolError::SyntaxError)
        ));
    }

    #[test]
    fn missing_key_is_an_error() {
        let (operation, reply) = request(&["XSETID", "s", "1-0"]).init();
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message) if message == "ERR no such key"
        ));
    }

    #[test]
    fn wrong_type_is_an_error() {
        let wrong_type = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        let (operation, reply) = request(&["XSETID", "s", "1-0"]).mutate(wrong_type, 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
    }

    #[test]
    fn rejects_ids_behind_the_tombstone_or_top_entry() {
        let mut stream = stream_with_entries(2);
        stream.xdel(&[StreamId::new(2, 0)]);
        let (operation, reply) = request(&["XSETID", "s", "1-0"]).mutate(snapshot(stream), 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message)
                if message.contains("current max_deleted_entry_id")
        ));

        let (operation, reply) =
            request(&["XSETID", "s", "1-0"]).mutate(snapshot(stream_with_entries(2)), 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message) if message.contains("target stream top item")
        ));
    }

    #[test]
    fn rejects_entries_added_smaller_than_the_length() {
        let (operation, reply) = request(&["XSETID", "s", "5-0", "ENTRIESADDED", "1"])
            .mutate(snapshot(stream_with_entries(2)), 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message)
                if message.contains("entries_added specified in XSETID")
        ));
    }

    #[test]
    fn updates_metadata_and_returns_ok() {
        let (operation, reply) = request(&[
            "XSETID",
            "s",
            "10-0",
            "ENTRIESADDED",
            "20",
            "MAXDELETEDID",
            "9-0",
        ])
        .mutate(snapshot(stream_with_entries(2)), 0);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("XSETID must rewrite the stream");
        };
        assert!(matches!(reply, Value::SimpleString(ref status) if status == "OK"));
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        let stream = stream.read();
        assert_eq!(stream.last_generated_id(), StreamId::new(10, 0));
        assert_eq!(stream.entries_added(), 20);
        assert_eq!(stream.max_deleted_entry_id(), StreamId::new(9, 0));
    }

    #[test]
    fn max_deleted_zero_keeps_the_existing_tombstone() {
        let mut stream = stream_with_entries(2);
        stream.xdel(&[StreamId::new(1, 0)]);
        let (operation, _) =
            request(&["XSETID", "s", "10-0", "MAXDELETEDID", "0-0"]).mutate(snapshot(stream), 0);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("XSETID must rewrite the stream");
        };
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        assert_eq!(stream.read().max_deleted_entry_id(), StreamId::new(1, 0));
    }

    #[test]
    fn raft_request_uses_the_xsetid_base_operation() {
        let operation = XSetIdCommand
            .raft_request(&arguments(&["XSETID", "s", "1-0"]))
            .unwrap();
        assert!(matches!(
            operation,
            Operation::Base(BaseOperation::XSetId(_))
        ));
    }
}
