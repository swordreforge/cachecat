//! XACKDEL command implementation.
//!
//! `XACKDEL key group [KEEPREF|DELREF|ACKED] IDS numids id [id ...]`
//!
//! Acknowledges PEL entries and deletes their payloads according to the
//! reference policy. The command is all-or-nothing: Redis checks the key type,
//! then the options, then the group and only then parses IDs. Raw options are
//! therefore kept in the replicated request and validated inside the command,
//! so malformed IDs cannot leave a partial acknowledgement behind.

use super::{delete_status_reply, not_found_statuses, parse_delete_args, parse_strict_ids};
use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::DeleteStatus;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Replicated XACKDEL request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XAckDelReq {
    pub key: Bytes,
    pub group: Bytes,
    /// `[KEEPREF|DELREF|ACKED] IDS numids id ...`, validated at apply time.
    pub args: Vec<Bytes>,
}

impl fmt::Display for XAckDelReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "XACKDEL {} {} ({} args)",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.group),
            self.args.len()
        )
    }
}

impl XAckDelReq {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // XACKDEL key group IDS numids id
        if items.len() < 6 {
            return Err(ProtocolError::WrongArgCount("xackdel"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let group = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("group"))?;
        let args = items[3..]
            .iter()
            .map(|value| {
                value
                    .string_bytes_clone()
                    .ok_or(ProtocolError::InvalidArgument("argument"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { key, group, args })
    }
}

impl ComputeCommand for XAckDelReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::XAckDel(self)
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
        // Redis checks the type before it parses the options.
        let parsed = match parse_delete_args(&self.args) {
            Ok(parsed) => parsed,
            Err(error) => return (MochaOperation::Abort, error.into()),
        };
        let statuses = {
            let mut state = stream.write();
            // A missing group answers one -1 per ID without parsing them.
            if !state.has_group(&self.group) {
                return (MochaOperation::Abort, not_found_statuses(parsed.ids.len()));
            }
            let ids = match parse_strict_ids(&parsed.ids) {
                Ok(ids) => ids,
                Err(error) => return (MochaOperation::Abort, error.into()),
            };
            state.xackdel(&self.group, &ids, parsed.references)
        };
        let changed = statuses
            .iter()
            .any(|status| *status != DeleteStatus::NotFound);
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

/// XACKDEL command handler.
pub struct XAckDelCommand;

impl RaftCommand for XAckDelCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(BaseOperation::XAckDel(XAckDelReq::parse(
            items,
        )?)))
    }
}

#[async_trait]
impl Command for XAckDelCommand {
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

    fn parse(args: &[&str]) -> Result<XAckDelReq, ProtocolError> {
        XAckDelReq::parse(&arguments(args))
    }

    fn request(args: &[&str]) -> XAckDelReq {
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
        for id in [StreamId::new(1, 0), StreamId::new(2, 0)] {
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
            parse(&["XACKDEL", "s", "g", "IDS", "1"]),
            Err(ProtocolError::WrongArgCount("xackdel"))
        ));
        assert_eq!(
            parse(&["XACKDEL", "s", "g", "IDS", "1", "1-0"])
                .unwrap()
                .args,
            vec![
                Bytes::from_static(b"IDS"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"1-0")
            ]
        );
    }

    #[test]
    fn missing_key_and_group_report_one_minus_one_per_id() {
        // Options are still validated before the missing-key reply.
        let (operation, reply) = request(&["XACKDEL", "s", "g", "IDS", "2", "1-0", "2-0"]).init();
        assert!(matches!(operation, MochaOperation::Abort));
        assert_eq!(statuses(&reply), [-1, -1]);

        assert!(matches!(
            request(&["XACKDEL", "s", "g", "BOGUS", "IDS", "1", "1-0"])
                .init()
                .1,
            Value::Error(_)
        ));
    }

    #[test]
    fn wrong_type_is_checked_before_options() {
        let wrong_type = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        let (operation, reply) =
            request(&["XACKDEL", "s", "g", "BOGUS", "IDS", "1", "1-0"]).mutate(wrong_type, 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
    }

    #[test]
    fn missing_group_does_not_parse_ids() {
        let mut stream = stream_with_pending();
        stream.xgroup_destroy(b"g");
        let (operation, reply) =
            request(&["XACKDEL", "s", "nope", "IDS", "1", "bogus"]).mutate(snapshot(stream), 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert_eq!(statuses(&reply), [-1]);
    }

    #[test]
    fn invalid_ids_are_an_error_and_leave_the_pel_intact() {
        let (operation, reply) = request(&["XACKDEL", "s", "g", "IDS", "2", "1-0", "bogus"])
            .mutate(snapshot(stream_with_pending()), 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message) if message.starts_with("ERR Invalid stream ID")
        ));
    }

    #[test]
    fn acknowledges_and_deletes_payloads() {
        let (operation, reply) = request(&["XACKDEL", "s", "g", "IDS", "2", "1-0", "2-0"])
            .mutate(snapshot(stream_with_pending()), 0);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("XACKDEL must rewrite the stream");
        };
        assert_eq!(statuses(&reply), [1, 1]);
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        assert_eq!(stream.read().xlen(), 0);
        assert_eq!(stream.read().xpending(b"g").unwrap().total, 0);
    }

    #[test]
    fn acked_keeps_entries_still_referenced_by_another_group() {
        let mut stream = stream_with_pending();
        stream
            .xgroup_create(b"other", GroupStart::Id(StreamId::ZERO), None)
            .unwrap();
        let (operation, reply) =
            request(&["XACKDEL", "s", "g", "ACKED", "IDS", "1", "1-0"]).mutate(snapshot(stream), 0);
        assert!(matches!(operation, MochaOperation::Insert { .. }));
        assert_eq!(statuses(&reply), [2]);
    }

    #[test]
    fn delref_clears_other_group_references() {
        let mut stream = stream_with_pending();
        stream
            .xgroup_create(b"other", GroupStart::Id(StreamId::ZERO), None)
            .unwrap();
        let (operation, reply) = request(&["XACKDEL", "s", "g", "DELREF", "IDS", "1", "1-0"])
            .mutate(snapshot(stream), 0);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("XACKDEL must rewrite the stream");
        };
        assert_eq!(statuses(&reply), [1]);
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        assert_eq!(stream.read().xlen(), 1);
        assert_eq!(stream.read().xpending(b"other").unwrap().total, 0);
    }

    #[test]
    fn unknown_ids_report_not_found_without_rewriting() {
        let (operation, reply) = request(&["XACKDEL", "s", "g", "IDS", "1", "9-0"])
            .mutate(snapshot(stream_with_pending()), 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert_eq!(statuses(&reply), [-1]);
    }

    #[test]
    fn raft_request_uses_the_xackdel_base_operation() {
        let operation = XAckDelCommand
            .raft_request(&arguments(&["XACKDEL", "s", "g", "IDS", "1", "1-0"]))
            .unwrap();
        assert!(matches!(
            operation,
            Operation::Base(BaseOperation::XAckDel(_))
        ));
    }
}
