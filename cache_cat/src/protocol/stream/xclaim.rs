//! XCLAIM command implementation.
//!
//! `XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms]
//!         [TIME unix-ms] [RETRYCOUNT count] [FORCE] [JUSTID] [LASTID id]`
//!
//! Claiming rewrites the group PEL and delivery counters, so it is a replicated
//! single-key read-modify-write command. The delivery timestamp comes from the
//! replicated write clock, never from a local wall clock.

use super::{no_group, parse_stream_id};
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
    ClaimOptions, ClaimReply, DeliveryTime, StreamId,
};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Replicated XCLAIM request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XClaimReq {
    pub key: Bytes,
    pub group: Bytes,
    pub consumer: Bytes,
    pub ids: Vec<StreamId>,
    pub options: ClaimOptions,
}

impl fmt::Display for XClaimReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "XCLAIM {} {} {} {:?}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.group),
            String::from_utf8_lossy(&self.consumer),
            self.ids
        )
    }
}

fn option_error(option: &str) -> ProtocolError {
    ProtocolError::response(format!("ERR Invalid {option} option argument"))
}

fn non_negative(value: &Value, option: &str) -> Result<u64, ProtocolError> {
    let parsed = value.try_parse_canonical_i64()?;
    u64::try_from(parsed).map_err(|_| option_error(option))
}

impl XClaimReq {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 6 {
            return Err(ProtocolError::WrongArgCount("xclaim"));
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
        let min_idle = items[4].try_parse_canonical_i64()?;
        let mut options = ClaimOptions {
            // Redis treats a negative min-idle-time as zero.
            min_idle_ms: if min_idle < 0 { 0 } else { min_idle as u64 },
            ..ClaimOptions::default()
        };
        let mut ids = Vec::new();
        let mut index = 5;
        while index < items.len() {
            let option = items[index]
                .string_bytes_clone()
                .ok_or(ProtocolError::SyntaxError)?;
            if option_eq(&option, b"IDLE") && index + 1 < items.len() {
                options.delivery_time =
                    DeliveryTime::IdleMs(non_negative(&items[index + 1], "IDLE")?);
                index += 2;
            } else if option_eq(&option, b"TIME") && index + 1 < items.len() {
                options.delivery_time =
                    DeliveryTime::UnixMs(non_negative(&items[index + 1], "TIME")?);
                index += 2;
            } else if option_eq(&option, b"RETRYCOUNT") && index + 1 < items.len() {
                options.retry_count = Some(non_negative(&items[index + 1], "RETRYCOUNT")?);
                index += 2;
            } else if option_eq(&option, b"LASTID") && index + 1 < items.len() {
                options.last_id = Some(parse_stream_id(&items[index + 1])?);
                index += 2;
            } else if option_eq(&option, b"FORCE") {
                options.force = true;
                index += 1;
            } else if option_eq(&option, b"JUSTID") {
                options.just_id = true;
                index += 1;
            } else {
                ids.push(parse_stream_id(&items[index])?);
                index += 1;
            }
        }
        Ok(Self {
            key,
            group,
            consumer,
            ids,
            options,
        })
    }
}

/// Shared with XAUTOCLAIM, whose second reply element has the same shape.
pub(super) fn claim_reply(reply: ClaimReply) -> Value {
    match reply {
        ClaimReply::Entries(entries) => Value::Array(Some(
            entries
                .into_iter()
                .map(|entry| {
                    let fields = entry
                        .fields
                        .into_iter()
                        .flat_map(|(field, value)| {
                            [
                                Value::BulkString(Some(field.into())),
                                Value::BulkString(Some(value.into())),
                            ]
                        })
                        .collect();
                    Value::Array(Some(vec![
                        Value::BulkString(Some(entry.id.to_string().into())),
                        Value::Array(Some(fields)),
                    ]))
                })
                .collect(),
        )),
        ClaimReply::Ids(ids) => Value::Array(Some(
            ids.into_iter()
                .map(|id| Value::BulkString(Some(id.to_string().into())))
                .collect(),
        )),
    }
}

impl ComputeCommand for XClaimReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::XClaim(self)
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
        let result = stream.write().xclaim_at(
            &self.group,
            &self.consumer,
            &self.ids,
            self.options,
            write_clock,
        );
        match result {
            Ok(claimed) => (
                MochaOperation::Insert {
                    value: MyValue {
                        version,
                        data: ValueObject::Stream(stream),
                    },
                    expire,
                },
                claim_reply(claimed),
            ),
            Err(_) => (MochaOperation::Abort, no_group(&self.key, &self.group)),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (MochaOperation::Abort, no_group(&self.key, &self.group))
    }
}

/// XCLAIM command handler.
pub struct XClaimCommand;

impl RaftCommand for XClaimCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(BaseOperation::XClaim(XClaimReq::parse(
            items,
        )?)))
    }
}

#[async_trait]
impl Command for XClaimCommand {
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

    fn parse(args: &[&str]) -> Result<XClaimReq, ProtocolError> {
        XClaimReq::parse(&arguments(args))
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

    fn request(args: &[&str]) -> XClaimReq {
        parse(args).unwrap()
    }

    #[test]
    fn parses_ids_and_options_in_any_order() {
        let claim = request(&[
            "XCLAIM",
            "s",
            "g",
            "c",
            "-5",
            "1-0",
            "IDLE",
            "500",
            "2-0",
            "FORCE",
            "JUSTID",
            "RETRYCOUNT",
            "7",
            "LASTID",
            "3-0",
        ]);
        assert_eq!(claim.ids, vec![StreamId::new(1, 0), StreamId::new(2, 0)]);
        assert_eq!(claim.options.min_idle_ms, 0, "negative idle is clamped");
        assert_eq!(claim.options.delivery_time, DeliveryTime::IdleMs(500));
        assert!(claim.options.force);
        assert!(claim.options.just_id);
        assert_eq!(claim.options.retry_count, Some(7));
        assert_eq!(claim.options.last_id, Some(StreamId::new(3, 0)));

        let time = request(&["XCLAIM", "s", "g", "c", "0", "1-0", "TIME", "123"]);
        assert_eq!(time.options.delivery_time, DeliveryTime::UnixMs(123));
    }

    #[test]
    fn rejects_bad_arguments() {
        assert!(matches!(
            parse(&["XCLAIM", "s", "g", "c", "0"]),
            Err(ProtocolError::WrongArgCount("xclaim"))
        ));
        assert!(matches!(
            parse(&["XCLAIM", "s", "g", "c", "x", "1-0"]),
            Err(ProtocolError::NotAnInteger)
        ));
        assert!(matches!(
            parse(&["XCLAIM", "s", "g", "c", "0", "1-0", "IDLE", "-1"]),
            Err(ProtocolError::Response(ref message)) if message.starts_with("ERR Invalid IDLE")
        ));
        assert!(matches!(
            parse(&["XCLAIM", "s", "g", "c", "0", "bogus"]),
            Err(ProtocolError::Response(ref message)) if message.starts_with("ERR Invalid stream ID")
        ));
    }

    #[test]
    fn missing_key_or_group_is_nogroup() {
        let (operation, reply) = request(&["XCLAIM", "s", "g", "c", "0", "1-0"]).init();
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message) if message.starts_with("NOGROUP")
        ));
    }

    #[test]
    fn wrong_type_is_an_error() {
        let snapshot = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        let (operation, reply) =
            request(&["XCLAIM", "s", "g", "c", "0", "1-0"]).mutate(snapshot, 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
    }

    #[test]
    fn claims_pending_entries_and_moves_ownership() {
        let (operation, reply) = request(&["XCLAIM", "s", "g", "new", "0", "1-0"])
            .mutate(snapshot_with_pending(), 5_000);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("claiming must rewrite the stream");
        };
        let Value::Array(Some(entries)) = reply else {
            panic!("expected claimed entries");
        };
        assert_eq!(entries.len(), 1);
        let Value::Array(Some(entry)) = &entries[0] else {
            panic!("expected [id, fields]");
        };
        assert_eq!(
            entry[0].string_bytes_clone().as_deref(),
            Some(b"1-0".as_slice())
        );

        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        let info = stream.read().xpending(b"g").unwrap();
        assert_eq!(info.total, 1);
        assert_eq!(info.consumers.len(), 1);
        assert_eq!(info.consumers[0].0, b"new".to_vec());
    }

    #[test]
    fn justid_returns_ids_only() {
        let (_, reply) = request(&["XCLAIM", "s", "g", "new", "0", "1-0", "JUSTID"])
            .mutate(snapshot_with_pending(), 5_000);
        let Value::Array(Some(entries)) = reply else {
            panic!("expected ids");
        };
        assert_eq!(
            entries[0].string_bytes_clone().as_deref(),
            Some(b"1-0".as_slice())
        );
    }

    #[test]
    fn min_idle_time_filters_the_claim() {
        let (operation, reply) = request(&["XCLAIM", "s", "g", "new", "1000000", "1-0"])
            .mutate(snapshot_with_pending(), 5_000);
        assert!(matches!(reply, Value::Array(Some(ref entries)) if entries.is_empty()));
        // The consumer touch rewrites the stream, but ownership must not move.
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("an XCLAIM still touches the consumer");
        };
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        let info = stream.read().xpending(b"g").unwrap();
        assert_eq!(info.total, 1);
        assert_eq!(info.consumers[0].0, b"c".to_vec());
    }

    #[test]
    fn force_creates_a_pending_record_for_a_live_entry() {
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
        let snapshot = EntrySnapshot {
            value: MyValue::new(ValueObject::Stream(Arc::new(RwLock::new(stream)))),
            expire_at: None,
        };
        let (operation, reply) =
            request(&["XCLAIM", "s", "g", "c", "0", "1-0", "FORCE"]).mutate(snapshot, 5_000);
        assert!(matches!(operation, MochaOperation::Insert { .. }));
        assert!(matches!(reply, Value::Array(Some(ref entries)) if entries.len() == 1));
    }

    #[test]
    fn raft_request_uses_the_xclaim_base_operation() {
        let operation = XClaimCommand
            .raft_request(&arguments(&["XCLAIM", "s", "g", "c", "0", "1-0"]))
            .unwrap();
        assert!(matches!(
            operation,
            Operation::Base(BaseOperation::XClaim(_))
        ));
    }
}
