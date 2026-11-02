//! XPENDING command implementation.
//!
//! Two forms are supported:
//! - `XPENDING key group` — summary of the group PEL.
//! - `XPENDING key group [IDLE ms] start end count [consumer]` — PEL entries.
//!
//! Idle times come from the supplied logical clock, so a replied read inside
//! EXEC or Lua produces the same values on every replica.

use super::{no_group, parse_interval};
use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::option_eq;
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::{
    IdRange, PendingInfo, PendingQuery, PendingSummary, RedisStream, StreamId,
};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Extended-form arguments for XPENDING.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XPendingRange {
    pub start: StreamId,
    pub end: StreamId,
    pub count: usize,
    pub consumer: Option<Bytes>,
    pub idle_ms: Option<u64>,
}

/// Parsed arguments for XPENDING.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XPendingParams {
    pub key: Bytes,
    pub group: Bytes,
    /// `None` selects the summary form.
    pub range: Option<XPendingRange>,
}

impl XPendingParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("xpending"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let group = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("group"))?;
        if items.len() == 3 {
            return Ok(Self {
                key,
                group,
                range: None,
            });
        }
        let mut index = 3;
        let mut idle_ms = None;
        if option_eq(&items[3].string_bytes_clone().unwrap_or_default(), b"IDLE") {
            let idle = items
                .get(4)
                .ok_or(ProtocolError::WrongArgCount("xpending"))?
                .try_parse_canonical_i64()?;
            // Redis clamps a negative IDLE to zero instead of rejecting it.
            idle_ms = Some(if idle < 0 { 0 } else { idle as u64 });
            index = 5;
        }
        if items.len() < index + 3 {
            return Err(ProtocolError::WrongArgCount("xpending"));
        }
        if items.len() > index + 4 {
            return Err(ProtocolError::SyntaxError);
        }
        let (start, end) = parse_interval(&items[index], &items[index + 1])?;
        let count = items[index + 2].try_parse_canonical_i64()?;
        let consumer = items
            .get(index + 3)
            .map(|value| {
                value
                    .string_bytes_clone()
                    .ok_or(ProtocolError::InvalidArgument("consumer"))
            })
            .transpose()?;
        Ok(Self {
            key,
            group,
            range: Some(XPendingRange {
                start,
                end,
                count: usize::try_from(count.max(0)).map_err(|_| ProtocolError::NotAnInteger)?,
                consumer,
                idle_ms,
            }),
        })
    }

    fn query<'a>(&self, range: &'a XPendingRange) -> PendingQuery<'a> {
        PendingQuery {
            range: IdRange::inclusive(range.start, range.end),
            count: range.count,
            consumer: range.consumer.as_deref(),
            min_idle_ms: range.idle_ms,
        }
    }

    fn summary(stream: &RedisStream, params: &Self) -> Value {
        match stream.xpending(&params.group) {
            Ok(summary) => summary_reply(summary),
            Err(_) => no_group(&params.key, &params.group),
        }
    }

    fn page(stream: &RedisStream, params: &Self, range: &XPendingRange, clock: u64) -> Value {
        match stream.xpending_range_at(&params.group, params.query(range), clock) {
            Ok(entries) => Value::Array(Some(entries.into_iter().map(entry_reply).collect())),
            Err(_) => no_group(&params.key, &params.group),
        }
    }

    fn reply(&self, stream: &RedisStream, clock: Option<u64>) -> Value {
        match &self.range {
            None => Self::summary(stream, self),
            Some(range) => match clock {
                // `execute` has no logical clock; use the stream's own clock,
                // because such a call is by definition not replicated.
                None => match stream.xpending_range(&self.group, self.query(range)) {
                    Ok(entries) => {
                        Value::Array(Some(entries.into_iter().map(entry_reply).collect()))
                    }
                    Err(_) => no_group(&self.key, &self.group),
                },
                Some(clock) => Self::page(stream, self, range, clock),
            },
        }
    }
}

fn bulk(id: Option<StreamId>) -> Value {
    match id {
        Some(id) => Value::BulkString(Some(id.to_string().into())),
        None => Value::BulkString(None),
    }
}

fn summary_reply(summary: PendingSummary) -> Value {
    let consumers = summary
        .consumers
        .into_iter()
        .map(|(name, count)| {
            Value::Array(Some(vec![
                Value::BulkString(Some(name.into())),
                Value::Integer(i64::try_from(count).unwrap_or(i64::MAX)),
            ]))
        })
        .collect();
    Value::Array(Some(vec![
        Value::Integer(i64::try_from(summary.total).unwrap_or(i64::MAX)),
        bulk(summary.min_id),
        bulk(summary.max_id),
        Value::Array(Some(consumers)),
    ]))
}

fn entry_reply(entry: PendingInfo) -> Value {
    Value::Array(Some(vec![
        Value::BulkString(Some(entry.id.to_string().into())),
        Value::BulkString(Some(entry.consumer.into())),
        Value::Integer(i64::try_from(entry.idle_ms).unwrap_or(i64::MAX)),
        Value::Integer(i64::try_from(entry.deliveries).unwrap_or(i64::MAX)),
    ]))
}

impl Display for XPendingParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "XPENDING {} {}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.group)
        )?;
        if let Some(range) = &self.range {
            if let Some(idle) = range.idle_ms {
                write!(f, " IDLE {idle}")?;
            }
            write!(f, " {} {} {}", range.start, range.end, range.count)?;
            if let Some(consumer) = &range.consumer {
                write!(f, " {}", String::from_utf8_lossy(consumer))?;
            }
        }
        Ok(())
    }
}

impl ReadCommand for XPendingParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => no_group(&self.key, &self.group),
            Some(entry) => match entry.value.data {
                ValueObject::Stream(stream) => self.reply(&stream.read(), None),
                _ => ProtocolError::WrongType.into(),
            },
        }
    }

    fn execute_with_clock(&self, value: Option<EntrySnapshot<MyValue>>, read_clock: u64) -> Value {
        match value {
            None => no_group(&self.key, &self.group),
            Some(entry) => match entry.value.data {
                ValueObject::Stream(stream) => self.reply(&stream.read(), Some(read_clock)),
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

/// XPENDING command handler.
pub struct XPendingCommand;

impl ReadRaftCommand for XPendingCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::XPending(XPendingParams::parse(items)?))
    }
}

#[async_trait]
impl Command for XPendingCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }
        server
            .app
            .read(self.read_operation(items)?, client.db_number)
            .await
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

    fn parse(args: &[&str]) -> Result<XPendingParams, ProtocolError> {
        XPendingParams::parse(&arguments(args))
    }

    fn grouped() -> EntrySnapshot<MyValue> {
        let mut stream = RedisStream::new();
        stream
            .xadd(
                AddId::Explicit(StreamId::new(1, 0)),
                vec![(b"field".to_vec(), b"one".to_vec())],
            )
            .unwrap();
        stream
            .xadd(
                AddId::Explicit(StreamId::new(2, 0)),
                vec![(b"field".to_vec(), b"two".to_vec())],
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

    fn snapshot(stream: RedisStream) -> EntrySnapshot<MyValue> {
        EntrySnapshot {
            value: MyValue::new(ValueObject::Stream(Arc::new(RwLock::new(stream)))),
            expire_at: None,
        }
    }

    #[test]
    fn parses_both_forms() {
        let summary = parse(&["XPENDING", "s", "g"]).unwrap();
        assert!(summary.range.is_none());

        let extended = parse(&["XPENDING", "s", "g", "-", "+", "10"]).unwrap();
        let range = extended.range.unwrap();
        assert_eq!(range.start, StreamId::ZERO);
        assert_eq!(range.end, StreamId::MAX);
        assert_eq!(range.count, 10);
        assert_eq!(range.consumer, None);
        assert_eq!(range.idle_ms, None);

        let idle = parse(&["XPENDING", "s", "g", "IDLE", "500", "(1-0", "+", "5", "c"]).unwrap();
        let range = idle.range.unwrap();
        assert_eq!(range.idle_ms, Some(500));
        assert_eq!(range.start, StreamId::new(1, 1));
        assert_eq!(range.consumer.as_deref(), Some(b"c".as_slice()));

        // A negative IDLE is clamped, a negative count yields no entries.
        let clamped = parse(&["XPENDING", "s", "g", "IDLE", "-1", "-", "+", "-1"]).unwrap();
        let range = clamped.range.unwrap();
        assert_eq!(range.idle_ms, Some(0));
        assert_eq!(range.count, 0);
    }

    #[test]
    fn rejects_bad_arity_and_syntax() {
        assert!(matches!(
            parse(&["XPENDING", "s"]),
            Err(ProtocolError::WrongArgCount("xpending"))
        ));
        assert!(matches!(
            parse(&["XPENDING", "s", "g", "-", "+"]),
            Err(ProtocolError::WrongArgCount("xpending"))
        ));
        assert!(matches!(
            parse(&["XPENDING", "s", "g", "-", "+", "1", "c", "extra"]),
            Err(ProtocolError::SyntaxError)
        ));
        assert!(matches!(
            parse(&["XPENDING", "s", "g", "-", "+", "x"]),
            Err(ProtocolError::NotAnInteger)
        ));
        assert!(matches!(
            parse(&["XPENDING", "s", "g", "IDLE", "x", "-", "+", "1"]),
            Err(ProtocolError::NotAnInteger)
        ));
    }

    #[test]
    fn summary_reports_total_bounds_and_consumers() {
        let params = parse(&["XPENDING", "s", "g"]).unwrap();
        let Value::Array(Some(reply)) = params.execute_with_clock(Some(grouped()), 2_000) else {
            panic!("expected a summary array");
        };
        assert!(matches!(reply[0], Value::Integer(2)));
        assert_eq!(
            reply[1].string_bytes_clone().as_deref(),
            Some(b"1-0".as_slice())
        );
        assert_eq!(
            reply[2].string_bytes_clone().as_deref(),
            Some(b"2-0".as_slice())
        );
        let Value::Array(Some(consumers)) = &reply[3] else {
            panic!("expected consumers array");
        };
        assert_eq!(consumers.len(), 1);
        let Value::Array(Some(consumer)) = &consumers[0] else {
            panic!("expected [name, count]");
        };
        assert_eq!(
            consumer[0].string_bytes_clone().as_deref(),
            Some(b"c".as_slice())
        );
        assert!(matches!(consumer[1], Value::Integer(2)));
    }

    #[test]
    fn empty_summary_has_nil_bounds() {
        let mut stream = RedisStream::new();
        stream
            .xgroup_create(b"g", GroupStart::Id(StreamId::ZERO), None)
            .unwrap();
        let params = parse(&["XPENDING", "s", "g"]).unwrap();
        let Value::Array(Some(reply)) = params.execute(Some(snapshot(stream))) else {
            panic!("expected a summary array");
        };
        assert!(matches!(reply[0], Value::Integer(0)));
        assert!(matches!(reply[1], Value::BulkString(None)));
        assert!(matches!(reply[2], Value::BulkString(None)));
        assert!(matches!(&reply[3], Value::Array(Some(items)) if items.is_empty()));
    }

    #[test]
    fn extended_form_reports_idle_from_the_supplied_clock() {
        // Deliveries happened at the stream clock (wall time); asking with a
        // logical clock of 0 must not underflow and reports non-negative idle.
        let params = parse(&["XPENDING", "s", "g", "-", "+", "10"]).unwrap();
        let Value::Array(Some(entries)) = params.execute_with_clock(Some(grouped()), 0) else {
            panic!("expected a PEL array");
        };
        assert_eq!(entries.len(), 2);
        let Value::Array(Some(first)) = &entries[0] else {
            panic!("expected [id, consumer, idle, deliveries]");
        };
        assert_eq!(
            first[0].string_bytes_clone().as_deref(),
            Some(b"1-0".as_slice())
        );
        assert_eq!(
            first[1].string_bytes_clone().as_deref(),
            Some(b"c".as_slice())
        );
        assert!(matches!(first[2], Value::Integer(_)));
        assert!(matches!(first[3], Value::Integer(1)));
    }

    #[test]
    fn filters_by_idle_and_consumer() {
        let params = parse(&["XPENDING", "s", "g", "IDLE", "1", "-", "+", "10", "other"]).unwrap();
        let Value::Array(Some(entries)) = params.execute_with_clock(Some(grouped()), u64::MAX / 2)
        else {
            panic!("expected a PEL array");
        };
        // No entry belongs to the "other" consumer.
        assert!(entries.is_empty());
    }

    #[test]
    fn missing_key_or_group_is_nogroup() {
        let params = parse(&["XPENDING", "missing", "g"]).unwrap();
        assert!(matches!(
            params.execute(None),
            Value::Error(ref message) if message.starts_with("NOGROUP")
        ));

        // Wrong type must be reported before the group is looked up.
        let snapshot = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        assert!(matches!(
            params.execute(Some(snapshot)),
            Value::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
    }
}
