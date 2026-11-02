//! XREVRANGE command implementation.
//!
//! `XREVRANGE key end start [COUNT count]`
//!
//! This is XRANGE in reverse: the bounds keep Redis' argument order (the end
//! comes first) but are normalized into the same inclusive `start`/`end` pair
//! before the request is replicated. `COUNT` follows the exact XRANGE rules.

use super::parse_count_option;
use super::parse_interval;
use super::xread::entry_reply;
use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::{IdRange, StreamId};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Parsed arguments for XREVRANGE.
///
/// `start`/`end` are stored as the LOW/HIGH bounds, even though the wire
/// arguments are `end start`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XRevRangeParams {
    pub key: Bytes,
    pub start: StreamId,
    pub end: StreamId,
    /// `None` means no COUNT option; `Some(0)` produces the null array reply.
    pub count: Option<usize>,
}

impl XRevRangeParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("xrevrange"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        // Redis parses the end before the start in XREVRANGE.
        let (start, end) = parse_interval(&items[3], &items[2])?;
        let count = parse_count_option(&items[4..])?;
        Ok(Self {
            key,
            start,
            end,
            count,
        })
    }
}

impl Display for XRevRangeParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "XREVRANGE {} {} {}",
            String::from_utf8_lossy(&self.key),
            self.end,
            self.start
        )?;
        if let Some(count) = self.count {
            write!(f, " COUNT {count}")?;
        }
        Ok(())
    }
}

impl ReadCommand for XRevRangeParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let Some(entry) = value else {
            // A missing key answers with an empty array even for COUNT 0.
            return Value::Array(Some(Vec::new()));
        };
        let ValueObject::Stream(stream) = entry.value.data else {
            return ProtocolError::WrongType.into();
        };
        if self.count == Some(0) {
            return Value::Array(None);
        }
        let entries = stream
            .read()
            .xrevrange(IdRange::inclusive(self.start, self.end), self.count);
        Value::Array(Some(entries.into_iter().map(entry_reply).collect()))
    }
}

/// XREVRANGE command handler.
pub struct XRevRangeCommand;

impl ReadRaftCommand for XRevRangeCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::XRevRange(XRevRangeParams::parse(items)?))
    }
}

#[async_trait]
impl Command for XRevRangeCommand {
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
    use crate::raft::types::core::structure::stream::{AddId, RedisStream};
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn arguments(args: &[&str]) -> Vec<Value> {
        args.iter()
            .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
            .collect()
    }

    fn parse(args: &[&str]) -> Result<XRevRangeParams, ProtocolError> {
        XRevRangeParams::parse(&arguments(args))
    }

    fn snapshot(ids: &[(u64, u64)]) -> EntrySnapshot<MyValue> {
        let mut stream = RedisStream::new();
        for (ms, seq) in ids {
            stream
                .xadd(
                    AddId::Explicit(StreamId::new(*ms, *seq)),
                    vec![(b"field".to_vec(), b"value".to_vec())],
                )
                .unwrap();
        }
        EntrySnapshot {
            value: MyValue::new(ValueObject::Stream(Arc::new(RwLock::new(stream)))),
            expire_at: None,
        }
    }

    fn reply_ids(reply: Value) -> Vec<String> {
        let Value::Array(Some(entries)) = reply else {
            panic!("expected an array of entries, got {reply:?}");
        };
        entries
            .into_iter()
            .map(|entry| {
                let Value::Array(Some(mut parts)) = entry else {
                    panic!("expected an [id, fields] entry");
                };
                let id = parts.remove(0).string_bytes_clone().unwrap();
                assert!(matches!(parts.as_slice(), [Value::Array(Some(_))]));
                String::from_utf8(id.to_vec()).unwrap()
            })
            .collect()
    }

    #[test]
    fn rejects_bad_arity_and_syntax() {
        assert!(matches!(
            parse(&["XREVRANGE", "s", "+"]),
            Err(ProtocolError::WrongArgCount("xrevrange"))
        ));
        assert!(matches!(
            parse(&["XREVRANGE", "s", "+", "-", "BOGUS", "1"]),
            Err(ProtocolError::SyntaxError)
        ));
        assert!(matches!(
            parse(&["XREVRANGE", "s", "+", "-", "COUNT"]),
            Err(ProtocolError::SyntaxError)
        ));
        assert!(matches!(
            parse(&["XREVRANGE", "s", "+", "-", "COUNT", "abc"]),
            Err(ProtocolError::NotAnInteger)
        ));
    }

    #[test]
    fn accepts_the_wire_order_end_then_start() {
        // `+` is the end and `-` the start, unlike the ascending command.
        let params = parse(&["XREVRANGE", "s", "+", "-"]).unwrap();
        assert_eq!(params.start, StreamId::ZERO);
        assert_eq!(params.end, StreamId::MAX);

        // A bare millisecond keeps Redis' start/end sequence padding.
        let params = parse(&["XREVRANGE", "s", "7", "5"]).unwrap();
        assert_eq!(params.start, StreamId::new(5, 0));
        assert_eq!(params.end, StreamId::new(7, u64::MAX));
    }

    #[test]
    fn keeps_the_last_count_and_clamps_negatives() {
        let params = parse(&["XREVRANGE", "s", "+", "-", "COUNT", "5", "COUNT", "2"]).unwrap();
        assert_eq!(params.count, Some(2));
        assert_eq!(
            parse(&["XREVRANGE", "s", "+", "-", "COUNT", "-3"])
                .unwrap()
                .count,
            Some(0)
        );
    }

    #[test]
    fn returns_entries_in_descending_id_order() {
        let params = parse(&["XREVRANGE", "s", "+", "-"]).unwrap();
        assert_eq!(
            reply_ids(params.execute(Some(snapshot(&[(1, 0), (1, 1), (2, 0)])))),
            ["2-0", "1-1", "1-0"]
        );

        let params = parse(&["XREVRANGE", "s", "2-0", "1-1"]).unwrap();
        assert_eq!(
            reply_ids(params.execute(Some(snapshot(&[(1, 0), (1, 1), (2, 0)])))),
            ["2-0", "1-1"]
        );

        // An exclusive end is shifted inwards before the reverse scan.
        let params = parse(&["XREVRANGE", "s", "(2-0", "-"]).unwrap();
        assert_eq!(
            reply_ids(params.execute(Some(snapshot(&[(1, 0), (1, 1), (2, 0)])))),
            ["1-1", "1-0"]
        );

        let params = parse(&["XREVRANGE", "s", "+", "-", "COUNT", "2"]).unwrap();
        assert_eq!(
            reply_ids(params.execute(Some(snapshot(&[(1, 0), (1, 1), (2, 0)])))),
            ["2-0", "1-1"]
        );
    }

    #[test]
    fn empty_and_null_replies_follow_redis() {
        let params = parse(&["XREVRANGE", "s", "+", "-"]).unwrap();
        assert!(matches!(params.execute(None), Value::Array(Some(ref v)) if v.is_empty()));

        let params = parse(&["XREVRANGE", "s", "10", "9"]).unwrap();
        assert!(matches!(
            params.execute(Some(snapshot(&[(1, 0)]))),
            Value::Array(Some(ref v)) if v.is_empty()
        ));

        // COUNT 0 is a null array only after the key was found.
        let params = parse(&["XREVRANGE", "s", "+", "-", "COUNT", "0"]).unwrap();
        assert!(matches!(
            params.execute(Some(snapshot(&[(1, 0)]))),
            Value::Array(None)
        ));
        assert!(matches!(params.execute(None), Value::Array(Some(ref v)) if v.is_empty()));
    }

    #[test]
    fn wrong_type_is_an_error() {
        let params = parse(&["XREVRANGE", "s", "+", "-"]).unwrap();
        let snapshot = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        assert!(matches!(
            params.execute(Some(snapshot)),
            Value::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
    }

    #[test]
    fn read_operation_is_replicated_as_xrevrange() {
        assert!(matches!(
            XRevRangeCommand
                .read_operation(&arguments(&["XREVRANGE", "s", "+", "-"]))
                .unwrap(),
            ReadOperation::XRevRange(_)
        ));
    }
}
