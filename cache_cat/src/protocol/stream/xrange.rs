//! XRANGE command implementation.
//!
//! `XRANGE key start end [COUNT count]`
//!
//! Bounds accept the Redis stream interval syntax: `-`, `+`, inclusive bare
//! millisecond values and `(id` / `(ms-seq` exclusive bounds. `COUNT` is a
//! limit hint; a negative count behaves like zero and replies with a null
//! array, exactly like Redis.

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

/// Parsed arguments for XRANGE.
///
/// Exclusive bounds are resolved into the inclusive `start`/`end` pair before
/// the request can be replicated, so every replica runs the same query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XRangeParams {
    pub key: Bytes,
    pub start: StreamId,
    pub end: StreamId,
    /// `None` means no COUNT option; `Some(0)` produces the null array reply.
    pub count: Option<usize>,
}

impl XRangeParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("xrange"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let (start, end) = parse_interval(&items[2], &items[3])?;
        let count = parse_count_option(&items[4..])?;
        Ok(Self {
            key,
            start,
            end,
            count,
        })
    }
}

impl Display for XRangeParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "XRANGE {} {} {}",
            String::from_utf8_lossy(&self.key),
            self.start,
            self.end
        )?;
        if let Some(count) = self.count {
            write!(f, " COUNT {count}")?;
        }
        Ok(())
    }
}

impl ReadCommand for XRangeParams {
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
            .xrange(IdRange::inclusive(self.start, self.end), self.count);
        Value::Array(Some(entries.into_iter().map(entry_reply).collect()))
    }
}

/// XRANGE command handler.
pub struct XRangeCommand;

impl ReadRaftCommand for XRangeCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::XRange(XRangeParams::parse(items)?))
    }
}

#[async_trait]
impl Command for XRangeCommand {
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

    fn parse(args: &[&str]) -> Result<XRangeParams, ProtocolError> {
        XRangeParams::parse(&arguments(args))
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
            parse(&["XRANGE", "s", "-"]),
            Err(ProtocolError::WrongArgCount("xrange"))
        ));
        assert!(matches!(
            parse(&["XRANGE", "s", "-", "+", "BOGUS", "1"]),
            Err(ProtocolError::SyntaxError)
        ));
        assert!(matches!(
            parse(&["XRANGE", "s", "-", "+", "COUNT"]),
            Err(ProtocolError::SyntaxError)
        ));
        assert!(matches!(
            parse(&["XRANGE", "s", "-", "+", "COUNT", "abc"]),
            Err(ProtocolError::NotAnInteger)
        ));
    }

    #[test]
    fn rejects_invalid_ids() {
        assert!(matches!(
            parse(&["XRANGE", "s", "nope", "+"]),
            Err(ProtocolError::Response(_))
        ));
        // An exclusive special ID is invalid inside parentheses.
        assert!(matches!(
            parse(&["XRANGE", "s", "(-", "+"]),
            Err(ProtocolError::Response(_))
        ));
        assert!(matches!(
            parse(&["XRANGE", "s", "-", "(+"]),
            Err(ProtocolError::Response(_))
        ));
        // Excluding the smallest/largest possible ID cannot be represented.
        assert!(matches!(
            parse(&["XRANGE", "s", "(18446744073709551615-18446744073709551615", "+"]),
            Err(ProtocolError::Response(ref message))
                if message.contains("invalid start ID")
        ));
        assert!(matches!(
            parse(&["XRANGE", "s", "-", "(0-0"]),
            Err(ProtocolError::Response(ref message))
                if message.contains("invalid end ID")
        ));
    }

    #[test]
    fn resolves_special_incomplete_and_exclusive_bounds() {
        let params = parse(&["XRANGE", "s", "-", "+"]).unwrap();
        assert_eq!(params.start, StreamId::ZERO);
        assert_eq!(params.end, StreamId::MAX);
        assert_eq!(params.count, None);

        // A bare millisecond uses sequence 0 for the start and MAX for the end.
        let params = parse(&["XRANGE", "s", "5", "7"]).unwrap();
        assert_eq!(params.start, StreamId::new(5, 0));
        assert_eq!(params.end, StreamId::new(7, u64::MAX));

        // Exclusive bounds are shifted to the neighbouring inclusive IDs.
        let params = parse(&["XRANGE", "s", "(5-0", "(7-3"]).unwrap();
        assert_eq!(params.start, StreamId::new(5, 1));
        assert_eq!(params.end, StreamId::new(7, 2));
    }

    #[test]
    fn keeps_the_last_count_and_clamps_negatives() {
        let params = parse(&["XRANGE", "s", "-", "+", "COUNT", "5", "COUNT", "2"]).unwrap();
        assert_eq!(params.count, Some(2));
        assert_eq!(
            parse(&["XRANGE", "s", "-", "+", "COUNT", "-3"])
                .unwrap()
                .count,
            Some(0)
        );
    }

    #[test]
    fn returns_the_inclusive_range_in_id_order() {
        let params = parse(&["XRANGE", "s", "-", "+"]).unwrap();
        assert_eq!(
            reply_ids(params.execute(Some(snapshot(&[(1, 0), (1, 1), (2, 0)])))),
            ["1-0", "1-1", "2-0"]
        );

        let params = parse(&["XRANGE", "s", "1-1", "2-0"]).unwrap();
        assert_eq!(
            reply_ids(params.execute(Some(snapshot(&[(1, 0), (1, 1), (2, 0)])))),
            ["1-1", "2-0"]
        );

        let params = parse(&["XRANGE", "s", "(1-0", "+"]).unwrap();
        assert_eq!(
            reply_ids(params.execute(Some(snapshot(&[(1, 0), (1, 1), (2, 0)])))),
            ["1-1", "2-0"]
        );

        let params = parse(&["XRANGE", "s", "-", "+", "COUNT", "2"]).unwrap();
        assert_eq!(
            reply_ids(params.execute(Some(snapshot(&[(1, 0), (1, 1), (2, 0)])))),
            ["1-0", "1-1"]
        );
    }

    #[test]
    fn empty_and_null_replies_follow_redis() {
        let params = parse(&["XRANGE", "s", "-", "+"]).unwrap();
        assert!(matches!(params.execute(None), Value::Array(Some(ref v)) if v.is_empty()));

        let params = parse(&["XRANGE", "s", "9", "10"]).unwrap();
        assert!(matches!(
            params.execute(Some(snapshot(&[(1, 0)]))),
            Value::Array(Some(ref v)) if v.is_empty()
        ));

        // COUNT 0 (and negative COUNT) is a null array, but only once the key
        // has been checked; a missing key stays an empty array.
        let params = parse(&["XRANGE", "s", "-", "+", "COUNT", "0"]).unwrap();
        assert!(matches!(
            params.execute(Some(snapshot(&[(1, 0)]))),
            Value::Array(None)
        ));
        assert!(matches!(params.execute(None), Value::Array(Some(ref v)) if v.is_empty()));
    }

    #[test]
    fn wrong_type_is_an_error() {
        let params = parse(&["XRANGE", "s", "-", "+"]).unwrap();
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
    fn read_operation_is_replicated_as_xrange() {
        assert!(matches!(
            XRangeCommand
                .read_operation(&arguments(&["XRANGE", "s", "-", "+"]))
                .unwrap(),
            ReadOperation::XRange(_)
        ));
    }
}
