//! XLEN command implementation.
//!
//! `XLEN key` returns the number of entries inside a stream.
//!
//! Replies:
//! - Integer: the number of entries
//! - Integer 0 when the key does not exist
//! - WRONGTYPE when the key holds another type

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Parsed arguments for XLEN.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XLenParams {
    pub key: Bytes,
}

impl XLenParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("xlen"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        Ok(Self { key })
    }
}

impl Display for XLenParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "XLEN {}", String::from_utf8_lossy(&self.key))
    }
}

impl ReadCommand for XLenParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => Value::Integer(0),
            Some(entry) => match entry.value.data {
                ValueObject::Stream(stream) => {
                    let len = stream.read().xlen();
                    Value::Integer(i64::try_from(len).unwrap_or(i64::MAX))
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

/// XLEN command handler.
pub struct XLenCommand;

impl ReadRaftCommand for XLenCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::XLen(XLenParams::parse(items)?))
    }
}

#[async_trait]
impl Command for XLenCommand {
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
    use crate::raft::types::core::structure::stream::{AddId, RedisStream, StreamId};
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn arguments(args: &[&str]) -> Vec<Value> {
        args.iter()
            .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
            .collect()
    }

    fn stream_with_entries(count: u64) -> EntrySnapshot<MyValue> {
        let mut stream = RedisStream::new();
        for index in 1..=count {
            stream
                .xadd(
                    AddId::Explicit(StreamId::new(index, 0)),
                    vec![(b"field".to_vec(), b"value".to_vec())],
                )
                .unwrap();
        }
        EntrySnapshot {
            value: MyValue::new(ValueObject::Stream(Arc::new(RwLock::new(stream)))),
            expire_at: None,
        }
    }

    fn parse(args: &[&str]) -> Result<XLenParams, ProtocolError> {
        XLenParams::parse(&arguments(args))
    }

    #[test]
    fn parses_the_key_and_rejects_extra_arguments() {
        assert_eq!(parse(&["XLEN", "s"]).unwrap().key.as_ref(), b"s");
        assert!(matches!(
            parse(&["XLEN"]),
            Err(ProtocolError::WrongArgCount("xlen"))
        ));
        assert!(matches!(
            parse(&["XLEN", "s", "extra"]),
            Err(ProtocolError::WrongArgCount("xlen"))
        ));
    }

    #[test]
    fn counts_entries() {
        let params = parse(&["XLEN", "s"]).unwrap();
        assert!(matches!(
            params.execute(Some(stream_with_entries(3))),
            Value::Integer(3)
        ));
        assert!(matches!(
            params.execute(Some(stream_with_entries(0))),
            Value::Integer(0)
        ));
    }

    #[test]
    fn missing_key_is_zero() {
        let params = parse(&["XLEN", "s"]).unwrap();
        assert!(matches!(params.execute(None), Value::Integer(0)));
    }

    #[test]
    fn wrong_type_is_an_error() {
        let params = parse(&["XLEN", "s"]).unwrap();
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
    fn read_operation_is_replicated_as_xlen() {
        assert!(matches!(
            XLenCommand
                .read_operation(&arguments(&["XLEN", "s"]))
                .unwrap(),
            ReadOperation::XLen(_)
        ));
    }
}
