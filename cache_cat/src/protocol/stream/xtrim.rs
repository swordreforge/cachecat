//! XTRIM command implementation.
//!
//! `XTRIM key MAXLEN [~|=] count [LIMIT n] [KEEPREF|DELREF|ACKED]`
//! `XTRIM key MINID [~|=] id [LIMIT n] [KEEPREF|DELREF|ACKED]`
//!
//! Trimming removes payloads from the head of the stream. Group cursors and the
//! last-generated ID survive; the reference policy decides what happens to PEL
//! records:
//! - `KEEPREF` (default) leaves pending entries untouched.
//! - `DELREF` removes the PEL references of trimmed entries.
//! - `ACKED` trims only entries no group still references.
//!
//! Redis parses the options before looking up the key, so an invalid option or
//! threshold is an error even when the key is missing. A missing key answers 0
//! and is never created.

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
    ReferencePolicy, StreamId, TrimMode, TrimOptions, TrimStrategy,
};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::parse_reference_policy;

/// Replicated XTRIM request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XTrimReq {
    pub key: Bytes,
    pub options: TrimOptions,
}

impl fmt::Display for XTrimReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "XTRIM {} {:?} {:?} {:?}",
            String::from_utf8_lossy(&self.key),
            self.options.strategy,
            self.options.mode,
            self.options.references
        )
    }
}

fn syntax(message: &'static str) -> ProtocolError {
    ProtocolError::response(message)
}

/// Split an optional `~`/`=` operator from its threshold.
///
/// Redis only consumes the operator when at least one argument follows it, so
/// `XTRIM key MAXLEN ~` treats `~` as the (invalid) threshold.
fn threshold(items: &[Value], option_index: usize) -> Result<(bool, &Value, usize), ProtocolError> {
    let next = items
        .get(option_index + 1)
        .ok_or(ProtocolError::WrongArgCount("xtrim"))?;
    let operator = if option_index + 2 < items.len() {
        match next.as_str_lossy().as_deref() {
            Some("~") => Some(true),
            Some("=") => Some(false),
            _ => None,
        }
    } else {
        None
    };
    match operator {
        Some(approximate) => Ok((approximate, &items[option_index + 2], option_index + 3)),
        None => Ok((false, next, option_index + 2)),
    }
}

impl XTrimReq {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("xtrim"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let mut strategy = None;
        let mut approximate = false;
        let mut limit = None;
        let mut limit_given = false;
        let mut references = None;
        let mut index = 2;
        while index < items.len() {
            let option = items[index]
                .string_bytes_clone()
                .ok_or(ProtocolError::SyntaxError)?;
            if option_eq(&option, b"MAXLEN") && index + 1 < items.len() {
                if strategy.is_some() {
                    return Err(syntax(
                        "ERR syntax error, MAXLEN and MINID options at the same time are not compatible",
                    ));
                }
                let (is_approx, value, next) = threshold(items, index)?;
                let parsed = value.try_parse_canonical_i64()?;
                if parsed < 0 {
                    return Err(syntax("ERR The MAXLEN argument must be >= 0."));
                }
                approximate = is_approx;
                strategy = Some(TrimStrategy::MaxLen(
                    usize::try_from(parsed).map_err(|_| ProtocolError::NotAnInteger)?,
                ));
                index = next;
            } else if option_eq(&option, b"MINID") && index + 1 < items.len() {
                if strategy.is_some() {
                    return Err(syntax(
                        "ERR syntax error, MAXLEN and MINID options at the same time are not compatible",
                    ));
                }
                let (is_approx, value, next) = threshold(items, index)?;
                approximate = is_approx;
                strategy = Some(TrimStrategy::MinId(super::parse_stream_id(value)?));
                index = next;
            } else if option_eq(&option, b"LIMIT") && index + 1 < items.len() {
                let parsed = items[index + 1].try_parse_canonical_i64()?;
                if parsed < 0 {
                    return Err(syntax("ERR The LIMIT argument must be >= 0."));
                }
                limit = Some(usize::try_from(parsed).map_err(|_| ProtocolError::NotAnInteger)?);
                limit_given = true;
                index += 2;
            } else if references.is_none()
                && let Some(reference) = parse_reference_policy(&option)
            {
                references = Some(reference);
                index += 1;
            } else {
                return Err(ProtocolError::SyntaxError);
            }
        }

        // Redis checks a nonzero LIMIT before the missing-strategy message, so
        // `XTRIM key LIMIT 0` reports the missing strategy instead.
        if limit.unwrap_or(0) != 0 && strategy.is_none() {
            return Err(syntax(
                "ERR syntax error, LIMIT cannot be used without specifying a trimming strategy",
            ));
        }
        let Some(strategy) = strategy else {
            return Err(syntax(
                "ERR syntax error, XTRIM must be called with a trimming strategy",
            ));
        };
        if limit_given && !approximate {
            return Err(syntax(
                "ERR syntax error, LIMIT cannot be used without the special ~ option",
            ));
        }

        Ok(Self {
            key,
            options: TrimOptions {
                strategy,
                mode: if approximate {
                    // None uses the 10,000-entry default; Some(0) is unlimited.
                    TrimMode::Approximate { limit }
                } else {
                    TrimMode::Exact
                },
                references: references.unwrap_or(ReferencePolicy::KeepRef),
            },
        })
    }
}

impl ComputeCommand for XTrimReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::XTrim(self)
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
        let trimmed = stream.write().xtrim(self.options);
        if trimmed == 0 {
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
            Value::Integer(i64::try_from(trimmed).unwrap_or(i64::MAX)),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        // A missing key answers 0 and must not be created.
        (MochaOperation::Abort, Value::Integer(0))
    }
}

/// XTRIM command handler.
pub struct XTrimCommand;

impl RaftCommand for XTrimCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(BaseOperation::XTrim(XTrimReq::parse(
            items,
        )?)))
    }
}

#[async_trait]
impl Command for XTrimCommand {
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

    fn parse(args: &[&str]) -> Result<XTrimReq, ProtocolError> {
        XTrimReq::parse(&arguments(args))
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

    fn snapshot(stream: RedisStream) -> EntrySnapshot<MyValue> {
        EntrySnapshot {
            value: MyValue::new(ValueObject::Stream(Arc::new(RwLock::new(stream)))),
            expire_at: None,
        }
    }

    fn request(key: &str, options: TrimOptions) -> XTrimReq {
        XTrimReq {
            key: Bytes::copy_from_slice(key.as_bytes()),
            options,
        }
    }

    fn exact_max_len(len: usize) -> TrimOptions {
        TrimOptions {
            strategy: TrimStrategy::MaxLen(len),
            mode: TrimMode::Exact,
            references: ReferencePolicy::KeepRef,
        }
    }

    #[test]
    fn parses_exact_and_approximate_strategies() {
        let req = parse(&["XTRIM", "s", "MAXLEN", "5"]).unwrap();
        assert_eq!(req.options, exact_max_len(5));

        let req = parse(&["XTRIM", "s", "MAXLEN", "~", "5"]).unwrap();
        assert_eq!(req.options.mode, TrimMode::Approximate { limit: None });

        let req = parse(&["XTRIM", "s", "MAXLEN", "=", "5"]).unwrap();
        assert_eq!(req.options.mode, TrimMode::Exact);

        let req = parse(&["XTRIM", "s", "MINID", "2"]).unwrap();
        assert_eq!(
            req.options.strategy,
            TrimStrategy::MinId(StreamId::new(2, 0))
        );

        // LIMIT is only valid with the approximate form; 0 means unlimited.
        let req = parse(&["XTRIM", "s", "MAXLEN", "~", "5", "LIMIT", "2"]).unwrap();
        assert_eq!(req.options.mode, TrimMode::Approximate { limit: Some(2) });
        let req = parse(&["XTRIM", "s", "MAXLEN", "~", "5", "LIMIT", "0"]).unwrap();
        assert_eq!(req.options.mode, TrimMode::Approximate { limit: Some(0) });
    }

    #[test]
    fn parses_reference_policies_in_any_order() {
        let req = parse(&["XTRIM", "s", "ACKED", "MAXLEN", "5"]).unwrap();
        assert_eq!(req.options.references, ReferencePolicy::Acked);
        let req = parse(&["XTRIM", "s", "MAXLEN", "5", "DELREF"]).unwrap();
        assert_eq!(req.options.references, ReferencePolicy::DelRef);
        assert_eq!(
            parse(&["XTRIM", "s", "MAXLEN", "5"])
                .unwrap()
                .options
                .references,
            ReferencePolicy::KeepRef
        );
    }

    #[test]
    fn rejects_malformed_options_like_redis() {
        assert!(matches!(
            parse(&["XTRIM", "s", "MAXLEN"]),
            Err(ProtocolError::WrongArgCount("xtrim"))
        ));
        assert!(matches!(
            parse(&["XTRIM", "s", "BOGUS", "1"]),
            Err(ProtocolError::SyntaxError)
        ));
        assert!(matches!(
            parse(&["XTRIM", "s", "MAXLEN", "5", "LIMIT", "2"]),
            Err(ProtocolError::Response(ref message))
                if message.contains("without the special ~ option")
        ));
        assert!(matches!(
            parse(&["XTRIM", "s", "LIMIT", "2"]),
            Err(ProtocolError::Response(ref message))
                if message.contains("without specifying a trimming strategy")
        ));
        assert!(matches!(
            parse(&["XTRIM", "s", "LIMIT", "0"]),
            Err(ProtocolError::Response(ref message))
                if message.contains("must be called with a trimming strategy")
        ));
        assert!(matches!(
            parse(&["XTRIM", "s", "MAXLEN", "-1"]),
            Err(ProtocolError::Response(ref message))
                if message.contains("MAXLEN argument must be >= 0")
        ));
        assert!(matches!(
            parse(&["XTRIM", "s", "MAXLEN", "~", "5", "LIMIT", "-1"]),
            Err(ProtocolError::Response(ref message))
                if message.contains("LIMIT argument must be >= 0")
        ));
        assert!(matches!(
            parse(&["XTRIM", "s", "MAXLEN", "1", "MINID", "2"]),
            Err(ProtocolError::Response(ref message))
                if message.contains("MAXLEN and MINID")
        ));
        assert!(matches!(
            parse(&["XTRIM", "s", "MAXLEN", "5", "DELREF", "ACKED"]),
            Err(ProtocolError::SyntaxError)
        ));
        // An operator without a threshold is not consumed.
        assert!(matches!(
            parse(&["XTRIM", "s", "MAXLEN", "~"]),
            Err(ProtocolError::NotAnInteger)
        ));
        assert!(matches!(
            parse(&["XTRIM", "s", "MINID", "~"]),
            Err(ProtocolError::Response(ref message))
                if message.starts_with("ERR Invalid stream ID")
        ));
    }

    #[test]
    fn missing_key_is_zero_without_creating_it() {
        let (operation, reply) = request("missing", exact_max_len(0)).init();
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(reply, Value::Integer(0)));
    }

    #[test]
    fn wrong_type_is_an_error() {
        let snapshot = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        let (operation, reply) = request("s", exact_max_len(0)).mutate(snapshot, 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            reply,
            Value::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
    }

    #[test]
    fn exact_maxlen_removes_old_entries_and_keeps_metadata() {
        let (operation, reply) =
            request("s", exact_max_len(1)).mutate(snapshot(stream_with_entries(3)), 0);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("trimming must rewrite the stream");
        };
        assert!(matches!(reply, Value::Integer(2)));
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        let stream = stream.read();
        assert_eq!(stream.xlen(), 1);
        assert_eq!(stream.last_generated_id(), StreamId::new(3, 0));
        // XTRIM, unlike XDEL, does not advance the tombstone marker.
        assert_eq!(stream.xinfo_stream().max_deleted_entry_id, StreamId::ZERO);
    }

    #[test]
    fn minid_removes_only_older_entries() {
        let options = TrimOptions {
            strategy: TrimStrategy::MinId(StreamId::new(2, 0)),
            mode: TrimMode::Exact,
            references: ReferencePolicy::KeepRef,
        };
        let (operation, reply) = request("s", options).mutate(snapshot(stream_with_entries(3)), 0);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("expected a stream rewrite");
        };
        assert!(matches!(reply, Value::Integer(1)));
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        assert_eq!(stream.read().xlen(), 2);
    }

    #[test]
    fn approximate_limit_caps_the_work() {
        let options = TrimOptions {
            strategy: TrimStrategy::MaxLen(0),
            mode: TrimMode::Approximate { limit: Some(2) },
            references: ReferencePolicy::KeepRef,
        };
        let (operation, reply) = request("s", options).mutate(snapshot(stream_with_entries(5)), 0);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("expected a stream rewrite");
        };
        assert!(matches!(reply, Value::Integer(2)));
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        assert_eq!(stream.read().xlen(), 3);
    }

    #[test]
    fn delref_clears_pending_references() {
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
                    count: Some(1),
                    no_ack: false,
                },
            )
            .unwrap();
        assert_eq!(stream.xpending(b"g").unwrap().total, 1);

        let options = TrimOptions {
            strategy: TrimStrategy::MaxLen(1),
            mode: TrimMode::Exact,
            references: ReferencePolicy::DelRef,
        };
        let (operation, reply) = request("s", options).mutate(snapshot(stream), 0);
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("expected a stream rewrite");
        };
        assert!(matches!(reply, Value::Integer(1)));
        let ValueObject::Stream(stream) = &value.data else {
            panic!("expected a stream");
        };
        assert_eq!(stream.read().xlen(), 1);
        assert_eq!(stream.read().xpending(b"g").unwrap().total, 0);
    }

    #[test]
    fn acked_skips_entries_still_referenced_by_a_group() {
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
                    count: Some(1),
                    no_ack: false,
                },
            )
            .unwrap();

        let options = TrimOptions {
            strategy: TrimStrategy::MaxLen(0),
            mode: TrimMode::Exact,
            references: ReferencePolicy::Acked,
        };
        let (operation, _) = request("s", options).mutate(snapshot(stream), 0);
        // 1-0 is pending and 2-0 is above the group cursor, so nothing may go.
        assert!(matches!(operation, MochaOperation::Abort));
    }

    #[test]
    fn raft_request_uses_the_xtrim_base_operation() {
        let operation = XTrimCommand
            .raft_request(&arguments(&["XTRIM", "s", "MAXLEN", "5"]))
            .unwrap();
        assert!(matches!(
            operation,
            Operation::Base(BaseOperation::XTrim(_))
        ));
    }
}
