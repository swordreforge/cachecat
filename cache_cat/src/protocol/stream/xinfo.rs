//! XINFO command implementation.
//!
//! `XINFO STREAM key [FULL [COUNT count]]`
//! `XINFO GROUPS key`
//! `XINFO CONSUMERS key group`
//! `XINFO HELP`
//!
//! All lookups are single-key reads, so each form is a `ReadCommand`. Idle and
//! active times are derived from the logical read clock to keep replicas
//! deterministic. cache-cat stores entries in a B-tree instead of Redis' radix
//! tree, so the internal `radix-tree-keys` / `radix-tree-nodes` fields are not
//! reported.

use super::xread::entry_reply;
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
    ConsumerInfo, FullConsumerInfo, FullGroupInfo, FullStreamInfo, GroupInfo, PendingInfo,
    StreamId, StreamInfo,
};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// The validated `XINFO` subcommand plus its arguments.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum XInfoSubcommand {
    /// `FULL` selects the extended form. `count` is `None` for unlimited
    /// (`FULL COUNT 0`) and defaults to 10 otherwise.
    Stream {
        full: bool,
        count: Option<usize>,
    },
    Groups,
    Consumers {
        group: Bytes,
    },
    Help,
}

/// Parsed arguments for XINFO.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XInfoParams {
    /// Empty for `XINFO HELP`, which does not touch the keyspace.
    pub key: Bytes,
    pub subcommand: XInfoSubcommand,
}

impl XInfoParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        let Some(subcommand_value) = items.get(1) else {
            return Err(ProtocolError::WrongArgCount("xinfo"));
        };
        let subcommand = subcommand_value
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("subcommand"))?;
        // Redis handles HELP before looking at the key and ignores extra args.
        if option_eq(&subcommand, b"HELP") {
            return Ok(Self {
                key: Bytes::new(),
                subcommand: XInfoSubcommand::Help,
            });
        }
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("xinfo"));
        }
        let key = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        if option_eq(&subcommand, b"STREAM") {
            let (full, count) = Self::parse_stream_options(&subcommand, &items[3..])?;
            return Ok(Self {
                key,
                subcommand: XInfoSubcommand::Stream { full, count },
            });
        }
        if option_eq(&subcommand, b"GROUPS") && items.len() == 3 {
            return Ok(Self {
                key,
                subcommand: XInfoSubcommand::Groups,
            });
        }
        if option_eq(&subcommand, b"CONSUMERS") && items.len() == 4 {
            let group = items[3]
                .string_bytes_clone()
                .ok_or(ProtocolError::InvalidArgument("group"))?;
            return Ok(Self {
                key,
                subcommand: XInfoSubcommand::Consumers { group },
            });
        }
        Err(subcommand_syntax_error(&subcommand))
    }

    /// Parse `[FULL [COUNT count]]` exactly like Redis:
    /// no options -> basic form, `FULL` -> default COUNT 10,
    /// `FULL COUNT 0` -> unlimited, a negative count -> default 10.
    fn parse_stream_options(
        subcommand: &Bytes,
        options: &[Value],
    ) -> Result<(bool, Option<usize>), ProtocolError> {
        match options {
            [] => Ok((false, None)),
            [full] if option_eq(&full.string_bytes_clone().unwrap_or_default(), b"FULL") => {
                Ok((true, Some(10)))
            }
            [full, count_keyword, count_value]
                if option_eq(&full.string_bytes_clone().unwrap_or_default(), b"FULL")
                    && option_eq(
                        &count_keyword.string_bytes_clone().unwrap_or_default(),
                        b"COUNT",
                    ) =>
            {
                let count = count_value.try_parse_canonical_i64()?;
                let count = if count == 0 {
                    None
                } else if count < 0 {
                    Some(10)
                } else {
                    Some(usize::try_from(count).map_err(|_| ProtocolError::NotAnInteger)?)
                };
                Ok((true, count))
            }
            _ => Err(subcommand_syntax_error(subcommand)),
        }
    }
}

fn subcommand_syntax_error(subcommand: &[u8]) -> ProtocolError {
    ProtocolError::response(format!(
        "ERR unknown subcommand or wrong number of arguments for '{}'. Try XINFO HELP.",
        String::from_utf8_lossy(subcommand)
    ))
}

impl Display for XInfoParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "XINFO")?;
        match &self.subcommand {
            XInfoSubcommand::Stream { full, count } => {
                write!(f, " STREAM {}", String::from_utf8_lossy(&self.key))?;
                if *full {
                    write!(f, " FULL")?;
                    match count {
                        Some(count) => write!(f, " COUNT {count}")?,
                        // COUNT 0 is the unlimited form.
                        None => write!(f, " COUNT 0")?,
                    }
                }
            }
            XInfoSubcommand::Groups => {
                write!(f, " GROUPS {}", String::from_utf8_lossy(&self.key))?;
            }
            XInfoSubcommand::Consumers { group } => {
                write!(
                    f,
                    " CONSUMERS {} {}",
                    String::from_utf8_lossy(&self.key),
                    String::from_utf8_lossy(group)
                )?;
            }
            XInfoSubcommand::Help => write!(f, " HELP")?,
        }
        Ok(())
    }
}

impl XInfoParams {
    fn reply(&self, value: Option<EntrySnapshot<MyValue>>, clock: Option<u64>) -> Value {
        if matches!(self.subcommand, XInfoSubcommand::Help) {
            return help_reply();
        }
        let Some(entry) = value else {
            // Redis answers a missing key before dispatching the subcommand.
            return ProtocolError::response("ERR no such key").into();
        };
        let ValueObject::Stream(stream) = entry.value.data else {
            return ProtocolError::WrongType.into();
        };
        let stream = stream.read();
        // One clock reading drives both the lookup and the absolute times we
        // reconstruct below, so a reply is internally consistent.
        let now = clock.unwrap_or_else(|| stream.now_ms());
        match &self.subcommand {
            XInfoSubcommand::Stream { full: false, .. } => stream_info_reply(stream.xinfo_stream()),
            XInfoSubcommand::Stream { full: true, count } => {
                full_stream_reply(stream.xinfo_stream_full_at(*count, now), now)
            }
            XInfoSubcommand::Groups => Value::Array(Some(
                stream
                    .xinfo_groups()
                    .into_iter()
                    .map(group_info_reply)
                    .collect(),
            )),
            XInfoSubcommand::Consumers { group } => match stream.xinfo_consumers_at(group, now) {
                Ok(consumers) => Value::Array(Some(
                    consumers.into_iter().map(consumer_info_reply).collect(),
                )),
                Err(_) => no_such_group(&self.key, group),
            },
            XInfoSubcommand::Help => unreachable!("handled before the keyspace lookup"),
        }
    }
}

impl ReadCommand for XInfoParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        self.reply(value, None)
    }

    fn execute_with_clock(&self, value: Option<EntrySnapshot<MyValue>>, read_clock: u64) -> Value {
        self.reply(value, Some(read_clock))
    }
}

/// XINFO command handler.
pub struct XInfoCommand;

impl ReadRaftCommand for XInfoCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::XInfo(XInfoParams::parse(items)?))
    }
}

#[async_trait]
impl Command for XInfoCommand {
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

fn help_reply() -> Value {
    Value::Array(Some(
        [
            "XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "CONSUMERS <key> <groupname>",
            "    Show consumers of <groupname>.",
            "GROUPS <key>",
            "    Show the stream consumer groups.",
            "STREAM <key> [FULL [COUNT <count>]",
            "    Show information about the stream.",
            "HELP",
            "    Print this help.",
        ]
        .into_iter()
        .map(|line| Value::SimpleString(line.to_string()))
        .collect(),
    ))
}

fn no_such_group(key: &Bytes, group: &[u8]) -> Value {
    ProtocolError::response(format!(
        "NOGROUP No such consumer group '{}' for key name '{}'",
        String::from_utf8_lossy(group),
        String::from_utf8_lossy(key)
    ))
    .into()
}

fn field(name: &str, value: Value) -> (Value, Value) {
    (
        Value::BulkString(Some(Bytes::copy_from_slice(name.as_bytes()))),
        value,
    )
}

fn bulk(bytes: Vec<u8>) -> Value {
    Value::BulkString(Some(Bytes::from(bytes)))
}

fn stream_id(id: StreamId) -> Value {
    Value::BulkString(Some(id.to_string().into()))
}

fn count(value: usize) -> Value {
    Value::Integer(i64::try_from(value).unwrap_or(i64::MAX))
}

fn unsigned(value: u64) -> Value {
    Value::Integer(i64::try_from(value).unwrap_or(i64::MAX))
}

fn optional_unsigned(value: Option<u64>) -> Value {
    value.map(unsigned).unwrap_or(Value::Null)
}

/// The fields every XINFO STREAM form starts with.
fn stream_base_fields(info: &StreamInfo) -> Vec<(Value, Value)> {
    vec![
        field("length", count(info.length)),
        field("last-generated-id", stream_id(info.last_generated_id)),
        field("max-deleted-entry-id", stream_id(info.max_deleted_entry_id)),
        field("entries-added", unsigned(info.entries_added)),
        field(
            "recorded-first-entry-id",
            stream_id(info.recorded_first_entry_id),
        ),
    ]
}

fn stream_info_reply(info: StreamInfo) -> Value {
    let mut fields = stream_base_fields(&info);
    fields.push(field("groups", count(info.groups)));
    fields.push(field(
        "first-entry",
        info.first_entry.map(entry_reply).unwrap_or(Value::Null),
    ));
    fields.push(field(
        "last-entry",
        info.last_entry.map(entry_reply).unwrap_or(Value::Null),
    ));
    Value::Map(fields)
}

fn full_stream_reply(info: FullStreamInfo, now: u64) -> Value {
    let mut fields = stream_base_fields(&info.info);
    fields.push(field(
        "entries",
        Value::Array(Some(info.entries.into_iter().map(entry_reply).collect())),
    ));
    fields.push(field(
        "groups",
        Value::Array(Some(
            info.groups
                .into_iter()
                .map(|group| full_group_reply(group, now))
                .collect(),
        )),
    ));
    Value::Map(fields)
}

fn group_info_reply(info: GroupInfo) -> Value {
    Value::Map(vec![
        field("name", bulk(info.name)),
        field("consumers", count(info.consumers)),
        field("pending", count(info.pending)),
        field("last-delivered-id", stream_id(info.last_delivered_id)),
        field("entries-read", optional_unsigned(info.entries_read)),
        field("lag", optional_unsigned(info.lag)),
    ])
}

fn full_group_reply(group: FullGroupInfo, now: u64) -> Value {
    Value::Map(vec![
        field("name", bulk(group.info.name)),
        field("last-delivered-id", stream_id(group.info.last_delivered_id)),
        field("entries-read", optional_unsigned(group.info.entries_read)),
        field("lag", optional_unsigned(group.info.lag)),
        field("pel-count", count(group.info.pending)),
        field(
            "pending",
            Value::Array(Some(
                group.pending.into_iter().map(group_pending_reply).collect(),
            )),
        ),
        field(
            "consumers",
            Value::Array(Some(
                group
                    .consumers
                    .into_iter()
                    .map(|consumer| full_consumer_reply(consumer, now))
                    .collect(),
            )),
        ),
    ])
}

/// Group PEL entries include the owning consumer name, unlike consumer PELs.
fn group_pending_reply(pending: PendingInfo) -> Value {
    Value::Array(Some(vec![
        stream_id(pending.id),
        bulk(pending.consumer),
        unsigned(pending.last_delivery_ms),
        unsigned(pending.deliveries),
    ]))
}

fn consumer_pending_reply(pending: PendingInfo) -> Value {
    Value::Array(Some(vec![
        stream_id(pending.id),
        unsigned(pending.last_delivery_ms),
        unsigned(pending.deliveries),
    ]))
}

fn consumer_info_reply(info: ConsumerInfo) -> Value {
    let inactive = info
        .inactive_ms
        .map(unsigned)
        // Redis reports -1 for a consumer that has never been active.
        .unwrap_or(Value::Integer(-1));
    Value::Map(vec![
        field("name", bulk(info.name)),
        field("pending", count(info.pending)),
        field("idle", unsigned(info.idle_ms)),
        field("inactive", inactive),
    ])
}

fn full_consumer_reply(consumer: FullConsumerInfo, now: u64) -> Value {
    let info = consumer.info;
    // The core reports relative times; Redis' FULL form wants absolute ones.
    let seen_time = now.saturating_sub(info.idle_ms);
    let active_time = info
        .inactive_ms
        .map(|idle| now.saturating_sub(idle))
        .map(unsigned)
        .unwrap_or(Value::Integer(-1));
    Value::Map(vec![
        field("name", bulk(info.name)),
        field("seen-time", unsigned(seen_time)),
        field("active-time", active_time),
        field("pel-count", count(info.pending)),
        field(
            "pending",
            Value::Array(Some(
                consumer
                    .pending
                    .into_iter()
                    .map(consumer_pending_reply)
                    .collect(),
            )),
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::structure::stream::{
        AddId, GroupRead, GroupStart, ReadGroupOptions, RedisStream,
    };
    use parking_lot::RwLock;
    use std::sync::Arc;

    const CLOCK: u64 = 600;

    fn arguments(args: &[&str]) -> Vec<Value> {
        args.iter()
            .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
            .collect()
    }

    fn parse(args: &[&str]) -> Result<XInfoParams, ProtocolError> {
        XInfoParams::parse(&arguments(args))
    }

    fn snapshot(stream: RedisStream) -> EntrySnapshot<MyValue> {
        EntrySnapshot {
            value: MyValue::new(ValueObject::Stream(Arc::new(RwLock::new(stream)))),
            expire_at: None,
        }
    }

    /// One entry, one group and one consumer with one pending entry.
    fn stream_with_group() -> RedisStream {
        let mut stream = RedisStream::new();
        stream
            .xadd(
                AddId::Explicit(StreamId::new(1, 0)),
                vec![(b"field".to_vec(), b"value".to_vec())],
            )
            .unwrap();
        stream
            .xgroup_create(b"g", GroupStart::Id(StreamId::ZERO), Some(0))
            .unwrap();
        stream
            .xreadgroup_at(b"g", b"c", GroupRead::New, ReadGroupOptions::default(), 500)
            .unwrap();
        stream
    }

    fn map_get<'a>(value: &'a Value, key: &str) -> &'a Value {
        let Value::Map(entries) = value else {
            panic!("expected a map, got {value:?}");
        };
        entries
            .iter()
            .find_map(|(name, value)| {
                (name.string_bytes_clone().as_deref() == Some(key.as_bytes())).then_some(value)
            })
            .unwrap_or_else(|| panic!("missing field {key} in {value:?}"))
    }

    fn text(value: &Value) -> String {
        String::from_utf8(value.string_bytes_clone().unwrap().to_vec()).unwrap()
    }

    #[test]
    fn parses_each_subcommand() {
        assert!(matches!(
            parse(&["XINFO", "STREAM", "s"]).unwrap().subcommand,
            XInfoSubcommand::Stream {
                full: false,
                count: None
            }
        ));
        // FULL defaults to COUNT 10, COUNT 0 means unlimited and negative
        // counts fall back to the default, exactly like Redis.
        assert!(matches!(
            parse(&["XINFO", "STREAM", "s", "FULL"]).unwrap().subcommand,
            XInfoSubcommand::Stream {
                full: true,
                count: Some(10)
            }
        ));
        assert!(matches!(
            parse(&["XINFO", "STREAM", "s", "FULL", "COUNT", "0"])
                .unwrap()
                .subcommand,
            XInfoSubcommand::Stream {
                full: true,
                count: None
            }
        ));
        assert!(matches!(
            parse(&["XINFO", "STREAM", "s", "FULL", "COUNT", "-2"])
                .unwrap()
                .subcommand,
            XInfoSubcommand::Stream {
                full: true,
                count: Some(10)
            }
        ));
        assert!(matches!(
            parse(&["XINFO", "STREAM", "s", "FULL", "COUNT", "3"])
                .unwrap()
                .subcommand,
            XInfoSubcommand::Stream {
                full: true,
                count: Some(3)
            }
        ));
        assert!(matches!(
            parse(&["XINFO", "GROUPS", "s"]).unwrap().subcommand,
            XInfoSubcommand::Groups
        ));
        let parsed = parse(&["XINFO", "CONSUMERS", "s", "g"]).unwrap();
        assert!(matches!(
            parsed.subcommand,
            XInfoSubcommand::Consumers { ref group } if group.as_ref() == b"g"
        ));
    }

    #[test]
    fn help_needs_no_key_and_ignores_extra_arguments() {
        let parsed = parse(&["XINFO", "HELP"]).unwrap();
        assert!(matches!(parsed.subcommand, XInfoSubcommand::Help));
        assert!(parsed.key.is_empty());
        assert!(matches!(
            parse(&["XINFO", "HELP", "extra"]).unwrap().subcommand,
            XInfoSubcommand::Help
        ));
        let Value::Array(Some(lines)) = parsed.execute(None) else {
            panic!("expected help lines");
        };
        assert!(
            matches!(&lines[0], Value::SimpleString(line) if line.starts_with("XINFO <subcommand>"))
        );
    }

    #[test]
    fn rejects_malformed_forms_like_redis() {
        assert!(matches!(
            parse(&["XINFO"]),
            Err(ProtocolError::WrongArgCount("xinfo"))
        ));
        assert!(matches!(
            parse(&["XINFO", "STREAM"]),
            Err(ProtocolError::WrongArgCount("xinfo"))
        ));
        assert!(matches!(
            parse(&["XINFO", "STREAM", "s", "BOGUS"]),
            Err(ProtocolError::Response(ref message))
                if message.contains("unknown subcommand")
        ));
        assert!(matches!(
            parse(&["XINFO", "STREAM", "s", "FULL", "COUNT"]),
            Err(ProtocolError::Response(ref message))
                if message.contains("unknown subcommand")
        ));
        assert!(matches!(
            parse(&["XINFO", "STREAM", "s", "FULL", "COUNT", "abc"]),
            Err(ProtocolError::NotAnInteger)
        ));
        assert!(matches!(
            parse(&["XINFO", "GROUPS", "s", "extra"]),
            Err(ProtocolError::Response(_))
        ));
        assert!(matches!(
            parse(&["XINFO", "CONSUMERS", "s"]),
            Err(ProtocolError::Response(_))
        ));
        assert!(matches!(
            parse(&["XINFO", "BOGUS", "s"]),
            Err(ProtocolError::Response(_))
        ));
    }

    #[test]
    fn missing_key_and_wrong_type_are_errors() {
        let params = parse(&["XINFO", "STREAM", "s"]).unwrap();
        assert!(matches!(
            params.execute_with_clock(None, CLOCK),
            Value::Error(ref message) if message == "ERR no such key"
        ));

        let wrong_type = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        assert!(matches!(
            params.execute_with_clock(Some(wrong_type), CLOCK),
            Value::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
    }

    #[test]
    fn stream_form_reports_metadata_and_edges() {
        let params = parse(&["XINFO", "STREAM", "s"]).unwrap();
        let reply = params.execute_with_clock(Some(snapshot(stream_with_group())), CLOCK);

        assert!(matches!(map_get(&reply, "length"), Value::Integer(1)));
        assert!(matches!(map_get(&reply, "groups"), Value::Integer(1)));
        assert_eq!(text(map_get(&reply, "last-generated-id")), "1-0");
        assert_eq!(text(map_get(&reply, "max-deleted-entry-id")), "0-0");
        assert!(matches!(
            map_get(&reply, "entries-added"),
            Value::Integer(1)
        ));
        assert_eq!(text(map_get(&reply, "recorded-first-entry-id")), "1-0");
        assert!(matches!(
            map_get(&reply, "first-entry"),
            Value::Array(Some(_))
        ));
        assert!(matches!(
            map_get(&reply, "last-entry"),
            Value::Array(Some(_))
        ));
    }

    #[test]
    fn empty_stream_reports_null_edges() {
        let params = parse(&["XINFO", "STREAM", "s"]).unwrap();
        let reply = params.execute_with_clock(Some(snapshot(RedisStream::new())), CLOCK);
        assert!(matches!(map_get(&reply, "first-entry"), Value::Null));
        assert!(matches!(map_get(&reply, "last-entry"), Value::Null));
        assert!(matches!(map_get(&reply, "length"), Value::Integer(0)));
    }

    #[test]
    fn groups_form_reports_one_map_per_group() {
        let params = parse(&["XINFO", "GROUPS", "s"]).unwrap();
        let reply = params.execute_with_clock(Some(snapshot(stream_with_group())), CLOCK);
        let Value::Array(Some(groups)) = reply else {
            panic!("expected an array of groups");
        };
        assert_eq!(groups.len(), 1);
        assert_eq!(text(map_get(&groups[0], "name")), "g");
        assert!(matches!(
            map_get(&groups[0], "consumers"),
            Value::Integer(1)
        ));
        assert!(matches!(map_get(&groups[0], "pending"), Value::Integer(1)));
        assert_eq!(text(map_get(&groups[0], "last-delivered-id")), "1-0");
        assert!(matches!(
            map_get(&groups[0], "entries-read"),
            Value::Integer(1)
        ));
        assert!(matches!(map_get(&groups[0], "lag"), Value::Integer(0)));
    }

    #[test]
    fn consumers_form_uses_the_read_clock_for_idle_times() {
        let params = parse(&["XINFO", "CONSUMERS", "s", "g"]).unwrap();
        let reply = params.execute_with_clock(Some(snapshot(stream_with_group())), CLOCK);
        let Value::Array(Some(consumers)) = reply else {
            panic!("expected an array of consumers");
        };
        assert_eq!(consumers.len(), 1);
        assert_eq!(text(map_get(&consumers[0], "name")), "c");
        assert!(matches!(
            map_get(&consumers[0], "pending"),
            Value::Integer(1)
        ));
        // Seen and active were both 500, so the 600 clock reports 100ms.
        assert!(matches!(
            map_get(&consumers[0], "idle"),
            Value::Integer(100)
        ));
        assert!(matches!(
            map_get(&consumers[0], "inactive"),
            Value::Integer(100)
        ));
    }

    #[test]
    fn consumers_form_reports_a_missing_group() {
        let params = parse(&["XINFO", "CONSUMERS", "s", "nope"]).unwrap();
        assert!(matches!(
            params.execute_with_clock(Some(snapshot(stream_with_group())), CLOCK),
            Value::Error(ref message)
                if message == "NOGROUP No such consumer group 'nope' for key name 's'"
        ));
    }

    #[test]
    fn full_form_embeds_entries_groups_and_absolute_times() {
        let params = parse(&["XINFO", "STREAM", "s", "FULL", "COUNT", "5"]).unwrap();
        let reply = params.execute_with_clock(Some(snapshot(stream_with_group())), CLOCK);
        assert!(matches!(
            map_get(&reply, "entries"),
            Value::Array(Some(entries)) if entries.len() == 1
        ));
        let Value::Array(Some(groups)) = map_get(&reply, "groups") else {
            panic!("expected groups");
        };
        let group = &groups[0];
        assert!(matches!(map_get(group, "pel-count"), Value::Integer(1)));
        let Value::Array(Some(pending)) = map_get(group, "pending") else {
            panic!("expected group PEL");
        };
        // [id, consumer, absolute delivery time, deliveries]
        let Value::Array(Some(items)) = &pending[0] else {
            panic!("expected one PEL entry");
        };
        assert_eq!(text(&items[0]), "1-0");
        assert_eq!(text(&items[1]), "c");
        assert!(matches!(items[2], Value::Integer(500)));
        assert!(matches!(items[3], Value::Integer(1)));

        let Value::Array(Some(consumers)) = map_get(group, "consumers") else {
            panic!("expected consumers");
        };
        let consumer = &consumers[0];
        assert_eq!(text(map_get(consumer, "name")), "c");
        assert!(matches!(
            map_get(consumer, "seen-time"),
            Value::Integer(500)
        ));
        assert!(matches!(
            map_get(consumer, "active-time"),
            Value::Integer(500)
        ));
        let Value::Array(Some(pending)) = map_get(consumer, "pending") else {
            panic!("expected consumer PEL");
        };
        let Value::Array(Some(items)) = &pending[0] else {
            panic!("expected one consumer PEL entry");
        };
        assert!(matches!(items.len(), 3));
    }

    #[test]
    fn full_form_with_unlimited_count_keeps_all_pels() {
        let params = parse(&["XINFO", "STREAM", "s", "FULL", "COUNT", "0"]).unwrap();
        let reply = params.execute_with_clock(Some(snapshot(stream_with_group())), CLOCK);
        let Value::Array(Some(groups)) = map_get(&reply, "groups") else {
            panic!("expected groups");
        };
        assert!(matches!(
            map_get(&groups[0], "pending"),
            Value::Array(Some(pending)) if pending.len() == 1
        ));
    }

    #[test]
    fn read_operation_is_replicated_as_xinfo() {
        assert!(matches!(
            XInfoCommand
                .read_operation(&arguments(&["XINFO", "STREAM", "s"]))
                .unwrap(),
            ReadOperation::XInfo(_)
        ));
    }
}
