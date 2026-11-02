//! XREADGROUP: consumer-group reads mutate replicated metadata.
//!
//! The read itself (PEL assignment, delivery counters, consumer activity) is a
//! write and always goes through Raft. Only the waiting for new entries stays
//! local: a blocked client registers an ordinary stream waiter that wakes when
//! XADD appends to one of the keys, then submits the real group read.

use super::xread::{XReadId, XReadParams};
use super::{arg, no_group_read};
use crate::error::{CacheCatError, Error, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::common::blocking::buffer_until_disconnect;
use crate::protocol::raft_command::RaftCommand;
use crate::raft::application::blocking_keys::WaitError;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::{MyCache, MyValue, Update};
use crate::raft::types::core::response_value::{Resp2MapEncoding, Value};
use crate::raft::types::core::structure::stream::{GroupEntry, GroupRead, ReadGroupOptions};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::{Operation, RedisOperation};
use crate::raft::types::raft_types::CacheCatApp;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{fmt, time::Duration};
use tokio::time::Instant;

/// One replicated multi-key consumer-group read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XReadGroupReq {
    pub keys: Vec<Bytes>,
    pub group: Bytes,
    pub consumer: Bytes,
    pub modes: Vec<GroupRead>,
    pub count: Option<usize>,
    pub no_ack: bool,
}

impl fmt::Display for XReadGroupReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "XREADGROUP GROUP {} {} STREAMS {:?} {:?}",
            String::from_utf8_lossy(&self.group),
            String::from_utf8_lossy(&self.consumer),
            self.keys,
            self.modes
        )
    }
}

/// Single-stream part of an XREADGROUP. Every key is an independent atomic
/// read/write, which is also what a snapshot records.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XReadGroupStreamReq {
    pub key: Bytes,
    pub group: Bytes,
    pub consumer: Bytes,
    pub mode: GroupRead,
    pub count: Option<usize>,
    pub no_ack: bool,
}

impl fmt::Display for XReadGroupStreamReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "XREADGROUP {} {} {:?}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.group),
            self.mode
        )
    }
}

/// Entry encoding shared with XREAD:
/// - live entry: `[id, [field, value, ...]]`
/// - tombstoned PEL record: `[id, nil]`
fn encode_entry(entry: GroupEntry) -> Value {
    let fields = entry.fields.map(|fields| {
        fields
            .into_iter()
            .flat_map(|(field, value)| {
                [
                    Value::BulkString(Some(field.into())),
                    Value::BulkString(Some(value.into())),
                ]
            })
            .collect()
    });
    Value::Array(Some(vec![
        Value::BulkString(Some(entry.id.to_string().into())),
        Value::Array(fields),
    ]))
}

impl ComputeCommand for XReadGroupStreamReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::XReadGroup(self)
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (MochaOperation::Abort, no_group_read(&self.key, &self.group))
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expire = entry.get_expire_policy();
        let ValueObject::Stream(stream) = entry.value.data else {
            return (MochaOperation::Abort, ProtocolError::WrongType.into());
        };
        let result = stream.write().xreadgroup_at(
            &self.group,
            &self.consumer,
            self.mode,
            ReadGroupOptions {
                count: self.count,
                no_ack: self.no_ack,
            },
            write_clock,
        );
        match result {
            Ok(entries) => (
                // Even an empty read creates or touches the consumer, so the
                // key is rewritten (COW and TTL are handled by execute_compute).
                MochaOperation::Insert {
                    value: MyValue::new(ValueObject::Stream(stream)),
                    expire,
                },
                Value::Array(Some(entries.into_iter().map(encode_entry).collect())),
            ),
            Err(_) => (MochaOperation::Abort, no_group_read(&self.key, &self.group)),
        }
    }
}

impl MyCache {
    /// Apply a replicated XREADGROUP. All keys are validated before any group
    /// cursor or PEL entry changes, so a bad stream in the list changes nothing.
    pub(crate) fn redis_xreadgroup(
        &self,
        request: XReadGroupReq,
        update: &mut Update,
        external: bool,
    ) -> Value {
        // A multi-key write must exclude concurrent reads for atomicity.
        let _read = external.then(|| self.read_lock.write());
        let database = match self.get_cache(update.db_number) {
            Ok(database) => database,
            Err(error) => return error,
        };
        for key in &request.keys {
            let Some(entry) = database.mocha.get_entry(key) else {
                return no_group_read(key, &request.group);
            };
            let ValueObject::Stream(stream) = entry.value.data else {
                return ProtocolError::WrongType.into();
            };
            if !stream.read().has_group(&request.group) {
                return no_group_read(key, &request.group);
            }
        }
        let XReadGroupReq {
            keys,
            group,
            consumer,
            modes,
            count,
            no_ack,
        } = request;
        let mut result = Vec::new();
        for (key, mode) in keys.into_iter().zip(modes) {
            let reply = self.execute_compute(
                XReadGroupStreamReq {
                    key: key.clone(),
                    group: group.clone(),
                    consumer: consumer.clone(),
                    mode,
                    count,
                    no_ack,
                },
                update,
            );
            match reply {
                Value::Array(Some(entries)) => {
                    // A history read with an explicit ID always returns its
                    // (possibly empty) array; a `>` read without new entries is
                    // omitted from the map.
                    if !entries.is_empty() || matches!(mode, GroupRead::PendingAfter(_)) {
                        result.push((Value::BulkString(Some(key)), Value::Array(Some(entries))));
                    }
                }
                error => return error,
            }
        }
        if result.is_empty() {
            Value::Array(None)
        } else {
            Value::MapWithResp2 {
                entries: result,
                resp2: Resp2MapEncoding::Pairs,
            }
        }
    }
}

/// Parsed XREADGROUP arguments. Waiting itself is node-local and is not part of
/// the replicated request.
pub struct XReadGroupParams {
    pub request: XReadGroupReq,
    pub block_ms: Option<u64>,
}

impl XReadGroupParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 7 {
            return Err(ProtocolError::WrongArgCount("xreadgroup"));
        }
        let mut group = None;
        let mut consumer = None;
        let mut count = None;
        let mut no_ack = false;
        let mut block_ms = None;
        let mut index = 1;
        let streams = loop {
            let option = arg(items.get(index).ok_or(ProtocolError::SyntaxError)?)?;
            if option.eq_ignore_ascii_case(b"STREAMS") {
                break &items[index + 1..];
            } else if option.eq_ignore_ascii_case(b"GROUP") {
                group = Some(arg(items
                    .get(index + 1)
                    .ok_or(ProtocolError::SyntaxError)?)?);
                consumer = Some(arg(items
                    .get(index + 2)
                    .ok_or(ProtocolError::SyntaxError)?)?);
                index += 3;
            } else if option.eq_ignore_ascii_case(b"COUNT") {
                let value = items
                    .get(index + 1)
                    .ok_or(ProtocolError::SyntaxError)?
                    .try_parse_canonical_i64()?;
                count = if value > 0 {
                    Some(usize::try_from(value).map_err(|_| ProtocolError::NotAnInteger)?)
                } else {
                    None
                };
                index += 2;
            } else if option.eq_ignore_ascii_case(b"BLOCK") {
                let ms = items
                    .get(index + 1)
                    .ok_or(ProtocolError::SyntaxError)?
                    .try_parse_canonical_i64()
                    .map_err(|_| {
                        ProtocolError::response("ERR timeout is not an integer or out of range")
                    })?;
                if ms < 0 {
                    return Err(ProtocolError::response("ERR timeout is negative"));
                }
                if Instant::now()
                    .checked_add(Duration::from_millis(ms as u64))
                    .is_none()
                {
                    return Err(ProtocolError::response("ERR timeout is out of range"));
                }
                block_ms = Some(ms as u64);
                index += 2;
            } else if option.eq_ignore_ascii_case(b"NOACK") {
                no_ack = true;
                index += 1;
            } else {
                return Err(ProtocolError::SyntaxError);
            }
        };
        let group = group
            .ok_or_else(|| ProtocolError::response("ERR Missing GROUP option for XREADGROUP"))?;
        if streams.is_empty() || !streams.len().is_multiple_of(2) {
            return Err(ProtocolError::response(
                "ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.",
            ));
        }
        let length = streams.len() / 2;
        let keys = streams[..length]
            .iter()
            .map(arg)
            .collect::<Result<Vec<_>, _>>()?;
        let modes = streams[length..]
            .iter()
            .map(|value| {
                let id = arg(value)?;
                if id.as_ref() == b">" {
                    return Ok(GroupRead::New);
                }
                if id.as_ref() == b"+" {
                    return Err(ProtocolError::response(
                        "ERR The + ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The + ID would just return an empty result set.",
                    ));
                }
                if id.as_ref() == b"$" {
                    return Err(ProtocolError::response(
                        "ERR The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.",
                    ));
                }
                std::str::from_utf8(&id)
                    .ok()
                    .and_then(|id| id.parse().ok())
                    .map(GroupRead::PendingAfter)
                    .ok_or_else(super::invalid_stream_id)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            request: XReadGroupReq {
                keys,
                group,
                consumer: consumer.expect("GROUP parsed together with its consumer"),
                modes,
                count,
                no_ack,
            },
            block_ms,
        })
    }

    fn blocks_on_new(&self) -> bool {
        self.request
            .modes
            .iter()
            .any(|mode| matches!(mode, GroupRead::New))
    }

    async fn submit(&self, app: &CacheCatApp, db: u16) -> Result<Value, CacheCatError> {
        app.write(
            Operation::Redis(RedisOperation::RedisXReadGroup(self.request.clone())),
            db,
        )
        .await
    }

    async fn read(self, app: &CacheCatApp, db: u16) -> Result<Value, CacheCatError> {
        let result = self.submit(app, db).await?;
        let Some(block_ms) = self.block_ms else {
            return Ok(result);
        };
        // Redis only waits when at least one stream asks for `>` entries; an
        // explicitly requested history is returned (possibly empty) at once.
        if !matches!(result, Value::Array(None)) || !self.blocks_on_new() {
            return Ok(result);
        }
        let deadline = (block_ms != 0).then(|| Instant::now() + Duration::from_millis(block_ms));
        let cache = &app.state_machine.data.kvs;
        loop {
            // Check readiness and register the waiter while holding the global
            // writer. Applying an XADD needs the same lock, so no wake-up can
            // slip between the observation and the registration.
            let registration = {
                let mut write = cache.write_lock.lock().await;
                let _read = cache.read_lock.read();
                let clock = cache.get_and_update_read_clock();
                let database = match cache.get_cache(db) {
                    Ok(database) => database,
                    Err(error) => return Ok(error),
                };
                let mut wait_keys = Vec::with_capacity(self.request.keys.len());
                let mut ids = Vec::with_capacity(self.request.keys.len());
                let mut ready = false;
                for (key, mode) in self.request.keys.iter().zip(&self.request.modes) {
                    let Some(entry) = database.mocha.get_with_read_clock(key, Some(clock)) else {
                        return Ok(no_group_read(key, &self.request.group));
                    };
                    let ValueObject::Stream(stream) = entry.value.data else {
                        return Ok(ProtocolError::WrongType.into());
                    };
                    let stream = stream.read();
                    let last = match stream.group_last_delivered(&self.request.group) {
                        Ok(last) => last,
                        Err(_) => return Ok(no_group_read(key, &self.request.group)),
                    };
                    if matches!(mode, GroupRead::New) {
                        ready |= stream.group_has_new(&self.request.group).unwrap_or(false);
                        wait_keys.push((db, key.clone()));
                        ids.push(XReadId::After(last));
                    }
                }
                if ready {
                    None
                } else {
                    let params = XReadParams {
                        keys: wait_keys.iter().map(|(_, key)| key.clone()).collect(),
                        ids,
                        count: self.request.count,
                        max_count: None,
                        max_size: None,
                        block_ms: None,
                        resp_version: 2,
                    };
                    Some(
                        write
                            .register_stream(wait_keys, deadline, params)
                            .map_err(|error| {
                                Error::internal(format!("cannot register XREADGROUP: {error:?}"))
                            })?,
                    )
                }
            };
            if let Some(registration) = registration {
                match registration.wait().await {
                    // The payload is only a readiness token: the authoritative
                    // group read below is what assigns the PEL entries.
                    Ok(_) => {}
                    Err(WaitError::Timeout) => return Ok(Value::Array(None)),
                    Err(WaitError::Closed) => {
                        return Err(Error::internal("blocking registry is closed").into());
                    }
                    Err(WaitError::EmptyKeys) => {
                        unreachable!("XREADGROUP registers only existing keys")
                    }
                }
            }
            let result = self.submit(app, db).await?;
            if !matches!(result, Value::Array(None)) {
                return Ok(result);
            }
        }
    }
}

pub struct XReadGroupCommand;

impl RaftCommand for XReadGroupCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Redis(RedisOperation::RedisXReadGroup(
            XReadGroupParams::parse(items)?.request,
        )))
    }
}

#[async_trait]
impl Command for XReadGroupCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = XReadGroupParams::parse(items)?;
        if let Some(queue) = client.transaction_queue.as_mut() {
            // MULTI cannot block; EXEC runs the plain replicated read.
            queue.push(Operation::Redis(RedisOperation::RedisXReadGroup(
                params.request,
            )));
            return Ok(Value::queued());
        }
        let blocking = params.block_ms.is_some();
        let read = params.read(&server.app, client.db_number);
        if !blocking {
            return read.await;
        }
        let mut shutdown = server.app.shutdown_tx.subscribe();
        client.flag.blocking = true;
        let result = tokio::select! {
            result = read => result,
            result = buffer_until_disconnect(client) => {
                client.closed = true;
                result.map(|()| Value::Array(None)).map_err(CacheCatError::from)
            }
            _ = shutdown.recv() => {
                client.closed = true;
                Ok(Value::Array(None))
            }
        };
        client.flag.blocking = false;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::structure::stream::StreamId;

    fn arguments(args: &[&str]) -> Vec<Value> {
        args.iter()
            .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
            .collect()
    }

    fn parse(args: &[&str]) -> Result<XReadGroupParams, ProtocolError> {
        XReadGroupParams::parse(&arguments(args))
    }

    #[test]
    fn parses_group_options_and_stream_ids() {
        let params = parse(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "COUNT",
            "5",
            "NOACK",
            "BLOCK",
            "100",
            "STREAMS",
            "s1",
            "s2",
            ">",
            "0",
        ])
        .unwrap();
        assert_eq!(params.request.group.as_ref(), b"g");
        assert_eq!(params.request.consumer.as_ref(), b"c");
        assert_eq!(params.request.keys.len(), 2);
        assert_eq!(params.request.count, Some(5));
        assert!(params.request.no_ack);
        assert_eq!(params.block_ms, Some(100));
        assert_eq!(params.request.modes[0], GroupRead::New);
        assert_eq!(
            params.request.modes[1],
            GroupRead::PendingAfter(StreamId::ZERO)
        );
    }

    #[test]
    fn rejects_missing_group() {
        assert!(matches!(
            parse(&[
                "XREADGROUP",
                "NOACK",
                "NOACK",
                "COUNT",
                "1",
                "STREAMS",
                "s",
                ">"
            ]),
            Err(ProtocolError::Response(ref message))
                if message.contains("Missing GROUP option")
        ));
    }

    #[test]
    fn rejects_special_ids_and_unbalanced_streams() {
        assert!(matches!(
            parse(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "$"]),
            Err(ProtocolError::Response(ref message)) if message.contains("$ ID")
        ));
        assert!(matches!(
            parse(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "+"]),
            Err(ProtocolError::Response(ref message)) if message.contains("+ ID")
        ));
        assert!(matches!(
            parse(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">", "extra"]),
            Err(ProtocolError::Response(ref message)) if message.contains("Unbalanced")
        ));
        assert!(matches!(
            parse(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "bogus"]),
            Err(ProtocolError::Response(ref message)) if message.starts_with("ERR Invalid stream ID")
        ));
    }

    #[test]
    fn rejects_invalid_block_and_count() {
        assert!(matches!(
            parse(&["XREADGROUP", "GROUP", "g", "c", "BLOCK", "-1", "STREAMS", "s", ">"]),
            Err(ProtocolError::Response(ref message)) if message.contains("timeout is negative")
        ));
        assert!(matches!(
            parse(&["XREADGROUP", "GROUP", "g", "c", "BLOCK", "abc", "STREAMS", "s", ">"]),
            Err(ProtocolError::Response(ref message)) if message.contains("timeout is not an integer")
        ));
        assert!(matches!(
            parse(&[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "COUNT",
                "x",
                "STREAMS",
                "s",
                ">"
            ]),
            Err(ProtocolError::NotAnInteger)
        ));
    }

    #[test]
    fn raft_request_carries_the_replicated_read() {
        let operation = XReadGroupCommand
            .raft_request(&arguments(&[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "STREAMS",
                "s",
                ">",
            ]))
            .unwrap();
        assert!(matches!(
            operation,
            Operation::Redis(RedisOperation::RedisXReadGroup(_))
        ));
    }
}
