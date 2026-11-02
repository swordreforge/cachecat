use crate::error::ProtocolError;
use crate::raft::types::core::cache::stream::no_group_read;
use crate::raft::types::core::mocha::core::{ApplyEffects, MyCache, StreamWaiter, Update};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::AddId;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::{Operation, RedisOperation};
use bytes::Bytes;
use std::sync::Arc;

pub fn read_request(
    my_cache: &MyCache,
    read_operation: ReadOperation,
    db_number: u16,
    read_clock: u64,
) -> Value {
    read_request_inner::<false>(my_cache, read_operation, db_number, read_clock)
}

fn read_request_inner<const REPLICATED: bool>(
    my_cache: &MyCache,
    read_operation: ReadOperation,
    db_number: u16,
    read_clock: u64,
) -> Value {
    match read_operation {
        ReadOperation::Exists(param) => my_cache.execute_multi_read(param, db_number, read_clock),
        ReadOperation::Get(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::LRange(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::MGet(param) => my_cache.execute_multi_read(param, db_number, read_clock),
        ReadOperation::XRead(param) => my_cache.execute_multi_read(param, db_number, read_clock),
        ReadOperation::XLen(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::XRange(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::XPending(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::XRevRange(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::XInfo(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZRange(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HGet(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::SMembers(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::HMGet(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::GetBit(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZRangeByScore(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::StrLen(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HGetAll(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::HScan(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::HKeys(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::HVals(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::LLen(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::Type(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::LIndex(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::SIsMember(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HExists(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::PTtl(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::Ttl(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HLen(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HRandField(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::HStrLen(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::BitCount(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::BitPos(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::SCard(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::SRandMember(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::SInter(param) => {
            my_cache.execute_multi_read_with_mode::<_, REPLICATED>(&param, db_number, read_clock)
        }
        ReadOperation::SUnion(param) => {
            my_cache.execute_multi_read_with_mode::<_, REPLICATED>(&param, db_number, read_clock)
        }
        ReadOperation::SDiff(param) => {
            my_cache.execute_multi_read_with_mode::<_, REPLICATED>(&param, db_number, read_clock)
        }
        ReadOperation::Keys(param) => {
            if REPLICATED {
                param.execute_with_clock(my_cache, db_number, read_clock)
            } else {
                my_cache.keys(param, db_number, Some(read_clock))
            }
        }
        ReadOperation::ZScore(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZScan(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::ZCard(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZCount(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZRank(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZRevRank(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::DbSize(param) => {
            if REPLICATED {
                param.execute_with_clock(my_cache, db_number, read_clock)
            } else {
                my_cache.dbsize(db_number)
            }
        }
        ReadOperation::MemoryUsage(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::PFCount(param) => my_cache.execute_multi_read(param, db_number, read_clock),
        ReadOperation::BfExists(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::BfMExists(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::BfInfo(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::BfCard(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::BfScanDump(param) => my_cache.execute_read(param, db_number, read_clock),
    }
}

pub fn base_request(
    my_cache: &MyCache,
    base_operation: BaseOperation,
    update: &mut Update,
) -> Value {
    // Snapshot replay has no outer Lua/EXEC lock to reuse.
    base_request_inner(my_cache, base_operation, update, true)
}

fn base_request_inner(
    my_cache: &MyCache,
    base_operation: BaseOperation,
    update: &mut Update,
    external: bool,
) -> Value {
    match base_operation {
        BaseOperation::Empty => {
            for db in &my_cache.databases {
                db.mocha.active_expire_cycle_blocking();
            }
            Value::ok()
        }
        BaseOperation::Set(param) => my_cache.set(param, update),
        BaseOperation::XAdd(mut param) => {
            // Resolve automatic IDs before both initialization and mutation so
            // every Raft replica uses the replicated write clock.
            if matches!(param.id, AddId::Auto) {
                param.id = AddId::AutoSequence(update.write_clock);
            }
            my_cache.execute_compute(param, update)
        }
        BaseOperation::Expire(param) => my_cache.expire(param, update),
        BaseOperation::PExpire(param) => my_cache.p_expire(param, update),
        BaseOperation::LPush(param) => my_cache.l_push(param, update),
        BaseOperation::Del(param) => my_cache.del(param, update),
        BaseOperation::Incr(param) => my_cache.incr(param, update),
        BaseOperation::IncrBy(param) => my_cache.incr_by(param, update),
        BaseOperation::IncrByFloat(param) => my_cache.execute_compute(param, update),
        BaseOperation::Append(param) => my_cache.append(param, update),
        BaseOperation::HSet(param) => my_cache.h_set(param, update),
        BaseOperation::HIncr(param) => my_cache.h_incr(param, update),
        BaseOperation::HIncrByFloat(param) => my_cache.execute_compute(param, update),
        BaseOperation::ZAdd(param) => my_cache.z_add(param, update),
        BaseOperation::SAdd(param) => my_cache.s_add(param, update),
        BaseOperation::Persist(param) => my_cache.persist(param, update),
        BaseOperation::Insert(param) => my_cache.insert(param, update),
        BaseOperation::HDel(param) => my_cache.h_del(param, update),
        BaseOperation::SRem(param) => my_cache.s_rem(param, update),
        BaseOperation::SetBit(param) => my_cache.set_bit(param, update),
        BaseOperation::LPop(param) => my_cache.l_pop(param, update),
        BaseOperation::RPush(param) => my_cache.r_push(param, update),
        BaseOperation::RPop(param) => my_cache.r_pop(param, update),
        BaseOperation::LRem(param) => my_cache.l_rem(param, update),
        BaseOperation::LSet(param) => my_cache.l_set(param, update),
        BaseOperation::DecrBy(param) => my_cache.decr_by(param, update),
        BaseOperation::HSetNx(param) => my_cache.h_set_nx(param, update),
        BaseOperation::Decr(param) => my_cache.decr(param, update),
        BaseOperation::ZRem(param) => my_cache.z_rem(param, update),
        BaseOperation::LTrim(param) => my_cache.l_trim(param, update),
        BaseOperation::FlushDB(param) => my_cache.flush_db(param, update, external),
        BaseOperation::FlushAll(param) => my_cache.flush_all(param, update, external),
        BaseOperation::BitField(param) => my_cache.bit_field(param, update),
        BaseOperation::SPop(param) => my_cache.s_pop(param, update),
        BaseOperation::ZPopMin(param) => my_cache.z_pop_min(param, update),
        BaseOperation::Unlink(param) => my_cache.unlink(param, update),
        BaseOperation::ZIncrBy(param) => my_cache.z_incr_by(param, update),
        BaseOperation::HMSet(param) => my_cache.h_mset(param, update),
        BaseOperation::PfAdd(param) => my_cache.p_f_add(param, update),
        BaseOperation::BfAdd(param) => my_cache.bf_add(param, update),
        BaseOperation::BfMAdd(param) => my_cache.bf_madd(param, update),
        BaseOperation::BfReserve(param) => my_cache.bf_reserve(param, update),
        BaseOperation::BfInsert(param) => my_cache.bf_insert(param, update),
        BaseOperation::BfLoadChunk(param) => my_cache.bf_load(param, update),
        BaseOperation::XGroup(param) => my_cache.x_group(param, update),
        BaseOperation::XAck(param) => my_cache.x_ack(param, update),
        BaseOperation::XReadGroup(param) => my_cache.x_read_group(param, update),
        BaseOperation::XClaim(param) => my_cache.x_claim(param, update),
        BaseOperation::XAutoClaim(param) => my_cache.x_autoclaim(param, update),
        BaseOperation::XAckDel(param) => my_cache.x_ackdel(param, update),
        BaseOperation::XDelex(param) => my_cache.x_delex(param, update),
        BaseOperation::XSetId(param) => my_cache.x_setid(param, update),
        BaseOperation::XDel(param) => my_cache.x_del(param, update),
        BaseOperation::XTrim(param) => my_cache.x_trim(param, update),
    }
}

#[inline]
pub fn do_request(
    my_cache: &MyCache,
    operation: Operation,
    update: &mut Update,
    external: bool, //用来防止多次加锁
) -> Value {
    let result = match operation {
        // Reads inside EXEC/Lua share the log's clock and deterministic order/RNG.
        Operation::Read(read) => {
            read_request_inner::<true>(my_cache, read, update.db_number, update.write_clock)
        }
        Operation::Base(base) => base_request_inner(my_cache, base, update, external),
        Operation::Redis(redis) => match redis {
            RedisOperation::RedisDel(param) => my_cache.redis_del(param, update, external),
            RedisOperation::RedisSet(param) => my_cache.redis_set(param, update),
            RedisOperation::RedisSetNx(param) => my_cache.redis_setnx(param, update),
            RedisOperation::RedisSetEx(param) => my_cache.redis_setex(param, update),
            RedisOperation::RedisPSetEx(param) => my_cache.redis_psetex(param, update),
            RedisOperation::RedisGetSet(param) => my_cache.redis_get_set(param, update),
            RedisOperation::RedisMset(param) => my_cache.redis_mset(param, update, external),
            RedisOperation::RedisBitOp(param) => my_cache.execute_multi_read_compute(param, update),
            RedisOperation::RedisSInterStore(param) => {
                my_cache.execute_multi_read_compute(param, update)
            }
            RedisOperation::RedisSUnionStore(param) => {
                my_cache.execute_multi_read_compute(param, update)
            }
            RedisOperation::RedisSDiffStore(param) => {
                my_cache.execute_multi_read_compute(param, update)
            }
            RedisOperation::RedisPFMerge(param) => {
                my_cache.execute_multi_read_compute(param, update)
            }
            RedisOperation::RedisRename(param) => {
                let _lock = if external {
                    Some(my_cache.read_lock.write())
                } else {
                    None
                };
                my_cache.execute_multi_read_compute(param, update)
            }
            RedisOperation::RedisRenameNx(param) => {
                let _lock = if external {
                    Some(my_cache.read_lock.write())
                } else {
                    None
                };
                my_cache.execute_multi_read_compute(param, update)
            }
            RedisOperation::RedisEval(param) => {
                let _exclusive_lock = if external {
                    Some(my_cache.read_lock.write())
                } else {
                    None
                };
                my_cache
                    .lua_env
                    .exec_lua(
                        my_cache,
                        &param.script,
                        &param.keys,
                        &param.args,
                        update,
                        param.proto,
                    )
                    .unwrap_or_else(|err| err.into())
            }
            RedisOperation::RedisExec(param) => {
                let _exclusive_lock = if external {
                    Some(my_cache.read_lock.write())
                } else {
                    None
                };
                let mut vec = Vec::new();
                for queued in param.operations {
                    update.db_number = queued.db_number;
                    vec.push(do_request(my_cache, queued.operation, update, false));
                }
                Value::Array(Some(vec))
            }
            RedisOperation::RedisUnlink(param) => my_cache.redis_unlink(param, update, external),
            RedisOperation::RedisReply(value) => value,
            RedisOperation::RedisInfo(param) => param.render_from_cache(my_cache),
            RedisOperation::RedisEvalSha { .. } => {
                // Unresolved hashes must never consult replica-local script caches.
                Value::error("unresolved EVALSHA in replicated request")
            }
            RedisOperation::RedisXReadGroup(param) => {
                my_cache.redis_xreadgroup(param, update, external)
            }
        },
    };
    if external && let Some(effects) = update.effects.as_deref_mut() {
        // The state machine owns this context under its existing write_lock.
        // Empty sets require no registry access, atomics, allocation or locks;
        // drain retains capacity for later commands in the same apply batch.
        if !effects.changed_streams.is_empty() {
            // Capture owned responses before a following DEL can erase data.
            let waiters = update
                .waiters
                .as_deref_mut()
                .expect("effects require an execution guard");
            for (key, appended) in effects.changed_streams.drain() {
                // Every waiter of the same group on this key shares one state
                // read instead of repeating lookups and stream locks.
                let mut groups: Vec<(Bytes, GroupState)> = Vec::new();
                waiters
                    .blocking_keys
                    .wake_ready(&key, |waiter| match waiter {
                        // Only an append can satisfy a blocked XREAD; XACK,
                        // XCLAIM and group reads must not pay for a scan.
                        StreamWaiter::Read(_) if !appended => None,
                        StreamWaiter::Read(params) => {
                            let result =
                                my_cache.execute_multi_read_ref(params, key.0, update.write_clock);
                            (!matches!(result, Value::Array(None))).then_some(result)
                        }
                        StreamWaiter::Group { group, identities } => {
                            match group_state(&mut groups, my_cache, &key, group) {
                                GroupState::Missing => Some(no_group_read(&key.1, group)),
                                GroupState::WrongType => Some(ProtocolError::WrongType.into()),
                                GroupState::Live {
                                    identity,
                                    has_new,
                                    woken,
                                } => {
                                    let expected = identities
                                        .iter()
                                        .find(|(waited, _)| waited == &key)
                                        .map(|(_, identity)| identity);
                                    match expected {
                                        // This waiter captured a group that was
                                        // destroyed or recreated: answer NOGROUP
                                        // without spending a Raft write.
                                        Some(expected) if !Arc::ptr_eq(expected, identity) => {
                                            Some(no_group_read(&key.1, group))
                                        }
                                        // New entries wake one waiter per group;
                                        // its delivery write wakes the next one.
                                        Some(_) if *has_new && !*woken => {
                                            *woken = true;
                                            Some(Value::ok())
                                        }
                                        _ => None,
                                    }
                                }
                            }
                        }
                    });
            }
            ApplyEffects::recycle_changed_keys(&mut effects.changed_streams);
        }
    }
    result
}

/// One cached view of a (key, group) pair, built once per changed key.
enum GroupState {
    /// No such key or group.
    Missing,
    /// The key exists but holds another type.
    WrongType,
    /// The group is alive. `woken` enforces one delivery per group per event.
    Live {
        identity: Arc<()>,
        has_new: bool,
        woken: bool,
    },
}

/// Return the shared state for `group`, computing it once per changed key.
fn group_state<'a>(
    groups: &'a mut Vec<(Bytes, GroupState)>,
    my_cache: &MyCache,
    key: &(u16, Bytes),
    group: &Bytes,
) -> &'a mut GroupState {
    if let Some(index) = groups.iter().position(|(name, _)| name == group) {
        return &mut groups[index].1;
    }
    groups.push((group.clone(), inspect_group(my_cache, key, group)));
    &mut groups.last_mut().expect("just pushed a group state").1
}

fn inspect_group(my_cache: &MyCache, key: &(u16, Bytes), group: &[u8]) -> GroupState {
    let Some(database) = my_cache.databases.get(key.0 as usize) else {
        return GroupState::Missing;
    };
    let Some(entry) = database.mocha.get_entry(&key.1) else {
        return GroupState::Missing;
    };
    let ValueObject::Stream(stream) = &entry.value.data else {
        return GroupState::WrongType;
    };
    let stream = stream.read();
    match stream.group_identity(group) {
        Ok(identity) => GroupState::Live {
            identity,
            has_new: stream.group_has_new(group).unwrap_or(false),
            woken: false,
        },
        Err(_) => GroupState::Missing,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocha::{EntrySnapshot, ExpirePolicy};
    use crate::protocol::key::del::{DelParams, DelReq};
    use crate::protocol::key::persist::PersistReq;
    use crate::protocol::key::unlink::UnlinkReq;
    use crate::protocol::lua::eval::EvalParams;
    use crate::protocol::stream::xadd::XAddReq;
    use crate::protocol::stream::xread::{XReadId, XReadParams};
    use crate::protocol::string::set::SetReq;
    use crate::protocol::transaction::QueuedOperation;
    use crate::protocol::transaction::exec::ExecParams;
    use crate::raft::types::core::mocha::core::StreamRegistration;
    use crate::raft::types::core::mocha::core::{ApplyEffects, MyValue, UpdateType};
    use crate::raft::types::core::structure::stream::StreamId;
    use crate::raft::types::core::value_object::ValueObject;
    use bytes::Bytes;
    use std::time::Duration;

    fn xadd(key: &Bytes, id: AddId) -> Operation {
        Operation::Base(BaseOperation::XAdd(XAddReq {
            key: key.clone(),
            id,
            fields: vec![(b"field".to_vec(), b"value".to_vec())],
        }))
    }

    async fn apply(
        cache: &MyCache,
        operation: Operation,
        db_number: u16,
        write_clock: u64,
    ) -> Value {
        apply_with_effects(
            cache,
            operation,
            db_number,
            write_clock,
            &mut ApplyEffects::default(),
        )
        .await
    }

    async fn apply_with_effects(
        cache: &MyCache,
        operation: Operation,
        db_number: u16,
        write_clock: u64,
        effects: &mut ApplyEffects,
    ) -> Value {
        apply_inner_with_effects(cache, operation, db_number, write_clock, effects, true).await
    }

    async fn apply_inner_with_effects(
        cache: &MyCache,
        operation: Operation,
        db_number: u16,
        write_clock: u64,
        effects: &mut ApplyEffects,
        external: bool,
    ) -> Value {
        let mut write_guard = cache.write_lock.lock().await;
        let mut update_type = UpdateType::None;
        let mut update = Update {
            db_number,
            write_clock,
            update_type: &mut update_type,
            effects: Some(effects),
            waiters: Some(&mut write_guard),
        };
        do_request(cache, operation, &mut update, external)
    }

    #[test]
    fn snapshot_restore_replays_persist_before_expiration() {
        let cache = MyCache::new(1).unwrap();
        let key = Bytes::from_static(b"restore-key");

        // Make the existing time wheel use a clock newer than the snapshot.
        cache.set_write_clock(1_000);
        cache.databases[0].mocha.active_expire_cycle_blocking();
        cache.pause_expire_workers();
        cache.reset_write_clock();

        cache.databases[0].mocha.insert_snapshot(
            key.clone(),
            EntrySnapshot {
                value: MyValue::new(ValueObject::String(Bytes::from_static(b"v"))),
                expire_at: Some(500),
            },
        );

        // Force the worker to process its queue at a clock beyond the deadline:
        // even then, a paused worker must not delete the key before replay.
        cache.set_write_clock(1_000);
        cache.databases[0].mocha.active_expire_cycle_blocking();
        cache.reset_write_clock();
        cache.set_write_clock(400);

        let mut update_type = UpdateType::CAS {
            expected_revision: crate::utils::OptionalU64::some(0),
            revision: 1,
        };
        let mut update = Update {
            db_number: 0,
            write_clock: 400,
            update_type: &mut update_type,
            effects: None,
            waiters: None,
        };
        let reply = base_request(
            &cache,
            BaseOperation::Persist(PersistReq { key: key.clone() }),
            &mut update,
        );
        assert!(matches!(reply, Value::Integer(1)));

        cache.set_write_clock(1_000);
        cache.resume_expire_workers();
        let entry = cache.databases[0]
            .mocha
            .get_entry(&key)
            .expect("persisted snapshot key");
        assert_eq!(entry.expire_at, None);
    }

    fn read_params(key: &Bytes) -> XReadParams {
        XReadParams {
            keys: vec![key.clone()],
            ids: vec![XReadId::After(StreamId::ZERO)],
            count: Some(1000),
            max_count: None,
            max_size: None,
            block_ms: Some(0),
            resp_version: 2,
        }
    }

    fn expected_reply(id: &str) -> Vec<u8> {
        format!(
            "*1\r\n*2\r\n$6\r\nstream\r\n*1\r\n*2\r\n${}\r\n{id}\r\n*2\r\n$5\r\nfield\r\n$5\r\nvalue\r\n",
            id.len()
        ).into_bytes()
    }

    #[tokio::test]
    async fn xadd_notifies_all_readers_only_in_its_database() {
        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        let first = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, key.clone())],
                None,
                StreamWaiter::Read(read_params(&key)),
            )
            .unwrap();
        let second = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, key.clone())],
                None,
                StreamWaiter::Read(read_params(&key)),
            )
            .unwrap();
        let mut other_db = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(1, key.clone())],
                None,
                StreamWaiter::Read(read_params(&key)),
            )
            .unwrap();

        let result = apply(&cache, xadd(&key, AddId::Auto), 0, 1234).await;
        assert_eq!(result.encode(), b"$6\r\n1234-0\r\n");
        for registration in [first, second] {
            let result = tokio::time::timeout(Duration::from_secs(1), registration.wait())
                .await
                .expect("successful XADD must notify every reader");
            let (ready_key, reply) = result.unwrap();
            assert_eq!(ready_key, (0, key.clone()));
            assert_eq!(reply.encode(), expected_reply("1234-0"));
        }
        assert!(
            other_db.cancel().await,
            "another database must remain blocked"
        );
        let entry = cache.databases[0].mocha.get_entry(&key).unwrap();
        let ValueObject::Stream(stream) = entry.value.data else {
            panic!("the stream must be stored before notifying readers");
        };
        assert_eq!(stream.read().last_generated_id(), StreamId::new(1234, 0));
    }

    #[tokio::test]
    async fn failed_xadd_keeps_readers_registered() {
        let cache = MyCache::new(1).unwrap();
        let key = Bytes::from_static(b"stream");
        let id = AddId::Explicit(StreamId::new(10, 0));
        assert!(matches!(
            apply(&cache, xadd(&key, id), 0, 10).await,
            Value::BulkString(Some(_))
        ));

        let mut reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, key.clone())],
                None,
                StreamWaiter::Read(read_params(&key)),
            )
            .unwrap();
        assert!(matches!(
            apply(&cache, xadd(&key, id), 0, 10).await,
            Value::Error(_)
        ));
        assert!(
            reader.cancel().await,
            "a rejected stream ID must not wake readers"
        );

        cache.databases[0].mocha.insert_entry(
            key.clone(),
            MyValue::new(ValueObject::String(Bytes::from_static(b"wrong type"))),
            ExpirePolicy::Persistent,
        );
        let mut reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, key.clone())],
                None,
                StreamWaiter::Read(read_params(&key)),
            )
            .unwrap();
        assert!(matches!(
            apply(&cache, xadd(&key, AddId::Auto), 0, 11).await,
            Value::Error(_)
        ));
        assert!(reader.cancel().await, "a type error must not wake readers");
    }

    #[tokio::test]
    async fn xadd_in_exec_and_lua_notifies_readers() {
        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        let operations = [
            (
                Operation::Redis(RedisOperation::RedisExec(ExecParams {
                    operations: vec![QueuedOperation::new(0, xadd(&key, AddId::Auto))],
                })),
                1,
            ),
            (
                Operation::Redis(RedisOperation::RedisEval(EvalParams {
                    script: "return redis.call('XADD', KEYS[1], '*', 'field', 'value')".into(),
                    numkeys: 1,
                    keys: vec![key.clone()],
                    args: vec![],
                    proto: 2,
                })),
                0,
            ),
        ];
        for (operation, db_number) in operations {
            let mut params = read_params(&key);
            if let Some(entry) = cache.databases[0].mocha.get_entry(&key) {
                let ValueObject::Stream(stream) = entry.value.data else {
                    unreachable!()
                };
                params.ids[0] = XReadId::After(stream.read().last_generated_id());
            }
            let reader = cache
                .write_lock
                .lock()
                .await
                .register_stream(vec![(0, key.clone())], None, StreamWaiter::Read(params))
                .unwrap();
            let result = apply(&cache, operation, db_number, 1234).await;
            assert!(!matches!(result, Value::Error(_)), "{result:?}");
            let result = tokio::time::timeout(Duration::from_secs(1), reader.wait())
                .await
                .expect("nested XADD must notify readers");
            let (ready_key, reply) = result.unwrap();
            assert_eq!(ready_key, (0, key.clone()));
            assert!(matches!(reply, Value::MapWithResp2 { .. }));
            assert!(cache.databases[1].mocha.get_entry(&key).is_none());
        }
    }

    #[tokio::test]
    async fn completed_xadd_reply_survives_a_following_del() {
        let cache = MyCache::new(1).unwrap();
        let key = Bytes::from_static(b"stream");
        let reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, key.clone())],
                None,
                StreamWaiter::Read(read_params(&key)),
            )
            .unwrap();

        apply(&cache, xadd(&key, AddId::Auto), 0, 1234).await;
        let deleted = apply(
            &cache,
            Operation::Base(BaseOperation::Del(DelReq { key: key.clone() })),
            0,
            1235,
        )
        .await;
        assert!(matches!(deleted, Value::Integer(1)));
        assert!(cache.databases[0].mocha.get_entry(&key).is_none());

        let (_, reply) = tokio::time::timeout(Duration::from_secs(1), reader.wait())
            .await
            .expect("the reply was produced before DEL")
            .unwrap();
        assert_eq!(reply.encode(), expected_reply("1234-0"));
    }

    #[tokio::test]
    async fn xadd_keeps_a_reader_until_its_cursor_has_new_entries() {
        let cache = MyCache::new(1).unwrap();
        let key = Bytes::from_static(b"stream");
        let mut params = read_params(&key);
        params.ids[0] = XReadId::After(StreamId::new(2000, 0));
        let reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(vec![(0, key.clone())], None, StreamWaiter::Read(params))
            .unwrap();

        apply(&cache, xadd(&key, AddId::Auto), 0, 1234).await;
        apply(&cache, xadd(&key, AddId::Auto), 0, 2001).await;

        let (_, reply) = tokio::time::timeout(Duration::from_secs(1), reader.wait())
            .await
            .expect("the later XADD must satisfy the same registration")
            .unwrap();
        assert_eq!(reply.encode(), expected_reply("2001-0"));
    }

    #[tokio::test]
    async fn exec_and_lua_do_not_publish_intermediate_xadd_data() {
        let cache = MyCache::new(1).unwrap();
        let key = Bytes::from_static(b"stream");
        let operations = [
            Operation::Redis(RedisOperation::RedisExec(ExecParams {
                operations: vec![
                    QueuedOperation::new(0, xadd(&key, AddId::Auto)),
                    QueuedOperation::new(
                        0,
                        Operation::Base(BaseOperation::Del(DelReq { key: key.clone() })),
                    ),
                ],
            })),
            Operation::Redis(RedisOperation::RedisEval(EvalParams {
                script: "redis.call('XADD', KEYS[1], '*', 'field', 'value'); return redis.call('DEL', KEYS[1])".into(),
                numkeys: 1,
                keys: vec![key.clone()],
                args: vec![],
                proto: 2,
            })),
        ];
        for operation in operations {
            let mut reader = cache
                .write_lock
                .lock()
                .await
                .register_stream(
                    vec![(0, key.clone())],
                    None,
                    StreamWaiter::Read(read_params(&key)),
                )
                .unwrap();
            let result = apply(&cache, operation, 0, 1234).await;
            assert!(!matches!(result, Value::Error(_)), "{result:?}");
            assert!(cache.databases[0].mocha.get_entry(&key).is_none());
            assert!(
                reader.cancel().await,
                "an intermediate XADD must not fulfill the read"
            );
        }
    }

    #[tokio::test]
    async fn automatic_xadd_ids_use_the_replicated_clock_for_new_and_existing_streams() {
        let key = Bytes::from_static(b"stream");
        for _ in 0..2 {
            let cache = MyCache::new(1).unwrap();
            let initial = apply(&cache, xadd(&key, AddId::Auto), 0, 1234).await;
            let next = apply(&cache, xadd(&key, AddId::Auto), 0, 1234).await;
            assert_eq!(initial.encode(), b"$6\r\n1234-0\r\n");
            assert_eq!(next.encode(), b"$6\r\n1234-1\r\n");
        }
    }

    #[tokio::test]
    async fn one_exec_publishes_stream_notifications_in_each_database() {
        use futures::FutureExt;
        let cache = MyCache::new(2).unwrap();
        let stream = Bytes::from_static(b"stream");
        let mut readers = Vec::new();
        for db in 0..2 {
            readers.push(
                cache
                    .write_lock
                    .lock()
                    .await
                    .register_stream(
                        vec![(db, stream.clone())],
                        None,
                        StreamWaiter::Read(read_params(&stream)),
                    )
                    .unwrap(),
            );
        }
        let mut effects = ApplyEffects::default();
        let result = apply_with_effects(
            &cache,
            Operation::Redis(RedisOperation::RedisExec(ExecParams {
                operations: vec![
                    QueuedOperation::new(0, xadd(&stream, AddId::Explicit(StreamId::new(10, 0)))),
                    QueuedOperation::new(1, xadd(&stream, AddId::Explicit(StreamId::new(20, 0)))),
                ],
            })),
            0,
            100,
            &mut effects,
        )
        .await;
        assert!(matches!(result, Value::Array(Some(_))));
        assert!(effects.changed_streams.is_empty());
        for (db, reader) in readers.into_iter().enumerate() {
            let (ready_key, reply) = reader.wait().now_or_never().unwrap().unwrap();
            assert_eq!(ready_key, (db as u16, stream.clone()));
            assert_eq!(
                reply.encode(),
                expected_reply(if db == 0 { "10-0" } else { "20-0" })
            );
        }
    }

    #[tokio::test]
    async fn lua_error_still_publishes_completed_xadd_and_drains_effects() {
        use futures::FutureExt;
        let cache = MyCache::new(1).unwrap();
        let stream = Bytes::from_static(b"stream");
        let reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, stream.clone())],
                None,
                StreamWaiter::Read(read_params(&stream)),
            )
            .unwrap();
        let mut effects = ApplyEffects::default();
        let result = apply_with_effects(
            &cache,
            Operation::Redis(RedisOperation::RedisEval(EvalParams {
                script:
                    "redis.call('XADD', KEYS[1], '*', 'field', 'value'); error('stop after write')"
                        .into(),
                numkeys: 1,
                keys: vec![stream],
                args: vec![],
                proto: 2,
            })),
            0,
            1234,
            &mut effects,
        )
        .await;
        assert!(matches!(result, Value::Error(_)));
        let (_, response) = reader.wait().now_or_never().unwrap().unwrap();
        assert_eq!(response.encode(), expected_reply("1234-0"));
        assert!(effects.changed_streams.is_empty());
    }

    #[tokio::test]
    async fn reused_effects_keep_capacity_without_republishing_previous_xadd() {
        use crate::protocol::string::set::SetReq;
        let cache = MyCache::new(1).unwrap();
        let stream = Bytes::from_static(b"stream");
        let mut reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, stream.clone())],
                None,
                StreamWaiter::Read(read_params(&stream)),
            )
            .unwrap();
        let mut effects = ApplyEffects::default();
        apply_inner_with_effects(
            &cache,
            xadd(&stream, AddId::Auto),
            0,
            1234,
            &mut effects,
            false,
        )
        .await;
        let capacity = effects.changed_streams.capacity();
        assert!(capacity > 0);
        assert!(reader.cancel().await);
        apply_with_effects(
            &cache,
            Operation::Redis(RedisOperation::RedisReply(Value::ok())),
            0,
            1234,
            &mut effects,
        )
        .await;
        assert!(effects.changed_streams.is_empty());
        assert_eq!(effects.changed_streams.capacity(), capacity);

        // Register directly so stale effects would produce an observable
        // response from the stream; an unrelated SET must not serve it.
        let mut later_reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, stream.clone())],
                None,
                StreamWaiter::Read(read_params(&stream)),
            )
            .unwrap();
        apply_with_effects(
            &cache,
            Operation::Base(BaseOperation::Set(SetReq {
                key: Bytes::from_static(b"unrelated"),
                value: Bytes::from_static(b"v"),
                ex_time: 0,
            })),
            0,
            1234,
            &mut effects,
        )
        .await;
        assert!(later_reader.cancel().await);
        assert_eq!(effects.changed_streams.capacity(), capacity);
    }

    #[tokio::test]
    async fn replayed_xadd_does_not_leak_notifications_to_live_apply() {
        let cache = MyCache::new(1).unwrap();
        let stream = Bytes::from_static(b"stream");
        let mut reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, stream.clone())],
                None,
                StreamWaiter::Read(read_params(&stream)),
            )
            .unwrap();
        let mut update_type = UpdateType::CAS {
            expected_revision: crate::utils::OptionalU64::NONE,
            revision: 1,
        };
        let mut update = Update {
            db_number: 0,
            write_clock: 1234,
            update_type: &mut update_type,
            effects: None,
            waiters: None,
        };
        let Operation::Base(stream_write) = xadd(&stream, AddId::Explicit(StreamId::new(1234, 0)))
        else {
            unreachable!()
        };
        assert!(matches!(
            base_request(&cache, stream_write, &mut update),
            Value::BulkString(Some(_))
        ));
        let mut effects = ApplyEffects::default();
        apply_with_effects(
            &cache,
            Operation::Redis(RedisOperation::RedisReply(Value::ok())),
            0,
            1235,
            &mut effects,
        )
        .await;
        assert!(reader.cancel().await);
        assert!(effects.changed_streams.is_empty());
        assert_eq!(effects.changed_streams.capacity(), 0);
    }

    #[tokio::test]
    async fn large_stream_notification_batch_releases_capacity_before_small_commands() {
        use futures::FutureExt;
        let cache = MyCache::new(1).unwrap();
        let keys: Vec<_> = (0..128)
            .map(|i| Bytes::from(format!("stream-{i}")))
            .collect();
        let mut params = read_params(&keys[0]);
        params.keys = keys.clone();
        params.ids = vec![XReadId::After(StreamId::ZERO); keys.len()];
        let reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                keys.iter().map(|key| (0, key.clone())).collect(),
                None,
                StreamWaiter::Read(params),
            )
            .unwrap();
        let mut effects = ApplyEffects::default();
        let operation = Operation::Redis(RedisOperation::RedisExec(ExecParams {
            operations: keys
                .iter()
                .map(|key| QueuedOperation::new(0, xadd(key, AddId::Auto)))
                .collect(),
        }));
        assert!(matches!(
            apply_with_effects(&cache, operation, 0, 1, &mut effects).await,
            Value::Array(Some(_))
        ));
        let (_, reply) = reader.wait().now_or_never().unwrap().unwrap();
        assert!(
            matches!(reply, Value::MapWithResp2 { entries, .. } if entries.len() == keys.len())
        );
        assert!(effects.changed_streams.is_empty());
        assert_eq!(effects.changed_streams.capacity(), 0);

        let mut params = read_params(&keys[0]);
        params.ids[0] = XReadId::After(StreamId::new(1, 0));
        let next = cache
            .write_lock
            .lock()
            .await
            .register_stream(vec![(0, keys[0].clone())], None, StreamWaiter::Read(params))
            .unwrap();
        apply_with_effects(&cache, xadd(&keys[0], AddId::Auto), 0, 2, &mut effects).await;
        assert!(next.wait().now_or_never().unwrap().is_ok());
        assert!(effects.changed_streams.capacity() > 0);
        assert!(effects.changed_streams.capacity() <= 64);
    }

    async fn seed_group_at_tail(cache: &MyCache, key: &Bytes) {
        apply(cache, xadd(key, AddId::Auto), 0, 1).await;
        apply(
            cache,
            Operation::Base(BaseOperation::XGroup(
                crate::protocol::stream::xgroup::XGroupReq::Create {
                    key: key.clone(),
                    group: Bytes::from_static(b"g"),
                    id: crate::raft::types::core::structure::stream::GroupStart::Last,
                    entries_read: None,
                    mkstream: false,
                },
            )),
            0,
            2,
        )
        .await;
    }

    fn group_identities(
        cache: &MyCache,
        key: &Bytes,
        group: &'static [u8],
    ) -> Vec<((u16, Bytes), Arc<()>)> {
        let entry = cache.databases[0].mocha.get_entry(key).unwrap();
        let ValueObject::Stream(stream) = &entry.value.data else {
            panic!("expected a stream");
        };
        vec![(
            (0, key.clone()),
            stream.read().group_identity(group).unwrap(),
        )]
    }

    #[tokio::test]
    async fn pel_only_changes_do_not_wake_group_waiters() {
        use futures::FutureExt;

        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        seed_group_at_tail(&cache, &key).await;
        let group = Bytes::from_static(b"g");
        let identities = group_identities(&cache, &key, b"g");
        assert!(matches!(
            inspect_group(&cache, &(0, key.clone()), b"g"),
            GroupState::Live { has_new: false, .. }
        ));

        let waiter = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, key.clone())],
                None,
                StreamWaiter::Group {
                    group: group.clone(),
                    identities: identities.clone(),
                },
            )
            .unwrap();

        // XCLAIM FORCE records a PEL entry but adds no deliverable entries.
        let mut effects = ApplyEffects::default();
        apply_with_effects(
            &cache,
            Operation::Base(BaseOperation::XClaim(
                crate::protocol::stream::xclaim::XClaimReq {
                    key: key.clone(),
                    group: group.clone(),
                    consumer: Bytes::from_static(b"c"),
                    ids: vec![StreamId::new(1, 0)],
                    options: crate::raft::types::core::structure::stream::ClaimOptions {
                        force: true,
                        ..Default::default()
                    },
                },
            )),
            0,
            3,
            &mut effects,
        )
        .await;
        // The event is drained by the dispatcher; what matters is that the
        // predicate refuses it and the waiter stays registered.
        assert!(
            waiter.wait().now_or_never().is_none(),
            "a PEL-only change must not wake a blocked group read"
        );
    }

    #[tokio::test]
    async fn xgroup_destroy_answers_nogroup_without_a_write() {
        use futures::FutureExt;

        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        seed_group_at_tail(&cache, &key).await;
        let group = Bytes::from_static(b"g");
        let identities = group_identities(&cache, &key, b"g");

        let waiter = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, key.clone())],
                None,
                StreamWaiter::Group {
                    group: group.clone(),
                    identities: identities.clone(),
                },
            )
            .unwrap();

        let mut effects = ApplyEffects::default();
        apply_with_effects(
            &cache,
            Operation::Base(BaseOperation::XGroup(
                crate::protocol::stream::xgroup::XGroupReq::Destroy {
                    key: key.clone(),
                    group: group.clone(),
                },
            )),
            0,
            3,
            &mut effects,
        )
        .await;
        let (_, payload) = waiter
            .wait()
            .now_or_never()
            .expect("DESTROY must wake the blocked group read")
            .unwrap();
        assert!(matches!(payload, Value::Error(ref error) if error.starts_with("NOGROUP")));

        // A recreated group has a new generation and a new identity.
        apply(
            &cache,
            Operation::Base(BaseOperation::XGroup(
                crate::protocol::stream::xgroup::XGroupReq::Create {
                    key: key.clone(),
                    group: group.clone(),
                    id: crate::raft::types::core::structure::stream::GroupStart::Last,
                    entries_read: None,
                    mkstream: false,
                },
            )),
            0,
            4,
        )
        .await;
        match inspect_group(&cache, &(0, key.clone()), b"g") {
            GroupState::Live { identity, .. } => {
                assert!(
                    !Arc::ptr_eq(&identities[0].1, &identity),
                    "a recreated group must have a new identity"
                );
            }
            _ => panic!("recreated group must be live"),
        }
    }

    async fn group_waiter(
        cache: &MyCache,
        key: &Bytes,
        group: &Bytes,
        identities: &[((u16, Bytes), Arc<()>)],
    ) -> StreamRegistration {
        cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, key.clone())],
                None,
                StreamWaiter::Group {
                    group: group.clone(),
                    identities: identities.to_vec(),
                },
            )
            .unwrap()
    }

    #[tokio::test]
    async fn only_one_group_waiter_wakes_per_append() {
        use futures::FutureExt;

        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        seed_group_at_tail(&cache, &key).await;
        let group = Bytes::from_static(b"g");
        let identities = group_identities(&cache, &key, b"g");

        let mut waiters = vec![
            group_waiter(&cache, &key, &group, &identities).await,
            group_waiter(&cache, &key, &group, &identities).await,
            group_waiter(&cache, &key, &group, &identities).await,
        ];
        assert_eq!(
            cache.write_lock.lock().await.blocking_keys.waiter_count(),
            3
        );

        let mut effects = ApplyEffects::default();
        apply_with_effects(&cache, xadd(&key, AddId::Auto), 0, 3, &mut effects).await;
        assert_eq!(
            cache.write_lock.lock().await.blocking_keys.waiter_count(),
            2,
            "a single append must not wake every consumer of the group"
        );

        // The oldest registration is served first; its own group read would
        // advance the cursor and wake the next waiter while entries remain.
        let oldest = waiters.remove(0);
        let (_, token) = oldest
            .wait()
            .now_or_never()
            .expect("oldest waiter")
            .unwrap();
        assert!(matches!(token, Value::SimpleString(_)));

        apply_with_effects(&cache, xadd(&key, AddId::Auto), 0, 4, &mut effects).await;
        assert_eq!(
            cache.write_lock.lock().await.blocking_keys.waiter_count(),
            1
        );
    }

    #[tokio::test]
    async fn each_distinct_group_wakes_once_per_append() {
        use futures::FutureExt;

        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        seed_group_at_tail(&cache, &key).await;
        apply(
            &cache,
            Operation::Base(BaseOperation::XGroup(
                crate::protocol::stream::xgroup::XGroupReq::Create {
                    key: key.clone(),
                    group: Bytes::from_static(b"h"),
                    id: crate::raft::types::core::structure::stream::GroupStart::Last,
                    entries_read: None,
                    mkstream: false,
                },
            )),
            0,
            3,
        )
        .await;
        let first_group = Bytes::from_static(b"g");
        let second_group = Bytes::from_static(b"h");
        let first = group_identities(&cache, &key, b"g");
        let second = group_identities(&cache, &key, b"h");

        let g1 = group_waiter(&cache, &key, &first_group, &first).await;
        let g2 = group_waiter(&cache, &key, &first_group, &first).await;
        let h1 = group_waiter(&cache, &key, &second_group, &second).await;
        assert_eq!(
            cache.write_lock.lock().await.blocking_keys.waiter_count(),
            3
        );

        let mut effects = ApplyEffects::default();
        apply_with_effects(&cache, xadd(&key, AddId::Auto), 0, 4, &mut effects).await;
        assert_eq!(
            cache.write_lock.lock().await.blocking_keys.waiter_count(),
            1,
            "one waiter per distinct group, not one per key"
        );
        assert!(g1.wait().now_or_never().is_some());
        assert!(h1.wait().now_or_never().is_some());
        // The second waiter of the first group remains blocked.
        drop(g2);
    }

    #[tokio::test]
    async fn destroy_answers_every_waiter_of_that_group() {
        use futures::FutureExt;

        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        seed_group_at_tail(&cache, &key).await;
        apply(
            &cache,
            Operation::Base(BaseOperation::XGroup(
                crate::protocol::stream::xgroup::XGroupReq::Create {
                    key: key.clone(),
                    group: Bytes::from_static(b"h"),
                    id: crate::raft::types::core::structure::stream::GroupStart::Last,
                    entries_read: None,
                    mkstream: false,
                },
            )),
            0,
            3,
        )
        .await;
        let doomed = Bytes::from_static(b"g");
        let survivor = Bytes::from_static(b"h");
        let doomed_identities = group_identities(&cache, &key, b"g");
        let survivor_identities = group_identities(&cache, &key, b"h");

        let first = group_waiter(&cache, &key, &doomed, &doomed_identities).await;
        let second = group_waiter(&cache, &key, &doomed, &doomed_identities).await;
        let survivor_waiter = group_waiter(&cache, &key, &survivor, &survivor_identities).await;

        let mut effects = ApplyEffects::default();
        apply_with_effects(
            &cache,
            Operation::Base(BaseOperation::XGroup(
                crate::protocol::stream::xgroup::XGroupReq::Destroy {
                    key: key.clone(),
                    group: doomed.clone(),
                },
            )),
            0,
            4,
            &mut effects,
        )
        .await;
        assert_eq!(
            cache.write_lock.lock().await.blocking_keys.waiter_count(),
            1,
            "only the unrelated group stays blocked"
        );
        for waiter in [first, second] {
            let (_, payload) = waiter.wait().now_or_never().unwrap().unwrap();
            assert!(matches!(payload, Value::Error(ref error) if error.starts_with("NOGROUP")));
        }
        drop(survivor_waiter);
    }

    #[tokio::test]
    async fn non_append_changes_do_not_scan_xread_waiters() {
        use futures::FutureExt;

        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        apply(&cache, xadd(&key, AddId::Auto), 0, 1).await;
        apply(
            &cache,
            Operation::Base(BaseOperation::XGroup(
                crate::protocol::stream::xgroup::XGroupReq::Create {
                    key: key.clone(),
                    group: Bytes::from_static(b"g"),
                    id: crate::raft::types::core::structure::stream::GroupStart::Last,
                    entries_read: None,
                    mkstream: false,
                },
            )),
            0,
            2,
        )
        .await;
        // FORCE records a PEL entry so a later XACK is a real stream write.
        apply(
            &cache,
            Operation::Base(BaseOperation::XClaim(
                crate::protocol::stream::xclaim::XClaimReq {
                    key: key.clone(),
                    group: Bytes::from_static(b"g"),
                    consumer: Bytes::from_static(b"c"),
                    ids: vec![StreamId::new(1, 0)],
                    options: crate::raft::types::core::structure::stream::ClaimOptions {
                        force: true,
                        ..Default::default()
                    },
                },
            )),
            0,
            3,
        )
        .await;

        // A reader whose cursor predates the existing entry would be served by
        // any evaluation, so it exposes whether non-append changes scan XREAD.
        let reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, key.clone())],
                None,
                StreamWaiter::Read(read_params(&key)),
            )
            .unwrap();

        let mut effects = ApplyEffects::default();
        apply_with_effects(
            &cache,
            Operation::Base(BaseOperation::XAck(
                crate::protocol::stream::xack::XAckReq {
                    key: key.clone(),
                    group: Bytes::from_static(b"g"),
                    ids: vec![Bytes::from_static(b"1-0")],
                },
            )),
            0,
            4,
            &mut effects,
        )
        .await;
        assert_eq!(
            cache.write_lock.lock().await.blocking_keys.waiter_count(),
            1,
            "an XACK must not evaluate (and therefore wake) blocked XREAD readers"
        );

        // An append still serves it.
        let mut effects = ApplyEffects::default();
        apply_with_effects(&cache, xadd(&key, AddId::Auto), 0, 5, &mut effects).await;
        let (_, reply) = reader
            .wait()
            .now_or_never()
            .expect("append wakes it")
            .unwrap();
        assert!(matches!(reply, Value::MapWithResp2 { .. }));
    }

    #[tokio::test]
    async fn stale_and_fresh_waiters_of_one_group_get_different_answers() {
        use futures::FutureExt;

        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        seed_group_at_tail(&cache, &key).await;
        let group = Bytes::from_static(b"g");
        let current = group_identities(&cache, &key, b"g");
        // Models a waiter that registered before the group was recreated.
        let mut stale = current.clone();
        stale[0].1 = Arc::new(());

        let old = group_waiter(&cache, &key, &group, &stale).await;
        let new = group_waiter(&cache, &key, &group, &current).await;

        let mut effects = ApplyEffects::default();
        apply_with_effects(&cache, xadd(&key, AddId::Auto), 0, 3, &mut effects).await;
        assert_eq!(
            cache.write_lock.lock().await.blocking_keys.waiter_count(),
            0
        );

        let (_, old_payload) = old.wait().now_or_never().unwrap().unwrap();
        assert!(matches!(old_payload, Value::Error(ref error) if error.starts_with("NOGROUP")));
        let (_, new_payload) = new.wait().now_or_never().unwrap().unwrap();
        assert!(matches!(new_payload, Value::SimpleString(_)));
    }

    #[tokio::test]
    async fn del_wakes_blocked_group_waiters_with_nogroup() {
        use futures::FutureExt;

        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        seed_group_at_tail(&cache, &key).await;
        let group = Bytes::from_static(b"g");
        let identities = group_identities(&cache, &key, b"g");
        let waiter = group_waiter(&cache, &key, &group, &identities).await;

        let deleted = apply(
            &cache,
            Operation::Base(BaseOperation::Del(DelReq { key: key.clone() })),
            0,
            3,
        )
        .await;
        assert!(matches!(deleted, Value::Integer(1)));
        assert_eq!(
            cache.write_lock.lock().await.blocking_keys.waiter_count(),
            0,
            "DEL must detach the blocked group waiter"
        );

        let (_, payload) = waiter
            .wait()
            .now_or_never()
            .expect("DEL wakes the group waiter")
            .unwrap();
        assert!(
            matches!(payload, Value::Error(ref error) if error.starts_with("NOGROUP")),
            "{payload:?}"
        );
    }

    #[tokio::test]
    async fn multi_key_del_wakes_every_group_waiter() {
        use futures::FutureExt;

        let cache = MyCache::new(2).unwrap();
        let first = Bytes::from_static(b"stream-one");
        let second = Bytes::from_static(b"stream-two");
        let group = Bytes::from_static(b"g");
        seed_group_at_tail(&cache, &first).await;
        seed_group_at_tail(&cache, &second).await;
        let first_waiter = group_waiter(
            &cache,
            &first,
            &group,
            &group_identities(&cache, &first, b"g"),
        )
        .await;
        let second_waiter = group_waiter(
            &cache,
            &second,
            &group,
            &group_identities(&cache, &second, b"g"),
        )
        .await;

        let deleted = apply(
            &cache,
            Operation::Redis(RedisOperation::RedisDel(DelParams {
                keys: vec![first, second],
            })),
            0,
            3,
        )
        .await;
        assert!(matches!(deleted, Value::Integer(2)));
        assert_eq!(
            cache.write_lock.lock().await.blocking_keys.waiter_count(),
            0
        );

        for waiter in [first_waiter, second_waiter] {
            let (_, payload) = waiter.wait().now_or_never().unwrap().unwrap();
            assert!(
                matches!(payload, Value::Error(ref error) if error.starts_with("NOGROUP")),
                "{payload:?}"
            );
        }
    }

    #[tokio::test]
    async fn set_overwriting_a_stream_wakes_group_waiters_with_wrongtype() {
        use futures::FutureExt;

        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        seed_group_at_tail(&cache, &key).await;
        let group = Bytes::from_static(b"g");
        let group_waiter =
            group_waiter(&cache, &key, &group, &group_identities(&cache, &key, b"g")).await;
        let mut reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, key.clone())],
                None,
                StreamWaiter::Read(read_params(&key)),
            )
            .unwrap();

        let reply = apply(
            &cache,
            Operation::Base(BaseOperation::Set(SetReq {
                key: key.clone(),
                value: Bytes::from_static(b"value"),
                ex_time: 0,
            })),
            0,
            3,
        )
        .await;
        assert!(matches!(reply, Value::SimpleString(ref status) if status == "OK"));

        let (_, payload) = group_waiter
            .wait()
            .now_or_never()
            .expect("SET wakes the group waiter")
            .unwrap();
        assert!(
            matches!(payload, Value::Error(ref error) if error.starts_with("WRONGTYPE")),
            "{payload:?}"
        );
        assert_eq!(
            cache.write_lock.lock().await.blocking_keys.waiter_count(),
            1,
            "SET must not evaluate a plain XREAD"
        );
        assert!(reader.cancel().await);
    }

    #[tokio::test]
    async fn unlink_wakes_group_waiters_but_not_plain_xread() {
        use futures::FutureExt;

        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        seed_group_at_tail(&cache, &key).await;
        let group = Bytes::from_static(b"g");
        let group_waiter =
            group_waiter(&cache, &key, &group, &group_identities(&cache, &key, b"g")).await;
        let mut reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, key.clone())],
                None,
                StreamWaiter::Read(read_params(&key)),
            )
            .unwrap();

        let reply = apply(
            &cache,
            Operation::Base(BaseOperation::Unlink(UnlinkReq { key: key.clone() })),
            0,
            3,
        )
        .await;
        assert!(matches!(reply, Value::Integer(1)));

        let (_, payload) = group_waiter
            .wait()
            .now_or_never()
            .expect("UNLINK wakes the group waiter")
            .unwrap();
        assert!(
            matches!(payload, Value::Error(ref error) if error.starts_with("NOGROUP")),
            "{payload:?}"
        );
        assert_eq!(
            cache.write_lock.lock().await.blocking_keys.waiter_count(),
            1,
            "UNLINK must not evaluate a plain XREAD"
        );
        assert!(reader.cancel().await);
    }

    #[tokio::test]
    async fn del_does_not_wake_blocked_plain_xread_waiters() {
        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        apply(&cache, xadd(&key, AddId::Auto), 0, 1).await;
        let mut reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                vec![(0, key.clone())],
                None,
                StreamWaiter::Read(read_params(&key)),
            )
            .unwrap();

        let deleted = apply(
            &cache,
            Operation::Base(BaseOperation::Del(DelReq { key: key.clone() })),
            0,
            2,
        )
        .await;
        assert!(matches!(deleted, Value::Integer(1)));
        assert_eq!(
            cache.write_lock.lock().await.blocking_keys.waiter_count(),
            1,
            "DEL must not evaluate (and therefore wake) a plain XREAD"
        );
        assert!(reader.cancel().await);
    }
}
