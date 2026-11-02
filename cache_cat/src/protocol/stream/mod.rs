use crate::error::ProtocolError;
use crate::protocol::option_eq;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::{
    DeleteStatus, IdRange, ReferencePolicy, StreamId,
};
use crate::utils::parse_canonical_i64;
use bytes::Bytes;
use std::ops::Bound;
use std::str::FromStr;

pub mod xack;
pub mod xackdel;
pub mod xadd;
pub mod xautoclaim;
pub mod xclaim;
pub mod xdel;
pub mod xdelex;
pub mod xgroup;
pub mod xinfo;
pub mod xlen;
pub mod xpending;
pub mod xrange;
pub mod xread;
pub mod xreadgroup;
pub mod xrevrange;
pub mod xsetid;
pub mod xtrim;

pub(super) fn arg(value: &Value) -> Result<Bytes, ProtocolError> {
    value
        .string_bytes_clone()
        .ok_or(ProtocolError::InvalidArgument("argument"))
}

/// Error text used by every stream command when an ID cannot be parsed.
pub(super) fn invalid_stream_id() -> ProtocolError {
    ProtocolError::response("ERR Invalid stream ID specified as stream command argument")
}

/// Strict single-ID parse: `-` and `+` are not valid standalone IDs.
pub(super) fn parse_stream_id(value: &Value) -> Result<StreamId, ProtocolError> {
    value
        .as_str_lossy()
        .and_then(|text| text.parse().ok())
        .ok_or_else(invalid_stream_id)
}

/// Parse XRANGE/XPENDING-style bounds, including `-`, `+`, `(id` exclusivity
/// and bare millisecond values. Exclusive bounds are shifted inwards, so the
/// returned pair is always inclusive and can be replicated verbatim.
pub(super) fn parse_interval(
    start: &Value,
    end: &Value,
) -> Result<(StreamId, StreamId), ProtocolError> {
    let start = start.as_str_lossy().ok_or_else(invalid_stream_id)?;
    let end = end.as_str_lossy().ok_or_else(invalid_stream_id)?;
    // The content of an exclusive bound is parsed strictly: `(-` and `(+`
    // are only valid as inclusive bounds.
    for raw in [&start, &end] {
        if matches!(raw.strip_prefix('('), Some("-" | "+")) {
            return Err(invalid_stream_id());
        }
    }
    let range = IdRange::parse(&start, &end).map_err(|_| invalid_stream_id())?;
    let start = match range.start {
        Bound::Included(id) => id,
        Bound::Excluded(id) => id
            .successor()
            .ok_or_else(|| ProtocolError::response("ERR invalid start ID for the interval"))?,
        Bound::Unbounded => StreamId::ZERO,
    };
    let end = match range.end {
        Bound::Included(id) => id,
        Bound::Excluded(id) => id
            .predecessor()
            .ok_or_else(|| ProtocolError::response("ERR invalid end ID for the interval"))?,
        Bound::Unbounded => StreamId::MAX,
    };
    Ok((start, end))
}

/// Parse a single interval start bound, including `-`, `+` and an exclusive
/// `(id` form. XAUTOCLAIM uses this to position its PEL scan.
pub(super) fn parse_interval_start(value: &Value) -> Result<StreamId, ProtocolError> {
    let raw = value.as_str_lossy().ok_or_else(invalid_stream_id)?;
    if matches!(raw.strip_prefix('('), Some("-" | "+")) {
        return Err(invalid_stream_id());
    }
    let range = IdRange::parse(&raw, "+").map_err(|_| invalid_stream_id())?;
    match range.start {
        Bound::Included(id) => Ok(id),
        Bound::Excluded(id) => id
            .successor()
            .ok_or_else(|| ProtocolError::response("ERR invalid start ID for the interval")),
        Bound::Unbounded => Ok(StreamId::ZERO),
    }
}

/// Parse the optional `COUNT n` suffix shared by XRANGE and XREVRANGE.
/// Redis keeps the last repeated COUNT, clamps a negative value to zero and
/// rejects a non-integer. `None` means the option was not supplied.
pub(super) fn parse_count_option(items: &[Value]) -> Result<Option<usize>, ProtocolError> {
    let mut count = None;
    let mut index = 0;
    while index < items.len() {
        let option = items[index]
            .string_bytes_clone()
            .ok_or(ProtocolError::SyntaxError)?;
        if option_eq(&option, b"COUNT") && index + 1 < items.len() {
            let value = items[index + 1].try_parse_canonical_i64()?;
            count = Some(if value < 0 {
                0
            } else {
                usize::try_from(value).map_err(|_| ProtocolError::NotAnInteger)?
            });
            index += 2;
        } else {
            return Err(ProtocolError::SyntaxError);
        }
    }
    Ok(count)
}

/// Parse a reference-policy token shared by XTRIM, XACKDEL and XDELEX.
pub(super) fn parse_reference_policy(option: &Bytes) -> Option<ReferencePolicy> {
    if option_eq(option, b"KEEPREF") {
        Some(ReferencePolicy::KeepRef)
    } else if option_eq(option, b"DELREF") {
        Some(ReferencePolicy::DelRef)
    } else if option_eq(option, b"ACKED") {
        Some(ReferencePolicy::Acked)
    } else {
        None
    }
}

/// Parsed `[policy] IDS numids id ...` options used by XACKDEL and XDELEX.
pub(super) struct DeleteArgs {
    pub references: ReferencePolicy,
    pub ids: Vec<Bytes>,
}

/// Parse the Redis 8.2 delete options. IDs stay raw so the command can check
/// the key/group before validating them, matching Redis' error order.
pub(super) fn parse_delete_args(args: &[Bytes]) -> Result<DeleteArgs, ProtocolError> {
    let mut references = None;
    let mut ids = None;
    let mut index = 0;
    while index < args.len() {
        let option = &args[index];
        if references.is_none()
            && let Some(policy) = parse_reference_policy(option)
        {
            references = Some(policy);
            index += 1;
        } else if option_eq(option, b"IDS") && index + 1 < args.len() {
            let numids = parse_canonical_i64(&args[index + 1]).ok_or_else(|| {
                ProtocolError::response("ERR Number of IDs must be a positive integer")
            })?;
            if numids < 1 {
                return Err(ProtocolError::response(
                    "ERR Number of IDs must be a positive integer",
                ));
            }
            let count = usize::try_from(numids).map_err(|_| {
                ProtocolError::response("ERR Number of IDs must be a positive integer")
            })?;
            let start = index + 2;
            if count > args.len() - start {
                return Err(ProtocolError::response(
                    "ERR The `numids` parameter must match the number of arguments",
                ));
            }
            // Repeated IDS blocks follow Redis: the last one wins.
            ids = Some(args[start..start + count].to_vec());
            index = start + count;
        } else {
            return Err(ProtocolError::SyntaxError);
        }
    }
    let ids = ids.ok_or_else(|| ProtocolError::response("ERR IDS option is required"))?;
    Ok(DeleteArgs {
        references: references.unwrap_or(ReferencePolicy::KeepRef),
        ids,
    })
}

/// Parse raw IDs with Redis' strict stream-ID rules (no `-`/`+`).
pub(super) fn parse_strict_ids(raw: &[Bytes]) -> Result<Vec<StreamId>, ProtocolError> {
    raw.iter()
        .map(|raw| {
            std::str::from_utf8(raw)
                .ok()
                .and_then(|text| StreamId::from_str(text).ok())
                .ok_or_else(invalid_stream_id)
        })
        .collect()
}

pub(super) fn delete_status_reply(statuses: Vec<DeleteStatus>) -> Value {
    Value::Array(Some(
        statuses
            .into_iter()
            .map(|status| Value::Integer(status as i8 as i64))
            .collect(),
    ))
}

/// XACKDEL/XDELEX answer one `-1` per requested ID when the key or group is
/// absent, so the caller still sees one status per ID.
pub(super) fn not_found_statuses(count: usize) -> Value {
    Value::Array(Some(vec![
        Value::Integer(
            DeleteStatus::NotFound as i8 as i64
        );
        count
    ]))
}

// The NOGROUP replies live in the state-machine layer so the blocking
// dispatcher can reuse them. Re-exported here for the stream commands.
pub(crate) use crate::raft::types::core::cache::stream::{no_group, no_group_read};
