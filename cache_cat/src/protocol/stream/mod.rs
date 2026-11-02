use crate::error::ProtocolError;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::{IdRange, StreamId};
use bytes::Bytes;
use std::ops::Bound;

pub mod xack;
pub mod xadd;
pub mod xclaim;
pub mod xlen;
pub mod xpending;
pub mod xrange;
pub mod xread;
pub mod xreadgroup;

pub mod xgroup;

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

/// `NOGROUP` reply for XCLAIM and XPENDING.
pub(super) fn no_group(key: &Bytes, group: &Bytes) -> Value {
    ProtocolError::response(format!(
        "NOGROUP No such key '{}' or consumer group '{}'",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(group)
    ))
    .into()
}

/// XREADGROUP appends a hint about the missing GROUP option, matching Redis.
pub(super) fn no_group_read(key: &Bytes, group: &Bytes) -> Value {
    ProtocolError::response(format!(
        "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP option",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(group)
    ))
    .into()
}
