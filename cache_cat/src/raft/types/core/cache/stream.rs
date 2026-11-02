use crate::error::ProtocolError;
use crate::protocol::stream::xack::XAckReq;
use crate::protocol::stream::xackdel::XAckDelReq;
use crate::protocol::stream::xautoclaim::XAutoClaimReq;
use crate::protocol::stream::xclaim::XClaimReq;
use crate::protocol::stream::xdel::XDelReq;
use crate::protocol::stream::xdelex::XDelexReq;
use crate::protocol::stream::xgroup::XGroupReq;
use crate::protocol::stream::xreadgroup::XReadGroupStreamReq;
use crate::protocol::stream::xsetid::XSetIdReq;
use crate::protocol::stream::xtrim::XTrimReq;
use crate::raft::types::core::mocha::core::{MyCache, Update};
use crate::raft::types::core::response_value::Value;
use bytes::Bytes;

/// `NOGROUP` reply for XCLAIM and XPENDING. Lives next to the state machine so
/// the blocking dispatcher can answer a destroyed group without an extra Raft
/// write.
pub(crate) fn no_group(key: &Bytes, group: &Bytes) -> Value {
    ProtocolError::response(format!(
        "NOGROUP No such key '{}' or consumer group '{}'",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(group)
    ))
    .into()
}

/// XREADGROUP appends a hint about the missing GROUP option, matching Redis.
pub(crate) fn no_group_read(key: &Bytes, group: &Bytes) -> Value {
    ProtocolError::response(format!(
        "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP option",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(group)
    ))
    .into()
}

impl MyCache {
    pub fn x_group(&self, param: XGroupReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    /// XACK mutates only the consumer group PEL. `execute_compute` supplies the
    /// snapshot revision and CAS replay handling shared by every single-key
    /// read-modify-write command.
    pub fn x_ack(&self, param: XAckReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    /// One stream of a replicated XREADGROUP. The multi-key command applies
    /// this per key so a snapshot records an independent single-key operation.
    pub fn x_read_group(&self, param: XReadGroupStreamReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    /// XCLAIM moves PEL ownership and rewrites delivery metadata.
    pub fn x_claim(&self, param: XClaimReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    /// XAUTOCLAIM scans the PEL and transfers all sufficiently idle entries.
    pub fn x_autoclaim(&self, param: XAutoClaimReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    /// XACKDEL acknowledges PEL entries and deletes their payloads.
    pub fn x_ackdel(&self, param: XAckDelReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    /// XDELEX deletes payloads according to the reference policy.
    pub fn x_delex(&self, param: XDelexReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    /// XSETID updates last-generated-ID and logical counters.
    pub fn x_setid(&self, param: XSetIdReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    /// XDEL removes entry payloads but keeps PEL records and the last ID.
    pub fn x_del(&self, param: XDelReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    /// XTRIM removes entries from the head, applying the reference policy.
    pub fn x_trim(&self, param: XTrimReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}
