use crate::protocol::stream::xack::XAckReq;
use crate::protocol::stream::xclaim::XClaimReq;
use crate::protocol::stream::xgroup::XGroupReq;
use crate::protocol::stream::xreadgroup::XReadGroupStreamReq;
use crate::raft::types::core::mocha::core::{MyCache, Update};
use crate::raft::types::core::response_value::Value;

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
}
