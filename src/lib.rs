use std::time::Instant;

#[derive(Debug, Clone)]
pub struct LeaderInfo {
    node_id: String,
    addr: String, // "host:port"
    epoch: u64,
    observed_at: Instant,
}
