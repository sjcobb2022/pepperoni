use std::time::{Duration, Instant};

use thiserror::Error;

#[derive(Debug, Clone)]
pub struct NodeId(pub String);

pub type Term = u64;

#[derive(Debug, Clone, Error)]
pub enum LeaseError {
    #[error("store unreachable")]
    Unreachable,
    #[error("conflict")]
    Conflict,
    #[error("not holder")]
    NotHolder,
    #[error("backend: {0}")]
    Backend(String),
}

#[derive(Debug, Clone, Copy)]
pub struct LeaseGrant {
    pub term: Term,
    pub expires_at: Instant,
}

#[derive(Debug, Clone, Copy)]
pub enum AcquireOutcome {
    Acquired(LeaseGrant),
    Contended, // someone else already holds lease
}

#[derive(Debug, Clone)]
pub enum RenewOutcome {
    Renewed { expires_at: Instant },
    Lost, // no longer holder
}

#[derive(Debug, Clone)]
pub enum LeaseObservation {
    NoLeader,
    Leader(NodeId),
}

pub trait LeaseClient {
    fn observe(&mut self) -> impl Future<Output = Result<LeaseObservation, LeaseError>>;
    fn try_acquire(
        &mut self,
        ttl: Duration,
    ) -> impl Future<Output = Result<AcquireOutcome, LeaseError>>;
    fn renew(&mut self, ttl: Duration) -> impl Future<Output = Result<RenewOutcome, LeaseError>>;
    fn release(&mut self) -> impl Future<Output = Result<(), LeaseError>>;
}
