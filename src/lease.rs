use std::time::{Duration, Instant};

use thiserror::Error;

pub type Term = u64;

#[derive(Debug, Error)]
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

#[derive(Debug, Clone, Copy)]
pub enum RenewOutcome {
    Renewed { expires_at: Instant },
    Lost, // no longer holder / CAS lost
}

#[derive(Debug, Clone, Copy)]
pub enum LeaseObservation {
    NoLeader,
    Leader(LeaseGrant), // add holder NodeId if needed
}

pub trait LeaseClient {
    async fn observe(&mut self) -> Result<LeaseObservation, LeaseError>;
    async fn try_acquire(&mut self, ttl: Duration) -> Result<AcquireOutcome, LeaseError>;
    async fn renew(&mut self, ttl: Duration) -> Result<RenewOutcome, LeaseError>;
    async fn release(&mut self) -> Result<(), LeaseError>;
}
