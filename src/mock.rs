use std::time::{Duration, Instant};

use crate::lease::{
    AcquireOutcome, LeaseClient, LeaseError, LeaseGrant, LeaseObservation, NodeId, RenewOutcome,
    Term,
};
use crate::pg::{PgCtl, PgError};

pub struct MockLease {
    node_id: NodeId,
    term: u64,
    renews_since_acquire: u32,
    observations_until_leader_vanishes: u32,
}

const GHOST_LEADER_OBSERVATIONS: u32 = 4;

impl MockLease {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            term: 0,
            renews_since_acquire: 0,
            observations_until_leader_vanishes: GHOST_LEADER_OBSERVATIONS,
        }
    }
}

impl LeaseClient for MockLease {
    async fn observe(&mut self) -> Result<LeaseObservation, LeaseError> {
        tokio::time::sleep(Duration::from_millis(50)).await;

        if self.observations_until_leader_vanishes > 0 {
            self.observations_until_leader_vanishes -= 1;
            return Ok(LeaseObservation::Leader(NodeId("node-ghost".to_string())));
        }

        Ok(LeaseObservation::NoLeader)
    }

    async fn try_acquire(&mut self, ttl: Duration) -> Result<AcquireOutcome, LeaseError> {
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.term += 1;
        self.renews_since_acquire = 0;
        println!("[lease] {:?} acquired term {}", self.node_id, self.term);
        Ok(AcquireOutcome::Acquired(LeaseGrant {
            term: self.term,
            expires_at: Instant::now() + ttl,
        }))
    }

    async fn renew(&mut self, ttl: Duration) -> Result<RenewOutcome, LeaseError> {
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.renews_since_acquire += 1;

        // Fake a lost lease every 4th renewal so the demo shows a full
        // leader -> demote -> drain -> re-elect cycle instead of just
        // sitting as leader forever.
        if self.renews_since_acquire % 4 == 0 {
            println!(
                "[lease] {:?} LOST term {} on renew",
                self.node_id, self.term
            );
            return Ok(RenewOutcome::Lost);
        }

        println!("[lease] {:?} renewed term {}", self.node_id, self.term);
        Ok(RenewOutcome::Renewed {
            expires_at: Instant::now() + ttl,
        })
    }

    async fn release(&mut self) -> Result<(), LeaseError> {
        tokio::time::sleep(Duration::from_millis(20)).await;
        println!("[lease] {:?} released term {}", self.node_id, self.term);
        // Simulate some other node picking up the lease for a while
        // before it, too, disappears -- so next time around we go
        // through Standby again instead of racing straight back in.
        self.observations_until_leader_vanishes = GHOST_LEADER_OBSERVATIONS;
        Ok(())
    }
}

/// Stand-in for real `pg_ctl` calls.
pub struct MockPg;

impl MockPg {
    pub fn new() -> Self {
        Self
    }
}

impl PgCtl for MockPg {
    async fn start_standby(&mut self) -> Result<(), PgError> {
        println!("[pg] starting as standby");
        tokio::time::sleep(Duration::from_millis(150)).await;
        Ok(())
    }

    async fn promote(&mut self) -> Result<(), PgError> {
        println!("[pg] promoting to primary...");
        tokio::time::sleep(Duration::from_millis(400)).await;
        println!("[pg] promotion complete");
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), PgError> {
        println!("[pg] stopping postgres");
        tokio::time::sleep(Duration::from_millis(150)).await;
        Ok(())
    }
}
