use std::{
    convert::Infallible,
    time::{Duration, Instant},
};

pub mod lease;
pub mod pg;

use lease::{AcquireOutcome, LeaseClient, LeaseError, LeaseObservation, RenewOutcome, Term};
use pg::PgCtl;

use crate::lease::NodeId;

#[derive(Clone, Copy)]
pub struct Config {
    /// The current node's id. Static and set by the user.
    pub id: NodeId,
    pub lease_ttl: Duration,
    pub renew_margin: Duration,
}

pub struct Ctx<L: LeaseClient, P: PgCtl> {
    pub lease: L,
    pub pg: P,
    pub cfg: Config,
}

pub struct Init<L: LeaseClient, P: PgCtl> {
    ctx: Ctx<L, P>,
}

pub struct Electing<L: LeaseClient, P: PgCtl> {
    ctx: Ctx<L, P>,
    since: Instant,
}

pub struct Promoting<L: LeaseClient, P: PgCtl> {
    ctx: Ctx<L, P>,
    term: Term,
    exp: Instant,
}

pub struct Leader<L: LeaseClient, P: PgCtl> {
    ctx: Ctx<L, P>,
    term: Term,
    exp: Instant,
}

pub struct Standby<L: LeaseClient, P: PgCtl> {
    ctx: Ctx<L, P>,
}

pub struct Demoting<L: LeaseClient, P: PgCtl> {
    ctx: Ctx<L, P>,
}

pub trait Tick: Sized {
    /// The next state when the tick succeeds.
    type Advance;
    /// The next state when the tick fails.
    type Retreat;

    async fn tick(self, now: Instant) -> Result<Self::Advance, Self::Retreat>;
}

impl<L: LeaseClient, P: PgCtl> Init<L, P> {
    pub fn new(ctx: Ctx<L, P>) -> Self {
        Self { ctx }
    }
}

impl<L: LeaseClient, P: PgCtl> Tick for Init<L, P> {
    type Advance = Standby<L, P>;
    type Retreat = Electing<L, P>;

    async fn tick(self, now: Instant) -> Result<Self::Advance, Self::Retreat> {
        let mut ctx = self.ctx;
        match ctx.lease.observe().await {
            Ok(LeaseObservation::Leader(id, _leader)) => {
                // TODO: Handle if the ID is the current nodes ID. Unlikely but still possible.
                // Would need the entire machine to die and reboot within the lease period.
                // Possible with a large lease.
                // If we are certain that watchdog kills the process this may be unnecessary.
                // Do some testing.

                let _ = ctx.pg.start_standby().await;
                Ok(Standby { ctx })
            }
            Ok(LeaseObservation::NoLeader) | Err(LeaseError::Unreachable) | Err(_) => {
                Err(Electing { ctx, since: now })
            }
        }
    }
}

impl<L: LeaseClient, P: PgCtl> Tick for Electing<L, P> {
    type Advance = Promoting<L, P>;
    type Retreat = Init<L, P>;

    async fn tick(self, now: Instant) -> Result<Self::Advance, Self::Retreat> {
        let mut ctx = self.ctx;

        let deadline = self.since + ctx.cfg.lease_ttl;

        let Some(remaining) = deadline.checked_duration_since(now) else {
            return Err(Init { ctx }); // already past budget, don't even try
        };

        match tokio::time::timeout(remaining, ctx.lease.try_acquire(ctx.cfg.lease_ttl)).await {
            Ok(Ok(AcquireOutcome::Acquired(grant))) => Ok(Promoting {
                ctx,
                term: grant.term,
                exp: grant.expires_at,
            }),
            Ok(Ok(AcquireOutcome::Contended)) | Ok(Err(_)) => Err(Init { ctx }),
            Err(_elapsed) => Err(Init { ctx }), // timed out past budget.
        }
    }
}

impl<L: LeaseClient, P: PgCtl> Tick for Promoting<L, P> {
    type Advance = Leader<L, P>;
    type Retreat = Demoting<L, P>;

    async fn tick(self, now: Instant) -> Result<Self::Advance, Self::Retreat> {
        let mut ctx = self.ctx;

        let Some(remaining) = self.exp.checked_duration_since(now) else {
            return Err(Demoting { ctx }); // already expired
        };

        match tokio::time::timeout(remaining, ctx.pg.promote()).await {
            Ok(Ok(())) => Ok(Leader {
                ctx,
                term: self.term,
                exp: self.exp,
            }),
            Ok(Err(_e)) => Err(Demoting { ctx }),
            Err(_elapsed) => Err(Demoting { ctx }),
        }
    }
}

impl<L: LeaseClient, P: PgCtl> Tick for Leader<L, P> {
    type Advance = Leader<L, P>;

    type Retreat = Demoting<L, P>;

    async fn tick(self, now: Instant) -> Result<Self::Advance, Self::Retreat> {
        // create mutable self
        let mut this = self;

        // TODO: Validate if this is the same.
        if this.exp.checked_duration_since(now).is_none() {
            return Err(Demoting { ctx: this.ctx }); // already expired
        }

        if now >= this.exp {
            return Err(Demoting { ctx: this.ctx }); // already expired
        }

        // We renew if we are in the proper range from our expiration.
        if now + this.ctx.cfg.renew_margin >= this.exp {
            let remaining = this.exp.saturating_duration_since(now);

            let renew_fut = this.ctx.lease.renew(this.ctx.cfg.lease_ttl);

            match tokio::time::timeout(remaining, renew_fut).await {
                Ok(Ok(RenewOutcome::Renewed { expires_at })) => this.exp = expires_at,
                Ok(Ok(RenewOutcome::Lost)) | Ok(Err(_)) | Err(_) => {
                    return Err(Demoting { ctx: this.ctx })
                }
            }
        }

        Ok(this)
    }
}

impl<L: LeaseClient, P: PgCtl> Tick for Standby<L, P> {
    type Advance = Standby<L, P>;
    type Retreat = Electing<L, P>;

    async fn tick(self, now: Instant) -> Result<Self::Advance, Self::Retreat> {
        let mut ctx = self.ctx;
        match ctx.lease.observe().await {
            // TODO: We need to be able to detect a change in the leader.
            // Even if it does not pass through NoLeader.
            Ok(LeaseObservation::Leader(_, _)) => Ok(Standby { ctx }),
            _ => Err(Electing { ctx, since: now }),
        }
    }
}

impl<L: LeaseClient, P: PgCtl> Tick for Demoting<L, P> {
    type Advance = Init<L, P>;
    type Retreat = Infallible; // We can never retreat since we just hang if we fail.

    async fn tick(self, _now: Instant) -> Result<Self::Advance, Self::Retreat> {
        let mut ctx = self.ctx;

        // We should hang if either of these fail,
        // and let watchdog kill the process.
        if ctx.pg.stop().await.is_err() {
            std::future::pending::<()>().await; // never returns
        }

        if ctx.lease.release().await.is_err() {
            std::future::pending::<()>().await; // never returns
        }

        Ok(Init { ctx })
    }
}
