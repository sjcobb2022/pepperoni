use std::time::{Duration, Instant};

pub mod lease;
pub mod pg;

use lease::{AcquireOutcome, LeaseClient, LeaseError, LeaseObservation, RenewOutcome, Term};
use pg::PgCtl;

#[derive(Clone, Copy)]
pub struct Config {
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

impl<L: LeaseClient, P: PgCtl> Init<L, P> {
    pub fn new(ctx: Ctx<L, P>) -> Self {
        Self { ctx }
    }

    pub async fn on_tick(self, now: Instant) -> Result<Standby<L, P>, Electing<L, P>> {
        let mut ctx = self.ctx;
        match ctx.lease.observe().await {
            Ok(LeaseObservation::Leader(_id, _leader)) => {
                let _ = ctx.pg.start_standby().await;
                Ok(Standby { ctx })
            }
            Ok(LeaseObservation::NoLeader) | Err(LeaseError::Unreachable) | Err(_) => {
                Err(Electing { ctx, since: now })
            }
        }
    }
}

impl<L: LeaseClient, P: PgCtl> Electing<L, P> {
    pub async fn campaign(self) -> Result<Promoting<L, P>, Init<L, P>> {
        let mut ctx = self.ctx;
        match ctx.lease.try_acquire(ctx.cfg.lease_ttl).await {
            Ok(AcquireOutcome::Acquired(grant)) => Ok(Promoting {
                ctx,
                term: grant.term,
                exp: grant.expires_at,
            }),
            Ok(AcquireOutcome::Contended) | Err(_) => Err(Init { ctx }),
        }
    }

    pub fn since(&self) -> Instant {
        self.since
    }
}

impl<L: LeaseClient, P: PgCtl> Promoting<L, P> {
    pub async fn promote(self) -> Result<Leader<L, P>, Demoting<L, P>> {
        let mut ctx = self.ctx;
        match ctx.pg.promote().await {
            Ok(()) => Ok(Leader {
                ctx,
                term: self.term,
                exp: self.exp,
            }),
            // TODO: Handle variants? Or just log?
            Err(_) => Err(Demoting { ctx }),
        }
    }
}

impl<L: LeaseClient, P: PgCtl> Leader<L, P> {
    pub async fn on_tick(mut self, now: Instant) -> Result<Leader<L, P>, Demoting<L, P>> {
        // We renew if we are in the proper range from our expiration.

        if now + self.ctx.cfg.renew_margin >= self.exp {
            match self.ctx.lease.renew(self.ctx.cfg.lease_ttl).await {
                Ok(RenewOutcome::Renewed { expires_at }) => self.exp = expires_at,
                Ok(RenewOutcome::Lost) | Err(_) => return Err(Demoting { ctx: self.ctx }),
            }
        }

        Ok(self)
    }

    pub fn term(&self) -> Term {
        self.term
    }
}

impl<L: LeaseClient, P: PgCtl> Standby<L, P> {
    pub async fn on_tick(self, now: Instant) -> Result<Standby<L, P>, Electing<L, P>> {
        let mut ctx = self.ctx;
        match ctx.lease.observe().await {
            Ok(LeaseObservation::Leader(_, _)) => Ok(Standby { ctx }),
            _ => Err(Electing { ctx, since: now }),
        }
    }
}

impl<L: LeaseClient, P: PgCtl> Demoting<L, P> {
    pub async fn finish(self) -> Init<L, P> {
        let mut ctx = self.ctx;
        // We should hang if either of these fail,
        // and let watchdog kill the process.

        if ctx.pg.stop().await.is_err() {
            std::future::pending::<()>().await; // never returns
        }

        if ctx.lease.release().await.is_err() {
            std::future::pending::<()>().await; // never returns
        }

        Init { ctx }
    }
}
