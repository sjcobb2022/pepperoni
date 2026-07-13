use std::time::{Duration, Instant};

pub mod lease;
pub mod mock;
pub mod pg;
pub mod proxy;

use lease::{AcquireOutcome, LeaseClient, LeaseObservation, NodeId, RenewOutcome, Term};
use pg::PgCtl;
use tokio::time::timeout;

use crate::pg::PgError;

#[derive(Clone)]
pub struct Config {
    /// The current node's id. Static and set by the user.
    pub id: NodeId,
    pub timeout: Duration,
    pub lease_ttl: Duration,
    pub renew_margin: Duration,
    pub drain_timeout: Duration,
    pub drain_refresh: Duration,
    pub watchdog_timeout: Duration,
}

pub struct Ctx<L: LeaseClient, P: PgCtl> {
    pub lease: L,
    pub pg: P,
    pub cfg: Config,
}

// TODO: SansIO

pub enum State {
    Init,
    Electing { since: Instant },
    Promoting { term: Term, expiry: Instant },
    Leader { term: Term, expiry: Instant },
    Standby { leader: NodeId },
    Demoting,
}

#[derive(Debug)]
pub enum TickError {
    StandbySetupFailed(PgError),
}

impl State {
    pub async fn tick<L: LeaseClient, P: PgCtl>(self, ctx: &mut Ctx<L, P>, now: Instant) -> Self {
        match self {
            Self::Init => {
                match ctx.lease.observe().await {
                    Ok(LeaseObservation::Leader(id)) => {
                        // If we are the leader move to demoting since that should not be the case.
                        if id.0 == ctx.cfg.id.0 {
                            return Self::Demoting;
                        }

                        return match ctx.pg.start_standby().await {
                            Ok(()) => Self::Standby { leader: id },
                            // TODO: If something in postgres is misconfigured and it fails to
                            // start, we may end up looping indefinitely.
                            // The alternative would be to force a crash/restart by stalling indefinitely.
                            Err(PgError::Command(command)) => {
                                // TODO: Return error
                                panic!("Test")
                            }
                            _ => Self::Init,
                        };
                    }
                    _ => return Self::Electing { since: now },
                }
            }

            Self::Electing { since } => {
                let deadline = since + ctx.cfg.lease_ttl;

                let Some(remaining) = deadline.checked_duration_since(now) else {
                    return Self::Init; // already past budget, don't even try
                };

                return match timeout(remaining, ctx.lease.try_acquire(ctx.cfg.lease_ttl)).await {
                    Ok(Ok(AcquireOutcome::Acquired(grant))) => Self::Promoting {
                        term: grant.term,
                        expiry: grant.expires_at,
                    },
                    // If we timeout, or we error for whatever reason, we should re-init our state.
                    _ => Self::Init,
                };
            }

            Self::Promoting { term, expiry } => {
                let Some(remaining) = expiry.checked_duration_since(now) else {
                    return Self::Demoting; // already expired
                };

                // NOTE: We are not really bothered if the promotion fails since we fallback to
                // demoting on failure. Demoting fails hard, and therefore if postgres is busy
                // it will be killed by watchdog.
                match timeout(remaining, ctx.pg.promote()).await {
                    Ok(Ok(())) => Self::Leader { term, expiry },
                    _ => Self::Demoting,
                }
            }

            Self::Leader { term, expiry } => {
                if now >= expiry {
                    return Self::Demoting; // already expired
                }

                // We renew if we are in the proper range from our expiration.
                if now + ctx.cfg.renew_margin >= expiry {
                    let remaining = expiry.saturating_duration_since(now);

                    let renew_fut = ctx.lease.renew(ctx.cfg.lease_ttl);

                    match timeout(remaining, renew_fut).await {
                        Ok(Ok(RenewOutcome::Renewed { expires_at })) => {
                            return Self::Leader {
                                term,
                                expiry: expires_at,
                            }
                        }
                        Ok(Ok(RenewOutcome::Lost)) | Ok(Err(_)) | Err(_) => {
                            return Self::Demoting;
                        }
                    }
                }

                Self::Leader { term, expiry }
            }

            Self::Standby { leader } => {
                // If we are in standby but we are the leader, then we should demote.
                if leader.0 == ctx.cfg.id.0 {
                    return Self::Demoting;
                }

                match ctx.lease.observe().await {
                    // Update state to new leader regardless
                    Ok(LeaseObservation::Leader(id)) => Self::Standby { leader: id },
                    // Go to re-election if no leader
                    Ok(LeaseObservation::NoLeader) => Self::Electing { since: now },
                    // Otherwise go back to init since we don't know the status.
                    _ => Self::Init,
                }
            }

            Self::Demoting => {
                // Should hang if demoting postgres or the lease fails and let watchdog kill the process.
                // Watchdog will also catch if the await hangs indefinitely.

                if ctx.pg.stop().await.is_err() {
                    std::future::pending::<()>().await; // never returns
                }

                if ctx.lease.release().await.is_err() {
                    std::future::pending::<()>().await; // never returns
                }

                Self::Init
            }
        }
    }
}
