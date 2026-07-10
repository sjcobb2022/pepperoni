use std::time::Instant;

use crate::lease::{
    AcquireOutcome, LeaseClient, LeaseError, LeaseObservation, NodeId, RenewOutcome, Term,
};
use crate::pg::PgCtl;

pub enum State {
    Init,
    Electing { since: Instant },
    Promoting { term: Term, expiry: Instant },
    Leader { term: Term, expiry: Instant },
    Standby { leader: NodeId },
    Demoting,
}

pub struct Ctx<L: LeaseClient, P: PgCtl> {
    pub lease: L,
    pub pg: P,
    pub cfg: crate::Config,
}

impl State {
    async fn tick<L: LeaseClient, P: PgCtl>(self, ctx: &mut Ctx<L, P>, now: Instant) -> Self {
        match self {
            Self::Init => {
                match ctx.lease.observe().await {
                    Ok(LeaseObservation::Leader(id, _leader)) => {
                        // TODO: Handle if the ID is the current nodes ID. Unlikely but still possible.
                        // Would need the entire machine to die and reboot within the lease period.
                        // Possible with a large lease.
                        // If we are certain that watchdog kills the process this may be unnecessary.
                        // Do some testing.

                        let _ = ctx.pg.start_standby().await;
                        return Self::Standby { leader: id };
                    }
                    Ok(LeaseObservation::NoLeader) | Err(LeaseError::Unreachable) | Err(_) => {
                        return Self::Electing { since: now }
                    }
                }
            }

            Self::Electing { since } => {
                let deadline = since + ctx.cfg.lease_ttl;

                if deadline.checked_duration_since(now).is_none() {
                    return Self::Init; // already past budget, don't even try
                };

                return match ctx.lease.try_acquire(ctx.cfg.lease_ttl).await {
                    Ok(AcquireOutcome::Acquired(grant)) => Self::Promoting {
                        term: grant.term,
                        expiry: grant.expires_at,
                    },
                    Ok(AcquireOutcome::Contended) | Err(_) => Self::Init,
                };
            }

            Self::Promoting { term, expiry } => {
                let Some(remaining) = expiry.checked_duration_since(now) else {
                    return Self::Demoting; // already expired
                };

                match tokio::time::timeout(remaining, ctx.pg.promote()).await {
                    Ok(Ok(())) => Self::Leader { term, expiry },
                    Ok(Err(_e)) => Self::Demoting,
                    Err(_elapsed) => Self::Demoting,
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

                    match tokio::time::timeout(remaining, renew_fut).await {
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
                let _ = leader; // noop
                match ctx.lease.observe().await {
                    // TODO: We need to be able to detect a change in the leader.
                    // Even if it does not pass through NoLeader.

                    // Currently, we will just move to a state with the new leader, and not do anything.
                    // We may need to handle this later.
                    Ok(LeaseObservation::Leader(id, _)) => Self::Standby { leader: id },
                    _ => Self::Electing { since: now },
                }
            }

            Self::Demoting => {
                // We should hang if either of these fail,
                // and let watchdog kill the process.
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
