use std::time::{Duration, Instant};

use crate::{
    lease::{AcquireOutcome, LeaseError, LeaseGrant, LeaseObservation, NodeId, RenewOutcome, Term},
    pg::PgError,
};

pub mod lease;
pub mod mock;
pub mod pg;
pub mod proxy;

// TODO: SansIO

pub enum State {
    Init,
    ObservingFromInit,
    ObservingFromStandby,
    AcquiringLease {
        since: Instant,
        deadline: Instant,
    },
    Promoting {
        term: Term,
        expiry: Instant,
    },
    Leader {
        term: Term,
        expiry: Instant,
    },
    Renewing {
        term: Term,
        expiry: Instant,
        deadline: Instant,
    },
    StartingStandby {
        leader: NodeId,
    },
    Standby {
        leader: NodeId,
    },
    Demoting,
    ReleaseingLease,
    // terminal state, we should let watchdog kill
    Stuck,
}

// TODO: Remove dependency on lease stuff here.
// We can do one of two things.
// Create more states which we trigger on, such as AcquireSuccess or AcquireFailure, but that just
// increase the amount of individual edge cases that we need to handle.
// The alternative would be to construct our own LeaseObservation types here, which external things
// depend on. That would make it more "pure".
// Honestly I am more in favor of separate states as that means we have no dependencies at all.
pub enum Event {
    Now(Instant),
    LeaseObserved(Result<LeaseObservation, LeaseError>),
    Acquired(Result<AcquireOutcome, LeaseError>),
    Promoted {
        term: Term,
        result: Result<(), LeaseError>,
    },
    Renewed {
        term: Term,
        result: Result<RenewOutcome, LeaseError>,
    },
    StandbyStarted(Result<(), PgError>),
    Stopped(Result<(), PgError>),
    Released(Result<(), LeaseError>),
}

pub enum Command {
    ObserveLease,
    TryAcquireLease { ttl: Duration },
    Promote { term: Term },
    RenewLease { term: Term, ttl: Duration },
    StartStandby,
    StopPg,
    ReleaseLease,
    ArmTimer(Instant),
}

pub fn step(state: State, event: Event) -> (State, Vec<Command>) {
    use State::*;
    // TODO: pass directly into function
    let now = Instant::now();

    match (state, event) {
        // If we get triggered immediately, we should attempt to observe the current lease
        (Init, Event::Now(_)) => (ObservingFromInit, vec![Command::ObserveLease]),
        // otherwise, we do nothing.
        (s @ Init, _) => (s, vec![]),

        // If we are observing, and we are the leader, then we should stop since that is a failure
        (ObservingFromInit, Event::LeaseObserved(Ok(LeaseObservation::Leader(id))))
        // TODO: cfg.id
            if id.0 == "DUMMY_REPLACE" =>
        {
            (Demoting, vec![Command::StopPg])
        }

        // otherwise, we move into standby with the leader.
        (ObservingFromInit, Event::LeaseObserved(Ok(LeaseObservation::Leader(id)))) => {
            (StartingStandby { leader: id }, vec![Command::StartStandby])
        }

        (ObservingFromInit, Event::LeaseObserved(_)) => {
            // If we either have failed to observe, or there is no leader, we should try and acquire
            // the lease.

            // let deadline = now + todo!();
            (
                AcquiringLease {
                    since: now,
                    deadline: now,
                },
                vec![
                    // Command::TryAcquireLease { ttl: todo!("cfg.lease_ttl") },
                    // Command::ArmTimer(deadline),
                ],
            )
        }

        (s @ ObservingFromInit, _) => (s, vec![]),

        (ObservingFromStandby, Event::LeaseObserved(Ok(LeaseObservation::Leader(id)))) => (
            Standby { leader: id },
            vec![
                // Command::ArmTimer(now + todo!("cfg.timeout"))
            ],
        ),

        (
            ObservingFromStandby,
            Event::LeaseObserved(Ok(LeaseObservation::NoLeader)),
        ) => {
            // let deadline = now + todo!("cfg.lease_ttl");
            (
                AcquiringLease { since: now, deadline: now},
                vec![
                    // Command::TryAcquireLease { ttl: todo!("cfg.lease_ttl") },
                    // Command::ArmTimer(deadline),
                ],
            )
        },

        (ObservingFromStandby, Event::LeaseObserved(Err(_))) => (Init, vec![Command::ArmTimer(now)]),


        (s @ ObservingFromStandby, _) => (s, vec![]),

        (AcquiringLease { deadline, .. }, Event::Now(t)) if t >= deadline => {
            (Init, vec![Command::ArmTimer(t)])
        }

        (s @ AcquiringLease { .. }, Event::Now(_)) => (s, vec![]),

        (
            AcquiringLease { .. },
            Event::Acquired(Ok(AcquireOutcome::Acquired(LeaseGrant { term, expires_at })))
        ) => (
            Promoting { term, expiry: expires_at },
            vec![Command::Promote { term }],
        ),

        (AcquiringLease { .. }, Event::Acquired(_)) => (Init, vec![Command::ArmTimer(now)]),

        (s @ AcquiringLease { .. }, _) => (s, vec![]),

        (s @ Stuck, _) => (s, vec![]),
        _ => todo!(),
    }
}

// pub enum State {
//     Init,
//     Electing { since: Instant },
//     Promoting { term: Term, expiry: Instant },
//     Leader { term: Term, expiry: Instant },
//     Standby { leader: NodeId },
//     Demoting,
// }

// impl State {
//     pub async fn tick<L: LeaseClient, P: PgCtl>(self, ctx: &mut Ctx<L, P>, now: Instant) -> Self {
//         match self {
//             Self::Init => {
//                 match ctx.lease.observe().await {
//                     Ok(LeaseObservation::Leader(id)) => {
//                         // If we are the leader move to demoting since that should not be the case.
//                         if id.0 == ctx.cfg.id.0 {
//                             return Self::Demoting;
//                         }
//
//                         return match ctx.pg.start_standby().await {
//                             Ok(()) => Self::Standby { leader: id },
//                             // TODO: If something in postgres is misconfigured and it fails to
//                             // start, we may end up looping indefinitely.
//                             // The alternative would be to force a crash/restart by stalling indefinitely.
//                             Err(PgError::Command(command)) => {
//                                 // TODO: Return error
//                                 panic!("Test")
//                             }
//                             _ => Self::Init,
//                         };
//                     }
//                     _ => return Self::Electing { since: now },
//                 }
//             }
//
//             Self::Electing { since } => {
//                 let deadline = since + ctx.cfg.lease_ttl;
//
//                 let Some(remaining) = deadline.checked_duration_since(now) else {
//                     return Self::Init; // already past budget, don't even try
//                 };
//
//                 return match timeout(remaining, ctx.lease.try_acquire(ctx.cfg.lease_ttl)).await {
//                     Ok(Ok(AcquireOutcome::Acquired(grant))) => Self::Promoting {
//                         term: grant.term,
//                         expiry: grant.expires_at,
//                     },
//                     // If we timeout, or we error for whatever reason, we should re-init our state.
//                     _ => Self::Init,
//                 };
//             }
//
//             Self::Promoting { term, expiry } => {
//                 let Some(remaining) = expiry.checked_duration_since(now) else {
//                     return Self::Demoting; // already expired
//                 };
//
//                 // NOTE: We are not really bothered if the promotion fails since we fallback to
//                 // demoting on failure. Demoting fails hard, and therefore if postgres is busy
//                 // it will be killed by watchdog.
//                 match timeout(remaining, ctx.pg.promote()).await {
//                     Ok(Ok(())) => Self::Leader { term, expiry },
//                     _ => Self::Demoting,
//                 }
//             }
//
//             Self::Leader { term, expiry } => {
//                 if now >= expiry {
//                     return Self::Demoting; // already expired
//                 }
//
//                 // We renew if we are in the proper range from our expiration.
//                 if now + ctx.cfg.renew_margin >= expiry {
//                     let remaining = expiry.saturating_duration_since(now);
//
//                     let renew_fut = ctx.lease.renew(ctx.cfg.lease_ttl);
//
//                     match timeout(remaining, renew_fut).await {
//                         Ok(Ok(RenewOutcome::Renewed { expires_at })) => {
//                             return Self::Leader {
//                                 term,
//                                 expiry: expires_at,
//                             }
//                         }
//                         Ok(Ok(RenewOutcome::Lost)) | Ok(Err(_)) | Err(_) => {
//                             return Self::Demoting;
//                         }
//                     }
//                 }
//
//                 Self::Leader { term, expiry }
//             }
//
//             Self::Standby { leader } => {
//                 // If we are in standby but we are the leader, then we should demote.
//                 if leader.0 == ctx.cfg.id.0 {
//                     return Self::Demoting;
//                 }
//
//                 match ctx.lease.observe().await {
//                     // Update state to new leader regardless
//                     Ok(LeaseObservation::Leader(id)) => Self::Standby { leader: id },
//                     // Go to re-election if no leader
//                     Ok(LeaseObservation::NoLeader) => Self::Electing { since: now },
//                     // Otherwise go back to init since we don't know the status.
//                     _ => Self::Init,
//                 }
//             }
//
//             Self::Demoting => {
//                 // Should hang if demoting postgres or the lease fails and let watchdog kill the process.
//                 // Watchdog will also catch if the await hangs indefinitely.
//
//                 if ctx.pg.stop().await.is_err() {
//                     std::future::pending::<()>().await; // never returns
//                 }
//
//                 if ctx.lease.release().await.is_err() {
//                     std::future::pending::<()>().await; // never returns
//                 }
//
//                 Self::Init
//             }
//         }
//     }
// }
