//TODO: #![no_std]

use std::time::{Duration, Instant};

pub struct NodeId(String);

impl PartialEq for NodeId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

pub type Term = u64;

pub struct Config {
    /// The current node's id. Static and set by the user.
    pub id: NodeId,
    /// How often standby re-checks who the leader is.
    pub timeout: Duration,
    /// How long a lease/promotion lasts once acquired.
    pub lease_ttl: Duration,
    /// How early before expiry we start trying to renew.
    pub renew_margin: Duration,
}

// TODO: Remove instant processing. Having to have knowledge of the current immediate time is not
// ideal, honestly it would be easier to simply deal in durations and timeouts, such that things are
// triggered as necessary. Therefore we should transition to only be duration based.
pub enum State {
    /// Initial state. Will only transition to [State::ObservingFromInit].
    Init,
    /// Initial lease observation state, observation in pending.
    ObservingFromInit,
    /// Re-observing state from standby.
    ObservingFromStandby {
        leader: NodeId,
    },
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
    ReleasingLease,
    // terminal state, we should let watchdog kill
    Stuck,
}

// TODO: Remove instant processing. Having to have knowledge of the current immediate time is not
// ideal, honestly it would be easier to simply deal in durations and timeouts, such that things are
// triggered as necessary. Therefore we should transition to only be duration based.
pub enum Event {
    /// Time has passed, so the state must be re-evaluated.
    Now(Instant),

    // Observation
    /// The lease is reachable and a leader is present.
    ObservedLeader(NodeId),
    /// The lease is reachable and no leader is present
    ObservedNoLeader,
    /// The lease is unreachable.
    ObserveFailed,

    // Lease acquiring
    /// Acquiring a lease has succeeded.
    AcquireSucceeded { term: Term, expiry: Instant },
    /// Acquiring a lease has failed.
    AcquireFailed,

    // Promoting
    /// Promotion has succeeded.
    PromoteSucceeded { term: Term },
    /// Promotion has failed.
    PromoteFailed { term: Term },

    // Renewing
    /// Lease renewal has succeeded
    RenewSucceeded { term: Term, expiry: Instant },
    /// Lease renewal has not succeeded.
    RenewFailed { term: Term },

    // Standby
    /// A node has been started in standby successfully.
    StandbyStarted,
    /// A node has failed to start.
    StandbyStartFailed,

    // Stopping
    /// A node has stopped succesfully.
    Stopped,
    /// A node has failed to stop.
    StopFailed,

    // Release
    /// A lease has been released voluntarily.
    Released,
    /// Releasing a lease has failed.
    ReleaseFailed,
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
    let renew_margin = Duration::from_secs(1);

    match (state, event) {
        // Init
        // If we get triggered immediately, we should attempt to observe the current lease
        (Init, Event::Now(_)) => (ObservingFromInit, vec![Command::ObserveLease]),
        // otherwise, we do nothing.
        (s @ Init, _) => (s, vec![]),

        // ObservingFromInit
        // If we are observing, and we are the leader, then we should stop since that is a failure
        (ObservingFromInit, Event::ObservedLeader(id)) if todo!("id.0 == cfg.id") => {
            (Demoting, vec![Command::StopPg])
        }
        // otherwise, we move into standby with the leader.
        (ObservingFromInit, Event::ObservedLeader(id)) => {
            (StartingStandby { leader: id }, vec![Command::StartStandby])
        }
        (ObservingFromInit, Event::ObservedNoLeader)
        | (ObservingFromInit, Event::ObserveFailed) => {
            // If we either have failed to observe, or there is no leader, we should try and acquire
            // the lease.

            let deadline = now + todo!("cfg.lease_ttl");
            (
                AcquiringLease {
                    since: now,
                    deadline: now,
                },
                vec![
                    Command::TryAcquireLease {
                        ttl: todo!("cfg.lease_ttl"),
                    },
                    Command::ArmTimer(deadline),
                ],
            )
        }
        (s @ ObservingFromInit, _) => (s, vec![]),

        // ObservingFromStandby
        (ObservingFromStandby { .. }, Event::ObservedLeader(id)) => (
            Standby { leader: id },
            vec![Command::ArmTimer(now + todo!("cfg.timeout"))],
        ),
        (ObservingFromStandby { .. }, Event::ObservedNoLeader) => {
            let deadline = now + todo!("cfg.lease_ttl");
            (
                AcquiringLease {
                    since: now,
                    deadline: now,
                },
                vec![
                    Command::TryAcquireLease {
                        ttl: todo!("cfg.lease_ttl"),
                    },
                    Command::ArmTimer(deadline),
                ],
            )
        }
        (ObservingFromStandby { .. }, Event::ObserveFailed) => (Init, vec![Command::ArmTimer(now)]),
        (s @ ObservingFromStandby { .. }, _) => (s, vec![]),

        // AcquiringLease
        (AcquiringLease { deadline, .. }, Event::Now(t)) if t >= deadline => {
            (Init, vec![Command::ArmTimer(t)])
        }
        (s @ AcquiringLease { .. }, Event::Now(_)) => (s, vec![]),
        (AcquiringLease { .. }, Event::AcquireSucceeded { term, expiry }) => {
            (Promoting { term, expiry }, vec![Command::Promote { term }])
        }
        (AcquiringLease { .. }, Event::AcquireFailed) => (Init, vec![Command::ArmTimer(now)]),
        (s @ AcquiringLease { .. }, _) => (s, vec![]),

        // Promoting
        (Promoting { expiry, .. }, Event::Now(t)) if t >= expiry => {
            (Demoting, vec![Command::StopPg])
        }
        (s @ Promoting { .. }, Event::Now(_)) => (s, vec![]),
        (Promoting { term, expiry }, Event::PromoteSucceeded { term: t }) if t == term => (
            Leader { term, expiry },
            vec![Command::ArmTimer(renew_deadline(
                expiry,
                todo!("cfg.renew_margin"),
            ))],
        ),
        (Promoting { term, .. }, Event::PromoteFailed { term: t }) if t == term => {
            (Demoting, vec![Command::StopPg])
        }
        (s @ Promoting { .. }, _) => (s, vec![]),

        // Leader
        (Leader { expiry, .. }, Event::Now(t)) if t >= expiry => (Demoting, vec![Command::StopPg]),
        (Leader { term, expiry }, Event::Now(t)) if t + todo!("cfg.renew_margin") >= expiry => (
            Renewing {
                term,
                expiry,
                deadline: expiry,
            },
            vec![
                Command::RenewLease {
                    term,
                    ttl: todo!("config.lease_ttl"),
                },
                Command::ArmTimer(expiry),
            ],
        ),
        (Leader { term, expiry }, Event::Now(_)) => (
            Leader { term, expiry },
            vec![Command::ArmTimer(renew_deadline(
                expiry,
                todo!("cfg.renew_margin"),
            ))],
        ),
        (s @ Leader { .. }, _) => (s, vec![]),

        // Renewing
        (Renewing { deadline, .. }, Event::Now(t)) if t >= deadline => {
            (Demoting, vec![Command::StopPg])
        }

        (s @ Renewing { .. }, Event::Now(_)) => (s, vec![]),

        (Renewing { term, .. }, Event::RenewSucceeded { term: t, expiry }) if t == term => (
            Leader { term, expiry },
            vec![Command::ArmTimer(renew_deadline(
                expiry,
                todo!("cfg.renew_margin"),
            ))],
        ),

        (Renewing { term, .. }, Event::RenewFailed { term: t }) if t == term => {
            (Demoting, vec![Command::StopPg])
        }
        (s @ Renewing { .. }, _) => (s, vec![]),

        // StartingStandby
        (StartingStandby { leader }, Event::StandbyStarted) => (
            Standby { leader },
            vec![Command::ArmTimer(now + todo!("cfg.timeout"))],
        ),

        (StartingStandby { .. }, Event::StandbyStartFailed) => (Init, vec![Command::ArmTimer(now)]),

        (s @ StartingStandby { .. }, _) => (s, vec![]),

        // Standby
        (Standby { leader }, Event::Now(_)) if todo!("leader.0 == cfg.id.0") => {
            (Demoting, vec![Command::StopPg])
        }

        (Standby { leader }, Event::Now(_)) => {
            (ObservingFromStandby { leader }, vec![Command::ObserveLease])
        }

        (s @ Standby { .. }, _) => (s, vec![]),

        // Demoting
        (Demoting, Event::Stopped) => (ReleasingLease, vec![Command::ReleaseLease]),
        (Demoting, Event::StopFailed) => (Stuck, vec![]),
        (s @ Demoting, _) => (s, vec![]),

        // ReleasingLease
        (ReleasingLease, Event::Released) => (Init, vec![Command::ArmTimer(now)]),
        (ReleasingLease, Event::ReleaseFailed) => (Stuck, vec![]),
        (s @ ReleasingLease, _) => (s, vec![]),

        // Stuck
        (s @ Stuck, _) => (s, vec![]),
        _ => todo!(),
    }
}

pub fn renew_deadline(expiry: Instant, renew_margin: Duration) -> Instant {
    expiry.checked_sub(renew_margin).unwrap_or(expiry)
}
