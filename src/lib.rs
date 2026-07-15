// #![no_std]

use core::time::Duration;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct NodeId(u64);

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

pub enum State {
    Init,
    ObservingFromInit,
    ObservingFromStandby { leader: NodeId },
    AcquiringLease,
    Promoting { term: Term, ttl: Duration },
    Leader { term: Term },
    Renewing { term: Term },
    StartingStandby { leader: NodeId },
    Standby { leader: NodeId },
    Demoting,
    ReleasingLease,
    Stuck,
}

pub enum Event {
    /// Timeout triggered, re-evaluate state.
    Timeout,

    ObservedLeader(NodeId),
    ObservedNoLeader,
    ObserveFailed,

    AcquireSucceeded {
        term: Term,
        ttl: Duration,
    },
    AcquireFailed,

    PromoteSucceeded {
        term: Term,
    },
    PromoteFailed {
        term: Term,
    },

    RenewSucceeded {
        term: Term,
        ttl: Duration,
    },
    RenewFailed {
        term: Term,
    },

    StandbyStarted,
    StandbyStartFailed,
    Stopped,
    StopFailed,
    Released,
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
    WakeAfter(Duration),
}

pub fn step(state: State, event: Event, cfg: &Config) -> (State, Vec<Command>) {
    use State::*;

    match (state, event) {
        // Init
        // If we get triggered immediately, we should attempt to observe the current lease
        (Init, Event::Timeout) => (ObservingFromInit, vec![Command::ObserveLease]),
        // otherwise, we do nothing.
        (s @ Init, _) => (s, vec![]),

        // ObservingFromInit
        // If we are observing, and we are the leader, then we should stop since that is a failure
        (ObservingFromInit, Event::ObservedLeader(id)) if id == cfg.id => {
            (Demoting, vec![Command::StopPg])
        }
        // otherwise, we move into standby with the leader.
        (ObservingFromInit, Event::ObservedLeader(id)) => {
            (StartingStandby { leader: id }, vec![Command::StartStandby])
        }
        // If there is no leader, or our observation failed, we should try and acquire a lease.
        // If we do not have a connection to etcd or the etcd cluster as a whole, then then try
        // acquire will most likely fail.
        (ObservingFromInit, Event::ObservedNoLeader)
        | (ObservingFromInit, Event::ObserveFailed) => (
            AcquiringLease,
            vec![
                Command::TryAcquireLease { ttl: cfg.lease_ttl },
                Command::WakeAfter(cfg.lease_ttl), // if we have not acquired the lease by this time
                                                   // then we will exit out of the acquiring stage
            ],
        ),
        (s @ ObservingFromInit, _) => (s, vec![]),

        // ObservingFromStandby
        (ObservingFromStandby { .. }, Event::ObservedLeader(leader)) => {
            (
                Standby { leader },
                vec![
                    Command::WakeAfter(cfg.timeout), // we should wait until the timeout.
                ],
            )
        }

        (ObservingFromStandby { .. }, Event::ObservedNoLeader) => (
            AcquiringLease,
            vec![
                Command::TryAcquireLease { ttl: cfg.lease_ttl },
                Command::WakeAfter(cfg.lease_ttl), // TODO: Change this
            ],
        ),
        (ObservingFromStandby { .. }, Event::ObserveFailed) => {
            (Init, vec![Command::WakeAfter(Duration::ZERO)])
        }
        (s @ ObservingFromStandby { .. }, _) => (s, vec![]),

        // AcquiringLease
        (AcquiringLease, Event::Timeout) => (Init, vec![Command::WakeAfter(Duration::ZERO)]),
        (AcquiringLease, Event::AcquireSucceeded { term, ttl }) => (
            Promoting { term, ttl },
            vec![Command::Promote { term }, Command::WakeAfter(ttl)],
        ),
        (AcquiringLease, Event::AcquireFailed) => (
            Init,
            vec![
                Command::WakeAfter(Duration::ZERO), // TODO: maybe backoff as to not throttle massively?
            ],
        ),
        (s @ AcquiringLease, _) => (s, vec![]),

        // Promoting
        (Promoting { .. }, Event::Timeout) => (Demoting, vec![Command::StopPg]),
        (Promoting { term, ttl }, Event::PromoteSucceeded { term: t }) if t == term => (
            Leader { term },
            vec![Command::WakeAfter(ttl.saturating_sub(cfg.renew_margin))],
        ),
        (Promoting { term, .. }, Event::PromoteFailed { term: t }) if t == term => {
            (Demoting, vec![Command::StopPg])
        }
        (s @ Promoting { .. }, _) => (s, vec![]),

        // Leader
        (Leader { term }, Event::Timeout) => (
            Renewing { term },
            vec![
                Command::RenewLease {
                    term,
                    ttl: cfg.lease_ttl,
                },
                Command::WakeAfter(cfg.renew_margin),
            ],
        ),
        (s @ Leader { .. }, _) => (s, vec![]),

        // Renewing
        (Renewing { .. }, Event::Timeout) => (Demoting, vec![Command::StopPg]),
        (Renewing { term }, Event::RenewSucceeded { term: t, ttl }) if t == term => (
            Leader { term },
            vec![Command::WakeAfter(ttl.saturating_sub(cfg.renew_margin))],
        ),
        (Renewing { term, .. }, Event::RenewFailed { term: t }) if t == term => {
            (Demoting, vec![Command::StopPg])
        }
        (s @ Renewing { .. }, _) => (s, vec![]),

        // StartingStandby
        (StartingStandby { leader }, Event::StandbyStarted) => {
            (Standby { leader }, vec![Command::WakeAfter(cfg.timeout)])
        }
        (StartingStandby { .. }, Event::StandbyStartFailed) => {
            (Init, vec![Command::WakeAfter(Duration::ZERO)])
        }
        (s @ StartingStandby { .. }, _) => (s, vec![]),

        // Standby
        (Standby { leader }, Event::Timeout) if leader == cfg.id => {
            (Demoting, vec![Command::StopPg])
        }
        (Standby { leader }, Event::Timeout) => {
            (ObservingFromStandby { leader }, vec![Command::ObserveLease])
        }
        (s @ Standby { .. }, _) => (s, vec![]),

        // Demoting
        (Demoting, Event::Stopped) => (ReleasingLease, vec![Command::ReleaseLease]),
        (Demoting, Event::StopFailed) => (Stuck, vec![]),
        (s @ Demoting, _) => (s, vec![]),

        // ReleasingLease
        (ReleasingLease, Event::Released) => (Init, vec![Command::WakeAfter(Duration::ZERO)]),
        (ReleasingLease, Event::ReleaseFailed) => (Stuck, vec![]),
        (s @ ReleasingLease, _) => (s, vec![]),

        // Stuck
        (s @ Stuck, _) => (s, vec![]),
    }
}
