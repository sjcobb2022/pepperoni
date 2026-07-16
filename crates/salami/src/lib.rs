#![no_std]

use core::time::Duration;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct NodeId(u64);

impl NodeId {
    pub fn from_u64(input: u64) -> Self {
        NodeId(input)
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

#[derive(Clone, Copy)]
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

#[derive(Clone, Copy)]
/// A hardcoded structure of items. It is limited to two as there is at most 2 actions that can be
/// followed at any one time. That being the immediate command evaluation, and a corresponding
/// timeout for that action.
pub struct Commands {
    items: [Option<Command>; 2],
}

impl Commands {
    /// Execute no commands.
    fn none() -> Self {
        Commands {
            items: [None, None],
        }
    }
    /// Execute one command.
    fn one(a: Command) -> Self {
        Commands {
            items: [Some(a), None],
        }
    }
    /// Execute 2 commands.
    fn two(a: Command, b: Command) -> Self {
        Commands {
            items: [Some(a), Some(b)],
        }
    }
}

impl IntoIterator for Commands {
    type Item = Command;
    type IntoIter = core::iter::Flatten<core::array::IntoIter<Option<Command>, 2>>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter().flatten()
    }
}

/// The main step function which contains the core logic for the library.
pub fn step(state: State, event: Event, cfg: &Config) -> (State, Commands) {
    use State::*;

    match (state, event) {
        // Init
        // If we get triggered immediately, we should attempt to observe the current lease
        (Init, Event::Timeout) => (ObservingFromInit, Commands::one(Command::ObserveLease)),
        // otherwise, we do nothing.
        (s @ Init, _) => (s, Commands::none()),

        // ObservingFromInit
        // If we are observing, and we are the leader, then we should stop since that is a failure
        (ObservingFromInit, Event::ObservedLeader(id)) if id == cfg.id => {
            (Demoting, Commands::one(Command::StopPg))
        }
        // otherwise, we move into standby with the leader.
        (ObservingFromInit, Event::ObservedLeader(id)) => (
            StartingStandby { leader: id },
            Commands::one(Command::StartStandby),
        ),
        // If there is no leader, or our observation failed, we should try and acquire a lease.
        // If we do not have a connection to etcd or the etcd cluster as a whole, then then try
        // acquire will most likely fail.
        (ObservingFromInit, Event::ObservedNoLeader)
        | (ObservingFromInit, Event::ObserveFailed) => (
            AcquiringLease,
            Commands::two(
                Command::TryAcquireLease { ttl: cfg.lease_ttl },
                Command::WakeAfter(cfg.lease_ttl),
            ),
        ),
        (s @ ObservingFromInit, _) => (s, Commands::none()),

        // ObservingFromStandby
        (ObservingFromStandby { .. }, Event::ObservedLeader(leader)) => (
            Standby { leader },
            Commands::one(Command::WakeAfter(cfg.timeout)),
        ),

        (ObservingFromStandby { .. }, Event::ObservedNoLeader) => (
            AcquiringLease,
            Commands::two(
                Command::TryAcquireLease { ttl: cfg.lease_ttl },
                Command::WakeAfter(cfg.lease_ttl),
            ),
        ),
        (ObservingFromStandby { .. }, Event::ObserveFailed) => {
            (Init, Commands::one(Command::WakeAfter(Duration::ZERO)))
        }
        (s @ ObservingFromStandby { .. }, _) => (s, Commands::none()),

        // AcquiringLease
        (AcquiringLease, Event::Timeout) => {
            (Init, Commands::one(Command::WakeAfter(Duration::ZERO)))
        }
        (AcquiringLease, Event::AcquireSucceeded { term, ttl }) => (
            Promoting { term, ttl },
            Commands::two(Command::Promote { term }, Command::WakeAfter(ttl)),
        ),
        (AcquiringLease, Event::AcquireFailed) => {
            (Init, Commands::one(Command::WakeAfter(Duration::ZERO)))
        }
        (s @ AcquiringLease, _) => (s, Commands::none()),

        // Promoting
        (Promoting { .. }, Event::Timeout) => (Demoting, Commands::one(Command::StopPg)),
        (Promoting { term, ttl }, Event::PromoteSucceeded { term: t }) if t == term => (
            Leader { term },
            Commands::one(Command::WakeAfter(ttl.saturating_sub(cfg.renew_margin))),
        ),
        (Promoting { term, .. }, Event::PromoteFailed { term: t }) if t == term => {
            (Demoting, Commands::one(Command::StopPg))
        }
        (s @ Promoting { .. }, _) => (s, Commands::none()),

        // Leader
        (Leader { term }, Event::Timeout) => (
            Renewing { term },
            Commands::two(
                Command::RenewLease {
                    term,
                    ttl: cfg.lease_ttl,
                },
                Command::WakeAfter(cfg.renew_margin),
            ),
        ),
        (s @ Leader { .. }, _) => (s, Commands::none()),

        // Renewing
        (Renewing { .. }, Event::Timeout) => (Demoting, Commands::one(Command::StopPg)),
        (Renewing { term }, Event::RenewSucceeded { term: t, ttl }) if t == term => (
            Leader { term },
            Commands::one(Command::WakeAfter(ttl.saturating_sub(cfg.renew_margin))),
        ),
        (Renewing { term, .. }, Event::RenewFailed { term: t }) if t == term => {
            (Demoting, Commands::one(Command::StopPg))
        }
        (s @ Renewing { .. }, _) => (s, Commands::none()),

        // StartingStandby
        (StartingStandby { leader }, Event::StandbyStarted) => (
            Standby { leader },
            Commands::one(Command::WakeAfter(cfg.timeout)),
        ),
        (StartingStandby { .. }, Event::StandbyStartFailed) => {
            (Init, Commands::one(Command::WakeAfter(Duration::ZERO)))
        }
        (s @ StartingStandby { .. }, _) => (s, Commands::none()),

        // Standby
        (Standby { leader }, Event::Timeout) if leader == cfg.id => {
            (Demoting, Commands::one(Command::StopPg))
        }
        (Standby { leader }, Event::Timeout) => (
            ObservingFromStandby { leader },
            Commands::one(Command::ObserveLease),
        ),
        (s @ Standby { .. }, _) => (s, Commands::none()),

        // Demoting
        (Demoting, Event::Stopped) => (ReleasingLease, Commands::one(Command::ReleaseLease)),
        (Demoting, Event::StopFailed) => (Stuck, Commands::none()),
        (s @ Demoting, _) => (s, Commands::none()),

        // ReleasingLease
        (ReleasingLease, Event::Released) => {
            (Init, Commands::one(Command::WakeAfter(Duration::ZERO)))
        }
        (ReleasingLease, Event::ReleaseFailed) => (Stuck, Commands::none()),
        (s @ ReleasingLease, _) => (s, Commands::none()),

        // Stuck
        (s @ Stuck, _) => (s, Commands::none()),
    }
}
