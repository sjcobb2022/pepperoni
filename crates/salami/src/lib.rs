#![no_std]

//! salami is a sans-io, no_std, no alloc crate for implementing opinionated postgres failover. It
//! relies on the consumer to ensure their failover operates are correct, not the logic.
//!
//! as already mentioned, this library is extremely opinionated, and restricted to the observed
//! behaviours of postgres and various failover alternatives (such as patroni).
//!
//! salami is meant to be (relatively) simple, and made for human consumption.
//!
//! It was originally written using a typestate pattern, but was migrated to a set of enums as the
//! sans-io pattern can become quite unruly.

use core::time::Duration;

/// A unique identifier for every node.
///
/// Uniqueness is not validated by salami, it is the responsibility of user to provide a
/// unique id for each node.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct NodeId(u64);

impl NodeId {
    pub fn new(input: u64) -> Self {
        NodeId(input)
    }
}

/// A counter type for the current term.
pub type Term = u64;

pub struct Config {
    /// The current node's id. Static and set by the user.
    pub id: NodeId,
    /// How often standby re-checks who the leader is.
    pub timeout: Duration,
    /// How long a lease/promotion lasts once acquired.
    pub lease_ttl: Duration,
    /// How early before expiry we start trying to renew. A renewal will happen at approximately `lease_ttl - renew_margin`
    pub renew_margin: Duration,
}

/// The core states at which we may reside. A user may inspect the state at any time in order to
/// take some further action.
pub enum State {
    /// The initial state, currently no known leader.
    Init,
    /// Attempting to observe the current leader.
    ObservingFromInit,
    /// In standby, but observing the leader in case an election has occurred.
    ObservingFromStandby { leader: NodeId },
    /// In the process of acquiring a lease
    AcquiringLease,
    /// In the process of promoting our current leader.
    Promoting { term: Term, ttl: Duration },
    /// The instance is currently the leader.
    Leader { term: Term },
    /// The lease is being renewed.
    Renewing { term: Term },
    /// The database instance is being started in standby mode.
    StartingStandby { leader: NodeId },
    /// The instance is in standby mode.
    Standby { leader: NodeId },
    /// The database instance is being demoted.
    Demoting,
    /// The lease is being released.
    ReleasingLease,
    /// We are stuck, we will let something external (a watchdog perhaps) kill our process.
    Stuck,
}

/// The events that should be handled by the user. All events should be handled for a functional
/// state machine.
pub enum Event {
    /// Timeout triggered, re-evaluate state.
    Timeout,

    /// A leader was observed
    ObservedLeader(NodeId),
    /// A leader was not observed, time to elect.
    ObservedNoLeader,
    /// Failed to observe the leader, try and elect again.
    ObserveFailed,

    /// Lease was acquired successfully.
    AcquireSucceeded { term: Term, ttl: Duration },
    /// Lease was not acquired, stop current instance.
    AcquireFailed,

    /// Database promotion succeeded.
    PromoteSucceeded { term: Term },
    /// Failed to promote the database instance.
    PromoteFailed { term: Term },

    /// Lease was renewed successfully.
    RenewSucceeded { term: Term, ttl: Duration },
    /// Lease did not renew.
    RenewFailed { term: Term },

    /// The database was not started successfully.
    StandbyStarted,
    /// The database failed to start in standby mode.
    StandbyStartFailed,

    /// Database stopped
    Stopped,
    /// Database failed to stop.
    StopFailed,

    /// Lease successfully released.
    Released,
    /// Lease was not released.
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

/// A hardcoded structure of items. It is limited to two as there is at most 2 actions that can be
/// followed at any one time. That being the immediate command evaluation, and a corresponding
/// timeout for that action.
#[derive(Clone, Copy)]
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
        (ObservingFromInit, Event::ObservedNoLeader) => (
            AcquiringLease,
            Commands::two(
                Command::TryAcquireLease { ttl: cfg.lease_ttl },
                Command::WakeAfter(cfg.lease_ttl),
            ),
        ),
        (ObservingFromInit, Event::ObserveFailed) => {
            (Init, Commands::one(Command::WakeAfter(Duration::ZERO))) // TODO: re-evaluate this
                                                                      // timeout duration
        }

        (s @ ObservingFromInit, _) => (s, Commands::none()),

        // ObservingFromStandby
        // If we observe a leader, but we are a leader, then we should demote because that is not ideal.
        (ObservingFromStandby { .. }, Event::ObservedLeader(id)) if id == cfg.id => {
            (Demoting, Commands::one(Command::StopPg))
        }
        // If we observe a leader and someone else is the leader then move back to standby.
        (ObservingFromStandby { .. }, Event::ObservedLeader(leader)) => (
            Standby { leader },
            Commands::one(Command::WakeAfter(cfg.timeout)),
        ),

        // If there is no leader, then we should try and become the leader ourselves.
        // We can be relatively certain that all other nodes are doing the same.
        (ObservingFromStandby { .. }, Event::ObservedNoLeader) => (
            AcquiringLease,
            Commands::two(
                Command::TryAcquireLease { ttl: cfg.lease_ttl },
                Command::WakeAfter(cfg.lease_ttl),
            ),
        ),
        // We should try and init again if we fail.
        (ObservingFromStandby { .. }, Event::ObserveFailed) => {
            (Init, Commands::one(Command::WakeAfter(Duration::ZERO))) // TODO: re-evaluate this
                                                                      // timeout duration.
        }
        (s @ ObservingFromStandby { .. }, _) => (s, Commands::none()),

        // AcquiringLease
        // If we receive a timeout because we have taken too long, then fail back to the initial state.
        (AcquiringLease, Event::Timeout) => {
            (Init, Commands::one(Command::WakeAfter(Duration::ZERO))) // TODO: re-evaluate this
                                                                      // timeout duration.
        }
        // If we have acquired the lease, then we should be promoting our local instance.
        (AcquiringLease, Event::AcquireSucceeded { term, ttl }) => (
            Promoting { term, ttl },
            Commands::two(Command::Promote { term }, Command::WakeAfter(ttl)),
        ),
        // If we fail to acquire the lease, then go back to initial state.
        (AcquiringLease, Event::AcquireFailed) => {
            (Init, Commands::one(Command::WakeAfter(Duration::ZERO))) // TODO: re-evaliate this
                                                                      // timeout duration.
        }
        (s @ AcquiringLease, _) => (s, Commands::none()),

        // Promoting
        // If we receive a timeout within the length of the lease, and we have not succeeded yet
        // then we should try and demote from our current state.
        (Promoting { .. }, Event::Timeout) => (Demoting, Commands::one(Command::StopPg)),
        // If we succeeded in our promotion, then we should become a leader, and set a timeout to
        // trigger the renew cycle.
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
