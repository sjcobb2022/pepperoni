use std::{
    convert::Infallible,
    time::{Duration, Instant},
};

use crate::lease::{
    AcquireOutcome, LeaseClient, LeaseError, LeaseObservation, NodeId, RenewOutcome, Term,
};
use crate::pg::PgCtl;

enum State {
    Init,
    Electing { since: Instant },
    Promoting { term: Term },
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
    fn tick<L: LeaseClient, P: PgCtl>(ctx: Ctx<L, P>, now: Instant) {}
}
