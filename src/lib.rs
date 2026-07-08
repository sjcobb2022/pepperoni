use std::time::{Duration, Instant};

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
}

pub struct Promoting<L: LeaseClient, P: PgCtl> {
    ctx: Ctx<L, P>,
}

pub struct Leader<L: LeaseClient, P: PgCtl> {
    ctx: Ctx<L, P>,
}

pub struct Standby<L: LeaseClient, P: PgCtl> {
    ctx: Ctx<L, P>,
}

pub struct Demoting<L: LeaseClient, P: PgCtl> {
    ctx: Ctx<L, P>,
}
