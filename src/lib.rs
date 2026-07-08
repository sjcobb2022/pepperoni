use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub struct Config {
    pub lease_ttl: Duration,
    pub renew_margin: Duration,
}

pub trait PgCtl {
    async fn stop(&mut self) -> Result<(), ()>;
    async fn promote(&mut self) -> Result<(), ()>;
    async fn start_standby(&mut self) -> Result<(), ()>;
}

pub trait LeaseClient {
    async fn observe(&mut self) -> Result<Option<(Term, Instant)>, String>;
    async fn try_acquire(&mut self, ttl: Duration) -> Result<Option<(Term, Instant)>, String>;
    async fn renew(&mut self, ttl: Duration) -> Result<Option<Instant>, String>;
    async fn release(&mut self) -> Result<(), String>;
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
