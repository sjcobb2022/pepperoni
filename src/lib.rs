use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub struct Config {
    pub lease_ttl: Duration,
    pub renew_margin: Duration,
}
