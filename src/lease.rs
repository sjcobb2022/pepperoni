use std::time::{Duration, Instant};

pub trait LeaseClient {
    fn observe(&mut self) -> Result<Option<(Term, Instant)>, ()>;
    fn try_acquire(&mut self, ttl: Duration) -> Result<Option<(Term, Instant)>, ()>;
    fn renew(&mut self, ttl: Duration) -> Result<Option<Instant>, ()>;
    fn release(&mut self) -> Result<(), ()>;
}
