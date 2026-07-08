use thiserror::Error;

#[derive(Debug, Error)]
pub enum PgError {
    #[error("postgres command failed: {0}")]
    Command(String),
}

pub trait PgCtl {
    async fn stop(&mut self) -> Result<(), PgError>;
    async fn promote(&mut self) -> Result<(), PgError>;
    async fn start_standby(&mut self) -> Result<(), PgError>;
}
