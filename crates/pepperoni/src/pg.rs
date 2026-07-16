use thiserror::Error;

#[derive(Debug, Error)]
pub enum PgError {
    #[error("postgres command failed: {0}")]
    Command(String),
}

// TODO: Make these sync instead? Realistically I think that we want a fully sync connection with
// postgres so that we can hard fail if any of our command fail. This means that we do not need to
// wait on postgres if it fails. However handling postgres promotion for example can be done via
// sqlx or a regular sql query, which is done asynchronously. Keep things async for the sake of a
// clean api?
pub trait PgCtl {
    async fn stop(&mut self) -> Result<(), PgError>;
    async fn promote(&mut self) -> Result<(), PgError>;
    async fn start_standby(&mut self) -> Result<(), PgError>;
}
