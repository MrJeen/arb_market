use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Msg(String),
    #[error("config: {0}")]
    Config(String),
    #[error("platform disabled: {0}")]
    PlatformDisabled(String),
    #[error("stale orderbook: {0}")]
    StaleBook(String),
    #[error("no opportunity")]
    NoOpportunity,
    #[error("http {status}: {message}")]
    Http { status: u16, message: String },
    #[error("order rejected ({code}): {message}")]
    Rejected { code: String, message: String },
    #[error("order unconfirmed ({status}): {message}")]
    Unconfirmed { status: String, message: String },
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Decimal(#[from] rust_decimal::Error),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Error {
    pub fn msg(value: impl std::fmt::Display) -> Self {
        Self::Msg(value.to_string())
    }
}
