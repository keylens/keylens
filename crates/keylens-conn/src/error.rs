use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConnError {
    #[error("invalid connection url: {0}")]
    Url(String),

    #[error("connection failed: {0}")]
    Connect(#[source] fred::error::Error),

    #[error("command `{cmd}` failed: {source}")]
    Command {
        cmd: &'static str,
        #[source]
        source: fred::error::Error,
    },

    #[error("unexpected reply shape for `{cmd}`: {detail}")]
    Reply { cmd: &'static str, detail: String },
}

pub type Result<T> = std::result::Result<T, ConnError>;
