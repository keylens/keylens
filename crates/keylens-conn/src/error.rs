use thiserror::Error;

use crate::url::redact_url;

#[derive(Debug, Error)]
pub enum ConnError {
    #[error("invalid connection url: {0}")]
    Url(String),

    #[error("connection failed: {0}")]
    Connect(#[source] fred::error::Error),

    /// A plaintext connection to a TLS-only port.
    ///
    /// Worth its own variant because the raw failure is `Protocol Error: Expected string`,
    /// which tells the user nothing. The server answered a TLS handshake and the client
    /// tried to parse those bytes as a RESP reply.
    #[error(
        "connection failed: the server did not speak RESP.\n\n\
         This usually means the port requires TLS and the url does not use it.\n\
         Try `rediss://` instead of `redis://`:\n\n    {suggestion}\n\n\
         Managed hosts — DigitalOcean, Upstash, Aiven, ElastiCache with encryption \
         in transit — are TLS-only.\n\nunderlying error: {source}"
    )]
    LikelyNeedsTls {
        suggestion: String,
        #[source]
        source: fred::error::Error,
    },

    /// The handshake never completed.
    ///
    /// Its own variant because the client retries internally: without a deadline this is
    /// a hang, not an error, and a frozen terminal is worse than any message.
    #[error("{0}")]
    ConnectTimeout(String),

    #[error("command `{cmd}` failed: {source}")]
    Command {
        cmd: &'static str,
        #[source]
        source: fred::error::Error,
    },

    #[error("unexpected reply shape for `{cmd}`: {detail}")]
    Reply { cmd: &'static str, detail: String },

    #[error("command `{0}` is not allowed through the read-only connection API")]
    UnsafeCommand(&'static str),
}

/// Classify a connection failure, upgrading the unhelpful protocol error into a TLS hint.
///
/// Only fires for plaintext `redis://` urls: if the user already asked for TLS, a protocol
/// error means something else and guessing would send them down the wrong path.
pub fn classify_connect(url: &str, source: fred::error::Error) -> ConnError {
    if looks_like_plaintext_to_tls(url, &source) {
        return ConnError::LikelyNeedsTls {
            suggestion: redact_url(&to_tls_url(url)),
            source,
        };
    }
    ConnError::Connect(source)
}

/// Build the message for a handshake that ran out of time.
///
/// A TLS-only port accepts the TCP connection and then closes it, so a plaintext client
/// reconnects in a loop and never reports anything. That silence is the single most
/// likely reason to end up here, so the TLS hint is offered for plaintext urls.
pub fn connect_timeout(url: &str, after: std::time::Duration) -> ConnError {
    let mut msg = format!(
        "connection failed: the handshake did not finish within {}s.",
        after.as_secs()
    );

    if is_plaintext_scheme(url) {
        // The most likely cause by a wide margin, and the one with a one-character fix.
        msg.push_str(&format!(
            "\n\nThe usual cause is a TLS-only port reached over plaintext — it accepts \
             the connection, then closes it.\nTry `rediss://`:\n\n    {}\n\n\
             Managed hosts — DigitalOcean, Upstash, Aiven, ElastiCache with encryption \
             in transit — are TLS-only.",
            redact_url(&to_tls_url(url))
        ));
    } else {
        // Over TLS the cause is genuinely ambiguous, so list what to check rather than
        // asserting something we cannot know from a timeout alone.
        msg.push_str(
            "\n\nThe scheme is already TLS, so check, in order:\n\n  \
             1. Your IP is allowed. Managed hosts block by default — DigitalOcean calls \
             this\n     \"Trusted Sources\", AWS uses security groups.\n  \
             2. The password is url-encoded. A `@`, `/`, `:` or `#` in the password will \
             corrupt\n     the url unless percent-encoded.\n  \
             3. The host and port are right, and the server is reachable:\n     \
             `redis-cli --tls -u '<your-url>' PING`\n\n\
             For the full handshake trace:\n  \
             KEYLENS_LOG=debug KEYLENS_LOG_FILE=/tmp/keylens.log keylens --url ... probe",
        );
    }

    ConnError::ConnectTimeout(msg)
}

fn is_plaintext_scheme(url: &str) -> bool {
    url.starts_with("redis://")
        || url.starts_with("redis-sentinel://")
        || url.starts_with("redis-cluster://")
}

fn looks_like_plaintext_to_tls(url: &str, source: &fred::error::Error) -> bool {
    if !is_plaintext_scheme(url) {
        return false;
    }

    // A TLS server's handshake bytes are not valid RESP, so the client reports a parse or
    // protocol failure rather than a refused connection.
    let msg = source.details().to_string().to_ascii_lowercase();
    matches!(
        source.kind(),
        fred::error::ErrorKind::Protocol | fred::error::ErrorKind::Parse
    ) || msg.contains("expected string")
        || msg.contains("protocol error")
}

/// Rewrite a url to its TLS scheme, so the suggestion can be pasted directly.
fn to_tls_url(url: &str) -> String {
    for (plain, tls) in [
        ("redis://", "rediss://"),
        ("redis-sentinel://", "rediss-sentinel://"),
        ("redis-cluster://", "rediss-cluster://"),
    ] {
        if let Some(rest) = url.strip_prefix(plain) {
            return format!("{tls}{rest}");
        }
    }
    url.to_string()
}

pub type Result<T> = std::result::Result<T, ConnError>;

#[cfg(test)]
mod tests {
    use super::*;
    use fred::error::{Error, ErrorKind};

    fn protocol_err() -> Error {
        Error::new(ErrorKind::Protocol, "Expected string.")
    }

    #[test]
    fn suggests_tls_for_a_plaintext_url_that_got_garbage_back() {
        // The real report: DigitalOcean managed Valkey on a TLS-only port, reached with
        // `redis://`. The raw error was "Protocol Error: Expected string."
        let url = "redis://default:pw@db-do-user-1-0.g.db.ondigitalocean.com:25061";
        let err = classify_connect(url, protocol_err());

        let msg = err.to_string();
        assert!(msg.contains("rediss://default:***@db-do-user-1-0.g.db.ondigitalocean.com:25061"));
        assert!(!msg.contains(":pw@"));
        assert!(msg.contains("TLS"));
    }

    #[test]
    fn does_not_suggest_tls_when_the_url_already_uses_it() {
        // A protocol error over TLS means something else; sending the user to `rediss://`
        // would be actively misleading.
        let err = classify_connect("rediss://host:6379", protocol_err());
        assert!(matches!(err, ConnError::Connect(_)));
    }

    #[test]
    fn ordinary_connection_failures_are_left_alone() {
        // Connection refused is self-explanatory and has nothing to do with TLS.
        let refused = Error::new(ErrorKind::IO, "Connection refused (os error 61)");
        assert!(matches!(
            classify_connect("redis://127.0.0.1:6379", refused),
            ConnError::Connect(_)
        ));
    }

    #[test]
    fn a_handshake_timeout_on_a_plaintext_url_suggests_tls() {
        // A TLS-only port accepts the TCP connection then closes it, so the client
        // reconnects forever. Silence is the symptom; TLS is the usual cause.
        let err = connect_timeout("redis://host:6390", std::time::Duration::from_secs(10));
        let msg = err.to_string();
        assert!(msg.contains("did not finish within 10s"));
        assert!(msg.contains("rediss://host:6390"));
    }

    #[test]
    fn a_handshake_timeout_over_tls_does_not_blame_tls() {
        let err = connect_timeout("rediss://host:6390", std::time::Duration::from_secs(10));
        let msg = err.to_string();
        assert!(!msg.contains("Try `rediss://`"));
        // Over TLS a timeout is ambiguous, so it must point at the real suspects rather
        // than claim to know what happened.
        assert!(
            msg.contains("Trusted Sources"),
            "should mention IP allowlisting"
        );
        assert!(
            msg.contains("url-encoded"),
            "should mention password encoding"
        );
        assert!(msg.contains("KEYLENS_LOG"), "should say how to get a trace");
    }

    #[test]
    fn rewrites_every_plaintext_scheme() {
        assert_eq!(to_tls_url("redis://h:1"), "rediss://h:1");
        assert_eq!(to_tls_url("redis-sentinel://h:1"), "rediss-sentinel://h:1");
        assert_eq!(to_tls_url("redis-cluster://h:1"), "rediss-cluster://h:1");
        // Already TLS, or something unrecognised: leave it be.
        assert_eq!(to_tls_url("rediss://h:1"), "rediss://h:1");
    }
}
