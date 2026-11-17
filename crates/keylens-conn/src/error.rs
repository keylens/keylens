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

    /// A requested page cannot be expressed as a non-negative Redis range.
    ///
    /// Its own variant because the alternative is worse than an error: Redis reads a
    /// negative bound as an offset from the end, so an offset that wrapped on the way to
    /// `i64` would quietly become a read of the entire collection — the one thing this
    /// crate promises never to issue.
    #[error("cannot page `{cmd}` from offset {offset}: {detail}")]
    Range {
        cmd: &'static str,
        offset: usize,
        detail: &'static str,
    },

    #[error("command `{0}` is not allowed through the read-only connection API")]
    UnsafeCommand(&'static str),

    /// The server rejected our credentials on a reconnect. Terminal: keylens has stopped
    /// reconnecting rather than retry a password the server has already refused.
    #[error(
        "the server rejected keylens' credentials when reconnecting ({0}); stopped \
         reconnecting. Was the password or ACL user changed? Restart keylens with valid \
         credentials."
    )]
    AuthLost(String),

    /// A read-only command whose arguments ask for an unbounded or oversized read --
    /// `LRANGE k 0 -1`, `XRANGE` without `COUNT`, `SLOWLOG GET -1`. See [`crate::bounds`].
    #[error("refusing `{cmd}`: {detail}")]
    Unbounded { cmd: &'static str, detail: String },
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

/// What a long-running reader should do after a failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recovery {
    /// Likely to clear on its own: a dropped connection, a timeout, a server restarting
    /// or loading its dataset. Back off and try again.
    Retry,
    /// Will fail the same way every time: an ACL denial, a wrong password, an unknown
    /// command, a malformed request. Retrying spends a command per attempt for nothing,
    /// so stop and say why.
    Stop(String),
}

impl ConnError {
    /// Classify this failure for a retry loop.
    ///
    /// Unknown failures retry. A reader that backs off to a ceiling costs one command per
    /// interval while it is wrong; one that stops on a transient error stays dead for the
    /// rest of the session, which is the failure this exists to remove.
    pub fn recovery(&self) -> Recovery {
        let first_line = |s: &str| s.lines().next().unwrap_or(s).trim().to_string();
        match self {
            ConnError::Command { source, .. } | ConnError::Connect(source) => {
                if *source.kind() == fred::error::ErrorKind::Auth {
                    return Recovery::Stop(first_line(source.details()));
                }
                let details = source.details();
                let code = details.split_whitespace().next().unwrap_or_default();
                match code {
                    "NOPERM" | "NOAUTH" | "WRONGPASS" | "WRONGTYPE" => {
                        Recovery::Stop(first_line(details))
                    }
                    "ERR" if details.to_ascii_lowercase().contains("unknown command") => {
                        Recovery::Stop(first_line(details))
                    }
                    _ => Recovery::Retry,
                }
            }
            ConnError::ConnectTimeout(_) => Recovery::Retry,
            ConnError::AuthLost(why) => Recovery::Stop(why.clone()),
            ConnError::LikelyNeedsTls { .. }
            | ConnError::Url(_)
            | ConnError::Reply { .. }
            | ConnError::Range { .. }
            | ConnError::UnsafeCommand(_)
            | ConnError::Unbounded { .. } => Recovery::Stop(first_line(&self.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(kind: fred::error::ErrorKind, msg: &'static str) -> ConnError {
        ConnError::Command {
            cmd: "XREAD",
            source: fred::error::Error::new(kind, msg),
        }
    }

    #[test]
    fn denials_stop_and_outages_retry() {
        use fred::error::ErrorKind as K;
        for (kind, msg) in [
            (
                K::Unknown,
                "NOPERM User x has no permissions to run the 'xread' command",
            ),
            (K::Auth, "WRONGPASS invalid username-password pair"),
            (K::Unknown, "NOAUTH Authentication required."),
            (K::Unknown, "ERR unknown command 'XREAD'"),
            (
                K::Unknown,
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ),
        ] {
            assert!(
                matches!(command(kind.clone(), msg).recovery(), Recovery::Stop(_)),
                "{msg} must stop"
            );
        }
        for (kind, msg) in [
            (K::Timeout, "Request timed out."),
            (K::IO, "Connection reset by peer"),
            (K::Unknown, "LOADING Redis is loading the dataset in memory"),
            (K::Unknown, "MASTERDOWN Link with MASTER is down"),
            (K::Canceled, "Canceled."),
        ] {
            assert_eq!(
                command(kind, msg).recovery(),
                Recovery::Retry,
                "{msg} must retry"
            );
        }
        assert_eq!(
            ConnError::ConnectTimeout("x".into()).recovery(),
            Recovery::Retry
        );
    }
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
