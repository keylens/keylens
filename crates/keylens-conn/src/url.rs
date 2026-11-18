//! Safe display of connection URLs.
//!
//! Redis URLs commonly contain credentials. They may be used for connecting, but must never
//! be copied verbatim into a terminal, diagnostic, or log file.

/// Return a connection URL with its password replaced by `***`.
///
/// This deliberately does not decode or otherwise normalize the URL: changing a diagnostic
/// URL can make it harder to compare with the configured value.
///
/// **The `@` is located before the path delimiters, not after.** RFC 3986 says an authority
/// ends at the first `/`, `?` or `#`, so scanning for those first splits a url whose
/// password contains one *in the middle of that password* — and the `@` then goes missing,
/// which used to mean the whole url, password included, was handed back untouched. Managed
/// hosts issue base64 passwords, so a literal `/` in one is ordinary rather than exotic.
///
/// **Ambiguity redacts more, never less.** A url that cannot be split confidently — a
/// password holding a path delimiter, say — loses its whole userinfo rather than any part
/// of it. That over-redacts the rare url carrying an `@` in its *path* and no credentials
/// at all; printing `***` for something that was never secret costs a little diagnostic
/// detail, and it is the only direction of error this function is allowed to make.
///
/// ```
/// use keylens_conn::redact_url;
///
/// assert_eq!(
///     redact_url("rediss://admin:hunter2@prod.example.com:6379/3"),
///     "rediss://admin:***@prod.example.com:6379/3"
/// );
///
/// // A base64 password with an unencoded `/` is still masked.
/// let masked = redact_url("rediss://default:AVNS_xK3/9pQ@db.example.com:25061");
/// assert!(!masked.contains("9pQ"));
///
/// // An ACL username with no password is not a secret, so it is left alone.
/// assert_eq!(redact_url("redis://user@host:6379"), "redis://user@host:6379");
/// ```
pub fn redact_url(url: &str) -> String {
    let (scheme, remainder) = match url.split_once("://") {
        Some((scheme, remainder)) => (Some(scheme), remainder),
        // No scheme is not a promise that there are no credentials: `user:pass@host` is a
        // perfectly ordinary thing to find in a config file.
        None => (None, url),
    };
    let masked = redact_authority(remainder).unwrap_or_else(|| remainder.to_string());
    let masked = redact_query(&masked);
    match scheme {
        Some(scheme) => format!("{scheme}://{masked}"),
        None => masked,
    }
}

/// Query parameters whose values are credentials.
///
/// fred reads the Sentinel password from `?sentinelPassword=`, so a Sentinel url carries
/// a secret *outside* the userinfo -- and 0.1.7 printed it verbatim from `keylens probe`,
/// `keylens connections` and the status bar. Matched by substring, case-insensitively, so
/// a parameter that merely looks like a credential is masked too: over-redacting is the
/// only direction of error allowed here.
const SECRET_PARAMS: &[&str] = &["pass", "pwd", "secret", "token", "auth", "key"];

/// Mask the value of every credential-looking query parameter after the authority.
fn redact_query(after_scheme: &str) -> String {
    // The query starts after the authority. The userinfo is already masked (or absent),
    // so the first `?` after the last `@` is the query's.
    let authority_end = after_scheme.rfind('@').map_or(0, |at| at + 1);
    let Some(q) = after_scheme
        .get(authority_end..)
        .and_then(|rest| rest.find('?'))
        .map(|i| i + authority_end)
    else {
        return after_scheme.to_string();
    };
    let (head, query) = after_scheme.split_at(q);
    let query = query.trim_start_matches('?');
    let (query, fragment) = match query.split_once('#') {
        Some((q, f)) => (q, Some(f)),
        None => (query, None),
    };
    let masked: Vec<String> = query
        .split('&')
        .map(|pair| match pair.split_once('=') {
            Some((k, _)) if is_secret_param(k) => format!("{k}=***"),
            _ => pair.to_string(),
        })
        .collect();
    let mut out = format!("{head}?{}", masked.join("&"));
    if let Some(f) = fragment {
        out.push('#');
        out.push_str(f);
    }
    out
}

fn is_secret_param(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    SECRET_PARAMS.iter().any(|s| lower.contains(s))
}

/// Mask the password in `<userinfo>@<host><suffix>`, or `None` when there is nothing to mask.
fn redact_authority(remainder: &str) -> Option<String> {
    // Last `@`, so a password with an unencoded `@` in it still ends up on the correct side
    // of the split.
    let at = remainder.rfind('@')?;
    let (credentials, rest) = remainder.split_at(at);
    let host = rest.strip_prefix('@')?;

    // ACL usernames without passwords are not secrets.
    let (username, _password) = credentials.split_once(':')?;

    if username.contains(['/', '?', '#']) {
        // The split landed past a path delimiter, so this is not a userinfo we can read.
        // Whatever it is, the password is inside it.
        Some(format!("***@{host}"))
    } else {
        Some(format!("{username}:***@{host}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_passwords_without_changing_the_rest() {
        assert_eq!(
            redact_url("rediss://admin:hunter2@prod.example.com:6379/3?x=1"),
            "rediss://admin:***@prod.example.com:6379/3?x=1"
        );
        assert_eq!(
            redact_url("redis://:secret@127.0.0.1:6379"),
            "redis://:***@127.0.0.1:6379"
        );
    }

    #[test]
    fn leaves_passwordless_and_unrecognised_values_alone() {
        assert_eq!(
            redact_url("redis://user@host:6379"),
            "redis://user@host:6379"
        );
        assert_eq!(redact_url("redis://host:6379"), "redis://host:6379");
        assert_eq!(redact_url("not-a-url"), "not-a-url");
    }

    #[test]
    fn masks_passwords_holding_a_path_delimiter() {
        // The regression this function exists for. A managed host's base64 password
        // contains `/` about half the time, and `keylens connections` prints stored urls
        // without ever parsing them — so an unencoded one used to be echoed in full to a
        // terminal that is very often being screen-shared.
        for url in [
            "rediss://default:AVNS_xK3/9pQ@db.ondigitalocean.com:25061",
            "rediss://default:pa#ssword@db.ondigitalocean.com:25061",
            "rediss://default:pa?ssword@db.ondigitalocean.com:25061",
        ] {
            let masked = redact_url(url);
            assert!(
                masked.contains("***") && masked.contains("db.ondigitalocean.com:25061"),
                "{url} -> {masked}"
            );
            assert!(
                !masked.contains("9pQ") && !masked.contains("ssword"),
                "password survived redaction: {url} -> {masked}"
            );
        }
    }

    #[test]
    fn masks_the_sentinel_password_in_the_query_string() {
        // 0.1.7: `keylens probe` printed this url with the sentinel password intact.
        let url = "redis-sentinel://u:pw@10.0.0.1:26379/0?sentinelServiceName=mymaster\
                   &sentinelUsername=admin&sentinelPassword=S3ntinelPw&node=10.0.0.2:26379";
        let masked = redact_url(url);
        assert!(!masked.contains("S3ntinelPw"), "{masked}");
        assert!(!masked.contains(":pw@"), "{masked}");
        assert!(masked.contains("sentinelPassword=***"), "{masked}");
        // Everything that is not a credential survives, for diagnosis.
        assert!(masked.contains("sentinelServiceName=mymaster"), "{masked}");
        assert!(masked.contains("sentinelUsername=admin"), "{masked}");
        assert!(masked.contains("node=10.0.0.2:26379"), "{masked}");
    }

    #[test]
    fn masks_credential_parameters_whatever_their_case_and_with_no_userinfo() {
        for (url, secret) in [
            ("redis://host:6379/0?password=hunter2", "hunter2"),
            ("redis://host:6379?PASS=hunter2", "hunter2"),
            ("rediss://host?x=1&auth_token=tok123#frag", "tok123"),
            ("host:6379?sentinelPassword=abc", "abc"),
        ] {
            let masked = redact_url(url);
            assert!(!masked.contains(secret), "{url} -> {masked}");
        }
        assert_eq!(
            redact_url("rediss://host?x=1&auth_token=tok123#frag"),
            "rediss://host?x=1&auth_token=***#frag"
        );
        // No query, nothing touched.
        assert_eq!(redact_url("redis://host:6379/0"), "redis://host:6379/0");
    }

    #[test]
    fn a_question_mark_inside_a_password_is_not_mistaken_for_the_query() {
        let masked = redact_url("rediss://default:pa?ss=word@db.example.com:25061?x=1");
        assert!(!masked.contains("word"), "{masked}");
        assert!(masked.ends_with("@db.example.com:25061?x=1"), "{masked}");
    }

    #[test]
    fn masks_credentials_that_carry_no_scheme() {
        let masked = redact_url("user:hunter2@host:6379");
        assert_eq!(masked, "user:***@host:6379");
    }

    #[test]
    fn an_unreadable_split_redacts_more_rather_than_less() {
        // No credentials here at all, but an `@` in the path is indistinguishable from a
        // password holding a `/`. Over-redacting is the only safe way to be wrong: the
        // port is masked as if it were a password, and nothing secret would have survived
        // had this been the other case.
        assert_eq!(redact_url("redis://host:6379/p@th"), "redis://host:***@th");

        // When the *username* is the half that spans the delimiter there is no readable
        // userinfo at all, so none of it is echoed back.
        assert_eq!(
            redact_url("redis://ho/st:hunter2@h:6379"),
            "redis://***@h:6379"
        );
    }
}
