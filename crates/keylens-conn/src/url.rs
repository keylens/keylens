//! Safe display of connection URLs.
//!
//! Redis URLs commonly contain credentials. They may be used for connecting, but must never
//! be copied verbatim into a terminal, diagnostic, or log file.

/// Return a connection URL with its password replaced by `***`.
///
/// This deliberately does not decode or otherwise normalize the URL: changing a diagnostic
/// URL can make it harder to compare with the configured value. Redis credentials containing
/// reserved characters must already be percent-encoded, so locating the authority delimiters
/// is sufficient here.
pub fn redact_url(url: &str) -> String {
    let Some((scheme, remainder)) = url.split_once("://") else {
        return url.to_string();
    };

    let authority_end = remainder.find(['/', '?', '#']).unwrap_or(remainder.len());
    let (authority, suffix) = remainder.split_at(authority_end);
    let Some((credentials, host)) = authority.rsplit_once('@') else {
        return url.to_string();
    };
    let Some((username, _password)) = credentials.split_once(':') else {
        // ACL usernames without passwords are not secrets.
        return url.to_string();
    };

    format!("{scheme}://{username}:***@{host}{suffix}")
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
}
