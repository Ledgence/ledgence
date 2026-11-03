//! Destination-bound client credentials. This format contains a reusable secret.
use ledgence_orchestration_api::{ContractError, Result, decode_unique_json};
use reqwest::{Url, header::HeaderValue};
use serde::Deserialize;
use std::{fmt, fs::OpenOptions, io::Read, net::IpAddr, path::Path};

const FILE_MAX_BYTES: usize = 16 * 1024;
const TOKEN_MAX_BYTES: usize = 8192;

#[derive(Clone)]
pub struct HttpCredentials {
    server: Url,
    authorization: HeaderValue,
}
impl fmt::Debug for HttpCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpCredentials")
            .field("server", &self.server)
            .field("authorization", &"[REDACTED]")
            .finish()
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialFile {
    server: String,
    token: String,
}
impl HttpCredentials {
    /// Tokens travel only to this normalized server base URL, using HTTPS or
    /// loopback HTTP for local development. They are never forwarded on redirects.
    pub fn new(server: &str, token: &str) -> Result<Self> {
        let server = normalize_server(server)?;
        let unpadded = token.trim_end_matches('=');
        if token.is_empty()
            || token.len() > TOKEN_MAX_BYTES
            || unpadded.is_empty()
            || !unpadded
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-._~+/".contains(&byte))
        {
            return Err(invalid("invalid bearer credential"));
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| invalid("invalid bearer credential"))?;
        authorization.set_sensitive(true);
        Ok(Self {
            server,
            authorization,
        })
    }

    /// Load a bounded regular JSON file containing `server` and `token`.
    /// Unix symlinks and non-regular files are rejected before reading secrets.
    pub fn from_file(path: &Path) -> Result<Self> {
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(
                (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
            );
        }
        let file = options
            .open(path)
            .map_err(|_| invalid("cannot open credential file"))?;
        if !file
            .metadata()
            .map_err(|_| invalid("cannot inspect credential file"))?
            .is_file()
        {
            return Err(invalid("credential file must be a regular file"));
        }
        let mut bytes = Vec::new();
        file.take(FILE_MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid("cannot read credential file"))?;
        let parsed: CredentialFile = decode_unique_json(&bytes, FILE_MAX_BYTES)
            .map_err(|_| invalid("invalid credential file JSON"))?;
        Self::new(&parsed.server, &parsed.token)
    }

    pub(super) fn for_server(&self, server: &Url) -> Result<HeaderValue> {
        if self.server != normalize_server(server.as_str())? {
            return Err(invalid("credential file belongs to a different server"));
        }
        Ok(self.authorization.clone())
    }
}
fn normalize_server(server: &str) -> Result<Url> {
    let mut url = Url::parse(server).map_err(|_| invalid("invalid credential server URL"))?;
    let host = url.host_str().unwrap_or_default();
    let loopback = host == "localhost"
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if url.host_str().is_none()
        || !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid(
            "credentials require HTTPS or loopback HTTP without URL credentials, query, or fragment",
        ));
    }
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn credentials_are_redacted_bounded_and_destination_bound() {
        let secret = "secret-canary-123";
        let credential = HttpCredentials::new("https://example.com/api", secret).unwrap();
        assert!(!format!("{credential:?}").contains(secret));
        let header = credential
            .for_server(&Url::parse("https://example.com/api/").unwrap())
            .unwrap();
        assert!(header.is_sensitive());
        assert!(!format!("{header:?}").contains(secret));
        for destination in [
            "https://other.example/api/",
            "https://example.com/",
            "http://example.com/api/",
        ] {
            assert!(
                credential
                    .for_server(&Url::parse(destination).unwrap())
                    .is_err()
            );
        }
        for server in [
            "http://127.0.0.1:8080",
            "http://[::1]:8080",
            "http://localhost:8080",
        ] {
            assert!(HttpCredentials::new(server, secret).is_ok());
        }
        for server in [
            "http://example.com",
            "http://localhost.example.com",
            "https://user@example.com",
            "https://example.com/?token=x",
            "file:///tmp/secret",
        ] {
            assert!(HttpCredentials::new(server, secret).is_err());
        }
        for token in ["", "bad\nheader", "a b", "=", "foo=bar"] {
            assert!(HttpCredentials::new("https://example.com", token).is_err());
        }
        assert!(
            HttpCredentials::new("https://example.com", &"a".repeat(TOKEN_MAX_BYTES + 1)).is_err()
        );
    }
    #[test]
    fn credential_file_rejects_duplicates_unknowns_oversize_and_nonfiles() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credential.json");
        for bytes in [
            br#"{"server":"https://example.com","token":"canary","token":"other"}"#.to_vec(),
            br#"{"server":"https://example.com","token":"canary","extra":true}"#.to_vec(),
            vec![b' '; FILE_MAX_BYTES + 1],
        ] {
            std::fs::write(&path, bytes).unwrap();
            assert!(HttpCredentials::from_file(&path).is_err());
        }
        assert!(HttpCredentials::from_file(directory.path()).is_err());
        std::fs::write(
            &path,
            br#"{"server":"https://example.com","token":"canary"}"#,
        )
        .unwrap();
        assert!(HttpCredentials::from_file(&path).is_ok());
        #[cfg(unix)]
        {
            let link = directory.path().join("link");
            std::os::unix::fs::symlink(&path, &link).unwrap();
            assert!(HttpCredentials::from_file(&link).is_err());
        }
    }
}
