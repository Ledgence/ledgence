//! Optional, instance-bound opaque credentials. Configuration stores verifiers,
//! never bearer secrets. Reload all serving replicas to apply policy changes.
use ledgence_orchestration_api::{
    AccessAudience, AccessCapability, ContractError, CredentialVerifier, PrincipalClass,
    PrincipalIdentity, Result, Scope, SelfHostedInstanceContext, VerifiedPrincipal,
    decode_unique_json, validate_text,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    io::Read,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;

pub const ACCESS_CONFIG_MAX_BYTES: usize = 1024 * 1024;
pub const ACCESS_MAX_CREDENTIALS: usize = 1024;
pub const CREDENTIAL_MAX_BYTES: usize = 102;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessConfig {
    pub version: u32,
    pub authority_id: String,
    pub instance_id: String,
    pub scope: Scope,
    pub credentials: Vec<CredentialRecord>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialRecord {
    pub credential_id: String,
    pub verifier_sha256: String,
    pub subject: String,
    pub class: PrincipalClass,
    pub expires_at_ms: u64,
    pub capabilities: Vec<AccessCapability>,
}
#[derive(Debug, Clone)]
pub struct CredentialSpec {
    pub subject: String,
    pub class: PrincipalClass,
    pub expires_at_ms: u64,
    pub capabilities: Vec<AccessCapability>,
}

/// Provisioning result. The caller must write its secret into a newly created
/// private file; it must not print it, put it in process arguments, or log it.
pub struct SecretCredential {
    credential_id: String,
    secret: String,
}
impl SecretCredential {
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }
    pub fn expose_secret(&self) -> &str {
        &self.secret
    }
}
impl fmt::Debug for SecretCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretCredential")
            .field("credential_id", &self.credential_id)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

impl AccessConfig {
    /// Create an installation authority with no enabled credentials yet.
    pub fn new(instance: &SelfHostedInstanceContext) -> Result<Self> {
        instance.validate()?;
        Ok(Self {
            version: 1,
            authority_id: format!("auth_{}", random_hex::<16>()?),
            instance_id: instance.instance_id.clone(),
            scope: instance.scope.clone(),
            credentials: Vec::new(),
        })
    }
    pub fn audience(&self) -> AccessAudience {
        AccessAudience {
            authority_id: self.authority_id.clone(),
            instance_id: self.instance_id.clone(),
            scope: self.scope.clone(),
        }
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let config: Self =
            decode_unique_json(bytes, ACCESS_CONFIG_MAX_BYTES).map_err(|_| invalid())?;
        config.validate()?;
        Ok(config)
    }
    /// Synchronous bounded server configuration loading. Credential-file client
    /// loading is a distinct operation: this file contains hashes, not secrets.
    pub fn load(path: &Path) -> Result<Self> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(
                (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
            );
        }
        let file = options.open(path).map_err(|_| load_error())?;
        if !file.metadata().map_err(|_| load_error())?.is_file() {
            return Err(load_error());
        }
        let mut bytes = Vec::new();
        file.take(ACCESS_CONFIG_MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| load_error())?;
        Self::decode(&bytes)
    }
    pub fn validate(&self) -> Result<()> {
        self.audience().validate()?;
        if self.version != 1 || self.credentials.len() > ACCESS_MAX_CREDENTIALS {
            return Err(invalid());
        }
        let mut ids = HashSet::new();
        for credential in &self.credentials {
            if !credential
                .credential_id
                .strip_prefix("cred_")
                .is_some_and(|id| is_hex(id, 32))
                || !ids.insert(&credential.credential_id)
                || !is_hex(&credential.verifier_sha256, 64)
            {
                return Err(invalid());
            }
            validate_spec(
                &credential.subject,
                credential.class,
                credential.expires_at_ms,
                &credential.capabilities,
            )?;
        }
        Ok(())
    }
    /// Append a verifier atomically in memory. Save the returned secret privately
    /// and persist the updated config before making it available to a server.
    pub fn issue(&mut self, spec: CredentialSpec, now_ms: u64) -> Result<SecretCredential> {
        self.validate()?;
        validate_spec(
            &spec.subject,
            spec.class,
            spec.expires_at_ms,
            &spec.capabilities,
        )?;
        if spec.expires_at_ms <= now_ms || self.credentials.len() >= ACCESS_MAX_CREDENTIALS {
            return Err(invalid());
        }
        let id = random_hex::<16>()?;
        let credential_id = format!("cred_{id}");
        if self
            .credentials
            .iter()
            .any(|item| item.credential_id == credential_id)
        {
            return Err(ContractError::Unavailable(
                "credential identifier collision; retry provisioning".into(),
            ));
        }
        let secret = format!("ldg1_{id}_{}", random_hex::<32>()?);
        let verifier_sha256 = hex(&Sha256::digest(secret.as_bytes()));
        self.credentials.push(CredentialRecord {
            credential_id: credential_id.clone(),
            verifier_sha256,
            subject: spec.subject,
            class: spec.class,
            expires_at_ms: spec.expires_at_ms,
            capabilities: spec.capabilities,
        });
        Ok(SecretCredential {
            credential_id,
            secret,
        })
    }
}

/// Verification has bounded token work and a single in-memory identifier lookup.
/// This adapter performs no network, filesystem or database calls per request.
pub struct OpaqueCredentialVerifier {
    audience: AccessAudience,
    credentials: HashMap<String, (VerifiedPrincipal, [u8; 32])>,
}
impl OpaqueCredentialVerifier {
    pub fn new(config: AccessConfig) -> Result<Self> {
        config.validate()?;
        let audience = config.audience();
        let mut credentials = HashMap::with_capacity(config.credentials.len());
        for credential in config.credentials {
            let digest = decode_digest(&credential.verifier_sha256)?;
            let principal = VerifiedPrincipal::new(
                PrincipalIdentity {
                    issuer: audience.issuer(),
                    subject: credential.subject,
                },
                audience.clone(),
                credential.credential_id.clone(),
                credential.class,
                credential.expires_at_ms,
                credential.capabilities,
            )?;
            credentials.insert(credential.credential_id, (principal, digest));
        }
        Ok(Self {
            audience,
            credentials,
        })
    }
}
impl CredentialVerifier for OpaqueCredentialVerifier {
    fn audience(&self) -> &AccessAudience {
        &self.audience
    }
    fn verify(&self, credential: &str, now_ms: u64) -> Result<VerifiedPrincipal> {
        // Validate ASCII before slicing. Never include supplied credentials in an error.
        if credential.len() != CREDENTIAL_MAX_BYTES
            || !credential.is_ascii()
            || !credential.starts_with("ldg1_")
            || credential.as_bytes()[37] != b'_'
            || !is_hex(&credential[5..37], 32)
            || !is_hex(&credential[38..], 64)
        {
            return Err(ContractError::Unauthenticated);
        }
        let id = format!("cred_{}", &credential[5..37]);
        let (principal, expected) = self
            .credentials
            .get(&id)
            .ok_or(ContractError::Unauthenticated)?;
        let actual: [u8; 32] = Sha256::digest(credential.as_bytes()).into();
        if !bool::from(actual.ct_eq(expected)) {
            return Err(ContractError::Unauthenticated);
        }
        principal.require_at(&self.audience, now_ms)?;
        Ok(principal.clone())
    }
}

/// Authentication uses a checked local wall clock, not orchestration DB time.
pub fn wall_clock_ms() -> Result<u64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| clock_error())?;
    u64::try_from(elapsed.as_millis()).map_err(|_| clock_error())
}
fn validate_spec(
    subject: &str,
    class: PrincipalClass,
    expires_at_ms: u64,
    capabilities: &[AccessCapability],
) -> Result<()> {
    validate_text(subject, 128)?;
    class.validate_capabilities(capabilities)?;
    if expires_at_ms == 0 || expires_at_ms > i64::MAX as u64 {
        return Err(invalid());
    }
    Ok(())
}
fn random_hex<const N: usize>() -> Result<String> {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).map_err(|_| {
        ContractError::Unavailable("operating-system randomness unavailable".into())
    })?;
    Ok(hex(&bytes))
}
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 15) as usize] as char);
    }
    out
}
fn is_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn decode_digest(value: &str) -> Result<[u8; 32]> {
    if !is_hex(value, 64) {
        return Err(invalid());
    }
    let mut out = [0u8; 32];
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        fn nibble(byte: u8) -> u8 {
            if byte <= b'9' {
                byte - b'0'
            } else {
                byte - b'a' + 10
            }
        }
        out[index] = (nibble(pair[0]) << 4) | nibble(pair[1]);
    }
    Ok(out)
}
fn invalid() -> ContractError {
    ContractError::InvalidInput("invalid access configuration or credential specification".into())
}
fn load_error() -> ContractError {
    ContractError::InvalidInput("cannot read access configuration as a bounded regular file".into())
}
fn clock_error() -> ContractError {
    ContractError::Unavailable("authentication clock unavailable".into())
}

#[cfg(test)]
mod tests;
