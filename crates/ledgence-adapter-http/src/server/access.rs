//! Optional bounded bearer and same-origin Console cookie admission. Credentials
//! remain transport values, never part of workflow commands or attribution.
use super::*;
use axum::http::HeaderMap;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

const LOGIN_MAX_BYTES: usize = 4096;
const CREDENTIAL_MAX_BYTES: usize = 1024;
const CONSOLE_ORIGIN: &str = "x-ledgence-console-origin";
const EXPECTED_ACTOR: &str = "x-ledgence-actor";

#[derive(Clone)]
pub(super) struct AccessControl {
    verifier: Arc<dyn CredentialVerifier>,
    origin: String,
    cookie_name: String,
    secure: bool,
}
impl AccessControl {
    pub(super) fn new(
        verifier: Arc<dyn CredentialVerifier>,
        instance: &SelfHostedInstanceConfig,
        allow_insecure: bool,
    ) -> Result<Self> {
        verifier.audience().require_instance(&instance.context())?;
        let (origin, secure) = validate_origin(instance, allow_insecure)?;
        // The name includes the configured origin to avoid accidental name clashes.
        // Cookies themselves are host-scoped, not port-isolated: local HTTP
        // requires a trusted machine; production uses a dedicated HTTPS host.
        let identity = serde_json::to_vec(&(verifier.audience(), &origin))
            .map_err(|_| invalid("invalid access audience"))?;
        let suffix = digest(&identity);
        let cookie_name = format!("{}ledgence-{}", if secure { "__Host-" } else { "" }, suffix);
        Ok(Self {
            verifier,
            origin,
            cookie_name,
            secure,
        })
    }

    fn verify(&self, credential: &str) -> Result<VerifiedPrincipal> {
        if !valid_credential(credential) {
            return Err(ContractError::Unauthenticated);
        }
        let at = wall_clock()?;
        let actor = self.verifier.verify(credential, at)?;
        actor.require_at(self.verifier.audience(), at)?;
        Ok(actor)
    }

    fn check_console_origin(&self, headers: &HeaderMap, method: &str) -> Result<()> {
        if single(headers, CONSOLE_ORIGIN)? != Some(self.origin.as_str()) {
            return Err(ContractError::Forbidden);
        }
        let origin = single(headers, "origin")?;
        if origin.is_some_and(|origin| origin != self.origin)
            || (method != "GET" && origin.is_none())
        {
            return Err(ContractError::Forbidden);
        }
        if single(headers, "sec-fetch-site")?
            .is_some_and(|site| !matches!(site, "same-origin" | "none"))
        {
            return Err(ContractError::Forbidden);
        }
        Ok(())
    }

    fn cookie<'a>(&self, headers: &'a HeaderMap) -> Result<Option<&'a str>> {
        let mut value = None;
        let mut size = 0usize;
        for header in headers.get_all(header::COOKIE) {
            size = size.saturating_add(header.as_bytes().len());
            if size > 8192 {
                return Err(ContractError::Unauthenticated);
            }
            let header = header
                .to_str()
                .map_err(|_| ContractError::Unauthenticated)?;
            for pair in header.split(';') {
                let Some((name, content)) = pair.trim().split_once('=') else {
                    continue;
                };
                if name == self.cookie_name {
                    if value.is_some() {
                        return Err(ContractError::Unauthenticated);
                    }
                    value = Some(content);
                }
            }
        }
        Ok(value)
    }

    fn set_cookie(&self, credential: Option<(&str, u64)>) -> Result<HeaderValue> {
        let (value, age) = match credential {
            Some((value, expires)) => (value, expires.saturating_sub(wall_clock()?).div_ceil(1000)),
            None => ("", 0),
        };
        let cookie = format!(
            "{}={value}; Path=/; HttpOnly; SameSite=Strict; Max-Age={age}{}",
            self.cookie_name,
            if self.secure { "; Secure" } else { "" }
        );
        let mut header =
            HeaderValue::from_str(&cookie).map_err(|_| ContractError::Unauthenticated)?;
        header.set_sensitive(true);
        Ok(header)
    }
}

pub(super) fn validate_origin(
    instance: &SelfHostedInstanceConfig,
    allow_insecure: bool,
) -> Result<(String, bool)> {
    instance.validate()?;
    let [origin] = instance.allowed_origins.as_slice() else {
        return Err(invalid(
            "credential access requires exactly one Console origin",
        ));
    };
    let uri: axum::http::Uri = origin
        .parse()
        .map_err(|_| invalid("invalid Console origin"))?;
    if uri.authority().is_none()
        || uri
            .authority()
            .is_some_and(|authority| authority.as_str().contains('@'))
        || uri.query().is_some()
        || origin.ends_with('/')
        || uri.path() != "/"
    {
        return Err(invalid(
            "Console origin must contain only scheme and authority",
        ));
    }
    let secure = uri.scheme_str() == Some("https");
    let local = uri.host().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !secure && !(allow_insecure && uri.scheme_str() == Some("http") && local) {
        return Err(invalid(
            "Console credential access requires HTTPS or explicit loopback HTTP",
        ));
    }
    if allow_insecure && (secure || !local) {
        return Err(invalid(
            "insecure Console access is only valid for a loopback HTTP origin",
        ));
    }
    Ok((origin.clone(), secure))
}

pub(super) fn control(server: &Server) -> Option<&AccessControl> {
    server
        .console
        .as_ref()
        .and_then(|console| console.access.as_ref())
}
fn single<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>> {
    let values = headers.get_all(name);
    if values.iter().count() > 1 {
        return Err(ContractError::Unauthenticated);
    }
    values
        .iter()
        .next()
        .map(|value| value.to_str().map_err(|_| ContractError::Unauthenticated))
        .transpose()
}
fn valid_credential(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= CREDENTIAL_MAX_BYTES
        && value.bytes().all(
            |byte| matches!(byte, 0x21 | 0x23..=0x2b | 0x2d..=0x3a | 0x3c..=0x5b | 0x5d..=0x7e),
        )
}
fn bearer(headers: &HeaderMap) -> Result<Option<&str>> {
    let Some(value) = single(headers, "authorization")? else {
        return Ok(None);
    };
    let Some((scheme, credential)) = value.split_once(' ') else {
        return Err(ContractError::Unauthenticated);
    };
    if !scheme.eq_ignore_ascii_case("bearer") || !valid_credential(credential) {
        return Err(ContractError::Unauthenticated);
    }
    Ok(Some(credential))
}
fn wall_clock() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .ok_or_else(|| unavailable("authentication clock is unavailable"))
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn actor_key(actor: &VerifiedPrincipal) -> String {
    let identity = serde_json::to_vec(&(actor.audience(), actor.identity(), actor.class()))
        .expect("validated principal serializes");
    digest(&identity)
}
fn require_actor(headers: &HeaderMap, actor: &VerifiedPrincipal) -> Result<()> {
    let expected = single(headers, EXPECTED_ACTOR)?.ok_or(ContractError::Forbidden)?;
    if expected != actor_key(actor) {
        return Err(ContractError::Forbidden);
    }
    Ok(())
}

pub(super) fn authorize(
    server: &Server,
    request: &Request,
    policy: Policy,
) -> Result<Option<VerifiedPrincipal>> {
    let Some(control) = control(server) else {
        return Ok(None);
    };
    authorize_control(control, request, policy)
}
fn authorize_control(
    control: &AccessControl,
    request: &Request,
    policy: Policy,
) -> Result<Option<VerifiedPrincipal>> {
    let console = request.uri().path().starts_with("/v1/console/");
    if policy == Policy::Login {
        control.check_console_origin(request.headers(), request.method().as_str())?;
        // Login intentionally replaces a stale cookie through a dedicated JSON
        // body. It never interprets mixed bearer and cookie credentials.
        if request.headers().contains_key(header::AUTHORIZATION) {
            return Err(ContractError::Unauthenticated);
        }
        return Ok(None);
    }
    let bearer = bearer(request.headers())?;
    let cookie = control.cookie(request.headers())?;
    if bearer.is_some() && cookie.is_some() {
        return Err(ContractError::Unauthenticated);
    }
    let from_cookie = bearer.is_none() && cookie.is_some();
    if from_cookie && !console {
        return Err(ContractError::Unauthenticated);
    }
    let credential = bearer.or(cookie).ok_or(ContractError::Unauthenticated)?;
    let actor = match control.verify(credential) {
        Ok(actor) => actor,
        Err(ContractError::Unauthenticated) if from_cookie && policy == Policy::Logout => {
            // An expired/revoked cookie grants no authority, but the browser can
            // still clear it. Never take this path for a valid replacement actor.
            control.check_console_origin(request.headers(), request.method().as_str())?;
            let expected =
                single(request.headers(), EXPECTED_ACTOR)?.ok_or(ContractError::Forbidden)?;
            if expected.len() != 64 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(ContractError::Forbidden);
            }
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    if from_cookie {
        if actor.class() == PrincipalClass::Worker {
            return Err(ContractError::Forbidden);
        }
        control.check_console_origin(request.headers(), request.method().as_str())?;
        if policy != Policy::Access {
            require_actor(request.headers(), &actor)?;
        }
    }
    if policy == Policy::Logout {
        if !from_cookie {
            return Err(ContractError::Forbidden);
        }
        require_actor(request.headers(), &actor)?;
    }
    policy.require(&actor)?;
    Ok(Some(actor))
}

pub(super) fn recheck(server: &Server, actor: &VerifiedPrincipal, policy: Policy) -> Result<()> {
    let control = control(server).ok_or(ContractError::Unauthenticated)?;
    actor.require_at(control.verifier.audience(), wall_clock()?)?;
    policy.require(actor)
}
fn snapshot(actor: Option<&VerifiedPrincipal>, operator_audit: bool) -> serde_json::Value {
    let Some(actor) = actor else {
        return serde_json::json!({"mode":"operator_trusted"});
    };
    serde_json::json!({
        "mode":"credentials", "actor_key":actor_key(actor),
        "issuer":actor.identity().issuer, "subject":actor.identity().subject,
        "class":actor.class(), "instance_id":actor.audience().instance_id,
        "scope":actor.audience().scope, "capabilities":actor.capabilities(),
        "expires_at_ms":actor.expires_at_ms(),
        "operator_audit":operator_audit,
    })
}
fn console_snapshot(server: &Server, actor: Option<&VerifiedPrincipal>) -> serde_json::Value {
    let mut value = snapshot(actor, audit::enabled(server));
    if actor.is_some() {
        value["native_effect_resolution"] = serde_json::json!(effect_resolution::available(server));
        value["provider_operation"] = serde_json::json!(provider::available(server));
    }
    value
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Login {
    credential: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Logout {}

pub(super) async fn dispatch(
    server: &Server,
    request: Request,
    policy: Policy,
    principal: Option<VerifiedPrincipal>,
) -> std::result::Result<(Vec<u8>, Option<HeaderValue>), Failure> {
    if request.uri().path().starts_with("/v1/console/") && server.console.is_none() {
        return Err(ContractError::NotFound.into());
    }
    if request.uri().query().is_some() {
        return Err(invalid("access endpoints do not accept query parameters").into());
    }
    if policy == Policy::Access {
        let generic = request.uri().path() == "/v1/access";
        let features = request.headers().get_all("ledgence-worker-features");
        let opted_in = features.iter().next().is_some();
        if opted_in
            && (features.iter().count() != 1 || features.iter().next().is_none_or(|v| v != "1"))
        {
            return Err(invalid("unsupported worker feature negotiation").into());
        }
        to_bytes(request.into_body(), 0)
            .await
            .map_err(|_| invalid("access lookup does not accept a body"))?;
        let mut value = if generic {
            snapshot(principal.as_ref(), audit::enabled(server))
        } else {
            console_snapshot(server, principal.as_ref())
        };
        if generic
            && principal.as_ref().is_some_and(|actor| {
                actor.class() == PrincipalClass::Worker
                    && actor.require(AccessCapability::WorkerExecute).is_ok()
            })
        {
            let effects = evidence::available(server);
            let admissions = admission::available(server);
            if effects || admissions {
                let actor = principal.as_ref().expect("verified worker checked");
                value["authority_id"] = serde_json::json!(actor.audience().authority_id);
            }
            if effects {
                value["runtime_capabilities"] =
                    serde_json::json!({"approved_effect_evidence_v2":true});
            }
            if opted_in {
                let mut supported = Vec::new();
                if effects && effect_resolution::available(server) {
                    supported.push(ledgence_worker_api::NATIVE_EFFECT_RESOLUTION_FEATURE);
                    if provider::available(server) {
                        supported.push(ledgence_worker_api::PROVIDER_OPERATION_FEATURE);
                    }
                }
                if provider_admission::available(server) {
                    supported.push(ledgence_worker_api::PROVIDER_ADMISSION_FEATURE);
                }
                if admissions {
                    supported.push(ledgence_worker_api::RESOURCE_ADMISSION_FEATURE);
                }
                if super::task_admission::available(server) {
                    supported.push(TASK_ADMISSION_FEATURE);
                }
                if !supported.is_empty() {
                    value["worker_features"] = serde_json::json!({"v":1,"supported":supported});
                }
            }
        }
        return Ok((server.encode(value).await?, None));
    }
    let control = control(server).ok_or(ContractError::NotFound)?;
    console::check_content_type(request.headers())?;
    let raw = to_bytes(request.into_body(), LOGIN_MAX_BYTES)
        .await
        .map_err(|_| Failure {
            status: 413,
            error: invalid("access body exceeds limit or was interrupted"),
        })?;
    match policy {
        Policy::Login => {
            let login: Login = decode_unique_json(&raw, LOGIN_MAX_BYTES)
                .map_err(|_| invalid("invalid login request"))?;
            let actor = control.verify(&login.credential)?;
            if actor.class() == PrincipalClass::Worker {
                return Err(ContractError::Forbidden.into());
            }
            let cookie = control.set_cookie(Some((&login.credential, actor.expires_at_ms())))?;
            Ok((
                server
                    .encode(console_snapshot(server, Some(&actor)))
                    .await?,
                Some(cookie),
            ))
        }
        Policy::Logout => {
            let _: Logout = decode_unique_json(&raw, LOGIN_MAX_BYTES)
                .map_err(|_| invalid("invalid logout request"))?;
            Ok((
                server
                    .encode(serde_json::json!({"signed_out":true}))
                    .await?,
                Some(control.set_cookie(None)?),
            ))
        }
        _ => Err(ContractError::NotFound.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    struct Verifier {
        audience: AccessAudience,
        actors: Mutex<BTreeMap<String, VerifiedPrincipal>>,
    }
    impl CredentialVerifier for Verifier {
        fn audience(&self) -> &AccessAudience {
            &self.audience
        }
        fn verify(&self, credential: &str, _: u64) -> Result<VerifiedPrincipal> {
            self.actors
                .lock()
                .unwrap()
                .get(credential)
                .cloned()
                .ok_or(ContractError::Unauthenticated)
        }
    }
    fn config() -> SelfHostedInstanceConfig {
        SelfHostedInstanceConfig {
            instance_id: "access-test".into(),
            name: "Access".into(),
            scope: Scope {
                tenant_id: "tenant".into(),
                namespace: "namespace".into(),
            },
            suggested_queues: vec![],
            allowed_origins: vec!["https://console.example".into()],
        }
    }
    fn fixture() -> (AccessControl, Arc<Verifier>) {
        let config = config();
        let audience = AccessAudience {
            authority_id: "authority".into(),
            instance_id: config.instance_id.clone(),
            scope: config.scope.clone(),
        };
        let mut actors = BTreeMap::new();
        for (token, class, capabilities) in [
            (
                "reader",
                PrincipalClass::Operator,
                vec![AccessCapability::MetadataRead],
            ),
            (
                "operator",
                PrincipalClass::Operator,
                vec![
                    AccessCapability::MetadataRead,
                    AccessCapability::SensitiveRead,
                    AccessCapability::ApprovalDecide,
                ],
            ),
            (
                "decider",
                PrincipalClass::Service,
                vec![AccessCapability::ApprovalDecide],
            ),
            (
                "worker",
                PrincipalClass::Worker,
                vec![AccessCapability::WorkerExecute],
            ),
        ] {
            actors.insert(
                token.into(),
                VerifiedPrincipal::new(
                    PrincipalIdentity {
                        issuer: audience.issuer(),
                        subject: token.into(),
                    },
                    audience.clone(),
                    format!("key-{token}"),
                    class,
                    4_102_444_800_000,
                    capabilities,
                )
                .unwrap(),
            );
        }
        let verifier = Arc::new(Verifier {
            audience,
            actors: Mutex::new(actors),
        });
        let access = AccessControl::new(verifier.clone(), &config, false).unwrap();
        (access, verifier)
    }
    fn request(method: &str, path: &str) -> Request {
        Request::builder()
            .method(method)
            .uri(path)
            .body(Body::empty())
            .unwrap()
    }
    fn cookie_request(access: &AccessControl, token: &str, method: &str, path: &str) -> Request {
        let mut request = request(method, path);
        request.headers_mut().insert(
            header::COOKIE,
            HeaderValue::from_str(&format!("{}={token}", access.cookie_name)).unwrap(),
        );
        request.headers_mut().insert(
            CONSOLE_ORIGIN,
            HeaderValue::from_static("https://console.example"),
        );
        let actor = access.verify(token).unwrap();
        request.headers_mut().insert(
            EXPECTED_ACTOR,
            HeaderValue::from_str(&actor_key(&actor)).unwrap(),
        );
        if method != "GET" {
            request.headers_mut().insert(
                header::ORIGIN,
                HeaderValue::from_static("https://console.example"),
            );
        }
        request
    }
    #[test]
    fn bearer_capabilities_worker_class_duplicates_and_cookie_mixing_fail_closed() {
        let (access, _) = fixture();
        for (token, policy, allowed) in [
            ("reader", Policy::Metadata, true),
            ("reader", Policy::Sensitive, false),
            ("operator", Policy::Approval, true),
            ("decider", Policy::Approval, false),
            ("operator", Policy::Worker, false),
            ("worker", Policy::Worker, true),
            ("worker", Policy::Metadata, false),
            ("worker", Policy::Sensitive, false),
        ] {
            let mut req = request("POST", "/v1/tasks");
            req.headers_mut().insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
            );
            assert_eq!(
                authorize_control(&access, &req, policy).is_ok(),
                allowed,
                "{token}:{policy:?}"
            );
        }
        let mut req = request("GET", "/v1/access");
        assert_eq!(
            authorize_control(&access, &req, Policy::Access),
            Err(ContractError::Unauthenticated)
        );
        req.headers_mut().append(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer reader"),
        );
        req.headers_mut().append(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer reader"),
        );
        assert_eq!(
            authorize_control(&access, &req, Policy::Access),
            Err(ContractError::Unauthenticated)
        );
        req.headers_mut().insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer reader"),
        );
        req.headers_mut().insert(
            header::COOKIE,
            HeaderValue::from_str(&format!("{}=reader", access.cookie_name)).unwrap(),
        );
        assert_eq!(
            authorize_control(&access, &req, Policy::Access),
            Err(ContractError::Unauthenticated)
        );
        req.headers_mut().remove(header::AUTHORIZATION);
        assert_eq!(
            authorize_control(&access, &req, Policy::Access),
            Err(ContractError::Unauthenticated)
        );
    }
    #[test]
    fn cookies_require_origin_and_frozen_actor_and_cannot_be_worker_credentials() {
        let (access, _) = fixture();
        let mut req = cookie_request(&access, "operator", "GET", "/v1/console/config");
        assert!(authorize_control(&access, &req, Policy::Metadata).is_ok());
        req.headers_mut().remove(EXPECTED_ACTOR);
        assert_eq!(
            authorize_control(&access, &req, Policy::Metadata),
            Err(ContractError::Forbidden)
        );
        assert!(authorize_control(&access, &req, Policy::Access).is_ok());
        for (header, value) in [
            (CONSOLE_ORIGIN, "https://foreign.example"),
            ("origin", "null"),
            ("sec-fetch-site", "cross-site"),
        ] {
            let mut req = cookie_request(&access, "operator", "GET", "/v1/console/config");
            req.headers_mut()
                .insert(header, HeaderValue::from_str(value).unwrap());
            assert_eq!(
                authorize_control(&access, &req, Policy::Metadata),
                Err(ContractError::Forbidden)
            );
        }
        let mut req = cookie_request(&access, "operator", "POST", "/v1/console/approvals/decide");
        req.headers_mut().remove(header::ORIGIN);
        assert_eq!(
            authorize_control(&access, &req, Policy::Approval),
            Err(ContractError::Forbidden)
        );
        let mut req = cookie_request(&access, "operator", "POST", "/v1/console/access/logout");
        req.headers_mut().insert(
            EXPECTED_ACTOR,
            HeaderValue::from_static("stale-other-actor"),
        );
        assert_eq!(
            authorize_control(&access, &req, Policy::Logout),
            Err(ContractError::Forbidden)
        );
        let req = cookie_request(&access, "worker", "GET", "/v1/console/access");
        assert_eq!(
            authorize_control(&access, &req, Policy::Access),
            Err(ContractError::Forbidden)
        );
        let mut req = cookie_request(&access, "reader", "GET", "/v1/console/config");
        req.headers_mut().append(
            header::COOKIE,
            HeaderValue::from_str(&format!("{}=reader", access.cookie_name)).unwrap(),
        );
        assert_eq!(
            authorize_control(&access, &req, Policy::Metadata),
            Err(ContractError::Unauthenticated)
        );
    }
    #[test]
    fn login_can_replace_stale_cookie_but_requires_exact_origin_without_bearer() {
        let (access, _) = fixture();
        let mut req = request("POST", "/v1/console/access/login");
        req.headers_mut().insert(
            CONSOLE_ORIGIN,
            HeaderValue::from_static("https://console.example"),
        );
        req.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_static("https://console.example"),
        );
        req.headers_mut().insert(
            header::COOKIE,
            HeaderValue::from_str(&format!("{}=expired-token", access.cookie_name)).unwrap(),
        );
        assert_eq!(authorize_control(&access, &req, Policy::Login), Ok(None));
        req.headers_mut().append(
            header::ORIGIN,
            HeaderValue::from_static("https://console.example"),
        );
        assert_eq!(
            authorize_control(&access, &req, Policy::Login),
            Err(ContractError::Unauthenticated)
        );
        req.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_static("https://console.example"),
        );
        req.headers_mut().insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer operator"),
        );
        assert_eq!(
            authorize_control(&access, &req, Policy::Login),
            Err(ContractError::Unauthenticated)
        );
    }
    #[test]
    fn verifier_is_consulted_each_time_and_rotation_keeps_actor_identity() {
        let (access, verifier) = fixture();
        let req = cookie_request(&access, "operator", "GET", "/v1/console/config");
        let prior = access.verify("operator").unwrap();
        let changed = VerifiedPrincipal::new(
            prior.identity().clone(),
            prior.audience().clone(),
            "rotated".into(),
            prior.class(),
            prior.expires_at_ms(),
            vec![AccessCapability::MetadataRead],
        )
        .unwrap();
        assert_eq!(actor_key(&prior), actor_key(&changed));
        verifier
            .actors
            .lock()
            .unwrap()
            .insert("operator".into(), changed);
        assert_eq!(
            authorize_control(&access, &req, Policy::Sensitive),
            Err(ContractError::Forbidden)
        );
        let expired = VerifiedPrincipal::new(
            prior.identity().clone(),
            prior.audience().clone(),
            "expired".into(),
            prior.class(),
            1,
            vec![AccessCapability::MetadataRead],
        )
        .unwrap();
        verifier
            .actors
            .lock()
            .unwrap()
            .insert("operator".into(), expired);
        assert_eq!(
            authorize_control(&access, &req, Policy::Metadata),
            Err(ContractError::Unauthenticated)
        );
        let mut wrong = prior.audience().clone();
        wrong.scope.namespace.push('x');
        let wrong = VerifiedPrincipal::new(
            prior.identity().clone(),
            wrong,
            "wrong".into(),
            prior.class(),
            prior.expires_at_ms(),
            vec![AccessCapability::MetadataRead],
        )
        .unwrap();
        verifier
            .actors
            .lock()
            .unwrap()
            .insert("operator".into(), wrong);
        assert_eq!(
            authorize_control(&access, &req, Policy::Metadata),
            Err(ContractError::Forbidden)
        );
        verifier.actors.lock().unwrap().remove("operator");
        assert_eq!(
            authorize_control(&access, &req, Policy::Metadata),
            Err(ContractError::Unauthenticated)
        );
    }
    #[test]
    fn logout_clears_revoked_cookie_but_not_a_valid_replacement_identity() {
        let (access, verifier) = fixture();
        let mut req = cookie_request(&access, "reader", "POST", "/v1/console/access/logout");
        verifier.actors.lock().unwrap().remove("reader");
        assert_eq!(authorize_control(&access, &req, Policy::Logout), Ok(None));
        req.headers_mut().insert(
            header::COOKIE,
            HeaderValue::from_str(&format!("{}=operator", access.cookie_name)).unwrap(),
        );
        assert_eq!(
            authorize_control(&access, &req, Policy::Logout),
            Err(ContractError::Forbidden)
        );
    }
    #[test]
    fn cookie_attributes_and_local_transport_opt_in_are_explicit() {
        let (access, verifier) = fixture();
        let cookie = access
            .set_cookie(Some(("reader", 4_102_444_800_000)))
            .unwrap();
        assert!(cookie.is_sensitive());
        let cookie = cookie.to_str().unwrap();
        for field in [
            "__Host-ledgence-",
            "Path=/",
            "HttpOnly",
            "SameSite=Strict",
            "Secure",
        ] {
            assert!(cookie.contains(field));
        }
        assert!(
            access
                .set_cookie(None)
                .unwrap()
                .to_str()
                .unwrap()
                .contains("Max-Age=0")
        );
        let mut config = config();
        config.allowed_origins = vec!["http://127.0.0.1:8080".into()];
        assert!(AccessControl::new(verifier.clone(), &config, false).is_err());
        let local = AccessControl::new(verifier.clone(), &config, true).unwrap();
        assert!(
            !local
                .set_cookie(None)
                .unwrap()
                .to_str()
                .unwrap()
                .contains("Secure")
        );
        config.allowed_origins = vec!["http://public.example".into()];
        assert!(AccessControl::new(verifier.clone(), &config, true).is_err());
        config.allowed_origins = vec![];
        assert!(AccessControl::new(verifier, &config, false).is_err());
    }
}
