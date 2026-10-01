//! Verified static distribution. No frontend toolchain is needed at runtime.
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{Request, State},
    http::{StatusCode, header},
    response::Response,
    routing::any,
};
use ledgence_orchestration_api::{
    ContractError, Result, console::CONSOLE_CONTRACT_VERSION, decode_unique_json,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    path::{Component, Path},
    sync::Arc,
};

const MANIFEST_LIMIT: usize = 1024 * 1024;
const BUILD_LIMIT: u64 = 64 * 1024 * 1024;
const ASSET_LIMIT: u64 = 32 * 1024 * 1024;
// Radix Dialog injects reviewed scroll-lock styles. Inline CSS is allowed;
// scripts remain local-only, with neither inline script nor eval permitted.
const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u32,
    console_version: String,
    console_contract_version: u32,
    source_revision: String,
    source_dirty: bool,
    toolchain: Toolchain,
    lockfile_sha256: String,
    assets: Vec<ManifestAsset>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Toolchain {
    node: String,
    pnpm: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestAsset {
    path: String,
    sha256: String,
    size_bytes: u64,
    content_type: String,
}
#[derive(Clone)]
struct Asset {
    bytes: Bytes,
    content_type: String,
    immutable: bool,
}
#[derive(Clone)]
pub struct ConsoleAssets {
    files: Arc<BTreeMap<String, Asset>>,
}
impl ConsoleAssets {
    /// Call during startup on a blocking executor, before coordinators start.
    /// Every served byte is verified once and retained immutably in memory.
    pub fn load(directory: &Path) -> Result<Self> {
        let invalid =
            || ContractError::InvalidInput("invalid or incompatible Console distribution".into());
        let root = directory.canonicalize().map_err(|_| invalid())?;
        if !root.is_dir() {
            return Err(invalid());
        }
        let manifest_path = root.join("console-manifest.json");
        let canonical = manifest_path.canonicalize().map_err(|_| invalid())?;
        if !canonical.starts_with(&root) {
            return Err(invalid());
        }
        let metadata = std::fs::metadata(&canonical).map_err(|_| invalid())?;
        if !metadata.is_file() || metadata.len() > MANIFEST_LIMIT as u64 {
            return Err(invalid());
        }
        let bytes = read_bounded(&canonical, MANIFEST_LIMIT as u64).map_err(|_| invalid())?;
        let manifest: Manifest =
            decode_unique_json(&bytes, MANIFEST_LIMIT).map_err(|_| invalid())?;
        if manifest.schema_version != 1
            || manifest.console_contract_version != CONSOLE_CONTRACT_VERSION
            || manifest.console_version.is_empty()
            || !hex(&manifest.source_revision, 40)
            || !hex(&manifest.lockfile_sha256, 64)
            || manifest.toolchain.node.is_empty()
            || manifest.toolchain.pnpm.is_empty()
            || manifest.assets.is_empty()
            || manifest.assets.len() > 4096
        {
            return Err(invalid());
        }
        // Dirty builds are useful locally; release packaging separately requires
        // a clean source revision and hashes the exact same distribution bytes.
        let _source_dirty = manifest.source_dirty;
        let mut files = BTreeMap::new();
        let mut total = 0u64;
        for item in manifest.assets {
            if !safe_path(&item.path)
                || !hex(&item.sha256, 64)
                || item.size_bytes > ASSET_LIMIT
                || mime(&item.path) != Some(item.content_type.as_str())
            {
                return Err(invalid());
            }
            total = total.checked_add(item.size_bytes).ok_or_else(invalid)?;
            if total > BUILD_LIMIT {
                return Err(invalid());
            }
            let canonical = root
                .join(&item.path)
                .canonicalize()
                .map_err(|_| invalid())?;
            if !canonical.starts_with(&root) {
                return Err(invalid());
            }
            let bytes = read_bounded(&canonical, item.size_bytes).map_err(|_| invalid())?;
            if bytes.len() as u64 != item.size_bytes
                || format!("{:x}", Sha256::digest(&bytes)) != item.sha256
            {
                return Err(invalid());
            }
            let immutable = item.path.starts_with("assets/")
                && item.path.rsplit_once('-').is_some_and(|(_, tail)| {
                    tail.split('.').next().is_some_and(|hash| {
                        hash.len() >= 8
                            && hash.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                    })
                });
            if files
                .insert(
                    item.path,
                    Asset {
                        bytes: Bytes::from(bytes),
                        content_type: item.content_type,
                        immutable,
                    },
                )
                .is_some()
            {
                return Err(invalid());
            }
        }
        if !files.contains_key("index.html")
            || !files.keys().any(|path| path.starts_with("notices/"))
        {
            return Err(invalid());
        }
        Ok(Self {
            files: Arc::new(files),
        })
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/console", any(serve))
            .route("/console/", any(serve))
            .route("/console/{*path}", any(serve))
            .with_state(self)
    }
}
fn read_bounded(path: &Path, limit: u64) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let file = std::fs::File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::other("not a regular asset"));
    }
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(std::io::Error::other("asset limit"));
    }
    Ok(bytes)
}
fn hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn safe_path(value: &str) -> bool {
    !value.is_empty()
        && !value.contains(['\\', '%', '?', '#', ':'])
        && !value.chars().any(char::is_control)
        && Path::new(value)
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
        && !value.contains("//")
}
fn mime(path: &str) -> Option<&'static str> {
    match path.rsplit('.').next()? {
        "html" => Some("text/html; charset=utf-8"),
        "js" => Some("text/javascript; charset=utf-8"),
        "css" => Some("text/css; charset=utf-8"),
        "json" => Some("application/json"),
        "txt" => Some("text/plain; charset=utf-8"),
        "svg" => Some("image/svg+xml"),
        "png" => Some("image/png"),
        "ico" => Some("image/x-icon"),
        "woff2" => Some("font/woff2"),
        _ if path.starts_with("notices/") => Some("text/plain; charset=utf-8"),
        _ => None,
    }
}
fn navigation(path: &str) -> bool {
    if path.is_empty() {
        return true;
    }
    let parts: Vec<_> = path.split('/').collect();
    // Only route identifiers are decoded, never filesystem paths. A percent-
    // escaped ID may contain a slash or Unicode without becoming another route.
    if parts.iter().any(|part| !navigation_segment(part)) {
        return false;
    }
    match parts.as_slice() {
        [] => true,
        ["executions" | "workflows" | "programs" | "agents" | "workers"] => true,
        [
            "executions" | "workflows" | "programs" | "agents" | "workers",
            id,
        ] => !id.is_empty(),
        ["programs" | "agents", _, "versions", _] => true,
        _ => false,
    }
}
fn decode_uri(value: &str) -> Option<String> {
    if value.len() > 16 * 1024 {
        return None;
    }
    let mut decoded = Vec::with_capacity(value.len());
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let high = bytes.next().and_then(|v| char::from(v).to_digit(16))?;
            let low = bytes.next().and_then(|v| char::from(v).to_digit(16))?;
            decoded.push((high * 16 + low) as u8);
        } else {
            decoded.push(byte);
        }
    }
    String::from_utf8(decoded).ok()
}
fn navigation_segment(value: &str) -> bool {
    if value.is_empty() || value.len() > 8192 {
        return false;
    }
    decode_uri(value).is_some_and(|id| {
        !matches!(id.as_str(), "." | "..")
            && !id.chars().any(char::is_control)
            && !id.contains('\\')
    })
}
async fn serve(State(assets): State<ConsoleAssets>, request: Request) -> Response {
    let method = request.method().as_str();
    if !matches!(method, "GET" | "HEAD") {
        return response(StatusCode::METHOD_NOT_ALLOWED, Body::empty(), None, false);
    }
    if request.uri().path() == "/console" {
        let mut reply = response(StatusCode::PERMANENT_REDIRECT, Body::empty(), None, false);
        reply.headers_mut().insert(
            header::LOCATION,
            header::HeaderValue::from_static("/console/"),
        );
        return reply;
    }
    let Some(path) = request.uri().path().strip_prefix("/console/") else {
        return response(StatusCode::NOT_FOUND, Body::empty(), None, false);
    };
    // Decoding is exclusively a key lookup in the startup-verified inventory.
    // It never selects a filesystem path; traversal cannot escape that map.
    let decoded = decode_uri(path);
    let asset_key = decoded.as_deref().map(|path| {
        if path == "notices/" {
            "notices/index.html"
        } else {
            path
        }
    });
    let asset = asset_key
        .filter(|path| safe_path(path))
        .and_then(|path| assets.files.get(path))
        .or_else(|| {
            navigation(path)
                .then(|| assets.files.get("index.html"))
                .flatten()
        });
    match asset {
        Some(asset) => {
            let body = if method == "HEAD" {
                Body::empty()
            } else {
                Body::from(asset.bytes.clone())
            };
            let mut reply = response(
                StatusCode::OK,
                body,
                Some(&asset.content_type),
                asset.immutable,
            );
            if let Ok(length) = header::HeaderValue::from_str(&asset.bytes.len().to_string()) {
                reply.headers_mut().insert(header::CONTENT_LENGTH, length);
            }
            reply
        }
        None => response(StatusCode::NOT_FOUND, Body::empty(), None, false),
    }
}
fn response(
    status: StatusCode,
    body: Body,
    content_type: Option<&str>,
    immutable: bool,
) -> Response {
    let mut reply = Response::new(body);
    *reply.status_mut() = status;
    for (name, value) in [
        (
            header::CACHE_CONTROL,
            if immutable {
                "public, max-age=31536000, immutable"
            } else {
                "no-store"
            },
        ),
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        (header::CONTENT_SECURITY_POLICY, CSP),
        (header::REFERRER_POLICY, "no-referrer"),
    ] {
        reply
            .headers_mut()
            .insert(name, header::HeaderValue::from_static(value));
    }
    if let Some(content_type) =
        content_type.and_then(|value| header::HeaderValue::from_str(value).ok())
    {
        reply
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    if status == StatusCode::METHOD_NOT_ALLOWED {
        reply
            .headers_mut()
            .insert(header::ALLOW, header::HeaderValue::from_static("GET, HEAD"));
    }
    reply
}

#[cfg(test)]
mod tests;
