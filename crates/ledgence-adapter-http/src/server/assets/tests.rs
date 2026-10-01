use super::*;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
fn distribution() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("assets")).unwrap();
    std::fs::create_dir(dir.path().join("notices")).unwrap();
    let files = [
        (
            "notices/@package.txt",
            "text/plain; charset=utf-8",
            b"Scoped notice".as_slice(),
        ),
        (
            "notices/index.html",
            "text/html; charset=utf-8",
            b"<main>Legal notices</main>".as_slice(),
        ),
        (
            "notices/inventory.json",
            "application/json",
            b"{}".as_slice(),
        ),
        (
            "index.html",
            "text/html; charset=utf-8",
            b"<!doctype html><main>Console</main>".as_slice(),
        ),
        (
            "assets/main-12345678.js",
            "text/javascript; charset=utf-8",
            b"console.log('test');".as_slice(),
        ),
        (
            "notices/LICENSE.md",
            "text/plain; charset=utf-8",
            b"MIT notice".as_slice(),
        ),
    ];
    let mut assets = vec![];
    for (path, content_type, bytes) in files {
        std::fs::write(dir.path().join(path), bytes).unwrap();
        assets.push(json!({"path":path,"content_type":content_type,"size_bytes":bytes.len(),"sha256":format!("{:x}",Sha256::digest(bytes))}));
    }
    std::fs::write(dir.path().join("console-manifest.json"),serde_json::to_vec(&json!({"schema_version":1,"console_version":"0.1.1","console_contract_version":CONSOLE_CONTRACT_VERSION,"source_revision":"a".repeat(40),"source_dirty":true,"toolchain":{"node":"24.21.0","pnpm":"11.27.1"},"lockfile_sha256":"b".repeat(64),"assets":assets})).unwrap()).unwrap();
    dir
}
#[test]
fn static_build_fails_on_missing_changed_or_incompatible_assets() {
    let dir = distribution();
    ConsoleAssets::load(dir.path()).unwrap();
    std::fs::write(dir.path().join("assets/main-12345678.js"), "corrupt").unwrap();
    assert!(ConsoleAssets::load(dir.path()).is_err());
    let dir = distribution();
    std::fs::remove_file(dir.path().join("index.html")).unwrap();
    assert!(ConsoleAssets::load(dir.path()).is_err());
    let dir = distribution();
    let path = dir.path().join("console-manifest.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    for incompatible in [1, 2, 3, CONSOLE_CONTRACT_VERSION + 1] {
        value["console_contract_version"] = json!(incompatible);
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(ConsoleAssets::load(dir.path()).is_err());
    }
}
#[cfg(unix)]
#[test]
fn static_build_rejects_symlinks_outside_its_root() {
    let dir = distribution();
    let other = tempfile::tempdir().unwrap();
    let bytes = std::fs::read(dir.path().join("index.html")).unwrap();
    std::fs::write(other.path().join("index.html"), bytes).unwrap();
    std::fs::remove_file(dir.path().join("index.html")).unwrap();
    std::os::unix::fs::symlink(
        other.path().join("index.html"),
        dir.path().join("index.html"),
    )
    .unwrap();
    assert!(ConsoleAssets::load(dir.path()).is_err());
}
#[tokio::test]
async fn static_routes_preserve_api_failures_and_only_fallback_for_navigation() {
    let dir = distribution();
    let router = ConsoleAssets::load(dir.path())
        .unwrap()
        .router()
        .fallback(|| async {
            (
                StatusCode::NOT_FOUND,
                [(header::CONTENT_TYPE, "application/json")],
                "{\"code\":\"route_not_found\"}",
            )
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    for (method, path, status, contains) in [
        ("GET", "/console", 308, "location: /console/"),
        ("GET", "/console/workflows/wf_1", 200, "<main>Console"),
        ("GET", "/console/programs", 200, "<main>Console"),
        ("GET", "/console/programs/name", 200, "<main>Console"),
        (
            "GET",
            "/console/programs/name/versions/v1",
            200,
            "<main>Console",
        ),
        (
            "GET",
            "/console/agents/name/versions/v1",
            200,
            "<main>Console",
        ),
        (
            "GET",
            "/console/programs/task%2F%20%C3%A9/versions/v%2F1",
            200,
            "<main>Console",
        ),
        ("GET", "/console/assets/missing.js", 404, "no-store"),
        ("GET", "/v1/missing", 404, "route_not_found"),
        ("GET", "/health/missing", 404, "route_not_found"),
        (
            "GET",
            "/console/assets/main-12345678.js",
            200,
            "max-age=31536000",
        ),
        ("HEAD", "/console/", 200, "content-type: text/html"),
        ("POST", "/console/", 405, "allow: GET, HEAD"),
        ("GET", "/console/%2e%2e/private", 404, "no-store"),
        ("GET", "/console/notices/LICENSE.md", 200, "text/plain"),
        ("GET", "/console/notices/index.html", 200, "text/html"),
        ("GET", "/console/notices/", 200, "Legal notices"),
        (
            "GET",
            "/console/notices/%40package.txt",
            200,
            "Scoped notice",
        ),
        (
            "GET",
            "/console/notices/inventory.json",
            200,
            "application/json",
        ),
        (
            "GET",
            "/console/agents/invoice-issuer",
            200,
            "<main>Console",
        ),
        (
            "GET",
            "/console/executions/task%2F%20%C3%A9",
            200,
            "<main>Console",
        ),
        ("GET", "/console/executions/%2e%2e", 404, "no-store"),
        ("GET", "/console/executions/%00", 404, "no-store"),
        ("GET", "/console/executions/%FF", 404, "no-store"),
        ("GET", "/console/agents//versions/v1", 404, "no-store"),
        ("GET", "/console/programs//versions/v1", 404, "no-store"),
        (
            "GET",
            "/console/programs/name/versions/%00",
            404,
            "no-store",
        ),
        (
            "GET",
            "/console/programs/name/versions/%FF",
            404,
            "no-store",
        ),
    ] {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut bytes = vec![];
        stream.read_to_end(&mut bytes).await.unwrap();
        let reply = String::from_utf8(bytes).unwrap();
        assert!(
            reply.starts_with(&format!("HTTP/1.1 {status}")),
            "{method} {path}: {reply}"
        );
        assert!(reply.contains(contains), "{method} {path}: {reply}");
        if method == "HEAD" {
            assert!(!reply.contains("<main>"));
        }
    }
    task.abort();
    let _ = task.await;
}
