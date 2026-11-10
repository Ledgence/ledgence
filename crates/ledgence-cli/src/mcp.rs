//! Composition only: MCP depends on portable contracts; HTTP stays at the edge.
use std::process::ExitCode;

#[cfg(not(feature = "mcp"))]
pub fn run(_arguments: Vec<String>) -> ExitCode {
    super::diagnose(
        crate::args::invalid("MCP support requires the mcp build feature"),
        None,
    )
}

#[cfg(feature = "mcp")]
pub fn run(mut arguments: Vec<String>) -> ExitCode {
    let credentials = match crate::credentials::take(&mut arguments) {
        Ok(value) => value,
        Err(error) => return super::diagnose(error, None),
    };
    use ledgence_adapter_http::HttpProgramCatalogService;
    use ledgence_adapter_mcp::ToolDispatcher;
    use std::{sync::Arc, time::Duration};
    let (url, scope, read_only, allow_effect_resolution, allow_resource_control) =
        match parse(arguments) {
            Ok(options) => options,
            Err(error) => return super::diagnose(error, None),
        };
    let client = match crate::credentials::task_client(&url, credentials.as_ref()) {
        Ok(client) => Arc::new(client),
        Err(error) => return super::diagnose(error, None),
    };
    let catalog = match HttpProgramCatalogService::new((*client).clone(), scope.clone()) {
        Ok(catalog) => Arc::new(catalog),
        Err(error) => return super::diagnose(error, None),
    };
    let dispatcher =
        match ToolDispatcher::new(client.clone(), client.clone(), catalog, scope, read_only) {
            Ok(dispatcher) => dispatcher
                .with_artifact_reader(Arc::new(client.artifacts()))
                .with_audit_reader(client),
            Err(error) => return super::diagnose(error, None),
        };
    let dispatcher = if allow_effect_resolution {
        match dispatcher.with_effect_resolution() {
            Ok(dispatcher) => dispatcher,
            Err(error) => return super::diagnose(error, None),
        }
    } else {
        dispatcher
    };
    let dispatcher = if allow_resource_control {
        match dispatcher.with_resource_control() {
            Ok(d) => d,
            Err(e) => return super::diagnose(e, None),
        }
    } else {
        dispatcher
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            return super::diagnose(
                ledgence_orchestration_api::ContractError::Unavailable(
                    "cannot start MCP runtime".into(),
                ),
                None,
            );
        }
    };
    // No tracing subscriber/exporter is installed here. Protocol stdout must
    // remain clean, and upstream SDK debug logs may include application data.
    let result = runtime.block_on(async {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let signal_shutdown = shutdown.clone();
        let signals = tokio::spawn(async move {
            wait_for_shutdown().await;
            signal_shutdown.cancel();
        });
        let result = ledgence_adapter_mcp::serve(
            dispatcher,
            tokio::io::stdin(),
            tokio::io::stdout(),
            shutdown,
        )
        .await;
        signals.abort();
        result
    });
    // Tokio's stdin uses a blocking read that cannot be interrupted on signal.
    // Bound joining it; this executable exits after the owned MCP session ends.
    runtime.shutdown_timeout(Duration::from_millis(100));
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => super::diagnose(
            ledgence_orchestration_api::ContractError::Unavailable(error.to_string()),
            None,
        ),
    }
}

#[cfg(feature = "mcp")]
async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = terminate.recv() => {},
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(feature = "mcp")]
fn parse(
    arguments: Vec<String>,
) -> ledgence_orchestration_api::Result<(String, ledgence_orchestration_api::Scope, bool, bool, bool)>
{
    use crate::args::invalid;
    let mut values = std::collections::HashMap::new();
    let mut arguments = arguments.into_iter();
    let mut read_only = false;
    let mut allow_effect_resolution = false;
    let mut allow_resource_control = false;
    while let Some(key) = arguments.next() {
        if key == "--read-only" && !read_only {
            read_only = true;
            continue;
        }
        if key == "--allow-effect-resolution" && !allow_effect_resolution {
            allow_effect_resolution = true;
            continue;
        }
        if key == "--allow-resource-control" && !allow_resource_control {
            allow_resource_control = true;
            continue;
        }
        if !matches!(key.as_str(), "--server" | "--tenant" | "--namespace") {
            return Err(invalid(
                "unknown or duplicate MCP option; use ledgence mcp serve --help",
            ));
        }
        let value = arguments
            .next()
            .ok_or_else(|| invalid(format!("missing value for {key}")))?;
        if value.starts_with("--") || values.insert(key.clone(), value).is_some() {
            return Err(invalid(format!("missing value or duplicate option {key}")));
        }
    }
    let server = values
        .remove("--server")
        .ok_or_else(|| invalid("missing --server"))?;
    let scope = ledgence_orchestration_api::Scope {
        tenant_id: values
            .remove("--tenant")
            .ok_or_else(|| invalid("missing --tenant"))?,
        namespace: values
            .remove("--namespace")
            .ok_or_else(|| invalid("missing --namespace"))?,
    };
    scope.validate()?;
    if read_only && (allow_effect_resolution || allow_resource_control) {
        return Err(invalid("--read-only conflicts with mutation opt-in"));
    }
    Ok((
        server,
        scope,
        read_only,
        allow_effect_resolution,
        allow_resource_control,
    ))
}

#[cfg(all(test, feature = "mcp"))]
mod tests {
    use super::*;
    #[test]
    fn mutation_opt_in_is_explicit_and_rejects_read_only_conflicts() {
        let base = [
            "--server",
            "http://localhost:8080",
            "--tenant",
            "acme",
            "--namespace",
            "demo",
        ];
        let options = |extra: &[&str]| {
            parse(
                base.into_iter()
                    .chain(extra.iter().copied())
                    .map(str::to_owned)
                    .collect(),
            )
        };
        assert!(!options(&[]).unwrap().3);
        assert!(!options(&[]).unwrap().4);
        assert!(options(&["--allow-resource-control"]).unwrap().4);
        assert!(options(&["--allow-effect-resolution"]).unwrap().3);
        for flags in [
            &["--allow-effect-resolution", "--read-only"][..],
            &["--allow-resource-control", "--read-only"][..],
            &["--allow-resource-control", "--allow-resource-control"][..],
            &["--allow-effect-resolution", "--allow-effect-resolution"][..],
        ] {
            assert!(options(flags).is_err());
        }
    }
}
