//! Operator policy mutations and bounded metadata/hold inspection for one queue.
use super::*;

pub(super) fn available(server: &Server) -> bool {
    access::control(server).is_some()
        && audit::enabled(server)
        && server
            .workflows
            .as_ref()
            .is_some_and(|s| s.operator_access().is_ok())
        && server.service.queue_policies().is_ok()
}

async fn command<T: DeserializeOwned + Send + 'static>(
    server: &Server,
    bytes: Vec<u8>,
    scope: Option<&Scope>,
) -> std::result::Result<T, Failure> {
    let Some(scope) = scope else {
        return server.decode(bytes, QUEUE_POLICY_COMMAND_MAX_BYTES).await;
    };
    let mut object: serde_json::Map<String, serde_json::Value> =
        server.decode(bytes, QUEUE_POLICY_COMMAND_MAX_BYTES).await?;
    if object.contains_key("scope") {
        return Err(invalid("Console queue commands must not supply a scope").into());
    }
    object.insert("scope".into(), serde_json::json!(scope));
    server
        .blocking(move || {
            serde_json::from_value(serde_json::Value::Object(object))
                .map_err(|_| invalid("malformed Console queue command").into())
        })
        .await
}
pub(super) async fn post(
    server: &Server,
    operation: &str,
    bytes: Vec<u8>,
    scope: Option<&Scope>,
    context: &audit::RequestContext,
) -> std::result::Result<Vec<u8>, Failure> {
    let principal = context.principal()?;
    if !available(server) {
        return Err(unavailable("queue policy management is unavailable").into());
    }
    let service = server.service.queue_policies()?;
    macro_rules! read_command {
        ($ty:ty,$($scope:ident).+) => {{
            let q:$ty=command(server,bytes,scope).await?;
            q.validate()?;
            server.require_scope(&q.$($scope).+)?;
            context.recheck(server)?;
            q
        }}
    }
    macro_rules! respond {
        ($q:ident,$method:ident,$mutation:expr) => {{
            let reply = service.$method(principal, &$q).await?;
            if $mutation {
                context.decision_committed();
            }
            reply.validate_for(&$q).map_err(|_| {
                unavailable("invalid queue policy response; reconcile the identical request")
            })?;
            server
                .blocking(move || encode_bounded(&reply, QUEUE_POLICY_PAGE_MAX_BYTES))
                .await
        }};
    }
    match operation {
        "put" => {
            let q = read_command!(QueuePolicyPut, scope);
            respond!(q, put_queue_policy, true)
        }
        "inspect" => {
            let q = read_command!(QueuePolicyQuery, scope);
            respond!(q, inspect_queue_policy, false)
        }
        "list" => {
            let q = read_command!(QueuePolicyList, scope);
            respond!(q, list_queue_policies, false)
        }
        "holds" => {
            let q = read_command!(QueueHoldList, scope);
            let bytes = respond!(q, list_queue_holds, false)?;
            context.sensitive(AuditResource::Queue { queue: q.queue })?;
            Ok(bytes)
        }
        "release" => {
            let q = read_command!(QueueHoldRelease, scope);
            respond!(q, release_queue_hold, true)
        }
        "close" => {
            let q = read_command!(QueueHoldClose, owner.scope);
            respond!(q, close_queue_hold, true)
        }
        _ => Err(ContractError::NotFound.into()),
    }
}
