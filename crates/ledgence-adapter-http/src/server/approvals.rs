//! Approval transport keeps proposal identity and effective arguments intact.
use super::*;
use ledgence_orchestration_api::console::ConsoleU64;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsoleReference {
    workflow_id: String,
    key: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsoleList {
    workflow_id: String,
    after_key: Option<String>,
    limit: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsoleDecision {
    workflow_id: String,
    key: String,
    activation_id: String,
    revision: ConsoleU64,
    action: ApprovalAction,
    decision_id: String,
    decision: ApprovalDecision,
    reviewer: String,
    #[serde(deserialize_with = "required_option")]
    reason: Option<String>,
}
fn required_option<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
#[derive(Serialize)]
struct ConsoleApproval {
    workflow_id: String,
    key: String,
    activation_id: String,
    revision: ConsoleU64,
    action: ApprovalAction,
    proposed_arguments: Option<serde_json::Value>,
    created_at: Timestamp,
    deadline: Timestamp,
    status: ApprovalStatus,
    decision: Option<ApprovalDecisionRecord>,
    resumed_activation_id: Option<String>,
}
impl From<ApprovalSnapshot> for ConsoleApproval {
    fn from(value: ApprovalSnapshot) -> Self {
        Self {
            workflow_id: value.workflow_id,
            key: value.key,
            activation_id: value.activation_id,
            revision: ConsoleU64(value.revision),
            action: value.action,
            proposed_arguments: value.proposed_arguments,
            created_at: value.created_at,
            deadline: value.deadline,
            status: value.status,
            decision: value.decision,
            resumed_activation_id: value.resumed_activation_id,
        }
    }
}
#[derive(Serialize)]
struct ConsolePage {
    items: Vec<ConsoleApproval>,
    next_cursor: Option<String>,
}
#[derive(Serialize)]
struct ConsoleReceipt {
    approval: ConsoleApproval,
    already_accepted: bool,
}

pub(super) async fn post(
    server: &Server,
    path: &str,
    bytes: Vec<u8>,
    console_scope: Option<&Scope>,
) -> std::result::Result<Vec<u8>, Failure> {
    let service = server.workflows.as_deref().ok_or(ContractError::NotFound)?;
    let console = console_scope.is_some();
    match path.rsplit('/').next() {
        Some("inspect") => {
            let command: ApprovalReference = if let Some(scope) = console_scope {
                let value: ConsoleReference =
                    server.decode(bytes, APPROVAL_SNAPSHOT_MAX_BYTES).await?;
                ApprovalReference {
                    scope: scope.clone(),
                    workflow_id: value.workflow_id,
                    key: value.key,
                }
            } else {
                server.decode(bytes, APPROVAL_SNAPSHOT_MAX_BYTES).await?
            };
            server.require_scope(&command.scope)?;
            command.scope.validate()?;
            validate_text(&command.workflow_id, 128)?;
            validate_text(&command.key, 128)?;
            record(&command.scope, &command.workflow_id);
            let reply = service
                .approval(&command.scope, &command.workflow_id, &command.key)
                .await?;
            server
                .blocking(move || {
                    reply.validate().map_err(|_| invalid_reply())?;
                    if reply.scope != command.scope
                        || reply.workflow_id != command.workflow_id
                        || reply.key != command.key
                    {
                        return Err(invalid_reply().into());
                    }
                    if console {
                        encode_bounded(&ConsoleApproval::from(reply), APPROVAL_SNAPSHOT_MAX_BYTES)
                    } else {
                        encode_bounded(&reply, APPROVAL_SNAPSHOT_MAX_BYTES)
                    }
                })
                .await
        }
        Some("list") => {
            let command: ApprovalListRequest = if let Some(scope) = console_scope {
                let value: ConsoleList = server.decode(bytes, APPROVAL_SNAPSHOT_MAX_BYTES).await?;
                ApprovalListRequest {
                    scope: scope.clone(),
                    workflow_id: value.workflow_id,
                    after_key: value.after_key,
                    limit: value.limit,
                }
            } else {
                server.decode(bytes, APPROVAL_SNAPSHOT_MAX_BYTES).await?
            };
            server.require_scope(&command.scope)?;
            command.scope.validate()?;
            validate_text(&command.workflow_id, 128)?;
            validate_approval_page(command.after_key.as_deref(), command.limit)?;
            record(&command.scope, &command.workflow_id);
            let reply = service
                .list_approvals(
                    &command.scope,
                    &command.workflow_id,
                    command.after_key.as_deref(),
                    command.limit,
                )
                .await?;
            server
                .blocking(move || {
                    reply.validate().map_err(|_| invalid_reply())?;
                    if !reply.matches(
                        &command.scope,
                        &command.workflow_id,
                        command.after_key.as_deref(),
                        command.limit,
                    ) {
                        return Err(invalid_reply().into());
                    }
                    if console {
                        encode_bounded(
                            &ConsolePage {
                                items: reply.items.into_iter().map(Into::into).collect(),
                                next_cursor: reply.next_cursor,
                            },
                            1024 * 1024,
                        )
                    } else {
                        encode_bounded(&reply, 1024 * 1024)
                    }
                })
                .await
        }
        Some("decide") => {
            let command: ApprovalDecisionCommand = if let Some(scope) = console_scope {
                let value: ConsoleDecision =
                    server.decode(bytes, APPROVAL_SNAPSHOT_MAX_BYTES).await?;
                ApprovalDecisionCommand {
                    scope: scope.clone(),
                    workflow_id: value.workflow_id,
                    key: value.key,
                    activation_id: value.activation_id,
                    revision: value.revision.0,
                    action: value.action,
                    decision_id: value.decision_id,
                    decision: value.decision,
                    reviewer: value.reviewer,
                    reason: value.reason,
                }
            } else {
                server.decode(bytes, APPROVAL_SNAPSHOT_MAX_BYTES).await?
            };
            let command = server
                .blocking(move || {
                    command.validate()?;
                    Ok(command)
                })
                .await?;
            server.require_scope(&command.scope)?;
            record(&command.scope, &command.workflow_id);
            let reply = service.decide_approval(&command).await?;
            server
                .blocking(move || {
                    reply.validate().map_err(|_| invalid_reply())?;
                    if !reply.matches(&command).map_err(|_| invalid_reply())? {
                        return Err(invalid_reply().into());
                    }
                    if console {
                        encode_bounded(
                            &ConsoleReceipt {
                                approval: reply.approval.into(),
                                already_accepted: reply.already_accepted,
                            },
                            APPROVAL_SNAPSHOT_MAX_BYTES + 1024,
                        )
                    } else {
                        encode_bounded(&reply, APPROVAL_SNAPSHOT_MAX_BYTES + 1024)
                    }
                })
                .await
        }
        _ => Err(ContractError::NotFound.into()),
    }
}
fn record(scope: &Scope, workflow_id: &str) {
    record_scope(scope);
    tracing::Span::current().record("ledgence.workflow.id", workflow_id);
}
fn invalid_reply() -> ContractError {
    unavailable("invalid approval service response")
}
