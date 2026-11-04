//! Resource authority is optional and remains behind portable admission ports.
use super::*;
pub(super) fn is_mutation(name: &str) -> bool {
    matches!(
        name,
        "ledgence_resource_put" | "ledgence_admission_release" | "ledgence_task_admission_release"
    )
}
pub(super) fn is_tool(name: &str) -> bool {
    is_mutation(name)
        || matches!(
            name,
            "ledgence_resource_list"
                | "ledgence_resource_inspect"
                | "ledgence_admission_inspect"
                | "ledgence_task_admission_inspect"
        )
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct List {
    after: Option<String>,
    #[serde(default = "default_limit")]
    limit: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Resource {
    name: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inspect {
    workflow_id: String,
    origin_activation_id: String,
    key: String,
}
impl Inspect {
    fn occurrence(self, scope: Scope) -> AdmissionOccurrence {
        AdmissionOccurrence {
            scope,
            workflow_id: self.workflow_id,
            origin_activation_id: self.origin_activation_id,
            key: self.key,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Put {
    name: String,
    request_id: String,
    #[serde(deserialize_with = "required_revision")]
    expected_revision: Option<u64>,
    policy: ResourcePolicy,
    reason: String,
}
fn required_revision<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<u64>, D::Error> {
    Option::<u64>::deserialize(d)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Release {
    workflow_id: String,
    origin_activation_id: String,
    key: String,
    source_id: String,
    expected_revision: u64,
    request_id: String,
    reason: String,
}
impl ToolDispatcher {
    pub(super) async fn call_admission(
        &self,
        name: &str,
        arguments: &Value,
    ) -> std::result::Result<Value, ToolError> {
        if matches!(
            name,
            "ledgence_task_admission_inspect" | "ledgence_task_admission_release"
        ) {
            return self.call_task_admission(name, arguments).await;
        }
        let client = remote(self.workflows.admission_client(), false)?;
        match name {
            "ledgence_resource_list" => {
                let a: List = parse(arguments)?;
                let q = ResourceListQuery {
                    scope: self.scope.clone(),
                    after: a.after,
                    limit: a.limit,
                };
                q.validate().map_err(ToolError::arguments)?;
                let r = remote(client.list_resources(&q).await, false)?;
                r.validate_for(&q).map_err(|_| invalid_reply(false))?;
                encode(r, false)
            }
            "ledgence_resource_inspect" => {
                let a: Resource = parse(arguments)?;
                let q = ResourceQuery {
                    scope: self.scope.clone(),
                    name: a.name,
                };
                q.validate().map_err(ToolError::arguments)?;
                let r = remote(client.resource(&q).await, false)?;
                r.validate().map_err(|_| invalid_reply(false))?;
                if r.resource.scope != q.scope || r.resource.name != q.name {
                    return Err(invalid_reply(false));
                }
                encode(r, false)
            }
            "ledgence_admission_inspect" => {
                let a: Inspect = parse(arguments)?;
                let q = AdmissionQuery {
                    occurrence: a.occurrence(self.scope.clone()),
                };
                q.validate().map_err(ToolError::arguments)?;
                let r = remote(client.inspect_admission(&q).await, false)?;
                r.validate().map_err(|_| invalid_reply(false))?;
                if r.occurrence != q.occurrence {
                    return Err(invalid_reply(false));
                }
                encode(r, false)
            }
            "ledgence_resource_put" => {
                let a: Put = parse(arguments)?;
                let c = ResourcePutCommand {
                    scope: self.scope.clone(),
                    name: a.name,
                    request_id: a.request_id,
                    expected_revision: a.expected_revision,
                    policy: a.policy,
                    reason: a.reason,
                };
                c.validate().map_err(ToolError::arguments)?;
                let r = remote(client.put_resource(&c).await, true)?;
                r.validate().map_err(|_| invalid_reply(true))?;
                if !r.matches(&c) {
                    return Err(invalid_reply(true));
                }
                encode(r, true)
            }
            "ledgence_admission_release" => {
                let a: Release = parse(arguments)?;
                let c = AdmissionReleaseCommand {
                    occurrence: AdmissionOccurrence {
                        scope: self.scope.clone(),
                        workflow_id: a.workflow_id,
                        origin_activation_id: a.origin_activation_id,
                        key: a.key,
                    },
                    source_id: a.source_id,
                    expected_revision: a.expected_revision,
                    request_id: a.request_id,
                    reason: a.reason,
                };
                c.validate().map_err(ToolError::arguments)?;
                let r = remote(client.release_admission(&c).await, true)?;
                r.validate().map_err(|_| invalid_reply(true))?;
                if !r.matches(&c) {
                    return Err(invalid_reply(true));
                }
                encode(r, true)
            }
            _ => unreachable!("checked registered admission tool"),
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskInspect {
    task_id: String,
    generation: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskRelease {
    task_id: String,
    generation: u32,
    source_id: String,
    expected_revision: u64,
    request_id: String,
    reason: String,
}
impl ToolDispatcher {
    async fn call_task_admission(
        &self,
        name: &str,
        arguments: &Value,
    ) -> std::result::Result<Value, ToolError> {
        let client = remote(self.tasks.task_admission_client(), false)?;
        if name == "ledgence_task_admission_inspect" {
            let a: TaskInspect = parse(arguments)?;
            let q = TaskAdmissionQuery {
                occurrence: TaskAdmissionOccurrence {
                    scope: self.scope.clone(),
                    task_id: a.task_id,
                    generation: a.generation,
                },
            };
            q.validate().map_err(ToolError::arguments)?;
            let reply = remote(client.inspect_task_admission(&q).await, false)?;
            reply.validate().map_err(|_| invalid_reply(false))?;
            if !reply.matches(&q) {
                return Err(invalid_reply(false));
            }
            encode(reply, false)
        } else {
            let a: TaskRelease = parse(arguments)?;
            let c = TaskAdmissionReleaseCommand {
                occurrence: TaskAdmissionOccurrence {
                    scope: self.scope.clone(),
                    task_id: a.task_id,
                    generation: a.generation,
                },
                source_id: a.source_id,
                expected_revision: a.expected_revision,
                request_id: a.request_id,
                reason: a.reason,
            };
            c.validate().map_err(ToolError::arguments)?;
            let reply = remote(client.release_task_admission(&c).await, true)?;
            reply.validate().map_err(|_| invalid_reply(true))?;
            if !reply.matches(&c) {
                return Err(invalid_reply(true));
            }
            encode(reply, true)
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mutation_opt_in_is_separate_and_read_only_cannot_override_it() {
        let d = test_dispatcher(false);
        assert!(!d.definitions().any(|t| is_mutation(t.name)));
        let d = d.with_resource_control().unwrap();
        let enabled = d
            .definitions()
            .filter(|t| is_mutation(t.name))
            .map(|t| t.name)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            enabled,
            [
                "ledgence_resource_put",
                "ledgence_admission_release",
                "ledgence_task_admission_release"
            ]
            .into_iter()
            .collect()
        );
        assert!(test_dispatcher(true).with_resource_control().is_err());
    }
    #[tokio::test]
    async fn mutation_cannot_bypass_hidden_tool_and_arguments_cannot_supply_scope() {
        let error = test_dispatcher(false)
            .call("ledgence_resource_put", &json!({}))
            .await
            .unwrap_err();
        assert_eq!(error.code, "unknown_tool");
        assert!(!error.outcome_unknown);
        assert!(
            parse::<Inspect>(
                &json!({"workflow_id":"w","origin_activation_id":"a","key":"k","scope":{}})
            )
            .is_err()
        );
        assert!(
            parse::<Put>(&json!({"name":"model","request_id":"r","policy":{},"reason":"why"}))
                .is_err()
        );
    }
}
