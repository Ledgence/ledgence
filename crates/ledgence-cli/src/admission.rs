//! Scope-bound resource administration; mutations reuse an operator-saved identity.
use crate::{
    args::{Command, invalid},
    encode, submission,
};
use ledgence_adapter_http::HttpTaskService;
use ledgence_orchestration_api::*;
use serde::Deserialize;
use std::{collections::BTreeMap, path::PathBuf};
#[derive(Debug)]
pub enum Operation {
    List(ResourceListQuery),
    Resource(ResourceQuery),
    Inspect(AdmissionQuery),
    TaskInspect(TaskAdmissionQuery),
    Put { scope: Scope, file: PathBuf },
    Release { scope: Scope, file: PathBuf },
}
pub fn parse(group: &str, mut args: impl Iterator<Item = String>) -> Result<Command> {
    let verb = args
        .next()
        .ok_or_else(|| invalid("missing resource/admission operation"))?;
    let mut options = BTreeMap::new();
    while let Some(key) = args.next() {
        if !key.starts_with("--") {
            return Err(invalid("expected --name value"));
        }
        let value = args
            .next()
            .ok_or_else(|| invalid(format!("missing value for {key}")))?;
        if value.starts_with("--") || options.insert(key, value).is_some() {
            return Err(invalid("missing value or duplicate option"));
        }
    }
    let task_subject = options.contains_key("--task");
    let mut take = |key: &str| {
        options
            .remove(key)
            .ok_or_else(|| invalid(format!("missing {key}")))
    };
    let server = take("--server")?;
    let scope = Scope {
        tenant_id: take("--tenant")?,
        namespace: take("--namespace")?,
    };
    scope.validate()?;
    let operation = match (group, verb.as_str()) {
        ("resource", "put") => Operation::Put {
            scope,
            file: take("--file")?.into(),
        },
        ("admission", "release") => Operation::Release {
            scope,
            file: take("--file")?.into(),
        },
        ("resource", "inspect") => {
            let q = ResourceQuery {
                scope,
                name: take("--name")?,
            };
            q.validate()?;
            Operation::Resource(q)
        }
        ("admission", "inspect") if task_subject => {
            let q = TaskAdmissionQuery {
                occurrence: TaskAdmissionOccurrence {
                    scope,
                    task_id: take("--task")?,
                    generation: take("--generation")?
                        .parse()
                        .map_err(|_| invalid("invalid task admission generation"))?,
                },
            };
            q.validate()?;
            Operation::TaskInspect(q)
        }
        ("admission", "inspect") => {
            let q = AdmissionQuery {
                occurrence: AdmissionOccurrence {
                    scope,
                    workflow_id: take("--workflow")?,
                    origin_activation_id: take("--activation")?,
                    key: take("--key")?,
                },
            };
            q.validate()?;
            Operation::Inspect(q)
        }
        ("resource", "list") => {
            let q = ResourceListQuery {
                scope,
                after: options.remove("--after"),
                limit: options
                    .remove("--limit")
                    .map(|v| v.parse().map_err(|_| invalid("invalid page size")))
                    .transpose()?
                    .unwrap_or(50),
            };
            q.validate()?;
            Operation::List(q)
        }
        _ => return Err(invalid("unknown resource/admission operation")),
    };
    if !options.is_empty() {
        return Err(invalid("unknown resource/admission option"));
    }
    Ok(Command::Admission { server, operation })
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedCommand {
    format: String,
    base_url: String,
    body: String,
}
fn restore(bytes: &[u8], endpoint: &str) -> Result<String> {
    let saved: SavedCommand = decode_unique_json(bytes, ADMISSION_COMMAND_MAX_BYTES * 2)?;
    if saved.format != "ledgence.admission.command.v1" || saved.base_url != endpoint {
        return Err(invalid(
            "saved admission command format or destination does not match this client",
        ));
    }
    Ok(saved.body)
}
async fn read(file: PathBuf, endpoint: String) -> Result<String> {
    tokio::task::spawn_blocking(move || {
        let bytes = submission::read_bytes(
            &file,
            ADMISSION_COMMAND_MAX_BYTES * 2,
            "saved admission command",
        )?;
        restore(&bytes, &endpoint)
    })
    .await
    .map_err(|_| ContractError::Unavailable("admission command preparation failed".into()))?
}
pub async fn execute(client: &HttpTaskService, operation: Operation) -> Result<Vec<u8>> {
    match operation {
        Operation::List(q) => encode(AdmissionClient::list_resources(client, &q).await?),
        Operation::Resource(q) => encode(AdmissionClient::resource(client, &q).await?),
        Operation::Inspect(q) => encode(AdmissionClient::inspect_admission(client, &q).await?),
        Operation::TaskInspect(q) => {
            encode(TaskAdmissionClient::inspect_task_admission(client, &q).await?)
        }
        Operation::Put { scope, file } => {
            let body = read(file, client.base_url().to_owned()).await?;
            let c: ResourcePutCommand =
                decode_unique_json(body.as_bytes(), ADMISSION_COMMAND_MAX_BYTES)?;
            c.validate()?;
            if c.scope != scope {
                return Err(invalid("saved resource command belongs to another scope"));
            }
            encode(AdmissionClient::put_resource(client, &c).await?)
        }
        Operation::Release { scope, file } => {
            let body = read(file, client.base_url().to_owned()).await?;
            #[derive(Deserialize)]
            #[serde(untagged)]
            enum Release {
                Task(TaskAdmissionReleaseCommand),
                Workflow(AdmissionReleaseCommand),
            }
            match decode_unique_json(body.as_bytes(), ADMISSION_COMMAND_MAX_BYTES)? {
                Release::Task(c) => {
                    c.validate()?;
                    if c.occurrence.scope != scope {
                        return Err(invalid("saved admission command belongs to another scope"));
                    }
                    encode(TaskAdmissionClient::release_task_admission(client, &c).await?)
                }
                Release::Workflow(c) => {
                    c.validate()?;
                    if c.occurrence.scope != scope {
                        return Err(invalid("saved admission command belongs to another scope"));
                    }
                    encode(AdmissionClient::release_admission(client, &c).await?)
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn saved_command_cannot_rebind_endpoint_or_omit_envelope_fields() {
        let original = " { \"request_id\": \"same-id\" } ";
        let bytes=serde_json::to_vec(&serde_json::json!({"format":"ledgence.admission.command.v1","base_url":"http://localhost:8080","body":original})).unwrap();
        assert_eq!(restore(&bytes, "http://localhost:8080").unwrap(), original);
        assert!(restore(&bytes, "http://localhost:8080/").is_err());
        assert!(restore(b"{}", "http://localhost:8080").is_err());
    }
    #[test]
    fn parser_preserves_origin_and_rejects_ambiguous_scope_or_options() {
        let base = [
            "inspect",
            "--server",
            "http://localhost:8080",
            "--tenant",
            "acme",
            "--namespace",
            "demo",
            "--workflow",
            "wf",
            "--activation",
            "origin",
            "--key",
            "call",
        ];
        let Command::Admission {
            operation: Operation::Inspect(q),
            ..
        } = parse("admission", base.map(str::to_owned).into_iter()).unwrap()
        else {
            panic!()
        };
        assert_eq!(q.occurrence.origin_activation_id, "origin");
        for suffix in [["--tenant", "elsewhere"], ["--unknown", "value"]] {
            assert!(
                parse(
                    "admission",
                    base.into_iter().chain(suffix).map(str::to_owned)
                )
                .is_err()
            );
        }
    }
}
