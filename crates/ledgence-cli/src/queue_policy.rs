//! Durable concurrency policy commands on the unified CLI.
use crate::{admission::read_saved, args::invalid, encode};
use ledgence_adapter_http::HttpTaskService;
use ledgence_orchestration_api::*;
use std::{collections::BTreeMap, path::PathBuf};

pub const FORMAT: &str = "ledgence.queue-policy.command.v1";

#[derive(Debug)]
pub enum Operation {
    Inspect(QueuePolicyQuery),
    List(QueuePolicyList),
    Holds(QueueHoldList),
    Put(Scope, PathBuf),
    Release(Scope, PathBuf),
}

pub fn parse(verb: &str, scope: Scope, opts: &mut BTreeMap<String, String>) -> Result<Operation> {
    fn take(opts: &mut BTreeMap<String, String>, flag: &str) -> Result<String> {
        opts.remove(flag)
            .ok_or_else(|| invalid(format!("missing {flag}")))
    }
    fn page(opts: &mut BTreeMap<String, String>) -> Result<(Option<String>, u32)> {
        let after = opts.remove("--after");
        let limit = opts
            .remove("--limit")
            .map(|n| n.parse().map_err(|_| invalid("invalid page size")))
            .transpose()?
            .unwrap_or(50);
        Ok((after, limit))
    }
    match verb {
        "inspect" => {
            let query = QueuePolicyQuery {
                scope,
                queue: take(opts, "--queue")?,
            };
            query.validate()?;
            Ok(Operation::Inspect(query))
        }
        "list" => {
            let (after, limit) = page(opts)?;
            let query = QueuePolicyList {
                scope,
                after,
                limit,
            };
            query.validate()?;
            Ok(Operation::List(query))
        }
        "holds" => {
            let (after, limit) = page(opts)?;
            let include_released = opts
                .remove("--include-released")
                .map(|value| {
                    value
                        .parse::<bool>()
                        .map_err(|_| invalid("use true or false for --include-released"))
                })
                .transpose()?
                .unwrap_or(false);
            let query = QueueHoldList {
                scope,
                queue: take(opts, "--queue")?,
                after,
                limit,
                include_released,
            };
            query.validate()?;
            Ok(Operation::Holds(query))
        }
        "put" => Ok(Operation::Put(scope, take(opts, "--file")?.into())),
        "release" => Ok(Operation::Release(scope, take(opts, "--file")?.into())),
        _ => Err(invalid("unknown queue operation")),
    }
}

pub async fn execute(client: &HttpTaskService, operation: Operation) -> Result<Vec<u8>> {
    match operation {
        Operation::Inspect(q) => encode(client.inspect_queue_policy(&q).await?),
        Operation::List(q) => encode(client.list_queue_policies(&q).await?),
        Operation::Holds(q) => encode(client.list_queue_holds(&q).await?),
        Operation::Put(scope, file) => {
            let body = read_saved(file, client.base_url().to_owned(), FORMAT).await?;
            let command: QueuePolicyPut = decode_unique_json(body.as_bytes(), 16 * 1024)?;
            command.validate()?;
            if command.scope != scope {
                return Err(invalid("saved queue policy belongs to another scope"));
            }
            encode(client.put_queue_policy(&command).await?)
        }
        Operation::Release(scope, file) => {
            let body = read_saved(file, client.base_url().to_owned(), FORMAT).await?;
            let command: QueueHoldRelease = decode_unique_json(body.as_bytes(), 16 * 1024)?;
            command.validate()?;
            if command.scope != scope {
                return Err(invalid("saved queue hold belongs to another scope"));
            }
            encode(client.release_queue_hold(&command).await?)
        }
    }
}
