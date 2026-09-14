use ledgence_adapter_postgres::MigrationOptions;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, time::Duration};

pub const HELP: &str = "Ledgence orchestrator\n\nCommands:\n  migrate [--timeout-ms 600000]\n  serve --store DIR_OR_URL [--bind 127.0.0.1:8080] [--delivery-config FILE]\n\nDATABASE_URL is required. Migrations are explicit; serve verifies the schema.\nMigration timeout is 1..2147483647 ms after connection (default: ten minutes).\nInterrupted migrations may have committed earlier steps; rerun migrate to reconcile.\nThe listener uses HTTP/1.1; an external proxy can provide HTTPS.\nFirst SIGINT/SIGTERM drains operations; a second signal forces a nonzero exit.\n";

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Help,
    Migrate {
        options: MigrationOptions,
    },
    Serve {
        bind: SocketAddr,
        store: String,
        delivery_config: Option<PathBuf>,
    },
}

pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Command, String> {
    let mut args = args.into_iter();
    let Some(command) = args.next() else {
        return Ok(Command::Help);
    };
    if ["--help", "-h", "help"].contains(&command.as_str()) {
        if args.next().is_some() {
            return Err("help does not take options".into());
        }
        return Ok(Command::Help);
    }
    if !["migrate", "serve"].contains(&command.as_str()) {
        return Err("unknown command; use --help".into());
    }
    let mut options = HashMap::new();
    while let Some(key) = args.next() {
        if !key.starts_with("--") {
            return Err("expected a --name value option".into());
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {key}"))?;
        if value.is_empty() || value.starts_with("--") {
            return Err(format!("missing value for {key}"));
        }
        if options.insert(key.clone(), value).is_some() {
            return Err(format!("duplicate option {key}"));
        }
    }
    let parsed = if command == "migrate" {
        let mut migration = MigrationOptions::default();
        if let Some(value) = options.remove("--timeout-ms") {
            if !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err("timeout-ms must be a positive integer number of milliseconds".into());
            }
            let milliseconds = value
                .parse::<u64>()
                .map_err(|_| "timeout-ms exceeds the supported range")?;
            migration.timeout = Duration::from_millis(milliseconds);
        }
        migration.validate().map_err(|error| error.to_string())?;
        Command::Migrate { options: migration }
    } else {
        let store = options.remove("--store").ok_or("missing --store")?;
        let bind = options
            .remove("--bind")
            .unwrap_or_else(|| "127.0.0.1:8080".into())
            .parse()
            .map_err(|_| "bind must be an IP address and port")?;
        let delivery_config = options.remove("--delivery-config").map(PathBuf::from);
        #[cfg(not(feature = "sqs"))]
        if delivery_config.is_some() {
            return Err("--delivery-config requires a binary built with the sqs feature".into());
        }
        Command::Serve {
            bind,
            store,
            delivery_config,
        }
    };
    if !options.is_empty() {
        return Err("unknown option; use --help".into());
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(args: &[&str]) -> Result<Command, String> {
        parse(args.iter().map(|arg| (*arg).to_owned()))
    }

    #[test]
    fn commands_require_explicit_store_and_migration() {
        assert_eq!(arguments(&[]).unwrap(), Command::Help);
        assert_eq!(
            arguments(&["migrate"]).unwrap(),
            Command::Migrate {
                options: MigrationOptions::default()
            }
        );
        assert_eq!(
            arguments(&["serve", "--store", "./programs"]).unwrap(),
            Command::Serve {
                bind: "127.0.0.1:8080".parse().unwrap(),
                store: "./programs".into(),
                delivery_config: None,
            }
        );
        assert!(arguments(&["serve"]).is_err());
        assert!(arguments(&["migrate", "--store", "./programs"]).is_err());
        assert!(arguments(&["serve", "--store", "--bind", "127.0.0.1:8080"]).is_err());
        assert!(arguments(&["serve", "--store", "a", "--store", "b"]).is_err());
        assert!(arguments(&["serve", "--store", "a", "--bind", "host:80"]).is_err());
        assert!(arguments(&["serve", "--store", "a", "--migrate", "yes"]).is_err());
    }
    #[test]
    fn migration_budget_accepts_only_bounded_integer_milliseconds() {
        for value in ["1", "600000", "2147483647", "00025"] {
            assert_eq!(
                arguments(&["migrate", "--timeout-ms", value]).unwrap(),
                Command::Migrate {
                    options: MigrationOptions {
                        timeout: Duration::from_millis(value.parse().unwrap()),
                    }
                }
            );
        }
        for value in [
            "0",
            "-1",
            "+1",
            "1.5",
            " 1",
            "1 ",
            "NaN",
            "true",
            "2147483648",
            "18446744073709551616",
            "",
            "١",
        ] {
            assert!(
                arguments(&["migrate", "--timeout-ms", value]).is_err(),
                "{value:?}"
            );
        }
        assert!(arguments(&["migrate", "--timeout-ms"]).is_err());
        assert!(arguments(&["migrate", "--timeout-ms", "100", "--timeout-ms", "200"]).is_err());
        assert!(arguments(&["serve", "--store", "programs", "--timeout-ms", "100"]).is_err());
    }
    #[test]
    fn delivery_configuration_is_explicit_and_feature_gated() {
        let result = arguments(&[
            "serve",
            "--store",
            "programs",
            "--delivery-config",
            "delivery.json",
        ]);
        #[cfg(feature = "sqs")]
        assert!(
            matches!(result, Ok(Command::Serve { delivery_config: Some(path), .. }) if path == std::path::Path::new("delivery.json"))
        );
        #[cfg(not(feature = "sqs"))]
        assert!(result.unwrap_err().contains("sqs feature"));
        assert!(arguments(&["migrate", "--delivery-config", "delivery.json"]).is_err());
    }
}
