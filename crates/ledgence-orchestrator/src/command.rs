use std::{collections::HashMap, net::SocketAddr};

pub const HELP: &str = "Ledgence orchestrator\n\nCommands:\n  migrate\n  serve --store DIR_OR_URL [--bind 127.0.0.1:8080]\n\nDATABASE_URL is required. Migrations are explicit; serve verifies the schema.\nThe listener uses HTTP/1.1; an external proxy can provide HTTPS.\nFirst SIGINT/SIGTERM drains operations; a second signal forces a nonzero exit.\n";

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Help,
    Migrate,
    Serve { bind: SocketAddr, store: String },
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
        Command::Migrate
    } else {
        let store = options.remove("--store").ok_or("missing --store")?;
        let bind = options
            .remove("--bind")
            .unwrap_or_else(|| "127.0.0.1:8080".into())
            .parse()
            .map_err(|_| "bind must be an IP address and port")?;
        Command::Serve { bind, store }
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
        assert_eq!(arguments(&["migrate"]).unwrap(), Command::Migrate);
        assert_eq!(
            arguments(&["serve", "--store", "./programs"]).unwrap(),
            Command::Serve {
                bind: "127.0.0.1:8080".parse().unwrap(),
                store: "./programs".into(),
            }
        );
        assert!(arguments(&["serve"]).is_err());
        assert!(arguments(&["migrate", "--store", "./programs"]).is_err());
        assert!(arguments(&["serve", "--store", "--bind", "127.0.0.1:8080"]).is_err());
        assert!(arguments(&["serve", "--store", "a", "--store", "b"]).is_err());
        assert!(arguments(&["serve", "--store", "a", "--bind", "host:80"]).is_err());
        assert!(arguments(&["serve", "--store", "a", "--migrate", "yes"]).is_err());
    }
}
