//! Explicit credential files shared by the API-facing command groups.
use ledgence_adapter_http::{HttpCredentials, HttpTaskService};
use ledgence_orchestration_api::Result;
use std::path::Path;

pub fn take(arguments: &mut Vec<String>) -> Result<Option<HttpCredentials>> {
    let positions: Vec<_> = arguments
        .iter()
        .enumerate()
        .filter_map(|(index, value)| (value == "--credentials-file").then_some(index))
        .collect();
    match positions.as_slice() {
        [] => Ok(None),
        [index] => {
            let path = arguments
                .get(index + 1)
                .filter(|value| !value.is_empty() && !value.starts_with("--"))
                .ok_or_else(|| crate::args::invalid("missing --credentials-file value"))?;
            let credentials = HttpCredentials::from_file(Path::new(path))?;
            arguments.drain(*index..*index + 2);
            Ok(Some(credentials))
        }
        _ => Err(crate::args::invalid("duplicate --credentials-file")),
    }
}
pub fn task_client(server: &str, credentials: Option<&HttpCredentials>) -> Result<HttpTaskService> {
    let client = HttpTaskService::new(server)?;
    match credentials {
        Some(credentials) => client.with_credentials(credentials.clone()),
        None => Ok(client),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn credentials_are_explicit_and_removed_without_changing_operation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credential.json");
        std::fs::write(
            &path,
            br#"{"server":"https://example.com","token":"secret"}"#,
        )
        .unwrap();
        let mut args = vec![
            "approval".into(),
            "decide".into(),
            "--credentials-file".into(),
            path.to_str().unwrap().into(),
            "--file".into(),
            "decision.json".into(),
        ];
        let credential = take(&mut args).unwrap().unwrap();
        assert_eq!(args, ["approval", "decide", "--file", "decision.json"]);
        assert!(task_client("https://other.example", Some(&credential)).is_err());
        for args in [
            vec!["--credentials-file"],
            vec!["--credentials-file", "--file"],
            vec!["--credentials-file", "a", "--credentials-file", "b"],
        ] {
            assert!(take(&mut args.into_iter().map(str::to_owned).collect()).is_err());
        }
    }
}
