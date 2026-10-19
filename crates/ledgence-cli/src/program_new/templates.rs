use super::{Kind, Options, operational};
use ledgence_orchestration_api::Result;
use std::collections::BTreeMap;

pub(super) fn render(options: &Options) -> Result<BTreeMap<&'static str, String>> {
    let program = match options.kind {
        Kind::Task => include_str!("task.py"),
        Kind::Workflow => include_str!("workflow.py"),
    };
    let tests = match options.kind {
        Kind::Task => include_str!("task_test.py"),
        Kind::Workflow => include_str!("workflow_test.py"),
    };
    let quoted = |value: &str| {
        serde_json::to_string(value).map_err(|_| operational("cannot encode program configuration"))
    };
    let (platform, python, image) = options.target.as_ref().map_or(
        (
            "REPLACE_WITH_LINUX_PLATFORM",
            "REPLACE_WITH_PYTHON_MAJOR_MINOR",
            "REPLACE_WITH_WORKER_IMAGE_DIGEST",
        ),
        |target| {
            (
                target.platform.as_str(),
                target.python.as_str(),
                target.image.as_str(),
            )
        },
    );
    let config = format!(
        "schema_version = 1\n\n[program]\nid = {}\nversion = \"0.1.0\"\nhandler = \"program:handle\"\nkind = \"{}\"\n\n[source]\nroot = \".\"\ninclude = [\"program.py\"]\n\n[dependencies]\nrequirements = \"requirements.txt\"\n\n[target]\n# Match the actual worker; these values are never inferred from this machine.\nplatform = {}\npython = {}\nprotocol = 3\nimage = {}\n",
        quoted(&options.program.id)?,
        options.kind.as_str(),
        quoted(platform)?,
        quoted(python)?,
        quoted(image)?,
    );
    let replace = |template: &str| {
        template
            .replace("{{PROGRAM_NAME}}", &options.program.id)
            .replace("{{LEDGENCE_VERSION}}", env!("CARGO_PKG_VERSION"))
    };
    Ok(BTreeMap::from([
        ("program.py", program.to_owned()),
        ("tests/test_program.py", tests.to_owned()),
        ("ledgence.toml", config),
        ("pyproject.toml", replace(include_str!("pyproject.toml.template"))),
        ("README.md", replace(include_str!("README.md.template"))),
        ("requirements.txt", "# Production dependencies only: exact versions and SHA-256 hashes, including transitive dependencies.\n# Keep ledgence-worker out of this file; the deployed worker provides its helper.\n".to_owned()),
        (".gitignore", ".venv/\n.ledgence/\n__pycache__/\n*.py[cod]\n.env\n.env.*\n".to_owned()),
    ]))
}
