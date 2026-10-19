use super::*;

#[test]
fn target_flags_are_all_or_nothing_and_options_are_strict() {
    for arguments in [
        vec![],
        vec!["."],
        vec!["new/.."],
        vec!["new/."],
        vec!["new", "--kind", "agent"],
        vec!["new", "--name", "trailing-"],
        vec!["new", "--name", "Leading"],
        vec!["new", "--kind"],
        vec!["new", "--kind", "task", "--kind", "task"],
        vec!["new", "--force"],
        vec!["new", "--platform", "linux/amd64"],
        vec!["new", "--python", "3.14", "--image", "example"],
        vec!["new", "extra"],
    ] {
        assert!(
            Options::parse(arguments.iter().map(|value| (*value).into()).collect()).is_err(),
            "{arguments:?}"
        );
    }
}

#[test]
fn task_default_and_explicit_project_name_are_distinct_from_directory() {
    let default = Options::parse(vec!["parent/greeting".into()]).unwrap();
    assert_eq!(default.program.id, "greeting");
    assert_eq!(default.kind, Kind::Task);
    assert!(default.target.is_none());
    let named = Options::parse(vec![
        "parent/My project".into(),
        "--name".into(),
        "hello-world".into(),
        "--kind".into(),
        "workflow".into(),
    ])
    .unwrap();
    assert_eq!(named.program.id, "hello-world");
    assert_eq!(named.kind, Kind::Workflow);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn atomic_publication_does_not_replace_a_racing_destination() {
    let root = tempfile::tempdir().unwrap();
    let staged = root.path().join("staged");
    let destination = root.path().join("destination");
    fs::create_dir(&staged).unwrap();
    fs::write(staged.join("new.py"), "new").unwrap();
    fs::create_dir(&destination).unwrap();
    // An empty directory created after the preflight check is still user data.
    assert!(publish_new(&staged, &destination).is_err());
    assert!(destination.is_dir());
    assert_eq!(fs::read_dir(&destination).unwrap().count(), 0);
    assert_eq!(fs::read_to_string(staged.join("new.py")).unwrap(), "new");
    fs::write(destination.join("existing.txt"), "preserve").unwrap();
    assert!(publish_new(&staged, &destination).is_err());
    assert_eq!(
        fs::read_to_string(destination.join("existing.txt")).unwrap(),
        "preserve"
    );
}

#[test]
fn rendered_targets_obey_the_real_build_configuration_contract() {
    for kind in ["task", "workflow"] {
        let arguments = vec![
            "greeting".into(),
            "--kind".into(),
            kind.into(),
            "--platform".into(),
            "linux/arm64".into(),
            "--python".into(),
            "3.14".into(),
            "--image".into(),
            format!("example/runtime@sha256:{}", "a".repeat(64)),
        ];
        let options = Options::parse(arguments).unwrap();
        let files = templates::render(&options).unwrap();
        crate::program_build::validate_configuration(&files["ledgence.toml"]).unwrap();
        let pending =
            Options::parse(vec!["greeting".into(), "--kind".into(), kind.into()]).unwrap();
        let files = templates::render(&pending).unwrap();
        assert!(crate::program_build::validate_configuration(&files["ledgence.toml"]).is_err());
    }
}
