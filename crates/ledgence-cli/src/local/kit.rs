//! Validate a versioned distribution before making it local installation state.

use crate::args::invalid;
use ledgence_orchestration_api::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path},
};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Distribution {
    pub format: u32,
    pub version: String,
    pub image: String,
    pub python: String,
    pub platforms: Vec<String>,
}

impl Distribution {
    pub fn validate(&self) -> Result<()> {
        let digest = self.image.rsplit_once("@sha256:");
        if self.format != 1
            || self.version.is_empty()
            || self.python != "3.14"
            || self.platforms.is_empty()
            || self.platforms.iter().collect::<BTreeSet<_>>().len() != self.platforms.len()
            || self
                .platforms
                .iter()
                .any(|platform| !matches!(platform.as_str(), "linux/amd64" | "linux/arm64"))
            || digest.is_none_or(|(repository, digest)| {
                repository.is_empty()
                    || repository.contains('@')
                    || repository.chars().any(char::is_whitespace)
                    || digest.len() != 64
                    || !digest
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            })
        {
            return Err(invalid(
                "unsupported local distribution; expected format 1, an immutable sha256 image, CPython 3.14, and supported Linux platforms",
            ));
        }
        Ok(())
    }
}

pub(super) fn read(directory: &Path) -> Result<Distribution> {
    verify(directory)?;
    let value: Distribution = serde_json::from_slice(
        &fs::read(directory.join("distribution.json"))
            .map_err(|error| invalid(format!("cannot read distribution.json: {error}")))?,
    )
    .map_err(|error| invalid(format!("invalid distribution.json: {error}")))?;
    value.validate()?;
    let compose = fs::read_to_string(directory.join("compose.yaml"))
        .map_err(|error| invalid(format!("cannot read compose.yaml: {error}")))?;
    if !compose.contains(&value.image) || compose.contains("@LEDGENCE_IMAGE@") {
        return Err(invalid(
            "compose.yaml does not contain the distribution's pinned image",
        ));
    }
    Ok(value)
}

pub(super) fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn inventory(directory: &Path) -> Result<Vec<(String, String)>> {
    let sums = fs::read_to_string(directory.join("SHA256SUMS")).map_err(|error| {
        invalid(format!(
            "cannot read local distribution SHA256SUMS: {error}"
        ))
    })?;
    let mut names = BTreeSet::new();
    let mut entries = Vec::new();
    for line in sums.lines() {
        let (digest, name) = line
            .split_once("  ")
            .ok_or_else(|| invalid("invalid local distribution SHA256SUMS entry"))?;
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            || name.is_empty()
            || name.split('/').any(|part| matches!(part, "" | "." | ".."))
            || name.contains('\\')
            || name == "SHA256SUMS"
            || !Path::new(name)
                .components()
                .all(|part| matches!(part, Component::Normal(_)))
            || !names.insert(name.to_owned())
        {
            return Err(invalid(
                "invalid or duplicate path/hash in local distribution SHA256SUMS",
            ));
        }
        entries.push((digest.into(), name.into()));
    }
    if !names.contains("distribution.json") || !names.contains("compose.yaml") {
        return Err(invalid(
            "local distribution inventory must include distribution.json and compose.yaml",
        ));
    }
    Ok(entries)
}

fn regular_file(directory: &Path, name: &str) -> Result<()> {
    let mut path = directory.to_path_buf();
    for part in Path::new(name).components() {
        path.push(part);
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            invalid(format!("cannot inspect distribution file {name}: {error}"))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(invalid(format!(
                "distribution files must not be symlinks: {name}"
            )));
        }
    }
    if !path.is_file() {
        return Err(invalid(format!("distribution entry is not a file: {name}")));
    }
    Ok(())
}

fn verify(directory: &Path) -> Result<()> {
    regular_file(directory, "SHA256SUMS")?;
    for (expected, name) in inventory(directory)? {
        regular_file(directory, &name)?;
        let bytes = fs::read(directory.join(&name))
            .map_err(|error| invalid(format!("cannot read distribution file {name}: {error}")))?;
        if hash(&bytes) != expected {
            return Err(invalid(format!(
                "local distribution checksum mismatch: {name}; preserve the existing installation and restore its original kit"
            )));
        }
    }
    Ok(())
}

pub(super) fn copy(source: &Path, destination: &Path) -> Result<()> {
    // Copy only the verified inventory; never bring along unrelated local state.
    let mut names: Vec<_> = inventory(source)?
        .into_iter()
        .map(|(_, name)| name)
        .collect();
    names.push("SHA256SUMS".into());
    for name in names {
        regular_file(source, &name)?;
        let target = destination.join(&name);
        fs::create_dir_all(target.parent().expect("file has parent"))
            .and_then(|()| fs::copy(source.join(&name), &target).map(|_| ()))
            .map_err(|error| invalid(format!("cannot copy local distribution {name}: {error}")))?;
        fs::File::open(&target)
            .and_then(|file| file.sync_all())
            .and_then(|()| fs::File::open(target.parent().expect("file has parent")))
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                invalid(format!("cannot persist local distribution {name}: {error}"))
            })?;
    }
    // Detect source changes between validation and copy before committing state.
    verify(destination)
}
