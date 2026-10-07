use super::{config::Config, invalid, operational};
use ledgence_orchestration_api::Result;
use ledgence_worker_api::ProgramManifest;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Component, Path, PathBuf},
};

const MAX_FILE: u64 = 64 * 1024 * 1024;
const MAX_TOTAL: u64 = 256 * 1024 * 1024;
const MAX_ENTRIES: usize = 4096;

pub(super) fn digest(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}
pub(super) fn relative(path: &Path, allow_dot: bool) -> Result<()> {
    if path.as_os_str().is_empty()
        || path.as_os_str().len() > 1024
        || path
            .components()
            .any(|c| !matches!(c, Component::Normal(_)) && !(allow_dot && c == Component::CurDir))
    {
        return Err(invalid(
            "build paths must be relative without parent traversal",
        ));
    }
    if !allow_dot && path == Path::new(".") {
        return Err(invalid(
            "include paths must name explicit files or directories",
        ));
    }
    Ok(())
}
fn excluded(name: &str) -> bool {
    name.starts_with('.')
        || matches!(
            name.to_ascii_lowercase().as_str(),
            "__pycache__"
                | "venv"
                | "env"
                | "node_modules"
                | "target"
                | "credentials"
                | "secrets"
                | "id_rsa"
                | "id_ed25519"
        )
        || name.ends_with(".pyc")
        || name.ends_with(".pyo")
}
fn reject_excluded(path: &Path) -> Result<()> {
    for component in path.components() {
        if component == Component::CurDir {
            continue;
        }
        if component.as_os_str().to_str().is_none_or(excluded) {
            return Err(invalid("explicit input names an excluded path"));
        }
    }
    Ok(())
}
fn portable(path: &Path) -> Result<String> {
    relative(path, false)?;
    let text = path
        .to_str()
        .ok_or_else(|| invalid("package paths must be portable ASCII"))?;
    if !text
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"._-/".contains(&b))
        || path
            .components()
            .any(|c| c.as_os_str().to_str().is_none_or(excluded))
    {
        return Err(invalid(
            "package paths contain excluded or nonportable names",
        ));
    }
    Ok(text.replace(std::path::MAIN_SEPARATOR, "/"))
}
/// Never inspect file contents until explicit inclusion and exclusion checks pass.
fn selected(root: &Path, relative: &Path) -> Result<PathBuf> {
    self::relative(relative, true)?;
    let mut path = root.to_owned();
    for part in relative.components() {
        if part == Component::CurDir {
            continue;
        }
        path.push(part);
        let metadata = fs::symlink_metadata(&path)
            .map_err(|_| invalid("an explicitly selected build input is missing"))?;
        if metadata.file_type().is_symlink() || (!metadata.is_dir() && !metadata.is_file()) {
            return Err(invalid(
                "build inputs cannot contain symlinks or special files",
            ));
        }
    }
    Ok(path)
}
pub(super) fn read_regular(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| invalid("cannot read a required build input"))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > limit {
        return Err(invalid(
            "build input must be a bounded regular file, without symlinks",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use nix::fcntl::OFlag;
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags((OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK).bits());
    }
    let file = options
        .open(path)
        .map_err(|_| invalid("cannot open a required build input"))?;
    if !file
        .metadata()
        .map_err(|_| invalid("cannot inspect build input"))?
        .is_file()
    {
        return Err(invalid("build input is not a regular file"));
    }
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("cannot read build input"))?;
    if bytes.len() as u64 > limit {
        return Err(invalid("build input exceeds size limit"));
    }
    Ok(bytes)
}
pub(super) struct Input {
    pub bytes: Vec<u8>,
    pub executable: bool,
}
pub(super) struct Plan {
    pub files: BTreeMap<String, Input>,
    directories: BTreeSet<String>,
    pub hashes: BTreeMap<String, String>,
    pub requirements: Option<Vec<u8>>,
    visited: usize,
}
impl Plan {
    pub fn create(
        root: &Path,
        config: &Config,
        config_bytes: &[u8],
        output: &Path,
    ) -> Result<Self> {
        let mut plan = Self {
            files: BTreeMap::new(),
            directories: BTreeSet::new(),
            hashes: BTreeMap::from([("config".into(), digest(config_bytes))]),
            requirements: None,
            visited: 0,
        };
        reject_excluded(&config.source.root)?;
        let source = selected(root, &config.source.root)?;
        if !source.is_dir() {
            return Err(invalid("source.root must be a directory"));
        }
        let manifest = config.manifest()?;
        let existing = root.join("ledgence-program.json");
        if existing
            .try_exists()
            .map_err(|_| invalid("cannot inspect existing program manifest"))?
        {
            verify_manifest(&existing, &manifest)?;
        }
        for include in &config.source.include {
            portable(include)?;
            let input = selected(&source, include)?;
            plan.collect(&input, include, output, &manifest)?;
        }
        for extra in &config.files {
            reject_excluded(&extra.source)?;
            portable(&extra.destination)?;
            let path = selected(root, &extra.source)?;
            if !path.is_file() {
                return Err(invalid("additional files must be regular files"));
            }
            plan.collect(&path, &extra.destination, output, &manifest)?;
        }
        if let Some(dependencies) = &config.dependencies {
            reject_excluded(&dependencies.requirements)?;
            let path = selected(root, &dependencies.requirements)?;
            let bytes = read_regular(&path, 1024 * 1024)?;
            validate_requirements(&bytes)?;
            plan.hashes.insert("requirements".into(), digest(&bytes));
            plan.requirements = Some(bytes);
        }
        if plan.files.is_empty() {
            return Err(invalid("no application files were selected"));
        }
        Ok(plan)
    }
    fn collect(
        &mut self,
        path: &Path,
        destination: &Path,
        output: &Path,
        manifest: &ProgramManifest,
    ) -> Result<()> {
        if path.starts_with(output) {
            return Err(invalid(
                "build output cannot be included as application input",
            ));
        }
        self.visited += 1;
        if self.visited > MAX_ENTRIES - 1 {
            return Err(invalid("too many selected build entries"));
        }
        let name = portable(destination)?;
        let metadata =
            fs::symlink_metadata(path).map_err(|_| invalid("cannot inspect selected input"))?;
        if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
            return Err(invalid(
                "selected application includes a symlink or special file",
            ));
        }
        self.check_collision(&name, metadata.is_dir())?;
        if metadata.is_dir() {
            self.directories.insert(name);
            let entries = fs::read_dir(path)
                .map_err(|_| invalid("cannot read selected application directory"))?;
            for entry in entries {
                let entry = entry
                    .map_err(|_| invalid("cannot enumerate selected application directory"))?;
                if entry.file_name().to_str().is_some_and(excluded) {
                    continue;
                }
                self.collect(
                    &entry.path(),
                    &destination.join(entry.file_name()),
                    output,
                    manifest,
                )?;
            }
        } else {
            if name == "ledgence-program.json" {
                verify_manifest(path, manifest)?;
                return Ok(());
            }
            if self.files.len() >= MAX_ENTRIES - 1 {
                return Err(invalid("too many build inputs"));
            }
            let bytes = read_regular(path, MAX_FILE)?;
            if self
                .files
                .values()
                .map(|v| v.bytes.len() as u64)
                .sum::<u64>()
                + bytes.len() as u64
                > MAX_TOTAL
            {
                return Err(invalid("build inputs exceed expanded package size limit"));
            }
            #[cfg(unix)]
            let executable = {
                use std::os::unix::fs::PermissionsExt;
                metadata.permissions().mode() & 0o111 != 0
            };
            #[cfg(not(unix))]
            let executable = false;
            self.hashes.insert(format!("source/{name}"), digest(&bytes));
            self.files.insert(name, Input { bytes, executable });
        }
        Ok(())
    }
    fn check_collision(&self, name: &str, directory: bool) -> Result<()> {
        for (previous, previous_directory) in self
            .directories
            .iter()
            .map(|p| (p, true))
            .chain(self.files.keys().map(|p| (p, false)))
        {
            let left = previous.to_ascii_lowercase();
            let right = name.to_ascii_lowercase();
            if (left == right && !(directory && previous_directory && previous == name))
                || (!directory && left.starts_with(&format!("{right}/")))
                || (!previous_directory && right.starts_with(&format!("{left}/")))
            {
                return Err(invalid(
                    "selected build inputs have colliding destination paths",
                ));
            }
            for (a, b) in previous.split('/').zip(name.split('/')) {
                if !a.eq_ignore_ascii_case(b) {
                    break;
                }
                if a != b {
                    return Err(invalid(
                        "selected build inputs have case-colliding directories",
                    ));
                }
            }
        }
        Ok(())
    }
    pub fn stage(&self, directory: &Path) -> Result<()> {
        for selected in &self.directories {
            fs::create_dir_all(directory.join("application").join(selected))
                .map_err(|_| operational("cannot stage selected application directory"))?;
        }
        for (name, input) in &self.files {
            let path = directory.join("application").join(name);
            fs::create_dir_all(path.parent().expect("staged file has a parent"))
                .map_err(|_| operational("cannot create build staging directory"))?;
            fs::write(&path, &input.bytes).map_err(|_| operational("cannot stage build input"))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(
                    path,
                    fs::Permissions::from_mode(if input.executable { 0o755 } else { 0o644 }),
                )
                .map_err(|_| operational("cannot set staged file permissions"))?;
            }
        }
        if let Some(requirements) = &self.requirements {
            fs::write(directory.join("requirements.txt"), requirements)
                .map_err(|_| operational("cannot stage requirements"))?;
        }
        Ok(())
    }
}
fn verify_manifest(path: &Path, expected: &ProgramManifest) -> Result<()> {
    let existing: ProgramManifest = serde_json::from_slice(&read_regular(path, 65536)?)
        .map_err(|_| invalid("existing program manifest is invalid"))?;
    if &existing != expected {
        return Err(invalid(
            "existing program manifest contradicts ledgence.toml",
        ));
    }
    Ok(())
}
/// pip's hash mode independently validates every active transitive dependency.
/// This grammar additionally rejects option directives, URLs and local inputs.
fn validate_requirements(bytes: &[u8]) -> Result<()> {
    let text = std::str::from_utf8(bytes).map_err(|_| invalid("requirements must be UTF-8"))?;
    let mut logical = String::new();
    for line in text.lines() {
        let line = line.split_once('#').map_or(line, |(left, _)| left).trim();
        if line.is_empty() {
            continue;
        }
        let continued = line.ends_with('\\');
        logical.push_str(line.strip_suffix('\\').unwrap_or(line));
        logical.push(' ');
        if continued {
            continue;
        }
        let Some((requirement, hashes)) = logical.split_once("--hash=sha256:") else {
            return Err(invalid(
                "every requirement must use == and at least one --hash=sha256 hash",
            ));
        };
        let requirement = requirement.split(';').next().unwrap_or("").trim();
        let Some((name, version)) = requirement.split_once("==") else {
            return Err(invalid("requirements must be exactly pinned with =="));
        };
        if name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-[],".contains(&b))
            || version.is_empty()
            || !version
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-+!".contains(&b))
        {
            return Err(invalid(
                "requirements must contain package names and exact versions; URLs, paths and pip options are unsupported",
            ));
        }
        for hash in hashes.split("--hash=sha256:") {
            let hash = hash.trim();
            if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(invalid("requirements hashes must be 64 hexadecimal digits"));
            }
        }
        logical.clear();
    }
    if !logical.trim().is_empty() {
        return Err(invalid("unfinished requirements continuation"));
    }
    Ok(())
}
pub(super) fn prepared_hashes(root: &Path) -> Result<BTreeMap<String, String>> {
    fn visit(root: &Path, path: &Path, hashes: &mut BTreeMap<String, String>) -> Result<()> {
        for entry in
            fs::read_dir(path).map_err(|_| operational("cannot inspect prepared output"))?
        {
            let entry = entry.map_err(|_| operational("cannot inspect prepared output"))?;
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|_| operational("cannot inspect prepared output"))?;
            if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
                return Err(invalid("builder emitted a link or special file"));
            }
            if metadata.is_dir() {
                visit(root, &entry.path(), hashes)?;
            } else {
                let key = entry
                    .path()
                    .strip_prefix(root)
                    .expect("visited child")
                    .to_str()
                    .ok_or_else(|| invalid("invalid prepared path"))?
                    .replace(std::path::MAIN_SEPARATOR, "/");
                hashes.insert(key, digest(&read_regular(&entry.path(), MAX_FILE)?));
                if hashes.len() > MAX_ENTRIES {
                    return Err(invalid("builder emitted too many files"));
                }
            }
        }
        Ok(())
    }
    let mut hashes = BTreeMap::new();
    visit(root, root, &mut hashes)?;
    Ok(hashes)
}
pub(super) fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(|_| operational("cannot synchronize build output"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn requires_hashes_for_exact_pins_and_rejects_directives() {
        let hash = "a".repeat(64);
        for text in [
            format!("package==1.0.0 --hash=sha256:{hash}\n"),
            format!("package[extra]==1.0.0 ; python_version >= '3.11' \\\n --hash=sha256:{hash}\n"),
            String::from("# no dependencies\n"),
        ] {
            validate_requirements(text.as_bytes()).unwrap();
        }
        for text in [
            "package>=1",
            "package==1",
            "--index-url https://secret.example/",
            "-r nested.txt",
            "--find-links ./wheels",
            "-e ./local",
            "package @ https://example/file.whl",
            "package==1 --hash=sha256:no",
            "package==1 \\",
        ] {
            assert!(validate_requirements(text.as_bytes()).is_err(), "{text}");
        }
    }
    #[test]
    fn excludes_credentials_before_reading_requirements() {
        let fixture = super::super::tests::Fixture::new();
        let config_path = fixture.root.join("ledgence.toml");
        let mut config = fs::read_to_string(&config_path).unwrap();
        config.push_str("\n[dependencies]\nrequirements = '.env'\n");
        fs::write(&config_path, config).unwrap();
        fs::write(fixture.root.join(".env"), "NEVER_READ").unwrap();
        let (config, bytes) = Config::read(&config_path).unwrap();
        assert!(
            Plan::create(
                &fixture.root,
                &config,
                &bytes,
                &fixture.root.join(".ledgence/prepared")
            )
            .is_err()
        );
    }
}
