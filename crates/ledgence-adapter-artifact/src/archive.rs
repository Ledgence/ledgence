use crate::{
    ArtifactLimits,
    error::{AdapterError, Result},
    filesystem::{executable_bits, open_regular},
};
use ledgence_worker_api::{ProgramDescriptor, ProgramManifest};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, Cursor, Read, Write},
    path::{Path, PathBuf},
};
use zip::ZipArchive;

pub(crate) struct ArchivePlan {
    archive: ZipArchive<Cursor<Vec<u8>>>,
    members: Vec<Member>,
    pub manifest: ProgramManifest,
    pub expanded_bytes: u64,
}
struct Member {
    name: String,
    size: u64,
    directory: bool,
    executable: u32,
}

pub(crate) fn digest_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) fn inspect(
    bytes: Vec<u8>,
    descriptor: &ProgramDescriptor,
    limits: &ArtifactLimits,
) -> Result<ArchivePlan> {
    let plan = inspect_for_publication(bytes, descriptor, limits)?;
    plan.manifest.validate_host()?;
    Ok(plan)
}

pub(crate) fn inspect_for_publication(
    bytes: Vec<u8>,
    descriptor: &ProgramDescriptor,
    limits: &ArtifactLimits,
) -> Result<ArchivePlan> {
    limits.descriptor(descriptor)?;
    if bytes.len() as u64 != descriptor.size || digest_hex(&bytes) != descriptor.digest.hex() {
        return Err(AdapterError::Invalid(
            "archive size or SHA-256 does not match descriptor".into(),
        ));
    }
    // zip-rs indexes by filename and can hide duplicate raw central-directory
    // records. Check the original directory before allowing it to allocate/parse.
    let raw_names = check_directory(&bytes, limits.max_entries)?;
    let mut archive = ZipArchive::new(Cursor::new(bytes))?;
    if archive.offset() != 0
        || archive.len() != raw_names.len()
        || archive.has_overlapping_files()?
    {
        return Err(AdapterError::Invalid(
            "overlapping or inconsistent ZIP records".into(),
        ));
    }
    let mut members = Vec::new();
    let mut paths = BTreeMap::new();
    let mut expanded_bytes = 0u64;
    let mut manifest = None;
    for (index, raw_name) in raw_names.into_iter().enumerate() {
        let mut entry = archive.by_index(index)?;
        if entry.name() != raw_name || entry.encrypted() {
            return Err(AdapterError::Invalid(
                "unsupported ZIP name encoding or encryption".into(),
            ));
        }
        let directory = entry.is_dir();
        let name = safe_name(entry.name(), directory)?;
        let unix_mode = entry.unix_mode().unwrap_or(0);
        let mode = unix_mode & 0o170000;
        if entry.is_symlink()
            || !matches!(mode, 0 | 0o040000 | 0o100000)
            || (mode == 0o040000 && !directory)
            || (mode == 0o100000 && directory)
        {
            return Err(AdapterError::Invalid(
                "only regular files and directories are allowed".into(),
            ));
        }
        if paths.insert(name.to_ascii_lowercase(), directory).is_some() {
            return Err(AdapterError::Invalid(
                "duplicate or case-colliding ZIP path".into(),
            ));
        }
        let size = entry.size();
        if (directory && size != 0) || size > limits.max_file_bytes {
            return Err(AdapterError::Limit(
                "invalid directory size or file expansion".into(),
            ));
        }
        expanded_bytes = expanded_bytes
            .checked_add(size)
            .ok_or_else(|| AdapterError::Limit("expanded size overflow".into()))?;
        if expanded_bytes > limits.max_expanded_bytes {
            return Err(AdapterError::Limit("total ZIP expansion".into()));
        }
        if name == "ledgence-program.json" {
            if directory || size > limits.max_manifest_bytes {
                return Err(AdapterError::Invalid("invalid root manifest".into()));
            }
            let data = read_bounded(&mut entry, limits.max_manifest_bytes)?;
            let value: ProgramManifest = serde_json::from_slice(&data)?;
            value.validate()?;
            if value.program != descriptor.program {
                return Err(AdapterError::Invalid(
                    "manifest program differs from descriptor".into(),
                ));
            }
            manifest = Some(value);
        }
        members.push(Member {
            name,
            size,
            directory,
            executable: unix_mode & 0o111,
        });
    }
    let mut all_paths: BTreeMap<String, (String, bool)> = BTreeMap::new();
    for member in &members {
        let mut path = Some((Path::new(&member.name), member.directory));
        while let Some((current, directory)) = path.filter(|(p, _)| !p.as_os_str().is_empty()) {
            let spelling = current.to_string_lossy().into_owned();
            let key = spelling.to_ascii_lowercase();
            if let Some((existing, existing_directory)) = all_paths.get(&key) {
                if existing != &spelling || *existing_directory != directory {
                    return Err(AdapterError::Invalid(
                        "file/directory or case-colliding ZIP path".into(),
                    ));
                }
            } else {
                all_paths.insert(key, (spelling, directory));
            }
            path = current.parent().map(|parent| (parent, true));
        }
        let mut parent = Path::new(&member.name).parent();
        while let Some(path) = parent.filter(|p| !p.as_os_str().is_empty()) {
            if paths.get(&path.to_string_lossy().to_ascii_lowercase()) == Some(&false) {
                return Err(AdapterError::Invalid(
                    "file/directory path collision".into(),
                ));
            }
            parent = path.parent();
        }
    }
    let manifest = manifest
        .ok_or_else(|| AdapterError::Invalid("missing root ledgence-program.json".into()))?;
    Ok(ArchivePlan {
        archive,
        members,
        manifest,
        expanded_bytes,
    })
}

impl ArchivePlan {
    pub fn extract(&mut self, root: &Path) -> Result<()> {
        self.extract_with_create(root, |path| File::create_new(path))
    }
    pub(crate) fn extract_with_create(
        &mut self,
        root: &Path,
        mut create: impl FnMut(&Path) -> io::Result<File>,
    ) -> Result<()> {
        fs::create_dir(root)?;
        for (index, member) in self.members.iter().enumerate() {
            let target = root.join(&member.name);
            if member.directory {
                fs::create_dir_all(&target)?;
                continue;
            }
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut output = create(&target)?;
            let mut entry = self.archive.by_index(index)?;
            let copied = io::copy(&mut (&mut entry).take(member.size + 1), &mut output)?;
            if copied != member.size {
                return Err(AdapterError::Invalid(
                    "ZIP file size differs from header".into(),
                ));
            }
            output.flush()?;
            make_readonly_file(&target, member.executable)?;
            output.sync_all()?;
        }
        readonly_directories(root)?;
        Ok(())
    }
    /// Verify persisted materialization against the verified archive, including
    /// absence of extra files and symlinks. Cache corruption is never a hit.
    pub fn verify(&mut self, root: &Path) -> Result<()> {
        let mut expected = BTreeSet::new();
        for (index, member) in self.members.iter().enumerate() {
            let target = root.join(&member.name);
            let metadata = fs::symlink_metadata(&target)?;
            if metadata.file_type().is_symlink()
                || metadata.is_dir() != member.directory
                || (!member.directory && (!metadata.is_file() || metadata.len() != member.size))
            {
                return Err(AdapterError::Invalid(
                    "cached materialization changed".into(),
                ));
            }
            expected.insert(PathBuf::from(&member.name));
            let mut parent = Path::new(&member.name).parent();
            while let Some(path) = parent.filter(|p| !p.as_os_str().is_empty()) {
                expected.insert(path.to_owned());
                parent = path.parent();
            }
            if !member.directory {
                let mut entry = self.archive.by_index(index)?;
                let expected_hash =
                    hash_reader(&mut (&mut entry).take(member.size + 1), member.size)?;
                let mut actual = open_regular(&target)?;
                if executable_bits(&actual.metadata()?) != member.executable {
                    return Err(AdapterError::Invalid(
                        "cached executable permissions changed".into(),
                    ));
                }
                let actual_hash = hash_reader(&mut actual, member.size)?;
                if expected_hash != actual_hash {
                    return Err(AdapterError::Invalid("cached file digest changed".into()));
                }
            }
        }
        let mut found = BTreeSet::new();
        collect_paths(root, root, &mut found)?;
        if found != expected {
            return Err(AdapterError::Invalid("unexpected cached files".into()));
        }
        Ok(())
    }
}

fn hash_reader(reader: &mut impl Read, expected_size: u64) -> Result<Vec<u8>> {
    let mut hasher = Sha256::new();
    let mut count = 0u64;
    let mut buffer = [0; 16 * 1024];
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        count += n as u64;
        if count > expected_size {
            return Err(AdapterError::Invalid("file exceeds expected size".into()));
        }
        hasher.update(&buffer[..n]);
    }
    if count != expected_size {
        return Err(AdapterError::Invalid("file shorter than expected".into()));
    }
    Ok(hasher.finalize().to_vec())
}

fn collect_paths(root: &Path, directory: &Path, output: &mut BTreeSet<PathBuf>) -> Result<()> {
    let metadata = fs::symlink_metadata(directory)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(AdapterError::Invalid(
            "cached directory is not a directory".into(),
        ));
    }
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || (!metadata.is_dir() && !metadata.is_file()) {
            return Err(AdapterError::Invalid("special cached file".into()));
        }
        output.insert(
            path.strip_prefix(root)
                .map_err(|e| AdapterError::Invalid(e.to_string()))?
                .to_owned(),
        );
        if metadata.is_dir() {
            collect_paths(root, &path, output)?;
        }
    }
    Ok(())
}

pub(crate) fn read_bounded(reader: &mut impl Read, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(AdapterError::Limit("input bytes".into()));
    }
    Ok(bytes)
}

fn safe_name(name: &str, directory: bool) -> Result<String> {
    if !name.is_ascii()
        || name.len() > 1024
        || name.contains(['\\', ':', '\0'])
        || name.starts_with('/')
    {
        return Err(AdapterError::Invalid(
            "ZIP path must be portable ASCII without absolute paths or backslashes".into(),
        ));
    }
    let name = if directory {
        name.strip_suffix('/').unwrap_or(name)
    } else {
        name
    };
    for component in name.split('/') {
        let stem = component
            .split('.')
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.len() > 255
            || component.ends_with(['.', ' '])
            || component.bytes().any(|b| b < 32 || b == 127)
            || [
                "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7",
                "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8",
                "LPT9",
            ]
            .contains(&stem.as_str())
        {
            return Err(AdapterError::Invalid(
                "ambiguous or unsafe ZIP component".into(),
            ));
        }
    }
    Ok(name.to_owned())
}

// This bounded ZIP32 structural check is deliberately small: zip-rs remains the
// decompressor and CRC validator. Reject unsupported ZIP64/multi-disk envelopes.
fn check_directory(bytes: &[u8], max_entries: usize) -> Result<Vec<String>> {
    let invalid = || AdapterError::Invalid("invalid or unsupported ZIP32 directory".into());
    if bytes.len() < 22 {
        return Err(invalid());
    }
    let eocd = (bytes.len().saturating_sub(22 + u16::MAX as usize)..=bytes.len() - 22)
        .rev()
        .find(|&i| {
            bytes[i..].starts_with(b"PK\x05\x06")
                && i + 22 + u16::from_le_bytes([bytes[i + 20], bytes[i + 21]]) as usize
                    == bytes.len()
        })
        .ok_or_else(invalid)?;
    let u16_at = |i| u16::from_le_bytes([bytes[i], bytes[i + 1]]);
    let u32_at = |i| u32::from_le_bytes([bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]]);
    let count = u16_at(eocd + 10) as usize;
    if u16_at(eocd + 4) != 0
        || u16_at(eocd + 6) != 0
        || u16_at(eocd + 8) as usize != count
        || count == u16::MAX as usize
    {
        return Err(invalid());
    }
    if count == 0 || count > max_entries {
        return Err(AdapterError::Limit("ZIP entry count".into()));
    }
    let start = u32_at(eocd + 16) as usize;
    if start.checked_add(u32_at(eocd + 12) as usize) != Some(eocd) {
        return Err(invalid());
    }
    let mut cursor = start;
    let mut names = Vec::new();
    let mut unique = BTreeSet::new();
    for _ in 0..count {
        if cursor.checked_add(46).is_none_or(|n| n > eocd)
            || !bytes[cursor..].starts_with(b"PK\x01\x02")
        {
            return Err(invalid());
        }
        let name_len = u16_at(cursor + 28) as usize;
        let extra = u16_at(cursor + 30) as usize;
        let comment = u16_at(cursor + 32) as usize;
        let next = cursor
            .checked_add(46 + name_len + extra + comment)
            .ok_or_else(invalid)?;
        if next > eocd
            || u16_at(cursor + 34) != 0
            || !matches!(u16_at(cursor + 10), 0 | 8)
            || [20, 24, 42]
                .into_iter()
                .any(|offset| u32_at(cursor + offset) == u32::MAX)
        {
            return Err(invalid());
        }
        let mut extra_cursor = cursor + 46 + name_len;
        let extra_end = extra_cursor + extra;
        while extra_cursor < extra_end {
            if extra_cursor + 4 > extra_end || u16_at(extra_cursor) == 1 {
                return Err(invalid());
            }
            extra_cursor += 4 + u16_at(extra_cursor + 2) as usize;
            if extra_cursor > extra_end {
                return Err(invalid());
            }
        }
        let name = std::str::from_utf8(&bytes[cursor + 46..cursor + 46 + name_len])
            .map_err(|_| invalid())?
            .to_owned();
        if !unique.insert(name.clone()) {
            return Err(AdapterError::Invalid(
                "duplicate ZIP central-directory entry".into(),
            ));
        }
        names.push(name);
        cursor = next;
    }
    if cursor != eocd {
        return Err(invalid());
    }
    Ok(names)
}

fn make_readonly_file(path: &Path, executable: u32) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            path,
            fs::Permissions::from_mode(0o444 | (executable & 0o111)),
        )?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = executable;
        make_readonly(path, false)
    }
}

pub(crate) fn make_readonly(path: &Path, directory: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            path,
            fs::Permissions::from_mode(if directory { 0o555 } else { 0o444 }),
        )?;
    }
    #[cfg(not(unix))]
    {
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions)?;
    }
    Ok(())
}
fn readonly_directories(root: &Path) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            readonly_directories(&entry.path())?;
        }
    }
    make_readonly(root, true)?;
    // Persist child names bottom-up before publishing this materialization.
    File::open(root)?.sync_all()?;
    Ok(())
}
/// Removes only this cache's own tree and never follows symbolic links.
pub(crate) fn remove_tree(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
        for entry in fs::read_dir(path)? {
            remove_tree(&entry?.path())?;
        }
        fs::remove_dir(path)?;
    } else {
        fs::remove_file(path)?;
    }
    Ok(())
}
