use crate::error::{AdapterError, Result};
use std::{
    fs::{self, File, OpenOptions},
    path::Path,
};

/// Reject special files before opening and validate the actual opened object.
/// On Unix a last-component replacement by a FIFO cannot block open, and a
/// replacement by a symlink cannot redirect it. Parent directories remain part
/// of the operator-trusted local filesystem, not a sandbox boundary.
pub(crate) fn open_regular(path: &Path) -> Result<File> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(AdapterError::Invalid(
            "artifact file must be regular".into(),
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use nix::fcntl::OFlag;
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags((OFlag::O_NONBLOCK | OFlag::O_NOFOLLOW).bits());
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(AdapterError::Invalid(
            "opened artifact file must be regular".into(),
        ));
    }
    Ok(file)
}

pub(crate) fn executable_bits(metadata: &fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{io::Write, os::unix::fs::PermissionsExt};

    #[test]
    fn immutable_materialization_can_be_synced() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("content");
        fs::create_dir(&directory).unwrap();
        let path = directory.join("program.py");
        let mut file = File::create_new(&path).unwrap();
        file.write_all(b"pass\n").unwrap();
        file.sync_all().expect("sync writable program file");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).unwrap();
        file.sync_all()
            .expect("sync read-only program through its original handle");
        File::open(&directory)
            .unwrap()
            .sync_all()
            .expect("sync writable content directory");
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o555)).unwrap();
        File::open(&directory)
            .unwrap()
            .sync_all()
            .expect("sync read-only content directory");
        let published = root.path().join("published");
        let renamed = fs::rename(&directory, &published);
        let cleanup = if renamed.is_ok() {
            &published
        } else {
            &directory
        };
        fs::set_permissions(cleanup, fs::Permissions::from_mode(0o700)).unwrap();
        renamed.expect("rename read-only content directory");
        File::open(root.path())
            .unwrap()
            .sync_all()
            .expect("sync publication parent");
    }
}
