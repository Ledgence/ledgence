//! Read original submission bytes before any JSON value can lose information.

use crate::args::invalid;
use ledgence_orchestration_api::{Result, SUBMISSION_MAX_BYTES, SubmitCommand};
use std::{io::Read, path::Path};

pub fn read(path: &Path) -> Result<SubmitCommand> {
    let io_error = |error: std::io::Error| invalid(format!("cannot read submission file: {error}"));
    if !std::fs::symlink_metadata(path).map_err(io_error)?.is_file() {
        return Err(invalid("submission must be a regular file"));
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use nix::fcntl::OFlag;
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags((OFlag::O_NONBLOCK | OFlag::O_NOFOLLOW).bits());
    }
    let file = options.open(path).map_err(io_error)?;
    if !file.metadata().map_err(io_error)?.is_file() {
        return Err(invalid("submission must be a regular file"));
    }
    let mut bytes = Vec::new();
    file.take(SUBMISSION_MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    SubmitCommand::decode(&bytes)
}
