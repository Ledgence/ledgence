use crate::{
    ArtifactLimits,
    archive::inspect_package,
    error::{AdapterError, Result},
    publish::{check_encoded_descriptor, validation_error},
};
use ledgence_worker_api::{
    ProgramDescriptor, ProgramPackageInspection, ProgramPackageInspector, PublicationLimits,
    PublicationResult,
};

/// Full borrowed ZIP inspection, using the same verifier as filesystem publication
/// and catalog registration. Run inspection on the caller's blocking executor.
#[derive(Clone, Copy, Debug, Default)]
pub struct ZipProgramPackageInspector;

impl ProgramPackageInspector for ZipProgramPackageInspector {
    fn inspect(
        &self,
        descriptor: &ProgramDescriptor,
        archive: &[u8],
        limits: &PublicationLimits,
    ) -> PublicationResult<ProgramPackageInspection> {
        inspect(descriptor, archive, limits).map_err(validation_error)
    }
}

fn inspect(
    descriptor: &ProgramDescriptor,
    archive: &[u8],
    limits: &PublicationLimits,
) -> Result<ProgramPackageInspection> {
    let limits = ArtifactLimits {
        max_archive_bytes: limits.max_archive_bytes,
        max_expanded_bytes: limits.max_expanded_bytes,
        max_file_bytes: limits.max_file_bytes,
        max_entries: usize::try_from(limits.max_entries)
            .map_err(|_| AdapterError::Invalid("invalid package entry limit".into()))?,
        max_manifest_bytes: limits.max_manifest_bytes,
        max_descriptor_bytes: limits.max_descriptor_bytes,
        ..ArtifactLimits::default()
    };
    limits.validate()?;
    limits.descriptor(descriptor)?;
    check_encoded_descriptor(descriptor, &limits)?;
    inspect_package(archive, descriptor, &limits)
}
