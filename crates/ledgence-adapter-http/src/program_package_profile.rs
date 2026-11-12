//! Profile names belong to this transport, not the portable package contracts.
use ledgence_worker_api::ProgramPackageProfile;

pub(crate) const HEADER: &str = "x-ledgence-program-package-profile";
pub(crate) const EXPANDED: &str = "entries-8192-v1";

pub(crate) fn parse<'a>(
    mut values: impl Iterator<Item = &'a [u8]>,
) -> Result<ProgramPackageProfile, ()> {
    match (values.next(), values.next()) {
        (None, None) => Ok(ProgramPackageProfile::Legacy4096),
        (Some(value), None) if value == EXPANDED.as_bytes() => {
            Ok(ProgramPackageProfile::Entries8192V1)
        }
        _ => Err(()),
    }
}

#[cfg(feature = "client")]
pub(crate) fn selected<'a>(
    requested: ProgramPackageProfile,
    values: impl Iterator<Item = &'a [u8]>,
) -> Result<ProgramPackageProfile, ()> {
    let profile = parse(values)?;
    if requested == ProgramPackageProfile::Legacy4096
        && profile == ProgramPackageProfile::Entries8192V1
    {
        return Err(());
    }
    Ok(profile)
}

pub(crate) fn value(profile: ProgramPackageProfile) -> Option<&'static str> {
    match profile {
        ProgramPackageProfile::Legacy4096 => None,
        ProgramPackageProfile::Entries8192V1 => Some(EXPANDED),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiles_require_one_exact_token_or_absence() {
        assert_eq!(
            parse(std::iter::empty()),
            Ok(ProgramPackageProfile::Legacy4096)
        );
        assert_eq!(
            parse([EXPANDED.as_bytes()].into_iter()),
            Ok(ProgramPackageProfile::Entries8192V1)
        );
        for invalid in [
            "",
            " ",
            "entries-8192-v2",
            "ENTRIES-8192-V1",
            " entries-8192-v1",
            "entries-8192-v1 ",
            "entries-8192-v1, entries-8192-v1",
        ] {
            assert!(parse([invalid.as_bytes()].into_iter()).is_err());
        }
        assert!(parse([EXPANDED.as_bytes(), EXPANDED.as_bytes()].into_iter()).is_err());
        assert!(parse([&[0xff][..]].into_iter()).is_err());
    }

    #[cfg(feature = "client")]
    #[test]
    fn response_cannot_select_an_unrequested_profile() {
        assert!(
            selected(
                ProgramPackageProfile::Legacy4096,
                [EXPANDED.as_bytes()].into_iter()
            )
            .is_err()
        );
        assert_eq!(
            selected(ProgramPackageProfile::Entries8192V1, std::iter::empty()),
            Ok(ProgramPackageProfile::Legacy4096)
        );
    }
}
