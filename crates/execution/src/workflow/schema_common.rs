use std::path::{Component, Path, PathBuf};

use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

pub(super) use um_support::{is_identifier, is_lowercase_hex, lowercase_hex};

/// The prefix of a `sha256:<hex>` digest as the definition and run records carry it.
pub(super) const DIGEST_PREFIX: &str = "sha256:";
const ULID_LENGTH: usize = 26;

/// The hex of a `sha256:<hex>` digest, or `None` for any other form.
pub(super) fn digest_hex(digest: &str) -> Option<&str> {
    digest
        .strip_prefix(DIGEST_PREFIX)
        .filter(|hex| is_lowercase_hex(hex, 64))
}

/// `prefix` and a lowercase Crockford ULID: 26 characters, the first `0` to `7`, the record
/// ids of the definition store (`def_`) and the run record (`run_`).
pub(super) fn is_prefixed_ulid(value: &str, prefix: &str) -> bool {
    value.strip_prefix(prefix).is_some_and(|ulid| {
        ulid.len() == ULID_LENGTH
            && ulid.starts_with(|first: char| ('0'..='7').contains(&first))
            && ulid.bytes().all(|byte| {
                byte.is_ascii_digit()
                    || (byte.is_ascii_lowercase() && !matches!(byte, b'i' | b'l' | b'o' | b'u'))
            })
    })
}

pub(super) fn utc_timestamp(value: OffsetDateTime) -> Result<String, time::error::Format> {
    value.to_offset(UtcOffset::UTC).format(&Rfc3339)
}

pub(super) fn parse_canonical_utc_timestamp(value: &str) -> Option<OffsetDateTime> {
    if !value.ends_with('Z') {
        return None;
    }
    let parsed = OffsetDateTime::parse(value, &Rfc3339).ok()?;
    (utc_timestamp(parsed).ok()?.as_str() == value).then_some(parsed)
}

pub(super) fn is_canonical_relative_path(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty()
        && !path.is_absolute()
        && path.components().all(|component| {
            matches!(component, Component::Normal(_)) && component.as_os_str().to_str().is_some()
        })
        && path.components().collect::<PathBuf>().as_os_str() == path.as_os_str()
}

pub(super) fn is_canonical_absolute_path(value: &str) -> bool {
    let path = Path::new(value);
    path.is_absolute()
        && !path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        && path.components().collect::<PathBuf>().as_os_str() == path.as_os_str()
}
