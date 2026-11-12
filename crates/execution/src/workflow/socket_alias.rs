//! Short socket addresses for private directories whose paths are too long for
//! `sockaddr_un`: a mode-0700 directory holding one symbolic link to the target.

use std::fs;
use std::io;
use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _, symlink};
use std::path::{Path, PathBuf};

use super::schema_common::lowercase_hex;

pub(crate) const SOCKET_ALIAS_NAME: &str = "e";
pub(crate) const SOCKET_ALIAS_ROOT: &str = "/tmp";

/// A candidate alias directory `<root>/<prefix><nonce>`, the nonce sixteen lowercase
/// hexadecimal characters of eight random bytes.
pub(crate) fn random_socket_alias_directory(root: &Path, prefix: &str) -> io::Result<PathBuf> {
    let mut nonce = [0_u8; 8];
    getrandom::fill(&mut nonce)
        .map_err(|error| io::Error::other(format!("socket alias nonce: {error}")))?;
    Ok(root.join(format!("{prefix}{}", lowercase_hex(&nonce))))
}

pub(crate) fn create_socket_alias_retry(
    target: &Path,
    mut next_directory: impl FnMut() -> io::Result<PathBuf>,
) -> io::Result<PathBuf> {
    for _ in 0..16 {
        let directory = next_directory()?;
        let alias = directory.join(SOCKET_ALIAS_NAME);
        match create_socket_alias(&directory, &alias, target) {
            Ok(()) => return Ok(directory),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("socket alias: {error}"),
                ));
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "socket alias collisions exhausted",
    ))
}

pub(crate) fn create_socket_alias(directory: &Path, alias: &Path, target: &Path) -> io::Result<()> {
    // AF_UNIX limits the address bytes even when the private staging path is valid.
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(directory)?;
    if let Err(error) = fs::set_permissions(directory, fs::Permissions::from_mode(0o700)) {
        let _ = fs::remove_dir(directory);
        return Err(error);
    }
    if let Err(error) = symlink(target, alias) {
        let _ = fs::remove_dir(directory);
        return Err(error);
    }
    Ok(())
}

pub(crate) fn remove_socket_alias(directory: &Path, alias: &Path) -> io::Result<()> {
    let alias_result = ignore_not_found(fs::remove_file(alias));
    let directory_result = ignore_not_found(fs::remove_dir(directory));
    alias_result.and(directory_result)
}

pub(crate) fn ignore_not_found(result: io::Result<()>) -> io::Result<()> {
    match result {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}
