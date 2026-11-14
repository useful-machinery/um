//! The runner's own agent-login directory, `.runner-agent-login/` in the
//! directory of `runnerStatePath`, mode `0700` (REQ-GRANT-36): the
//! runner's Codex home `codex/`, the Claude Code token `claude-code.token`,
//! and the grant tokens of the assignment the runner holds, `github.token`,
//! `gitlab.token` and `linear.token` (REQ-GRANT-86). Nothing but the runner
//! writes there, so whatever is at one of its locations is the runner's to
//! replace or remove.

use std::fs::{self, DirBuilder, Permissions};
use std::io;
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _};
use std::path::{Component, Path, PathBuf};

use zeroize::Zeroizing;

use super::{TOKEN_LIMIT, valid_token};
use crate::control_protocol::AgentLoginProvider;
use crate::enrollment::{
    read_private_file_if_present, sync_directory_io, write_private_file_atomically,
};

/// The directory's name in the directory of `runnerStatePath`.
pub(crate) const DIRECTORY: &str = ".runner-agent-login";
const CODEX_HOME: &str = "codex";
const CLAUDE_CODE_TOKEN: &str = "claude-code.token";
const GITHUB_TOKEN: &str = "github.token";
const GITLAB_TOKEN: &str = "gitlab.token";
const LINEAR_TOKEN: &str = "linear.token";
/// The prefixes of the temporary files each grant token file is written
/// through, in the order of `github`, `gitlab` and `linear`.
pub(crate) const GRANT_TOKEN_TEMPORARY_PREFIXES: [&str; 3] = [
    ".um-github.token.tmp",
    ".um-gitlab.token.tmp",
    ".um-linear.token.tmp",
];
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;

/// Where the runner whose protected state is at a given path keeps the
/// agent logins it is delivered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LoginDirectory(PathBuf);

impl LoginDirectory {
    pub(crate) fn beside(runner_state_path: &Path) -> Self {
        Self(runner_state_path.with_file_name(DIRECTORY))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }

    /// The Codex home the runner keeps a `codex` agent login in and offers
    /// as `CODEX_HOME` (REQ-GRANT-37).
    pub(crate) fn codex_home(&self) -> PathBuf {
        self.0.join(CODEX_HOME)
    }

    /// The file the runner keeps a `claude_code` agent login in
    /// (REQ-GRANT-44).
    pub(crate) fn claude_code_token(&self) -> PathBuf {
        self.0.join(CLAUDE_CODE_TOKEN)
    }

    /// The file the runner keeps an OAuth provider's grant token in for the
    /// assignment it holds (REQ-GRANT-86), and the prefix of the temporary
    /// files it is written through; `None` for an installable provider.
    pub(crate) fn grant_token(
        &self,
        provider: AgentLoginProvider,
    ) -> Option<(PathBuf, &'static str)> {
        let [github, gitlab, linear] = GRANT_TOKEN_TEMPORARY_PREFIXES;
        let (name, temporary_prefix) = match provider {
            AgentLoginProvider::Github => (GITHUB_TOKEN, github),
            AgentLoginProvider::Gitlab => (GITLAB_TOKEN, gitlab),
            AgentLoginProvider::Linear => (LINEAR_TOKEN, linear),
            AgentLoginProvider::Codex | AgentLoginProvider::ClaudeCode => return None,
        };
        Some((self.0.join(name), temporary_prefix))
    }

    /// Whether `path` names something inside the directory: below it, by
    /// no step that could lead back out. Only such a path is the runner's to
    /// remove.
    pub(crate) fn contains(&self, path: &Path) -> bool {
        path.strip_prefix(&self.0).is_ok_and(|rest| {
            rest.components().next().is_some()
                && rest
                    .components()
                    .all(|component| matches!(component, Component::Normal(_)))
        })
    }

    /// Removes whatever is at `path` inside the directory, a directory with
    /// everything in it included, without following a link: a link is
    /// removed as a link and what it points to is left. A path outside the
    /// directory, or reached through anything in it that is not a real
    /// directory, is refused; a location already empty is removed.
    pub(crate) fn remove_tree(&self, path: &Path) -> io::Result<()> {
        if !self.contains(path) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let parent = path.parent().ok_or(io::ErrorKind::InvalidInput)?;
        let mut within: Vec<&Path> = parent
            .ancestors()
            .take_while(|ancestor| ancestor.starts_with(&self.0))
            .collect();
        within.reverse();
        for ancestor in within {
            match fs::symlink_metadata(ancestor) {
                Ok(metadata) if metadata.file_type().is_dir() => {}
                Ok(_) => return Err(io::ErrorKind::NotADirectory.into()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        // `remove_dir_all` removes a link it meets, the one at `path`
        // included, rather than following it.
        let removed = match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_dir() => fs::remove_dir_all(path),
            Ok(_) => fs::remove_file(path),
            Err(error) => Err(error),
        };
        match removed {
            Ok(()) => sync_directory_io(parent),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// Makes `path` a real directory of the effective user with mode `0700`:
/// creates it, or repairs the mode of one already there. Anything else at
/// `path`, a link, a file or another user's directory, is refused.
pub(crate) fn ensure_private_directory(path: &Path) -> io::Result<()> {
    let mut builder = DirBuilder::new();
    builder.mode(PRIVATE_DIRECTORY_MODE);
    let created = match builder.create(path) {
        Ok(()) => true,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => false,
        Err(error) => return Err(error),
    };
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(io::ErrorKind::NotADirectory.into());
    }
    if metadata.uid() != rustix::process::geteuid().as_raw() {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    if metadata.mode() & 0o7777 != PRIVATE_DIRECTORY_MODE {
        fs::set_permissions(path, Permissions::from_mode(PRIVATE_DIRECTORY_MODE))?;
    }
    match path.parent() {
        Some(parent) if created => sync_directory_io(parent),
        _ => Ok(()),
    }
}

/// Replaces the file at `path` in the runner's directory with `bytes`, mode
/// `0600`, in one step.
pub(crate) fn write_file(path: &Path, bytes: &[u8], temporary_prefix: &str) -> io::Result<()> {
    write_private_file_atomically(path, bytes, temporary_prefix, |_| Ok(()), |error| error)
}

/// The token in the file at `path`, read through the runner's bounded
/// private-file read; `None` when there is none, or when it is not a private
/// file holding one valid token.
pub(crate) fn read_token(path: &Path) -> Option<Zeroizing<Vec<u8>>> {
    let bytes = Zeroizing::new(
        read_private_file_if_present(path, TOKEN_LIMIT as u64)
            .ok()
            .flatten()?,
    );
    valid_token(&bytes).then_some(bytes)
}

/// Whether `path` is a real directory, looked at without following a link:
/// one the runner may look for its own files in.
pub(crate) fn real_directory(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_dir())
}

/// Whether a regular file is at `path`, looked at without following a link
/// or opening it: what the runner installed there is still held.
pub(crate) fn holds_file(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_file())
}

/// Removes whatever file or link is at `path` in the runner's directory, or
/// the readiness document beside its state; a location already empty is
/// removed.
pub(crate) fn remove_file(path: &Path) -> io::Result<()> {
    let parent = path.parent().ok_or(io::ErrorKind::InvalidInput)?;
    match fs::remove_file(path) {
        Ok(()) => sync_directory_io(parent),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
