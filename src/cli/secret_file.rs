//! Reading a secret the member names: a private file, or `-` for standard input.

use std::fs::{self, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

use anyhow::{Context, bail};
use zeroize::Zeroizing;

/// Whether a file's mode is checked: a file the member names must be `0600`, but one a
/// provider's sign-in wrote into a `0700` directory the command made keeps the mode the
/// provider gave it.
#[derive(Clone, Copy)]
pub(super) enum FileMode {
    Private,
    Unchecked,
}

/// A kind of secret read from a file or standard input, and the bound on what is read.
pub(super) struct SecretInput {
    /// What messages call the secret, such as `GitLab token`.
    pub(super) name: &'static str,
    /// The most bytes accepted, including any line ending the caller trims.
    pub(super) max_bytes: u64,
    /// The bound as messages state it, such as `16 KiB`.
    pub(super) max_text: &'static str,
}

impl SecretInput {
    /// Reads the secret into a zeroizing buffer. A file must be a regular, non-symlink file
    /// owned by the effective user with mode `0600`, checked before and after opening it
    /// without following a link; at most one byte past the bound is read.
    pub(super) fn read(&self, path: &Path) -> anyhow::Result<Zeroizing<Vec<u8>>> {
        self.read_with(path, FileMode::Private)
    }

    /// Reads the secret as [`Self::read`] does, checking a file's mode only when `mode` is
    /// [`FileMode::Private`].
    pub(super) fn read_with(
        &self,
        path: &Path,
        mode: FileMode,
    ) -> anyhow::Result<Zeroizing<Vec<u8>>> {
        let name = self.name;
        let mut bytes = Zeroizing::new(Vec::new());
        if path == Path::new("-") {
            io::stdin()
                .lock()
                .take(self.max_bytes + 1)
                .read_to_end(&mut bytes)
                .with_context(|| format!("read {name} from standard input"))?;
        } else {
            let uid = rustix::process::geteuid().as_raw();
            let validate = |metadata: &fs::Metadata| match mode {
                FileMode::Private => self.validate(metadata, uid),
                FileMode::Unchecked => self.validate_file(metadata, uid, false),
            };
            validate(&fs::symlink_metadata(path).with_context(|| format!("inspect {name} file"))?)?;
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(path)
                .with_context(|| format!("open {name} file"))?;
            validate(
                &file
                    .metadata()
                    .with_context(|| format!("inspect opened {name} file"))?,
            )?;
            file.take(self.max_bytes + 1)
                .read_to_end(&mut bytes)
                .with_context(|| format!("read {name} file"))?;
        }
        if bytes.len() as u64 > self.max_bytes {
            bail!("{name} input exceeds {}", self.max_text);
        }
        Ok(bytes)
    }

    /// Accepts only a regular file owned by `uid` with mode `0600` within the bound.
    pub(super) fn validate(&self, metadata: &fs::Metadata, uid: u32) -> anyhow::Result<()> {
        self.validate_file(metadata, uid, true)
    }

    /// Accepts only a regular file owned by `uid` within the bound, with mode `0600` when
    /// `private`.
    fn validate_file(
        &self,
        metadata: &fs::Metadata,
        uid: u32,
        private: bool,
    ) -> anyhow::Result<()> {
        if !metadata.file_type().is_file()
            || metadata.uid() != uid
            || (private && metadata.mode() & 0o7777 != 0o600)
        {
            bail!(
                "{} file must be a regular non-symlink file owned by you{}",
                self.name,
                if private { " with mode 0600" } else { "" }
            );
        }
        if metadata.len() > self.max_bytes {
            bail!("{} file exceeds {}", self.name, self.max_text);
        }
        Ok(())
    }
}

/// Trims one trailing LF or CRLF.
pub(super) fn trim_line_ending(bytes: &mut Vec<u8>) {
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
}
