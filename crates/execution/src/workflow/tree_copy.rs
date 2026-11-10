//! The read-only copy of a validated workflow tree, shared by a run directory's
//! `workflow/tree/` (REQ-LOCALRUN-170) and the `um` home's `trees/sha256/<hex>/`
//! (REQ-WFDEF-17), with the descriptor-relative file operations it is written through.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Write as _};
use std::os::fd::{AsFd as _, AsRawFd as _, OwnedFd};
use std::path::{Path, PathBuf};

use rustix::fs::{Mode, OFlags, fchmod, mkdirat, openat};
use rustix::io::dup;

use super::schema_common::is_canonical_relative_path;
use super::tree::TreeFile;

/// A descriptor-relative file operation that failed, with the path it resolved to.
#[derive(Debug)]
pub(crate) struct FileOperationError {
    pub(crate) path: PathBuf,
    pub(crate) operation: &'static str,
    pub(crate) source: io::Error,
}

impl FileOperationError {
    pub(super) fn at(
        parent: &OwnedFd,
        name: impl AsRef<std::ffi::OsStr>,
        operation: &'static str,
        source: impl Into<io::Error>,
    ) -> Self {
        Self {
            path: file_locator(parent, name),
            operation,
            source: source.into(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum TreeCopyError {
    /// A tree path is not canonical, or one name is both a file and a directory.
    Layout,
    File(FileOperationError),
}

impl From<FileOperationError> for TreeCopyError {
    fn from(error: FileOperationError) -> Self {
        Self::File(error)
    }
}

/// Writes `files` read-only under `directory`: files `0444`, or `0555` when the source had
/// the owner execute bit, and directories `0555`, each set once its contents are written,
/// since a `0555` directory refuses new entries. The `programs` directories exist in the
/// copy even when they hold no file.
pub(crate) fn write_tree_copy<'a>(
    directory: &OwnedFd,
    files: impl Iterator<Item = (&'a str, &'a TreeFile)>,
    programs: &'a [String],
) -> Result<(), TreeCopyError> {
    let mut root = TreeCopyDirectory::default();
    for (path, file) in files {
        if !is_canonical_relative_path(path) {
            return Err(TreeCopyError::Layout);
        }
        let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
        let parent = root.directory(parent)?;
        if parent.directories.contains_key(name) || parent.files.insert(name, file).is_some() {
            return Err(TreeCopyError::Layout);
        }
    }
    for program in programs {
        root.directory(program.trim_end_matches('/'))?;
    }
    root.write(directory)
}

#[derive(Default)]
struct TreeCopyDirectory<'a> {
    directories: BTreeMap<&'a str, TreeCopyDirectory<'a>>,
    files: BTreeMap<&'a str, &'a TreeFile>,
}

impl<'a> TreeCopyDirectory<'a> {
    /// The directory at the tree-relative `path` (`""` is the root), created on demand.
    fn directory(&mut self, path: &'a str) -> Result<&mut Self, TreeCopyError> {
        if path.is_empty() {
            return Ok(self);
        }
        if !is_canonical_relative_path(path) {
            return Err(TreeCopyError::Layout);
        }
        let mut directory = self;
        for segment in path.split('/') {
            if directory.files.contains_key(segment) {
                return Err(TreeCopyError::Layout);
            }
            directory = directory.directories.entry(segment).or_default();
        }
        Ok(directory)
    }

    fn write(&self, handle: &OwnedFd) -> Result<(), TreeCopyError> {
        for (name, file) in &self.files {
            let mode = if file.executable {
                Mode::RUSR | Mode::XUSR | Mode::RGRP | Mode::XGRP | Mode::ROTH | Mode::XOTH
            } else {
                Mode::RUSR | Mode::RGRP | Mode::ROTH
            };
            write_new_file_with_final_mode(handle, name, &file.bytes, mode)?;
        }
        for (name, directory) in &self.directories {
            mkdir(handle, name)?;
            directory.write(&open_directory_at(handle, name)?)?;
        }
        fchmod(
            handle,
            Mode::RUSR | Mode::XUSR | Mode::RGRP | Mode::XGRP | Mode::ROTH | Mode::XOTH,
        )
        .map_err(|source| FileOperationError::at(handle, ".", "set permissions", source))?;
        Ok(sync_directory(handle)?)
    }
}

pub(super) fn file_locator(parent: &OwnedFd, name: impl AsRef<std::ffi::OsStr>) -> PathBuf {
    // Descriptor-relative operations remain bound to the opened directory even if
    // the requested pathname is replaced. Resolve the descriptor, not the request.
    #[cfg(target_os = "macos")]
    let resolved = rustix::fs::getpath(parent).ok().map(|path| {
        use std::os::unix::ffi::OsStringExt as _;
        PathBuf::from(std::ffi::OsString::from_vec(path.into_bytes()))
    });
    #[cfg(not(target_os = "macos"))]
    let resolved = std::fs::read_link(format!("/proc/self/fd/{}", parent.as_raw_fd()))
        .or_else(|_| std::fs::read_link(format!("/dev/fd/{}", parent.as_raw_fd())))
        .ok();
    resolved
        .unwrap_or_else(|| PathBuf::from(format!("directory fd {}", parent.as_raw_fd())))
        .join(Path::new(name.as_ref()))
}

/// Writes and syncs a new file, then sets `mode`, which may withhold write permission.
pub(super) fn write_new_file_with_final_mode(
    parent: &OwnedFd,
    name: &str,
    bytes: &[u8],
    mode: Mode,
) -> Result<(), FileOperationError> {
    let mut file = create_file(parent, name, Mode::RUSR | Mode::WUSR)?;
    file.write_all(bytes)
        .and_then(|()| file.flush())
        .and_then(|()| file.sync_all())
        .map_err(|source| FileOperationError::at(parent, name, "write and sync", source))?;
    fchmod(file.as_fd(), mode)
        .map_err(|source| FileOperationError::at(parent, name, "set permissions", source))?;
    file.sync_all()
        .map_err(|source| FileOperationError::at(parent, name, "sync", source))
}

pub(super) fn create_file(
    parent: &OwnedFd,
    name: &str,
    mode: Mode,
) -> Result<File, FileOperationError> {
    openat(
        parent,
        name,
        OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        mode,
    )
    .map(File::from)
    .map_err(|source| FileOperationError::at(parent, name, "create", source))
}

pub(super) fn mkdir(
    parent: &OwnedFd,
    name: impl AsRef<std::ffi::OsStr>,
) -> Result<(), FileOperationError> {
    let name = name.as_ref();
    mkdirat(parent, name, Mode::RWXU)
        .map_err(|source| FileOperationError::at(parent, name, "create directory", source))
}

pub(super) fn open_directory_at(
    parent: &OwnedFd,
    name: impl AsRef<std::ffi::OsStr>,
) -> Result<OwnedFd, FileOperationError> {
    let name = name.as_ref();
    openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|source| FileOperationError::at(parent, name, "open directory", source))
}

pub(super) fn sync_directory(directory: &OwnedFd) -> Result<(), FileOperationError> {
    let duplicate = dup(directory)
        .map_err(|source| FileOperationError::at(directory, ".", "duplicate directory", source))?;
    File::from(duplicate)
        .sync_all()
        .map_err(|source| FileOperationError::at(directory, ".", "sync directory", source))
}
