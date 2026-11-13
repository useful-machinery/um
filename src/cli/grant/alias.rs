//! The earlier commands `um grant` and `um runner grant` replace (REQ-GRANT-77). Each stays for
//! one release, absent from help, with its arguments, output and exit statuses unchanged; an
//! invocation of one first prints a line on standard error naming the command that replaces it.

use std::io::{self, Write};

/// Each earlier leaf, by its path under `um`, with the command that replaces it, or `None` for
/// the one leaf nothing replaces.
pub(in crate::cli) const EARLIER: [(&str, Option<&str>); 13] = [
    ("auth agent-login authorize", Some("um grant authorize")),
    ("auth agent-login deauthorize", Some("um grant deauthorize")),
    ("auth agent-login import", Some("um grant create PROVIDER")),
    ("auth agent-login list", Some("um grant list")),
    (
        "auth agent-login reconnect",
        Some("um grant create PROVIDER --replace"),
    ),
    ("auth agent-login revoke", Some("um grant revoke")),
    ("auth agent-login show", Some("um grant list GRANT")),
    ("auth credential status", Some("um grant list")),
    ("auth grant create", Some("um grant create PROVIDER")),
    ("auth grant list", Some("um grant list")),
    ("auth grant revoke", Some("um grant revoke")),
    ("auth grant uses", None),
    ("runner agent-login sync", Some("um runner grant sync")),
];

/// The earlier leaf an invocation named, by its path under `um`.
#[derive(Debug, Clone, Copy)]
pub(in crate::cli) struct Earlier {
    path: &'static str,
    replacement: Option<&'static str>,
}

impl Earlier {
    /// The earlier leaf at `path`, or `None` when `path` names no earlier leaf.
    pub(in crate::cli) fn at(path: &str) -> Option<Self> {
        EARLIER
            .iter()
            .find(|(earlier, _)| *earlier == path)
            .map(|&(path, replacement)| Self { path, replacement })
    }

    /// Writes the notice before the leaf runs. The leaf's own output and exit status do not
    /// depend on it, so a standard error that cannot be written does not stop the command.
    pub(in crate::cli) fn write_notice(self) {
        let mut stderr = io::stderr().lock();
        let _ = match self.replacement {
            Some(replacement) => writeln!(
                stderr,
                "warning: `um {}` is replaced by `{replacement}` and will be removed in a later release",
                self.path
            ),
            None => writeln!(
                stderr,
                "warning: `um {}` is no longer listed, has no replacement and will be removed in a later release",
                self.path
            ),
        };
    }
}
