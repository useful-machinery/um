//! The earlier commands `um grant` and `um runner grant` replace (REQ-GRANT-77). Each stays for
//! one release, absent from help, with its arguments, output and exit statuses unchanged; an
//! invocation of one first prints a line on standard error naming the command that replaces it.

use std::io::{self, Write};

/// Each earlier leaf, by its path under `um`, with the command that replaces it.
pub(in crate::cli) const EARLIER: [(&str, &str); 13] = [
    ("auth agent-login authorize", "um grant authorize"),
    ("auth agent-login deauthorize", "um grant deauthorize"),
    ("auth agent-login import", "um grant create PROVIDER"),
    ("auth agent-login list", "um grant list"),
    (
        "auth agent-login reconnect",
        "um grant create PROVIDER --replace",
    ),
    ("auth agent-login revoke", "um grant revoke"),
    ("auth agent-login show", "um grant list GRANT"),
    ("auth credential status", "um grant list"),
    ("auth grant create", "um grant create PROVIDER"),
    ("auth grant list", "um grant list"),
    ("auth grant revoke", "um grant revoke"),
    ("auth grant uses", "um grant list GRANT"),
    ("runner agent-login sync", "um runner grant sync"),
];

/// The earlier leaf an invocation named, by its path under `um`.
#[derive(Debug, Clone, Copy)]
pub(in crate::cli) struct Earlier {
    path: &'static str,
    replacement: &'static str,
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
        let _ = writeln!(
            stderr,
            "warning: `um {}` is replaced by `{}` and will be removed in a later release",
            self.path, self.replacement
        );
    }
}
