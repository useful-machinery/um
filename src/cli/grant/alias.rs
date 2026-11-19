//! The earlier commands `useful integration` and `useful runner integration` replace
//! (REQ-GRANT-77, REQ-INTEGRATION-15). Each stays, absent from help, with its arguments, output
//! and exit statuses unchanged; an invocation of one first prints a line on standard error
//! naming the command that replaces it.

use std::io::{self, Write};

/// Each earlier leaf, by its path under `useful`, with the command that replaces it.
pub(in crate::cli) const EARLIER: [(&str, &str); 19] = [
    ("auth agent-login authorize", "useful integration authorize"),
    (
        "auth agent-login deauthorize",
        "useful integration deauthorize",
    ),
    (
        "auth agent-login import",
        "useful integration connect PROVIDER",
    ),
    ("auth agent-login list", "useful integration list"),
    (
        "auth agent-login reconnect",
        "useful integration connect PROVIDER --replace",
    ),
    ("auth agent-login revoke", "useful integration revoke"),
    (
        "auth agent-login show",
        "useful integration list INTEGRATION",
    ),
    ("auth credential status", "useful integration list"),
    ("auth grant create", "useful integration connect PROVIDER"),
    ("auth grant list", "useful integration list"),
    ("auth grant revoke", "useful integration revoke"),
    ("auth grant uses", "useful integration list INTEGRATION"),
    ("grant authorize", "useful integration authorize"),
    ("grant create", "useful integration connect"),
    ("grant deauthorize", "useful integration deauthorize"),
    ("grant list", "useful integration list"),
    ("grant revoke", "useful integration revoke"),
    ("runner agent-login sync", "useful runner integration sync"),
    ("runner grant sync", "useful runner integration sync"),
];

/// The earlier leaf an invocation named, by its path under `useful`.
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
            "warning: `useful {}` is replaced by `{}` and will be removed in a later release",
            self.path, self.replacement
        );
    }
}
