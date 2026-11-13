//! Whether this machine has a browser to open a consent URL in, and opening it there
//! (REQ-GRANT-69). The answer comes from the environment alone, so a remote shell and a test
//! get the same answer a member's terminal would: no SSH session, on Linux a display, and the
//! platform's opener on `PATH`. Opening is a convenience: the URL is always printed, so an
//! opener that fails is ignored, and nothing waits for it.

use std::ffi::OsString;
use std::process::{Command, Stdio};

use super::program;

const OPENER: &str = if cfg!(target_os = "macos") {
    "open"
} else {
    "xdg-open"
};
// Set in a session reached over SSH, whose browser is on another machine.
const REMOTE: [&str; 3] = ["SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY"];
const DISPLAYS: [&str; 2] = ["DISPLAY", "WAYLAND_DISPLAY"];

/// Whether the environment `env` reads says this machine has a browser. A variable set to the
/// empty string counts as unset.
pub(super) fn available(env: impl Fn(&str) -> Option<OsString>) -> bool {
    let set = |name: &str| env(name).is_some_and(|value| !value.is_empty());
    !REMOTE.into_iter().any(set)
        && (!cfg!(target_os = "linux") || DISPLAYS.into_iter().any(set))
        && program::find(env("PATH").as_deref(), OPENER).is_some()
}

/// Starts the opener with `url` as its one argument, discarding its output and ignoring its
/// failure, and reaps it on a thread of its own: an opener such as `xdg-open` may run the
/// browser in the foreground until it quits, and the consent must not wait for that.
pub(super) fn open(url: &str) {
    let Some(opener) = program::find(std::env::var_os("PATH").as_deref(), OPENER) else {
        return;
    };
    let Ok(mut child) = Command::new(opener)
        .arg(url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    else {
        return;
    };
    // Without a thread the opener is left unreaped until `um` exits, which loses nothing.
    let _ignored = std::thread::Builder::new()
        .name("browser-opener".to_owned())
        .spawn(move || child.wait());
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::OsString;

    // REQ-GRANT-69
    #[test]
    fn a_remote_session_or_a_linux_session_without_a_display_has_no_browser() {
        let opener = tempfile::tempdir().unwrap();
        let script = opener.path().join(super::OPENER);
        std::fs::write(&script, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(
            &script,
            <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o755),
        )
        .unwrap();
        let path = opener.path().as_os_str().to_owned();
        let linux = cfg!(target_os = "linux");
        for (variables, expected) in [
            (&[("DISPLAY", ":0")][..], true),
            (&[("WAYLAND_DISPLAY", "wayland-0")][..], true),
            (&[][..], !linux),
            (&[("DISPLAY", "")][..], !linux),
            (
                &[("DISPLAY", ":0"), ("SSH_CONNECTION", "a 1 b 22")][..],
                false,
            ),
            (&[("DISPLAY", ":0"), ("SSH_CLIENT", "a 1 22")][..], false),
            (&[("DISPLAY", ":0"), ("SSH_TTY", "/dev/pts/1")][..], false),
        ] {
            let mut environment: BTreeMap<&str, OsString> = variables
                .iter()
                .map(|(name, value)| (*name, OsString::from(value)))
                .collect();
            environment.insert("PATH", path.clone());
            assert_eq!(
                super::available(|name| environment.get(name).cloned()),
                expected,
                "{variables:?}"
            );
            environment.insert("PATH", OsString::from("relative/bin"));
            assert!(!super::available(|name| environment.get(name).cloned()));
        }
    }
}
