//! Reading a coding agent's login: a Codex `auth.json` or a Claude Code token, from a file or
//! stream the member names or from the private home of a sign-in `useful` ran (REQ-GRANT-12,
//! REQ-GRANT-32, REQ-GRANT-33, INV-CLI-13). `useful integration connect` and the earlier
//! agent-login commands read through these alike, and a token service's token is held to the
//! form a Claude Code token has.

use std::path::Path;

use anyhow::bail;
use zeroize::Zeroizing;

use super::super::secret_file::{FileMode, SecretInput, trim_line_ending};

// The bounds Cloud enforces on material (REQ-GRANT-12).
const CODEX_LOGIN: SecretInput = SecretInput {
    name: "Codex login",
    max_bytes: 32 * 1024,
    max_text: "32 KiB",
};
pub(super) const MAX_CLAUDE_CODE_TOKEN: usize = 4096;
const CLAUDE_CODE_TOKEN: SecretInput = SecretInput {
    name: "Claude Code token",
    // One trailing LF or CRLF is trimmed after reading.
    max_bytes: MAX_CLAUDE_CODE_TOKEN as u64 + 2,
    max_text: "4 KiB",
};

/// Reads a Codex `auth.json` to send byte for byte. Only its shape is checked: one JSON
/// object, as Cloud requires; its fields are never read. `mode` is checked for a file the
/// member names, not for one `codex login` wrote in the command's private home.
pub(in crate::cli) fn read_codex_login(
    path: &Path,
    mode: FileMode,
) -> anyhow::Result<Zeroizing<Vec<u8>>> {
    let bytes = CODEX_LOGIN.read_with(path, mode)?;
    let object = bytes.iter().find(|byte| !byte.is_ascii_whitespace()) == Some(&b'{');
    // The parser's error can quote the input, so only the fact of failure is kept.
    if !object || serde_json::from_slice::<serde::de::IgnoredAny>(&bytes).is_err() {
        bail!("Codex login must be the JSON object of a Codex auth.json");
    }
    Ok(bytes)
}

/// Reads the token `claude setup-token` printed: one line of printable characters without
/// spaces.
pub(in crate::cli) fn read_claude_code_token(path: &Path) -> anyhow::Result<Zeroizing<Vec<u8>>> {
    one_line_token(CLAUDE_CODE_TOKEN.name, CLAUDE_CODE_TOKEN.read(path)?)
}

/// `bytes`, read as a token `name` names, once one trailing line ending is trimmed, when they
/// are one line of 1 to 4096 printable characters without a space: the form of a Claude Code
/// token and of an integration token (REQ-INTEGRATION-1). The refusal never quotes them.
pub(in crate::cli) fn one_line_token(
    name: &str,
    mut bytes: Zeroizing<Vec<u8>>,
) -> anyhow::Result<Zeroizing<Vec<u8>>> {
    trim_line_ending(&mut bytes);
    if bytes.is_empty()
        || bytes.len() > MAX_CLAUDE_CODE_TOKEN
        || !bytes.iter().all(|byte| (b'!'..=b'~').contains(byte))
    {
        bail!("{name} must be one line of 1 to 4096 printable characters without spaces");
    }
    Ok(bytes)
}
