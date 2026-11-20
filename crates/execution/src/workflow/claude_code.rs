#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClaudeCodeConfig {
    pub(crate) model: String,
    pub(crate) effort: ClaudeCodeEffort,
}

// Claude effort is deliberately independent of Pi thinking because the harness contracts differ.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub(crate) enum ClaudeCodeEffort {
    #[serde(rename = "low")]
    Low,
    #[serde(rename = "medium")]
    Medium,
    #[serde(rename = "high")]
    High,
    #[serde(rename = "xhigh")]
    XHigh,
    #[serde(rename = "max")]
    Max,
}

impl ClaudeCodeEffort {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
        }
    }
}

/// The model a login check asks for (INV-HARNESS-18), which has no workflow profile: the
/// least expensive current Anthropic model that accepts an effort, since the adapter always
/// passes `--effort`, and the one the live Claude Code smoke pins against the qualification
/// version `CLAUDE_CODE_STREAM_JSON_V1_QUALIFICATION_VERSION` (2.1.321). A login whose
/// account may not use it is refused, as the provider classifies that `unauthorized`.
pub(crate) const LOGIN_CHECK_MODEL: &str = "claude-sonnet-5";
/// The lowest effort the adapter accepts.
pub(crate) const LOGIN_CHECK_EFFORT: ClaudeCodeEffort = ClaudeCodeEffort::Low;

// Keep this direct decoder local so Claude's closed configuration can evolve independently.
pub(crate) fn resolve_config(value: &Value) -> Option<ClaudeCodeConfig> {
    let config = serde_json::from_value::<ClaudeCodeConfig>(value.clone()).ok()?;
    if config.model.is_empty() {
        return None;
    }

    Some(ClaudeCodeConfig {
        model: config.model,
        effort: config.effort,
    })
}

#[cfg(test)]
mod config_tests {
    use super::*;

    #[test]
    fn resolves_only_the_closed_nonempty_model_and_effort_shape() {
        for (effort, expected) in [
            ("low", ClaudeCodeEffort::Low),
            ("medium", ClaudeCodeEffort::Medium),
            ("high", ClaudeCodeEffort::High),
            ("xhigh", ClaudeCodeEffort::XHigh),
            ("max", ClaudeCodeEffort::Max),
        ] {
            assert_eq!(
                resolve_config(&serde_json::json!({
                    "model": "claude-opus-4-1",
                    "effort": effort,
                })),
                Some(ClaudeCodeConfig {
                    model: "claude-opus-4-1".to_owned(),
                    effort: expected,
                })
            );
        }

        for invalid in [
            serde_json::json!({"effort": "high"}),
            serde_json::json!({"model": "", "effort": "high"}),
            serde_json::json!({"model": "claude-opus-4-1"}),
            serde_json::json!({"model": "claude-opus-4-1", "effort": "off"}),
            serde_json::json!({
                "model": "claude-opus-4-1",
                "effort": "high",
                "fallbackModel": "claude-sonnet-4",
            }),
        ] {
            assert_eq!(resolve_config(&invalid), None);
        }
    }
}

pub(crate) mod adapter;
mod outcome;
mod reader;
mod result_channel;
pub(crate) mod result_tool;

use std::ffi::OsString;
use std::num::NonZeroU64;
use std::path::Path;

use serde::Deserialize;
use serde_json::{Value, json};

#[cfg(test)]
const QUALIFICATION_VERSION: &str =
    crate::claude_code::CLAUDE_CODE_STREAM_JSON_V1_QUALIFICATION_VERSION;

const MAXIMUM_FRAME_BYTES: u64 = 16 * 1024 * 1024;
const PERMISSION_MODE: &str = "bypassPermissions";

/// Set for every invocation over the runner's environment. Claude Code exits by itself five
/// seconds after its work is done (`CLAUDE_CODE_EXIT_AFTER_STOP_DELAY`), which is how a step
/// ends (REQ-AGENT-35).
pub(crate) const FIXED_INVOCATION_ENVIRONMENT: [(&str, &str); 6] = [
    ("DISABLE_UPDATES", "1"),
    ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1"),
    ("CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL", "1"),
    ("CLAUDE_CODE_DISABLE_AUTO_MEMORY", "1"),
    ("CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS", "1"),
    ("CLAUDE_CODE_EXIT_AFTER_STOP_DELAY", "5000"),
];

// Keep each closed profile's limits beside its native adapter so future profile changes
// cannot silently alter another harness's admission contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClaudeCodeStreamJsonV1ProtocolLimits {
    maximum_frame_bytes: NonZeroU64,
}

impl ClaudeCodeStreamJsonV1ProtocolLimits {
    pub(crate) const fn profile() -> Self {
        let maximum_frame_bytes = match NonZeroU64::new(MAXIMUM_FRAME_BYTES) {
            Some(value) => value,
            None => NonZeroU64::MIN,
        };
        Self {
            maximum_frame_bytes,
        }
    }

    /// The longest output line the reader reads; a longer one is dropped with a warning.
    pub(crate) const fn maximum_frame_bytes(self) -> NonZeroU64 {
        self.maximum_frame_bytes
    }
}

pub(crate) fn normal_mode_arguments(
    model: &str,
    effort: &str,
    session_id: &str,
    system_prompt_file: &Path,
) -> Vec<OsString> {
    [
        OsString::from("-p"),
        OsString::from("--input-format"),
        OsString::from("stream-json"),
        OsString::from("--output-format"),
        OsString::from("stream-json"),
        OsString::from("--verbose"),
        OsString::from("--include-partial-messages"),
        OsString::from("--forward-subagent-text"),
        OsString::from("--session-id"),
        OsString::from(session_id),
        OsString::from("--permission-mode"),
        OsString::from(PERMISSION_MODE),
        OsString::from("--setting-sources"),
        OsString::from("user,project,local"),
        OsString::from("--model"),
        OsString::from(model),
        OsString::from("--effort"),
        OsString::from(effort),
        OsString::from("--append-system-prompt-file"),
        system_prompt_file.as_os_str().to_owned(),
    ]
    .into()
}

/// The normal arguments and then `--mcp-config` naming the result tool's configuration. The
/// flag takes every value that follows it, so it must be last.
pub(crate) fn result_mode_arguments(
    model: &str,
    effort: &str,
    session_id: &str,
    system_prompt_file: &Path,
    mcp_configuration_file: &Path,
) -> Vec<OsString> {
    let mut arguments = normal_mode_arguments(model, effort, session_id, system_prompt_file);
    arguments.extend([
        OsString::from("--mcp-config"),
        mcp_configuration_file.as_os_str().to_owned(),
    ]);
    arguments
}

pub(crate) fn user_content_frame(content: Vec<Value>) -> Result<Vec<u8>, serde_json::Error> {
    let mut frame = serde_json::to_vec(&json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": content,
        },
    }))?;
    frame.push(b'\n');
    Ok(frame)
}

pub(crate) fn initial_user_text_frame(message: &str) -> Result<Vec<u8>, serde_json::Error> {
    user_content_frame(vec![json!({
        "type": "text",
        "text": message,
    })])
}

#[cfg(test)]
mod adapter_tests;
#[cfg(test)]
mod conformance_tests;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
