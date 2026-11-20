use std::ffi::OsString;
use std::path::Path;

use serde_json::{Value, json};

use super::*;

const SESSION_ID: &str = "00000000-0000-4000-8000-000000000001";

// REQ-AGENT-16, REQ-AGENT-60
// INV-HARNESS-3
#[test]
fn normal_mode_arguments_and_input_frame_are_exact() {
    let normal = [
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
        "--include-partial-messages",
        "--forward-subagent-text",
        "--session-id",
        SESSION_ID,
        "--permission-mode",
        "bypassPermissions",
        "--setting-sources",
        "user,project,local",
        "--model",
        "claude-profile-model",
        "--effort",
        "xhigh",
        "--append-system-prompt-file",
        "/synthetic/private/system-prompt",
    ]
    .map(OsString::from);
    assert_eq!(
        normal_mode_arguments(
            "claude-profile-model",
            "xhigh",
            SESSION_ID,
            Path::new("/synthetic/private/system-prompt"),
        ),
        normal
    );
    // `--mcp-config` takes every value after it, so it and its file come last.
    let mut result = normal.to_vec();
    result.extend(["--mcp-config", "/synthetic/private/result-tool-mcp.json"].map(OsString::from));
    assert_eq!(
        result_mode_arguments(
            "claude-profile-model",
            "xhigh",
            SESSION_ID,
            Path::new("/synthetic/private/system-prompt"),
            Path::new("/synthetic/private/result-tool-mcp.json"),
        ),
        result
    );
    assert_eq!(
        FIXED_INVOCATION_ENVIRONMENT,
        [
            ("DISABLE_UPDATES", "1"),
            ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1"),
            ("CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL", "1"),
            ("CLAUDE_CODE_DISABLE_AUTO_MEMORY", "1"),
            ("CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS", "1"),
            ("CLAUDE_CODE_EXIT_AFTER_STOP_DELAY", "5000"),
        ]
    );

    let frame = initial_user_text_frame("exact\nuser text").unwrap();
    assert_eq!(frame.last(), Some(&b'\n'));
    assert_eq!(frame.iter().filter(|byte| **byte == b'\n').count(), 1);
    assert_eq!(
        serde_json::from_slice::<Value>(&frame[..frame.len() - 1]).unwrap(),
        json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{"type": "text", "text": "exact\nuser text"}],
            },
        })
    );
}
