//! Bounded, redacted copies of harness stream lines kept beside a protocol rejection.
//!
//! `redact_secrets` is the twin of `SECRET_SHAPES` in `.um/cloud/scripts/workflow_support.py`;
//! the two languages cannot share one source, so change both together.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::LazyLock;

use regex::Regex;
use serde::{Deserialize, Serialize};

/// One stream line as retained: its redacted text cut to a bound, its original size in bytes
/// and whether the bound cut it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RetainedStreamLine {
    text: String,
    bytes: u64,
    truncated: bool,
}

impl RetainedStreamLine {
    /// `line` read as lossy UTF-8 with its secret shapes redacted, then cut at the last
    /// character boundary at or before `maximum_bytes`; redacting first means a cut never keeps
    /// the head of a secret.
    pub(crate) fn retain(line: &[u8], maximum_bytes: NonZeroUsize) -> Self {
        let text = String::from_utf8_lossy(line);
        let redacted = redact_secrets(&text);
        let end = redacted.floor_char_boundary(maximum_bytes.get());
        Self {
            text: redacted[..end].to_owned(),
            bytes: u64::try_from(line.len()).unwrap_or(u64::MAX),
            truncated: end < redacted.len(),
        }
    }
}

/// The rejected line, when the rejection has one, and the accepted lines before it, oldest first.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RejectedStreamExcerpt {
    #[serde(skip_serializing_if = "Option::is_none")]
    rejected_line: Option<RetainedStreamLine>,
    preceding_lines: Vec<RetainedStreamLine>,
}

impl RejectedStreamExcerpt {
    pub(crate) fn new(
        rejected_line: Option<RetainedStreamLine>,
        preceding_lines: Vec<RetainedStreamLine>,
    ) -> Self {
        Self {
            rejected_line,
            preceding_lines,
        }
    }
}

/// The most recent accepted stream lines, each retained at a bound.
#[derive(Debug)]
pub(crate) struct StreamTail {
    lines: VecDeque<RetainedStreamLine>,
    capacity: NonZeroUsize,
    maximum_line_bytes: NonZeroUsize,
}

impl StreamTail {
    pub(crate) fn new(lines: NonZeroUsize, maximum_line_bytes: NonZeroUsize) -> Self {
        Self {
            lines: VecDeque::with_capacity(lines.get()),
            capacity: lines,
            maximum_line_bytes,
        }
    }

    /// Retains `line`, evicting the oldest line once more than the capacity are held.
    pub(crate) fn push(&mut self, line: &[u8]) {
        if self.lines.len() == self.capacity.get() {
            self.lines.pop_front();
        }
        self.lines
            .push_back(RetainedStreamLine::retain(line, self.maximum_line_bytes));
    }

    /// The retained lines, oldest first.
    pub(crate) fn lines(&self) -> Vec<RetainedStreamLine> {
        self.lines.iter().cloned().collect()
    }
}

// The secret shapes a check or a finalizer may echo into text a failure record quotes, each
// replaced whole by `REDACTED`: GitHub token prefixes, a bearer credential, the value of a
// token-, key-, secret- or password-named query or assignment (the name may carry a prefix, as
// in `client_secret`), and a URL authority carrying a password. A GitHub shape matches without
// a leading word boundary where a stripped control character may have joined it to the text
// before it; a bearer credential is the rest of its line, never the next line's first word.
// An assignment's value is the word after `=`: a quoted piece runs through its closing quote,
// the quotes bare or each behind one backslash as a JSON string carries them (there a quote
// behind 4n+3 backslashes is the value's own and one behind 4n+1 closes it), a single-quoted
// piece that reading backslashes as escapes leaves unclosed closes at its next single quote,
// as a shell reads it, the pieces joined to it belong to the value, as does a
// backslash-escaped single quote before a single-quoted piece (`'it'\''s'`); after the first
// piece a double quote, bare or behind a backslash, ends the value unless it is the `"'"` that
// joins two single-quoted pieces, and an unquoted run stops before the backslashes in front of
// a double quote; and a quote that never closes takes the rest of its line or of the JSON
// string holding it.
const REDACTED: &str = "[redacted]";
const SECRET_SHAPES: [(&str, &str); 5] = [
    (r"gh[pousr]_[A-Za-z0-9]{16,}", REDACTED),
    (r"github_pat_[A-Za-z0-9_]{16,}", REDACTED),
    (r"(?i)(\bBearer[ \t]+)[A-Za-z0-9._~+/=-]+", "${1}[redacted]"),
    (
        r#"(?i)(\b[a-z_-]*(?:token|api[_-]?key|secret|password|passwd)=)(?:\\"(?:(?:\\{4})*(?:\\{3}"|\\{0,3}[^"\\\n]))*(?:\\{4})*\\"|"(?:\\.|[^"\\\n])*"|'(?:\\.|[^'\\\n])*'|'[^'\n]*'|\\*["'](?:\\.|[^"\\\n])*|[^&\s"'<>])(?:\\{1,2}''(?:\\.|[^'\\\n])*'|\\{1,2}''[^'\n]*'|\\*'(?:\\.|[^'\\\n])*'|\\*'[^'\n]*'|"'"|\\"'\\"|[^&\s"'<>\\]|\\+[^&\s"'<>\\])*"#,
        "${1}[redacted]",
    ),
    (r"(://)[^/\s@:]+:[^/\s@]+@", "${1}[redacted]@"),
];

static COMPILED_SECRET_SHAPES: LazyLock<Result<Vec<(Regex, &'static str)>, regex::Error>> =
    LazyLock::new(|| {
        SECRET_SHAPES
            .iter()
            .map(|(pattern, replacement)| Ok((Regex::new(pattern)?, *replacement)))
            .collect()
    });

/// `text` with every secret shape replaced by `[redacted]`, in `SECRET_SHAPES` order, borrowed
/// when no shape matched so an accepted frame is not copied on its way into the tail. If the
/// shapes could not be built, the whole text is withheld rather than kept unredacted.
pub(crate) fn redact_secrets(text: &str) -> Cow<'_, str> {
    let Ok(shapes) = COMPILED_SECRET_SHAPES.as_ref() else {
        return Cow::Borrowed(REDACTED);
    };
    let mut text = Cow::Borrowed(text);
    for (pattern, replacement) in shapes {
        let replaced = match pattern.replace_all(&text, *replacement) {
            Cow::Borrowed(_) => None,
            Cow::Owned(replaced) => Some(replaced),
        };
        if let Some(replaced) = replaced {
            text = Cow::Owned(replaced);
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use serde_json::json;

    use super::{REDACTED, RejectedStreamExcerpt, RetainedStreamLine, StreamTail, redact_secrets};

    fn bound(bytes: usize) -> NonZeroUsize {
        NonZeroUsize::new(bytes).unwrap_or(NonZeroUsize::MIN)
    }

    fn assert_redacted(line: &str, secret: &str, kept: &str) {
        let redacted = redact_secrets(line);
        assert!(!redacted.contains(secret), "{redacted}");
        assert!(redacted.contains(REDACTED), "{redacted}");
        assert!(redacted.contains(kept), "{redacted}");
    }

    /// A GitHub token shape for `prefix`, assembled at run time so the source never carries a
    /// literal a secret scan would flag as a credential.
    fn github_token_shaped(prefix: &str) -> String {
        format!("{prefix}{}", "abcdefghij0123456789")
    }

    // REQ-AGENT-71
    #[test]
    fn github_classic_token_is_redacted() {
        let token = github_token_shaped("ghp_");
        assert_redacted(&format!("push with {token} now"), &token, "push with ");
    }

    // REQ-AGENT-71
    #[test]
    fn github_fine_grained_token_is_redacted() {
        let token = github_token_shaped("github_pat_");
        assert_redacted(&format!("token {token}"), &token, "token ");
    }

    // REQ-AGENT-71
    #[test]
    fn bearer_credential_is_redacted_keeping_its_scheme() {
        assert_redacted(
            "Authorization: Bearer abc.def-ghi",
            "abc.def-ghi",
            "Authorization: Bearer [redacted]",
        );
    }

    // REQ-AGENT-71
    #[test]
    fn secret_named_assignments_are_redacted_keeping_the_name() {
        let redacted = redact_secrets("client_secret=hunter2&x=1");
        assert_eq!(redacted, "client_secret=[redacted]&x=1");
        assert_redacted("API_KEY=abc", "abc", "API_KEY=[redacted]");
    }

    /// A Claude Code assistant frame calling the shell tool with `command`.
    fn tool_use_frame(command: &str) -> String {
        json!({
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": "toolu_1",
                    "name": "Bash",
                    "input": {"command": command},
                }],
            },
            "parent_tool_use_id": null,
            "session_id": "session",
        })
        .to_string()
    }

    // REQ-AGENT-71
    #[test]
    fn quoted_assignment_value_is_redacted_through_its_closing_quote() {
        let cases = [
            ("client_secret=hunter2&x=1", "client_secret=[redacted]&x=1"),
            (
                r#"export API_TOKEN="s3cr3t v" && run"#,
                "export API_TOKEN=[redacted] && run",
            ),
            ("PGPASSWORD='s3cr3t v' psql", "PGPASSWORD=[redacted] psql"),
            (
                r#"DB_PASSWORD="a\" s3cr3t v" x"#,
                "DB_PASSWORD=[redacted] x",
            ),
            (
                r#"DB_PASSWORD="s3cr3t v\\" x "y""#,
                r#"DB_PASSWORD=[redacted] x "y""#,
            ),
            (r#"PASSWORD="" x"#, "PASSWORD=[redacted] x"),
            (
                r#"PGPASSWORD='a"s3cr3t v' psql"#,
                "PGPASSWORD=[redacted] psql",
            ),
            (
                r#"PGPASSWORD='it'"'"'s3cr3t v' psql"#,
                "PGPASSWORD=[redacted] psql",
            ),
            (
                r"PGPASSWORD='it'\''s3cr3t v' psql",
                "PGPASSWORD=[redacted] psql",
            ),
            (r#"TOKEN="s3cr3t v"tail next"#, "TOKEN=[redacted] next"),
            (r#"TOKEN="s3cr3t v"#, "TOKEN=[redacted]"),
            ("TOKEN='s3cr3t v", "TOKEN=[redacted]"),
            (r#"grep -n "token=" file"#, r#"grep -n "token=[redacted]"#),
            ("token=<TOKEN>", "token=<TOKEN>"),
            (
                r#"echo "TOKEN=abc" && echo "done""#,
                r#"echo "TOKEN=[redacted]" && echo "done""#,
            ),
            (r#"echo "TOKEN=abc""#, r#"echo "TOKEN=[redacted]""#),
            (r"PGPASSWORD=abc'def\' psql", "PGPASSWORD=[redacted] psql"),
            (
                r#"PGPASSWORD='a"b\'s3cr3t psql"#,
                "PGPASSWORD=[redacted] psql",
            ),
            (
                r"PGPASSWORD='it'\''s3cr3t v\' psql",
                "PGPASSWORD=[redacted] psql",
            ),
        ];
        for (line, redacted) in cases {
            assert_eq!(redact_secrets(line), redacted, "plain: {line}");
            let frame = tool_use_frame(line);
            let framed = redact_secrets(&frame);
            assert_eq!(framed, tool_use_frame(redacted), "framed: {line}");
            assert!(
                serde_json::from_str::<serde_json::Value>(&framed).is_ok(),
                "decodes: {framed}"
            );
        }
        assert_eq!(
            redact_secrets("TOKEN=\"s3cr3t v\nnext \"q\""),
            "TOKEN=[redacted]\nnext \"q\""
        );
        let nested = json!({"command": r#"export TOKEN="s3cr3t v" x"#}).to_string();
        assert_eq!(
            redact_secrets(&tool_use_frame(&nested)),
            tool_use_frame(r#"{"command":"export TOKEN=[redacted]"#)
        );
    }

    // REQ-AGENT-71
    #[test]
    fn quoted_assignment_in_a_rejected_stream_keeps_no_value() {
        let frame = tool_use_frame(r#"export API_TOKEN="s3cr3t v" && run"#);
        let mut tail = StreamTail::new(bound(8), bound(1024));
        tail.push(frame.as_bytes());
        let rejected = RetainedStreamLine::retain(frame.as_bytes(), bound(8192));
        let mut retained = tail.lines();
        retained.push(rejected);
        assert_eq!(retained.len(), 2);
        for line in retained {
            assert!(!line.text.contains("s3cr3t"), "{}", line.text);
            assert!(!line.truncated);
            let command = serde_json::from_str::<serde_json::Value>(&line.text)
                .ok()
                .and_then(|frame| {
                    frame
                        .pointer("/message/content/0/input/command")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                });
            assert_eq!(
                command.as_deref(),
                Some("export API_TOKEN=[redacted] && run")
            );
        }
    }

    // REQ-AGENT-71
    #[test]
    fn url_password_is_redacted_keeping_the_host() {
        assert_eq!(
            redact_secrets("clone https://user:pw@host/path"),
            "clone https://[redacted]@host/path"
        );
    }

    // REQ-AGENT-71
    #[test]
    fn line_without_a_secret_shape_is_unchanged() {
        let line = r#"{"type":"assistant","message":{"content":"tokens are fine"}}"#;
        assert_eq!(redact_secrets(line), line);
    }

    // REQ-AGENT-71
    #[test]
    fn secret_straddling_the_bound_is_redacted_before_the_cut() {
        let token = format!("ghp_{}", "Z".repeat(36));
        let line = format!("prefix: {token} trailing");
        let retained = RetainedStreamLine::retain(line.as_bytes(), bound(16));
        assert_eq!(retained.text, "prefix: [redacte");
        assert!(!retained.text.contains("gh"));
        assert!(!retained.text.contains('Z'));
        assert!(retained.truncated);
        assert_eq!(
            retained.bytes,
            u64::try_from(line.len()).unwrap_or(u64::MAX)
        );
    }

    // REQ-AGENT-71
    #[test]
    fn bound_inside_a_multibyte_character_cuts_before_it() {
        let line = "ab\u{20ac}cd";
        let retained = RetainedStreamLine::retain(line.as_bytes(), bound(3));
        assert_eq!(retained.text, "ab");
        assert!(retained.truncated);
        assert_eq!(retained.bytes, 7);
    }

    // REQ-AGENT-71
    #[test]
    fn line_within_the_bound_is_kept_whole() {
        let retained = RetainedStreamLine::retain(b"{\"type\":\"system\"}", bound(64));
        assert_eq!(retained.text, "{\"type\":\"system\"}");
        assert!(!retained.truncated);
        assert_eq!(retained.bytes, 17);
    }

    // REQ-AGENT-71
    #[test]
    fn invalid_utf8_keeps_the_lossy_text() {
        let retained = RetainedStreamLine::retain(b"ok\xffend", bound(64));
        assert_eq!(retained.text, "ok\u{fffd}end");
        assert!(!retained.truncated);
        assert_eq!(retained.bytes, 6);
    }

    // REQ-AGENT-71
    #[test]
    fn stream_tail_keeps_the_most_recent_lines_oldest_first() {
        let mut tail = StreamTail::new(bound(2), bound(4));
        tail.push(b"one");
        tail.push(b"two");
        tail.push(b"three");
        let texts: Vec<_> = tail.lines().into_iter().map(|line| line.text).collect();
        assert_eq!(texts, ["two", "thre"]);
    }

    // REQ-AGENT-71
    #[test]
    fn excerpt_names_its_lines_and_omits_an_absent_rejected_line() {
        let mut tail = StreamTail::new(bound(8), bound(1024));
        tail.push(b"accepted");
        let rejected = RetainedStreamLine::retain(b"rejected", bound(8192));
        let with_line = RejectedStreamExcerpt {
            rejected_line: Some(rejected),
            preceding_lines: tail.lines(),
        };
        assert_eq!(
            serde_json::to_value(&with_line).ok(),
            Some(json!({
                "rejectedLine": {"text": "rejected", "bytes": 8, "truncated": false},
                "precedingLines": [{"text": "accepted", "bytes": 8, "truncated": false}],
            }))
        );
        let without_line = RejectedStreamExcerpt {
            rejected_line: None,
            preceding_lines: tail.lines(),
        };
        let value = serde_json::to_value(&without_line).ok();
        assert_eq!(
            value,
            Some(json!({
                "precedingLines": [{"text": "accepted", "bytes": 8, "truncated": false}],
            }))
        );
        let decoded = value.and_then(|value| serde_json::from_value(value).ok());
        assert_eq!(decoded, Some(without_line));
    }
}
