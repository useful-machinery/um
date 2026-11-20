//! A stdio MCP client for tests that stand in for a harness: it starts the one server an MCP
//! configuration file names, as Claude Code does, and speaks newline-delimited JSON-RPC to it.

use std::fs;
use std::io::{self, BufRead as _, BufReader, Write as _};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;

use serde_json::{Map, Value, json};

use crate::watchdog::{FIXTURE_WAIT_BASE, WatchdogRecvError, host_scaled_recv};

/// The protocol version the client names in `initialize`, as Claude Code 2.1.321 does.
pub const MCP_PROTOCOL_VERSION: &str = "2025-06-18";

/// A tool call's answer: whether the server marked it an error, and its text content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpToolAnswer {
    pub is_error: bool,
    pub text: String,
}

/// A running stdio MCP server. Dropping it kills the server and reaps it.
pub struct McpServerProcess {
    child: Child,
    input: ChildStdin,
    messages: Receiver<io::Result<Value>>,
    next_id: u64,
}

impl McpServerProcess {
    /// Starts the one server under `mcpServers` in the configuration file at `configuration`
    /// with the entry's `command`, `args` and `env`, over the caller's environment.
    pub fn start(configuration: &Path) -> io::Result<Self> {
        let document: Value = serde_json::from_slice(&fs::read(configuration)?)
            .map_err(|error| invalid(format!("MCP configuration: {error}")))?;
        let servers = document
            .get("mcpServers")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("MCP configuration names no mcpServers"))?;
        let mut entries = servers.values();
        let (Some(server), None) = (entries.next(), entries.next()) else {
            return Err(invalid(
                "MCP configuration does not name exactly one server",
            ));
        };
        let command = server
            .get("command")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("MCP server has no command"))?;
        let mut process = Command::new(command);
        for argument in server
            .get("args")
            .and_then(Value::as_array)
            .map_or(&[][..], Vec::as_slice)
        {
            process.arg(
                argument
                    .as_str()
                    .ok_or_else(|| invalid("MCP server argument is not a string"))?,
            );
        }
        for (name, value) in server
            .get("env")
            .and_then(Value::as_object)
            .unwrap_or(&Map::new())
        {
            process.env(
                name,
                value
                    .as_str()
                    .ok_or_else(|| invalid("MCP server environment value is not a string"))?,
            );
        }
        let mut child = process
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let (Some(input), Some(output)) = (child.stdin.take(), child.stdout.take()) else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(invalid("MCP server pipes unavailable"));
        };
        let (sender, messages) = mpsc::channel();
        thread::Builder::new()
            .name("mcp-server-output".to_owned())
            .spawn(move || {
                for line in BufReader::new(output).lines() {
                    let message = line.and_then(|line| {
                        serde_json::from_str(&line)
                            .map_err(|error| invalid(format!("MCP server line: {error}")))
                    });
                    if sender.send(message).is_err() {
                        return;
                    }
                }
            })?;
        Ok(Self {
            child,
            input,
            messages,
            next_id: 1,
        })
    }

    /// Writes one JSON-RPC message as a line.
    pub fn send(&mut self, message: &Value) -> io::Result<()> {
        let mut line = serde_json::to_vec(message).map_err(invalid)?;
        line.push(b'\n');
        self.input.write_all(&line)?;
        self.input.flush()
    }

    /// The next message the server writes, under the host-scaled fixture bound; an error once
    /// the server has closed its output.
    pub fn receive(&mut self) -> io::Result<Value> {
        match host_scaled_recv("MCP server message", FIXTURE_WAIT_BASE, &self.messages) {
            Ok(message) => message,
            Err(WatchdogRecvError::Expired(expired)) => {
                Err(io::Error::new(io::ErrorKind::TimedOut, expired.to_string()))
            }
            Err(WatchdogRecvError::Disconnected { .. }) => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "MCP server closed its output",
            )),
        }
    }

    /// Sends a request with a fresh numeric id and returns the response that carries it.
    pub fn request(&mut self, method: &str, params: Value) -> io::Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        self.send(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))?;
        loop {
            let message = self.receive()?;
            if message.get("id") == Some(&json!(id)) {
                return Ok(message);
            }
        }
    }

    /// `initialize` as Claude Code sends it, then `notifications/initialized`; the response's
    /// `result`, or an error carrying the JSON-RPC error the server answered.
    pub fn initialize(&mut self) -> io::Result<Value> {
        let response = self.request(
            "initialize",
            json!({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"roots": {}, "elicitation": {}},
                "clientInfo": {"name": "claude-code", "version": "2.1.321"},
            }),
        )?;
        let result = response_result(response)?;
        self.send(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))?;
        Ok(result)
    }

    /// `tools/call` for the tool `name` with `arguments` verbatim, carrying the `_meta` Claude
    /// Code adds; the answer's `isError` and its text content joined.
    pub fn call_tool(&mut self, name: &str, arguments: Value) -> io::Result<McpToolAnswer> {
        let progress = self.next_id;
        let response = self.request(
            "tools/call",
            json!({
                "name": name,
                "arguments": arguments,
                "_meta": {
                    "claudecode/toolUseId": format!("toolu_{progress}"),
                    "progressToken": progress,
                },
            }),
        )?;
        tool_answer(&response_result(response)?)
    }
}

impl Drop for McpServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A `tools/call` result's `isError` and its text content joined.
pub fn tool_answer(result: &Value) -> io::Result<McpToolAnswer> {
    let content = result
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("tool answer has no content"))?;
    let mut text = String::new();
    for block in content {
        text.push_str(
            block
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("tool answer content is not text"))?,
        );
    }
    Ok(McpToolAnswer {
        is_error: result
            .get("isError")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        text,
    })
}

fn response_result(response: Value) -> io::Result<Value> {
    if let Some(error) = response.get("error") {
        return Err(io::Error::other(format!("MCP server answered {error}")));
    }
    response
        .get("result")
        .cloned()
        .ok_or_else(|| invalid("MCP response has no result"))
}

fn invalid(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
