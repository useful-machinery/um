use std::cell::Cell;
use std::ffi::OsString;
use std::fs;
use std::io::Read as _;
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use nix::sys::stat::Mode;
use nix::unistd::mkfifo;
use serde_json::{Value, json};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use super::adapter::ClaudeCodeStreamJsonV1Adapter;
use super::{ClaudeCodeStreamJsonV1ProtocolLimits, FIXED_INVOCATION_ENVIRONMENT, OutstandingWork};
use crate::claude_code::CLAUDE_CODE_STREAM_JSON_V1_QUALIFICATION_VERSION as QUALIFICATION_VERSION;
use crate::workflow::admission::{CancellationSource, EnvironmentSnapshot};
use crate::workflow::agent::{
    AdmittedAgentAdapter, AgentAdapter, AgentCompatibilityProfile, AgentDiagnosticLevel,
    AgentInvocation, AgentInvocationIdentity, AgentInvocationLimits, AgentInvocationStaging,
    AgentLifecycleMilestone, AgentObservation, AgentOutcome, AgentProcessContext,
    AgentProcessControl, AgentPrompt, AgentStartReceiver, AgentValueMode, PositiveDuration,
    WorkflowRunId, agent_start_channel,
};
use crate::workflow::agent_diagnostics::AgentDiagnosticSession;
use crate::workflow::claude_code::{ClaudeCodeConfig, ClaudeCodeEffort};
use crate::workflow::diagnostic::StepDiagnosticLog;
use crate::workflow::execution_root::AdmittedExecutionRoot;
use crate::workflow::observation::NoopExecutionObserver;
use crate::workflow::process_group::ProcessGuardRegistry;
use crate::workflow::runtime::{ActionId, TransitionSequence};

const MAXIMUM_HTTP_HEADER_BYTES: usize = 64 * 1024;
const MAXIMUM_PROVIDER_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const PLACEHOLDER_API_KEY: &str = "scherzo-loopback-placeholder";
const CONFORMANCE_SHELL_ENVIRONMENT: &str = "UM_CLAUDE_CODE_CONFORMANCE_SHELL";
/// Opaque placeholder for the native `signature_delta` that accompanies a thinking
/// block. Claude Code forwards this value without interpreting it in one exchange.
const THINKING_SIGNATURE: &str = "c2NoZXJ6by1sb29wYmFjay10aGlua2luZy1zaWduYXR1cmU=";
const STRUCTURED_OUTPUT_TOOL_NAME: &str = "StructuredOutput";

#[derive(Clone)]
pub(super) struct FixtureSignal {
    path: Arc<Path>,
    reader: Arc<fs::File>,
}

impl FixtureSignal {
    pub(super) fn create(path: PathBuf) -> Self {
        mkfifo(&path, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        let reader = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&path)
            .unwrap();
        Self {
            path: Arc::from(path),
            reader: Arc::new(reader),
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) async fn receive(&self) -> Vec<u8> {
        let reader = AsyncFd::new(self.reader.try_clone().unwrap()).unwrap();
        let mut signal = [0; 64];
        loop {
            let mut ready = reader.readable().await.unwrap();
            match ready.try_io(|reader| {
                let mut reader = reader.get_ref();
                reader.read(&mut signal)
            }) {
                Ok(Ok(0)) => panic!("fixture signal closed without a value"),
                Ok(Ok(read)) => return signal[..read].to_vec(),
                Ok(Err(error)) => panic!("failed to read fixture signal: {error}"),
                Err(_) => {}
            }
        }
    }
}

pub(super) use crate::workflow::test_support::{PendingClock, RecordingObservationSink};

pub(super) fn invocation_identity(run: &str, step: &str) -> AgentInvocationIdentity {
    AgentInvocationIdentity::new(
        WorkflowRunId::from(Arc::from(run)),
        Arc::from(step),
        ActionId {
            transition_sequence: TransitionSequence::default(),
        },
    )
}

pub(super) fn admitted_adapter(
    executable: PathBuf,
    model: &str,
) -> AdmittedAgentAdapter<ClaudeCodeConfig> {
    AdmittedAgentAdapter::new(
        AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
        executable,
        Arc::from(QUALIFICATION_VERSION),
        ClaudeCodeConfig {
            model: model.to_owned(),
            effort: ClaudeCodeEffort::XHigh,
        },
    )
}

pub(super) struct NativeResourceFixture {
    pub(super) hook_observation: PathBuf,
    pub(super) expected_hook_observation: String,
    pub(super) mcp_observation: PathBuf,
    pub(super) marketplace: PathBuf,
}

pub(super) struct SyntheticClaudeCodeRoot {
    _temporary: tempfile::TempDir,
    project: PathBuf,
    home: PathBuf,
    config: PathBuf,
    private: PathBuf,
    shell: PathBuf,
    system_prompt: PathBuf,
    environment_overrides: Vec<(OsString, OsString)>,
}

impl SyntheticClaudeCodeRoot {
    pub(super) fn new() -> Self {
        Self::with_environment_overrides(&[])
    }

    /// A root whose child environment also carries `overrides`, for a harness knob a case
    /// sets, such as Claude Code's foreground-to-background promotion timeout. The names
    /// must not repeat a variable the root already sets.
    pub(super) fn with_environment_overrides(overrides: &[(&str, &str)]) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let project = temporary.path().join("project");
        let home = temporary.path().join("home");
        let config = temporary.path().join("claude-config");
        let private = temporary.path().join("adapter-private");
        for directory in [&project, &home, &config, &private] {
            fs::create_dir(directory).unwrap();
        }
        fs::write(
            project.join("README.md"),
            b"synthetic conformance project\n",
        )
        .unwrap();
        let system_prompt = temporary.path().join("system-prompt.txt");
        fs::write(
            &system_prompt,
            b"Operate only on the deterministic synthetic conformance request.\n",
        )
        .unwrap();

        let repository = fs::canonicalize(env!("CARGO_MANIFEST_DIR"))
            .unwrap()
            .parent()
            .unwrap()
            .to_owned();
        let canonical_project = fs::canonicalize(&project).unwrap();
        assert!(!canonical_project.starts_with(repository));
        let shell = std::env::var_os(CONFORMANCE_SHELL_ENVIRONMENT)
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                panic!("{CONFORMANCE_SHELL_ENVIRONMENT} must name the controlled conformance shell")
            });
        let shell = fs::canonicalize(shell).unwrap();
        assert!(shell.is_file());

        Self {
            _temporary: temporary,
            project: canonical_project,
            home,
            config,
            private,
            shell,
            system_prompt,
            environment_overrides: overrides
                .iter()
                .map(|(name, value)| (OsString::from(name), OsString::from(value)))
                .collect(),
        }
    }

    pub(super) fn project(&self) -> &Path {
        &self.project
    }

    pub(super) fn system_prompt(&self) -> &Path {
        &self.system_prompt
    }

    pub(super) fn private(&self) -> &Path {
        &self.private
    }

    pub(super) fn install_native_resource_fixture(
        &self,
        instruction_marker: &str,
        skill_marker: &str,
        plugin_marker: &str,
        mcp_marker: &str,
    ) -> NativeResourceFixture {
        fs::write(
            self.project.join("CLAUDE.md"),
            format!("# Qualification instructions\n\n{instruction_marker}\n"),
        )
        .unwrap();
        let project_skill = self.project.join(".claude/skills/scherzo-resource");
        fs::create_dir_all(&project_skill).unwrap();
        fs::write(
            project_skill.join("SKILL.md"),
            format!(
                "---\nname: scherzo-resource\ndescription: {skill_marker}\n---\n\n# Qualification skill\n"
            ),
        )
        .unwrap();

        let hook_observation = self.private.join("native-hook-observation.txt");
        let hook_script = self.private.join("native-hook.sh");
        fs::write(
            &hook_script,
            format!(
                "set -eu\nprintf '%s\\n' \\\n  \"$UM_USER_SETTING\" \\\n  \"$UM_PROJECT_SETTING\" \\\n  \"$UM_LOCAL_SETTING\" \\\n  \"$CLAUDE_CONFIG_DIR\" \\\n  \"$DISABLE_UPDATES\" \\\n  \"$CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC\" \\\n  \"$CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL\" \\\n  \"$CLAUDE_CODE_DISABLE_AUTO_MEMORY\" \\\n  \"$CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS\" \\\n  > '{}'\n",
                hook_observation.display(),
            ),
        )
        .unwrap();

        fs::write(
            self.config.join("settings.json"),
            serde_json::to_vec_pretty(&json!({
                "env": {
                    "UM_USER_SETTING": "user"
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let project_settings = self.project.join(".claude");
        fs::create_dir_all(&project_settings).unwrap();
        fs::write(
            project_settings.join("settings.json"),
            serde_json::to_vec_pretty(&json!({
                "env": {
                    "UM_PROJECT_SETTING": "project"
                },
                "hooks": {
                    "UserPromptSubmit": [{
                        "hooks": [{
                            "type": "command",
                            "command": format!("{} {}", self.shell.display(), hook_script.display())
                        }]
                    }]
                }
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            project_settings.join("settings.local.json"),
            serde_json::to_vec_pretty(&json!({
                "env": {
                    "UM_LOCAL_SETTING": "local"
                }
            }))
            .unwrap(),
        )
        .unwrap();

        // Exercise the 2026-07-28 discovery path and a tools/list response without
        // optional resultType, ttlMs, or cacheScope members.
        let mcp_observation = self.private.join("native-mcp-observation.txt");
        let mcp_server = self.private.join("native-mcp-server.sh");
        fs::write(
            &mcp_server,
            format!(
                r#"set -eu
printf 'started\n' > '{}'
while IFS= read -r line; do
  printf '%s\n' "$line" >> '{}'
  if [[ "$line" =~ \"id\":\"([^\"]+)\" ]]; then
    id="\"${{BASH_REMATCH[1]}}\""
  elif [[ "$line" =~ \"id\":([0-9]+) ]]; then
    id="${{BASH_REMATCH[1]}}"
  else
    continue
  fi
  case "$line" in
    *server/discover*)
      printf '{{"jsonrpc":"2.0","id":%s,"result":{{"resultType":"complete","supportedVersions":["2026-07-28"],"capabilities":{{"tools":{{}}}},"_meta":{{"io.modelcontextprotocol/serverInfo":{{"name":"scherzo-native-resource","version":"1.0.0"}}}}}}}}\n' "$id"
      ;;
    *initialize*)
      printf '{{"jsonrpc":"2.0","id":%s,"result":{{"protocolVersion":"2026-07-28","capabilities":{{"tools":{{}}}},"serverInfo":{{"name":"scherzo-native-resource","version":"1.0.0"}}}}}}\n' "$id"
      ;;
    *tools/list*)
      printf '{{"jsonrpc":"2.0","id":%s,"result":{{"tools":[{{"name":"scherzo_native_resource","description":"{}","inputSchema":{{"type":"object","properties":{{}}}}}}]}}}}\n' "$id"
      ;;
    *ping*)
      printf '{{"jsonrpc":"2.0","id":%s,"result":{{}}}}\n' "$id"
      ;;
    *tools/call*)
      printf '{{"jsonrpc":"2.0","id":"elicitation-probe","method":"elicitation/create","params":{{"message":"controlled question","requestedSchema":{{"type":"object","properties":{{"answer":{{"type":"string"}}}}}}}}}}\n'
      ;;
    *)
      printf '{{"jsonrpc":"2.0","id":%s,"error":{{"code":-32601,"message":"Method not found"}}}}\n' "$id"
      ;;
  esac
done
"#,
                mcp_observation.display(),
                mcp_observation.display(),
                mcp_marker,
            ),
        )
        .unwrap();
        fs::write(
            self.project.join(".mcp.json"),
            serde_json::to_vec_pretty(&json!({
                "mcpServers": {
                    "scherzo-resource": {
                        "command": self.shell,
                        "args": [mcp_server]
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let marketplace = self.private.join("marketplace");
        let plugin = marketplace.join("plugins/scherzo-plugin");
        fs::create_dir_all(marketplace.join(".claude-plugin")).unwrap();
        fs::create_dir_all(plugin.join(".claude-plugin")).unwrap();
        let plugin_skill = plugin.join("skills/plugin-resource");
        fs::create_dir_all(&plugin_skill).unwrap();
        fs::write(
            marketplace.join(".claude-plugin/marketplace.json"),
            serde_json::to_vec_pretty(&json!({
                "name": "scherzo-marketplace",
                "owner": { "name": "Scherzo" },
                "plugins": [{
                    "name": "scherzo-plugin",
                    "source": "./plugins/scherzo-plugin",
                    "description": "Scherzo native qualification plugin",
                    "version": "1.0.0"
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            plugin.join(".claude-plugin/plugin.json"),
            serde_json::to_vec_pretty(&json!({
                "name": "scherzo-plugin",
                "description": "Scherzo native qualification plugin",
                "version": "1.0.0"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            plugin_skill.join("SKILL.md"),
            format!(
                "---\nname: plugin-resource\ndescription: {plugin_marker}\n---\n\n# Qualification plugin skill\n"
            ),
        )
        .unwrap();

        NativeResourceFixture {
            hook_observation,
            expected_hook_observation: format!(
                "user\nproject\nlocal\n{}\n1\n1\n1\n1\n1\n",
                self.config.display()
            ),
            mcp_observation,
            marketplace,
        }
    }

    pub(super) fn retained_transcript(&self) -> PathBuf {
        self.private.join("diagnostics/session/transcript.jsonl")
    }

    pub(super) fn retained_resources(&self) -> PathBuf {
        self.private.join("diagnostics/session/resources")
    }

    pub(super) fn ambient_session_paths(&self, session_id: &str) -> [PathBuf; 2] {
        let project = self
            .config
            .join("projects")
            .join(super::adapter::native_project_slug(
                self.project.to_str().unwrap(),
            ));
        [
            project.join(format!("{session_id}.jsonl")),
            project.join(session_id),
        ]
    }

    pub(super) fn environment_snapshot(&self, provider: &LoopbackProvider) -> EnvironmentSnapshot {
        EnvironmentSnapshot::new(
            [
                (OsString::from("HOME"), self.home.as_os_str().to_owned()),
                (
                    OsString::from("CLAUDE_CONFIG_DIR"),
                    self.config.as_os_str().to_owned(),
                ),
                (
                    OsString::from("CLAUDE_CODE_PROJECT_DIR_NAME"),
                    OsString::from("ambient-project-name"),
                ),
                (OsString::from("PATH"), OsString::from("/usr/bin:/bin")),
                (OsString::from("SHELL"), self.shell.as_os_str().to_owned()),
                (OsString::from("CI"), OsString::from("1")),
                (OsString::from("NO_COLOR"), OsString::from("1")),
                (
                    OsString::from("ANTHROPIC_API_KEY"),
                    OsString::from(PLACEHOLDER_API_KEY),
                ),
                (
                    OsString::from("ANTHROPIC_BASE_URL"),
                    OsString::from(provider.base_url()),
                ),
            ]
            .into_iter()
            .chain(self.environment_overrides.iter().cloned()),
        )
    }

    pub(super) fn configure_command(&self, command: &mut Command, provider: &LoopbackProvider) {
        command
            .env_clear()
            .current_dir(&self.project)
            .env("HOME", &self.home)
            .env("CLAUDE_CONFIG_DIR", &self.config)
            .env("PATH", "/usr/bin:/bin")
            .env("SHELL", &self.shell)
            .env("CI", "1")
            .env("NO_COLOR", "1")
            .env("ANTHROPIC_API_KEY", PLACEHOLDER_API_KEY)
            .env("ANTHROPIC_BASE_URL", provider.base_url());
        for (name, value) in FIXED_INVOCATION_ENVIRONMENT {
            command.env(name, value);
        }
        command.envs(self.environment_overrides.iter().cloned());
    }
}

pub(super) struct ControlledProviderRequest {
    path: String,
    body: Value,
    used_placeholder_key: bool,
    response: Option<oneshot::Sender<LoopbackProviderResponse>>,
}

impl ControlledProviderRequest {
    pub(super) fn path(&self) -> &str {
        &self.path
    }

    pub(super) fn body(&self) -> &Value {
        &self.body
    }

    pub(super) const fn used_placeholder_key(&self) -> bool {
        self.used_placeholder_key
    }

    /// Whether the conversation this request continues opened with `fragment`. A
    /// subagent's model calls reach the same provider as the main thread's, and only its
    /// own prompt opens its conversation, so this tells the threads apart.
    pub(super) fn opens_with(&self, fragment: &str) -> bool {
        self.body["messages"]
            .get(0)
            .is_some_and(|first| first.to_string().contains(fragment))
    }

    pub(super) fn release_text(self, text: &str) {
        self.release_blocks(vec![LoopbackBlock::text(text)]);
    }

    pub(super) fn release_structured_output(self, envelope: Value) {
        self.release_blocks(vec![LoopbackBlock::tool_use(
            STRUCTURED_OUTPUT_TOOL_NAME,
            envelope,
        )]);
    }

    pub(super) fn release_tool_use(self, name: &str, input: Value) {
        self.release_blocks(vec![LoopbackBlock::tool_use(name, input)]);
    }

    /// Releases one native assistant message built from the supplied ordered content
    /// blocks. Callers that need a specific block sequence, rather than one convenience
    /// shape, use this to fix exactly what Claude Code must correlate.
    pub(super) fn release_blocks(mut self, blocks: Vec<LoopbackBlock>) {
        self.response
            .take()
            .unwrap()
            .send(LoopbackProviderResponse::Blocks(blocks))
            .unwrap();
    }

    pub(super) fn release_overload(mut self) {
        self.response
            .take()
            .unwrap()
            .send(LoopbackProviderResponse::Overload)
            .unwrap();
    }

    pub(super) fn release_overload_after_thinking(mut self) {
        self.response
            .take()
            .unwrap()
            .send(LoopbackProviderResponse::OverloadAfterThinking)
            .unwrap();
    }

    pub(super) fn release_invalid_request(mut self) {
        self.response
            .take()
            .unwrap()
            .send(LoopbackProviderResponse::InvalidRequest)
            .unwrap();
    }
}

/// One content block of a native assistant message, in the shape the Anthropic
/// streaming API delivers it. `Thinking` accumulates through deltas, while
/// `RedactedThinking` arrives complete in its `content_block_start`.
#[derive(Debug)]
pub(super) enum LoopbackBlock {
    Text(String),
    Thinking(Vec<String>),
    RedactedThinking(String),
    ToolUse { name: String, input: Value },
}

impl LoopbackBlock {
    pub(super) fn text(text: &str) -> Self {
        Self::Text(text.to_owned())
    }

    pub(super) fn thinking(segments: &[&str]) -> Self {
        Self::Thinking(
            segments
                .iter()
                .map(|segment| (*segment).to_owned())
                .collect(),
        )
    }

    pub(super) fn redacted_thinking(data: &str) -> Self {
        Self::RedactedThinking(data.to_owned())
    }

    pub(super) fn tool_use(name: &str, input: Value) -> Self {
        Self::ToolUse {
            name: name.to_owned(),
            input,
        }
    }
}

#[derive(Debug)]
enum LoopbackProviderResponse {
    Blocks(Vec<LoopbackBlock>),
    InvalidRequest,
    Overload,
    OverloadAfterThinking,
}

pub(super) struct LoopbackProvider {
    address: std::net::SocketAddr,
    requests: mpsc::UnboundedReceiver<ControlledProviderRequest>,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl LoopbackProvider {
    pub(super) async fn start() -> Self {
        // The Claude fixture is bounded HTTP/SSE while Pi uses a Unix-socket extension;
        // keep their small controller loops separate so neither implies shared protocol.
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_sender, requests) = mpsc::unbounded_channel();
        let (shutdown_sender, mut shutdown) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else {
                            break;
                        };
                        let request_sender = request_sender.clone();
                        connections.spawn(async move {
                            serve_connection(stream, request_sender).await;
                        });
                    }
                }
            }
            connections.shutdown().await;
        });
        Self {
            address,
            requests,
            shutdown: Some(shutdown_sender),
            task,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }

    pub(super) async fn next_request(&mut self) -> ControlledProviderRequest {
        self.requests.recv().await.unwrap()
    }

    pub(super) fn has_pending_request(&mut self) -> bool {
        self.requests.try_recv().is_ok()
    }

    pub(super) async fn shutdown(mut self) {
        let _ = self.shutdown.take().unwrap().send(());
        self.task.await.unwrap();
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    requests: mpsc::UnboundedSender<ControlledProviderRequest>,
) {
    let Some(request) = read_request(&mut stream).await else {
        return;
    };
    // This controller exchange releases one Anthropic-compatible HTTP response; Pi's
    // similarly shaped channel releases native extension events with different authority.
    let (respond, response) = oneshot::channel();
    if requests
        .send(ControlledProviderRequest {
            path: request.path,
            body: request.body,
            used_placeholder_key: request.used_placeholder_key,
            response: Some(respond),
        })
        .is_err()
    {
        return;
    }
    let Ok(response) = response.await else {
        return;
    };
    let (status, content_type, payload) = match response {
        LoopbackProviderResponse::InvalidRequest => (
            "400 Bad Request",
            "application/json",
            serde_json::to_vec(&json!({
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "message": "synthetic provider rejection",
                },
                "request_id": "req_scherzo_loopback",
            }))
            .unwrap(),
        ),
        LoopbackProviderResponse::Overload => (
            "529 Overloaded",
            "application/json",
            serde_json::to_vec(&json!({
                "type": "error",
                "error": { "type": "overloaded_error", "message": "synthetic overload" },
            }))
            .unwrap(),
        ),
        LoopbackProviderResponse::OverloadAfterThinking => (
            "200 OK",
            "text/event-stream",
            overload_after_thinking_payload(),
        ),
        response => ("200 OK", "text/event-stream", response_payload(response)),
    };
    let header = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncache-control: no-cache\r\nconnection: close\r\ncontent-length: {}\r\n\r\n",
        payload.len()
    );
    if stream.write_all(header.as_bytes()).await.is_err() {
        return;
    }
    let _ = stream.write_all(&payload).await;
    let _ = stream.shutdown().await;
}

struct ProviderRequest {
    path: String,
    body: Value,
    used_placeholder_key: bool,
}

async fn read_request(stream: &mut TcpStream) -> Option<ProviderRequest> {
    let mut bytes = Vec::new();
    let header_end = loop {
        if bytes.len() >= MAXIMUM_HTTP_HEADER_BYTES {
            return None;
        }
        let mut chunk = [0_u8; 4096];
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(index) = find_subslice(&bytes, b"\r\n\r\n") {
            break index + 4;
        }
    };

    let header = std::str::from_utf8(&bytes[..header_end]).ok()?;
    let mut lines = header.split("\r\n");
    let mut request_line = lines.next()?.split_ascii_whitespace();
    if request_line.next()? != "POST" {
        return None;
    }
    let path = request_line.next()?.to_owned();
    if request_line.next()? != "HTTP/1.1" || request_line.next().is_some() {
        return None;
    }

    let mut content_length = None;
    let mut used_placeholder_key = false;
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':')?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.parse::<usize>().ok();
        } else if name.eq_ignore_ascii_case("x-api-key") {
            used_placeholder_key = value == PLACEHOLDER_API_KEY;
        }
    }
    let content_length = content_length?;
    if content_length > MAXIMUM_PROVIDER_REQUEST_BYTES {
        return None;
    }
    let total = header_end.checked_add(content_length)?;
    while bytes.len() < total {
        let remaining = total - bytes.len();
        let mut chunk = [0_u8; 4096];
        let chunk_length = remaining.min(chunk.len());
        let read = stream.read(&mut chunk[..chunk_length]).await.ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    if bytes.len() != total {
        return None;
    }
    let body = serde_json::from_slice(&bytes[header_end..]).ok()?;
    Some(ProviderRequest {
        path,
        body,
        used_placeholder_key,
    })
}

fn find_subslice(bytes: &[u8], needle: &[u8]) -> Option<usize> {
    bytes
        .windows(needle.len())
        .position(|window| window == needle)
}

fn response_payload(response: LoopbackProviderResponse) -> Vec<u8> {
    let events = match response {
        LoopbackProviderResponse::Blocks(blocks) => block_response_events(&blocks),
        LoopbackProviderResponse::InvalidRequest
        | LoopbackProviderResponse::Overload
        | LoopbackProviderResponse::OverloadAfterThinking => {
            panic!("provider errors use a separate response")
        }
    };

    let mut payload = Vec::new();
    for (name, event) in events {
        payload.extend_from_slice(format!("event: {name}\ndata: ").as_bytes());
        serde_json::to_writer(&mut payload, &event).unwrap();
        payload.extend_from_slice(b"\n\n");
    }
    payload
}

fn overload_after_thinking_payload() -> Vec<u8> {
    let events = block_response_events(&[LoopbackBlock::thinking(&["intermediate reasoning"])]);
    let mut payload = Vec::new();
    // End the stream with a provider error after thinking, before message_stop.
    // No timing or connection race is needed to make the retry decision observable.
    for (name, event) in events
        .into_iter()
        .take_while(|(name, _)| *name != "message_delta")
    {
        payload.extend_from_slice(format!("event: {name}\ndata: ").as_bytes());
        serde_json::to_writer(&mut payload, &event).unwrap();
        payload.extend_from_slice(b"\n\n");
    }
    payload.extend_from_slice(b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"synthetic overload\"}}\n\n");
    payload
}

fn block_response_events(blocks: &[LoopbackBlock]) -> Vec<(&'static str, Value)> {
    let mut events = vec![message_start_event("msg_scherzo_loopback")];
    for (index, block) in blocks.iter().enumerate() {
        events.extend(content_block_events(index, block));
    }
    let stop_reason = if blocks
        .iter()
        .any(|block| matches!(block, LoopbackBlock::ToolUse { .. }))
    {
        "tool_use"
    } else {
        "end_turn"
    };
    events.extend(terminal_message_events(stop_reason));
    events
}

fn content_block_events(index: usize, block: &LoopbackBlock) -> Vec<(&'static str, Value)> {
    let (start, deltas) = match block {
        LoopbackBlock::Text(text) => (
            json!({"type": "text", "text": ""}),
            vec![json!({"type": "text_delta", "text": text})],
        ),
        LoopbackBlock::Thinking(segments) => {
            let mut deltas = segments
                .iter()
                .map(|segment| json!({"type": "thinking_delta", "thinking": segment}))
                .collect::<Vec<_>>();
            deltas.push(json!({"type": "signature_delta", "signature": THINKING_SIGNATURE}));
            (
                json!({"type": "thinking", "thinking": "", "signature": ""}),
                deltas,
            )
        }
        // The native API delivers a redacted thinking block complete, with no deltas.
        LoopbackBlock::RedactedThinking(data) => (
            json!({"type": "redacted_thinking", "data": data}),
            Vec::new(),
        ),
        LoopbackBlock::ToolUse { name, input } => (
            json!({
                "type": "tool_use",
                "id": format!("tool_scherzo_loopback_{index}"),
                "name": name,
                "input": {},
            }),
            vec![json!({
                "type": "input_json_delta",
                "partial_json": serde_json::to_string(input).unwrap(),
            })],
        ),
    };

    let mut events = vec![(
        "content_block_start",
        json!({"type": "content_block_start", "index": index, "content_block": start}),
    )];
    events.extend(deltas.into_iter().map(|delta| {
        (
            "content_block_delta",
            json!({"type": "content_block_delta", "index": index, "delta": delta}),
        )
    }));
    events.push((
        "content_block_stop",
        json!({"type": "content_block_stop", "index": index}),
    ));
    events
}

fn message_start_event(message_id: &str) -> (&'static str, Value) {
    (
        "message_start",
        json!({
            "type": "message_start",
            "message": {
                "id": message_id,
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": "scherzo-loopback",
                "stop_reason": null,
                "stop_sequence": null,
                "usage": {
                    "input_tokens": 1,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 0,
                    "output_tokens": 0,
                },
            },
        }),
    )
}

fn terminal_message_events(stop_reason: &str) -> [(&'static str, Value); 2] {
    [
        (
            "message_delta",
            json!({
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": null},
                "usage": {"output_tokens": 3},
            }),
        ),
        ("message_stop", json!({"type": "message_stop"})),
    ]
}

pub(super) fn version_probe_environment(root: &Path) -> [(OsString, OsString); 2] {
    [
        (OsString::from("HOME"), root.join("home").into_os_string()),
        (
            OsString::from("CLAUDE_CONFIG_DIR"),
            root.join("claude-config").into_os_string(),
        ),
    ]
}

pub(super) const MODEL: &str = "scherzo-loopback";

pub(super) const WATCHDOG: Duration = Duration::from_secs(20);

static EXACT_BINARY_CONFORMANCE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(super) fn conformance_executable() -> PathBuf {
    std::env::var_os("UM_CLAUDE_CODE_CONFORMANCE_EXECUTABLE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            panic!(
                "UM_CLAUDE_CODE_CONFORMANCE_EXECUTABLE must name the pinned Claude Code executable"
            )
        })
}

pub(super) async fn exclusive_conformance_executable()
-> (PathBuf, tokio::sync::MutexGuard<'static, ()>) {
    let executable = conformance_executable();
    // Native startup is resource-intensive enough that concurrent qualification cases can
    // exhaust their anti-hang watchdogs before reaching controlled provider I/O, so this
    // mutex admits one exact-binary case at a time. Load from outside the suite is left to
    // `host_scaled_watchdog`, which scales each case's `WATCHDOG` base by the host's load.
    let exclusive = EXACT_BINARY_CONFORMANCE.lock().await;
    (executable, exclusive)
}

pub(super) fn conformance_limits() -> AgentInvocationLimits<ClaudeCodeStreamJsonV1ProtocolLimits> {
    AgentInvocationLimits::new(
        NonZeroU64::new(1024 * 1024).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        NonZeroU64::new(4096).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(512).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        ClaudeCodeStreamJsonV1ProtocolLimits::profile(),
    )
}

pub(super) struct RunningProductionClaudeCode {
    pub(super) task: tokio::task::JoinHandle<()>,
    started: AgentStartReceiver,
    pub(super) terminal: tokio::sync::oneshot::Receiver<AgentOutcome>,
    pub(super) cancellation: CancellationSource,
    pub(super) process_control: AgentProcessControl,
}

impl RunningProductionClaudeCode {
    pub(super) fn launch(
        executable: PathBuf,
        root: &SyntheticClaudeCodeRoot,
        provider: &LoopbackProvider,
        value_mode: AgentValueMode,
        observations: RecordingObservationSink,
    ) -> Self {
        // Lifecycle cases deliberately build their own invocation instead of borrowing the
        // attachment case's resources, so native cancellation cannot contaminate another root.
        let admitted_root = AdmittedExecutionRoot::admit(root.project()).unwrap();
        let working_directory = admitted_root.select_working_directory(None).unwrap();
        let cancellation = CancellationSource::new();
        let invocation = AgentInvocation::new(
            invocation_identity("claude-code-lifecycle-conformance", "agent"),
            admitted_adapter(executable, MODEL),
            AgentProcessContext::new(working_directory, root.environment_snapshot(provider)),
            AgentInvocationStaging::new(root.private().to_owned()),
            AgentDiagnosticSession::claude_code_fixture(root.private().join("diagnostics/session")),
            AgentPrompt::new(
                Arc::from(fs::read_to_string(root.system_prompt()).unwrap()),
                Arc::from("Complete the controlled lifecycle exchange."),
            ),
            Arc::from([]),
            value_mode.clone(),
            conformance_limits(),
            cancellation.clone(),
            ProcessGuardRegistry::default(),
            observations,
        );
        let process_control = invocation.process_control().clone();
        let adapter = ClaudeCodeStreamJsonV1Adapter::new_default(
            StepDiagnosticLog::default(),
            NonZeroU64::new(1024).unwrap(),
            PendingClock,
            NoopExecutionObserver,
        )
        .unwrap();
        let (started_callback, started) = agent_start_channel();
        let (terminal_callback, terminal) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = terminal_callback.send(adapter.invoke(invocation, started_callback).await);
        });
        Self {
            task,
            started,
            terminal,
            cancellation,
            process_control,
        }
    }

    pub(super) async fn await_started(self) -> RunningStartedClaudeCode {
        self.started.receive().await.unwrap();
        RunningStartedClaudeCode {
            task: self.task,
            terminal: self.terminal,
            cancellation: self.cancellation,
            process_control: self.process_control,
        }
    }
}

pub(super) struct RunningStartedClaudeCode {
    pub(super) task: tokio::task::JoinHandle<()>,
    pub(super) terminal: tokio::sync::oneshot::Receiver<AgentOutcome>,
    pub(super) cancellation: CancellationSource,
    pub(super) process_control: AgentProcessControl,
}

impl RunningStartedClaudeCode {
    pub(super) async fn finish(self) -> AgentOutcome {
        let outcome = self.terminal.await.unwrap();
        self.task.await.unwrap();
        outcome
    }
}

pub(super) const BACKGROUND_SUBAGENT_PROMPT: &str =
    "Run the controlled background subagent command.";
pub(super) const BACKGROUND_SUBAGENT_DESCRIPTION: &str = "Run a backgrounded subagent";
pub(super) const BACKGROUND_SUBAGENT_RESPONSE: &str = "background subagent finished";
/// The work a backgrounded subagent alone leaves outstanding, which its premature result's
/// feedback names (REQ-AGENT-35).
pub(super) const SUBAGENT_OUTSTANDING: OutstandingWork = OutstandingWork {
    commands: false,
    monitors: false,
    subagents: true,
};

/// A subagent the main thread starts with `run_in_background`, whose own Bash call blocks
/// on a fifo until the case releases it, so the main thread's turn ends while it runs.
pub(super) struct BackgroundSubagent {
    ready: FixtureSignal,
    blocker: PathBuf,
}

impl BackgroundSubagent {
    pub(super) fn new(root: &SyntheticClaudeCodeRoot) -> Self {
        Self::named(root, "subagent")
    }

    /// A subagent with its own signal and blocker, so that one case may run several.
    pub(super) fn named(root: &SyntheticClaudeCodeRoot, name: &str) -> Self {
        let ready = FixtureSignal::create(root.private().join(format!("{name}-ready")));
        let blocker = root.private().join(format!("{name}-blocker"));
        mkfifo(&blocker, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        Self { ready, blocker }
    }

    pub(super) fn launch_input() -> Value {
        json!({
            "description": BACKGROUND_SUBAGENT_DESCRIPTION,
            "prompt": BACKGROUND_SUBAGENT_PROMPT,
            "subagent_type": "general-purpose",
            "run_in_background": true,
        })
    }

    /// Serves the main thread's `Agent` call and the subagent's blocking command, returning
    /// the main thread's next request once the command is running. The subagent's request
    /// may arrive before or after the main thread's.
    pub(super) async fn start(
        &self,
        provider: &mut LoopbackProvider,
        terminal: &mut tokio::sync::oneshot::Receiver<AgentOutcome>,
    ) -> ControlledProviderRequest {
        let request = provider.next_request().await;
        request.release_tool_use("Agent", Self::launch_input());
        self.await_running(provider, terminal).await
    }

    /// Launches the subagent from `request`, a main-thread turn already under way, with a
    /// sentence before the `Agent` call as a lead launching a builder writes one, and returns
    /// the main thread's next request once the subagent's command is running.
    pub(super) async fn launch_from(
        &self,
        request: ControlledProviderRequest,
        provider: &mut LoopbackProvider,
        terminal: &mut tokio::sync::oneshot::Receiver<AgentOutcome>,
    ) -> ControlledProviderRequest {
        request.release_blocks(vec![
            LoopbackBlock::text("launching the next builder"),
            LoopbackBlock::tool_use("Agent", Self::launch_input()),
        ]);
        self.await_running(provider, terminal).await
    }

    pub(super) async fn await_running(
        &self,
        provider: &mut LoopbackProvider,
        terminal: &mut tokio::sync::oneshot::Receiver<AgentOutcome>,
    ) -> ControlledProviderRequest {
        let mut main_thread = None;
        let mut subagent_started = false;
        while main_thread.is_none() || !subagent_started {
            let request = next_request_or_outcome(provider, terminal).await;
            if request.opens_with(BACKGROUND_SUBAGENT_PROMPT) {
                assert!(
                    !subagent_started,
                    "the subagent asked again before its command ran"
                );
                request.release_tool_use(
                    "Bash",
                    json!({
                        "command": format!(
                            "printf 'ready\\n' > '{}'; IFS= read -r done < '{}'; printf 'subagent command finished\\n'",
                            self.ready.path().display(), self.blocker.display()
                        ),
                        "description": "Block the subagent until released",
                    }),
                );
                subagent_started = true;
            } else {
                assert!(
                    main_thread.is_none(),
                    "the main thread asked twice before the subagent ran"
                );
                main_thread = Some(request);
            }
        }
        assert_eq!(self.ready.receive().await, b"ready\n");
        main_thread.unwrap()
    }

    /// Releases the subagent's blocked command.
    pub(super) async fn release(&self) {
        let blocker = self.blocker.clone();
        tokio::task::spawn_blocking(move || fs::write(blocker, b"done\n").unwrap())
            .await
            .unwrap();
    }

    /// Releases the subagent's command, serves its closing text, and returns the request of
    /// the main-thread turn its notification starts.
    pub(super) async fn finish(
        self,
        provider: &mut LoopbackProvider,
        terminal: &mut tokio::sync::oneshot::Receiver<AgentOutcome>,
    ) -> ControlledProviderRequest {
        self.release().await;
        loop {
            let request = next_request_or_outcome(provider, terminal).await;
            if !request.opens_with(BACKGROUND_SUBAGENT_PROMPT) {
                return request;
            }
            request.release_text(BACKGROUND_SUBAGENT_RESPONSE);
        }
    }
}

pub(super) async fn next_request_or_outcome(
    provider: &mut LoopbackProvider,
    terminal: &mut tokio::sync::oneshot::Receiver<AgentOutcome>,
) -> ControlledProviderRequest {
    tokio::select! {
        request = provider.next_request() => request,
        outcome = terminal => panic!("invocation ended while the case awaited a request: {outcome:?}"),
    }
}

pub(super) fn completed_turns(observations: &RecordingObservationSink) -> usize {
    observations
        .snapshot()
        .iter()
        .filter(|envelope| {
            matches!(
                envelope.observation(),
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::TurnCompleted,
                }
            )
        })
        .count()
}

pub(super) async fn wait_for_completed_turns(
    observations: &RecordingObservationSink,
    turns: usize,
) {
    while completed_turns(observations) < turns {
        wait_for_a_newer_observation(observations).await;
    }
}

/// Waits until the backgrounded subagent has reported `status` `count` times.
pub(super) async fn wait_for_subagent_status(
    observations: &RecordingObservationSink,
    status: &str,
    count: usize,
) {
    let reported = || {
        backgrounded_subagent_statuses(observations)
            .iter()
            .filter(|reported| reported.as_str() == status)
            .count()
    };
    while reported() < count {
        wait_for_a_newer_observation(observations).await;
    }
}

/// Waits for an observation past those recorded so far: `wait_for` evaluates the record
/// from its start on each observation, so an evaluation past the length seen here is of a
/// newer observation.
async fn wait_for_a_newer_observation(observations: &RecordingObservationSink) {
    let seen = observations.snapshot().len();
    let evaluated = Cell::new(0);
    observations
        .wait_for(|_| {
            evaluated.set(evaluated.get() + 1);
            evaluated.get() > seen
        })
        .await;
}

/// The subagent's start and settlement are information diagnostics, and its own tool runs
/// are recognized rather than reported as unrecognized `task_started` events.
pub(super) fn assert_backgrounded_subagent_activity(observations: &RecordingObservationSink) {
    let statuses = backgrounded_subagent_statuses(observations);
    for status in ["started", "completed"] {
        assert!(
            statuses.iter().any(|reported| reported == status),
            "no subagent diagnostic names {status}: {statuses:?}"
        );
    }
    let observations = observations.snapshot();
    assert!(!observations.iter().any(|envelope| matches!(
        envelope.observation(),
        AgentObservation::UnrecognizedHarnessEvent { event } if event["subtype"] == "task_started"
    )));
}

/// The agent id Claude Code hands back in the Agent tool's result, which `SendMessage`
/// addresses.
pub(super) fn launched_agent_id(body: &Value) -> String {
    let text = body.to_string();
    let start = text
        .find("agentId: ")
        .expect("the Agent result names the agent id")
        + "agentId: ".len();
    text[start..]
        .chars()
        .take_while(|character| character.is_ascii_alphanumeric())
        .collect()
}

/// Information diagnostics naming the subagent `description`, reduced to their status
/// token, in order.
pub(super) fn subagent_statuses<'a>(
    observations: impl IntoIterator<Item = &'a AgentObservation>,
    description: &str,
) -> Vec<String> {
    observations
        .into_iter()
        .filter_map(|observation| match observation {
            AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Information,
                message,
            } if message.contains(description) => message
                .split(" · ")
                .next()
                .and_then(|head| head.split_whitespace().last())
                .map(str::to_owned),
            _ => None,
        })
        .collect()
}

/// Information diagnostics naming the backgrounded subagent, reduced to their status token.
pub(super) fn backgrounded_subagent_statuses(
    observations: &RecordingObservationSink,
) -> Vec<String> {
    let snapshot = observations.snapshot();
    subagent_statuses(
        snapshot.iter().map(|envelope| envelope.observation()),
        BACKGROUND_SUBAGENT_DESCRIPTION,
    )
}

pub(super) fn assistant_text(observation: &AgentObservation) -> Option<&str> {
    match observation {
        AgentObservation::AssistantText { text } => Some(text.as_ref()),
        _ => None,
    }
}
