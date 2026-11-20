use std::ffi::{OsStr, OsString};
use std::fs;
use std::future::{Future, pending};
use std::io::{self, Write as _};
use std::ops::Add as _;
use std::os::fd::OwnedFd;
use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rustix::fs::{AtFlags, FileType, statat, symlinkat, unlinkat};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt as _;
use tokio::net::UnixStream;
use tokio::sync::mpsc;

use super::outcome::{Ending, StepFacts, decide};
use super::reader::{DecidingResult, ResultText, StreamReader};
use super::result_channel::{
    PreparedResultChannel, ResultChannelEvent, ResultChannelHello, ResultSubmissions,
    SubmissionJudgement,
};
use super::result_tool::{ResultChannelAnswer, ResultChannelFailure, ResultChannelRequest};
use super::{
    FIXED_INVOCATION_ENVIRONMENT, initial_user_text_frame, normal_mode_arguments,
    result_mode_arguments, user_content_frame,
};
use crate::claude_code::compatibility_profile_for_version;
use crate::workflow::admission::{CancellationReason, CancellationSource};
use crate::workflow::agent::{
    AgentCompatibilityProfile, AgentDiagnosticLevel, AgentFailureCause, AgentHarnessSetupStage,
    AgentInvocation, AgentLifecycleMilestone, AgentObservation, AgentOutcome,
    AgentProcessDirective, AgentStartCallback, AgentValueKind, OrderedAgentObservationSink,
    StagedAgentAttachment, failed_agent_outcome, finish_agent_diagnostic_capture,
};
use crate::workflow::agent_process_driver::{self, StdioProcess};
use crate::workflow::agent_result_socket::IncomingResultRequest;
use crate::workflow::coordinator::CoordinatorClock;
use crate::workflow::observation::ExecutionObserver;
use crate::workflow::private_staging::open_directory_path;
// Both native adapters use the same containment and validator primitives, but their
// protocol state machines decide independently when those primitives gain authority.
use crate::workflow::result_validation::{
    AuthoritativeResultValidator, ProcessResultValidationWorker, ResultValidationWorker,
};

const SYSTEM_PROMPT_FILE_PREFIX: &str = "claude-code-system-prompt-";
pub(super) const MAXIMUM_INLINE_ATTACHMENT_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub(super) const NATIVE_TRANSCRIPT_CAPTURE_MISSING_DIAGNOSTIC: &str =
    "Claude Code native transcript capture missing";
/// How long Claude Code may write nothing and submit nothing before the adapter interrupts it
/// (REQ-AGENT-88, INV-HARNESS-24).
pub(super) const QUIET_BOUND: Duration = Duration::from_secs(2 * 60 * 60);
/// How long after the quiet bound's interrupt the adapter forces the group. The run's
/// cancellation grace never reaches the adapter, so this is its own.
pub(super) const QUIET_BOUND_FORCE_DELAY: Duration = Duration::from_secs(30);

#[derive(Clone, Default)]
pub(crate) struct ClaudeProfile;
pub(crate) type ClaudeCodeStreamJsonV1Adapter<
    Clock,
    Observer,
    Worker = ProcessResultValidationWorker,
> = agent_process_driver::AdapterCore<Clock, Observer, Worker, ClaudeProfile>;

agent_process_driver::native_process_adapter!(ClaudeProfile, "claude_code_stream_json_v1");

impl<Clock, Observer, Worker>
    agent_process_driver::AdapterCore<Clock, Observer, Worker, ClaudeProfile>
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Worker: ResultValidationWorker,
{
    async fn invoke_inner(
        &self,
        invocation: AgentInvocation,
        started: &AgentStartCallback,
    ) -> AgentOutcome {
        // Claude's init acknowledgement and stream input make this a distinct startup
        // transition from Pi's session header and extension preparation.
        if let Some(reason) = invocation.cancellation().cancellation_reason() {
            return AgentOutcome::Cancelled { reason };
        }
        let clock = self.clock.clone();
        let (invocation, mut plan) = match self
            .prepare_invocation(
                invocation,
                move |invocation| prepare_launch(invocation, clock),
                AgentFailureCause::start_failure("launch preparation", "unavailable"),
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        // The channel leaves the plan before launch, so that it is shut down whether or not
        // the launch succeeds.
        let channel = plan.result_channel.take();
        // The shared validator is wired into the result channel here, while Pi wires it
        // into an injected socket bridge; combining those lifecycles would hide authority.
        let validator = self.result_validator(&invocation);
        if channel.is_some() && validator.is_none() {
            shutdown_result_channel(channel).await;
            return failed_agent_outcome(AgentFailureCause::start_failure(
                "result tool",
                "result validator unavailable",
            ));
        }

        let (invocation, plan, process, standard_error, process_directives) = match self
            .launch_stdio(
                invocation,
                plan,
                AgentFailureCause::start_failure("launch preparation", "unavailable"),
            )
            .await
        {
            Ok(launched) => launched,
            Err(outcome) => {
                shutdown_result_channel(channel).await;
                return outcome;
            }
        };
        // Diagnostic drain is tied to Claude's stream driver lifetime rather than Pi's
        // result bridge and native-session settlement.
        let diagnostic = self.start_diagnostic(&invocation, standard_error);
        let result_tool = channel
            .zip(validator.as_ref())
            .map(|(channel, validator)| ResultTool {
                hello: channel.hello(),
                channel,
                submissions: ResultSubmissions::new(
                    invocation
                        .limits()
                        .maximum_result_rejection_feedback_bytes(),
                ),
                validator,
                listening: true,
            });
        let outcome = drive_process(
            &invocation,
            started,
            process,
            process_directives,
            ClaudeDriverInput {
                bytes: &plan.input,
                result_tool,
                clock: self.clock.clone(),
            },
        )
        .await;
        if !plan.native_session_bridge.transcript_capture_verified() {
            let _ = invocation
                .observations()
                .emit(AgentObservation::Diagnostic {
                    level: AgentDiagnosticLevel::Warning,
                    message: Arc::from(format!(
                        "{NATIVE_TRANSCRIPT_CAPTURE_MISSING_DIAGNOSTIC}: no transcript was written through the temporary history link"
                    )),
                })
                .await;
        }
        finish_agent_diagnostic_capture(invocation.diagnostic_session(), diagnostic, &outcome)
            .await;
        outcome
    }
}

async fn shutdown_result_channel(channel: Option<PreparedResultChannel>) {
    if let Some(channel) = channel {
        let _ = channel.shutdown().await;
    }
}

pub(super) struct ClaudeCodeStreamJsonV1LaunchPlan {
    arguments: Vec<OsString>,
    input: Vec<u8>,
    native_session_bridge: ClaudeCodeNativeSessionBridge,
    /// The result tool's channel, in Result mode; taken before launch.
    result_channel: Option<PreparedResultChannel>,
    _system_prompt_file: tempfile::NamedTempFile,
}

impl ClaudeCodeStreamJsonV1LaunchPlan {
    #[cfg(test)]
    pub(super) fn arguments(&self) -> &[OsString] {
        &self.arguments
    }

    #[cfg(test)]
    pub(super) fn input(&self) -> &[u8] {
        &self.input
    }

    #[cfg(test)]
    pub(super) fn system_prompt_file(&self) -> &std::path::Path {
        self._system_prompt_file.path()
    }
}

/// Prepares one launch. In Result mode it also prepares the result tool's channel, last, so
/// that nothing after it can fail and leave its socket behind.
pub(super) fn prepare_launch<Clock: CoordinatorClock>(
    invocation: &AgentInvocation,
    clock: Clock,
) -> Result<ClaudeCodeStreamJsonV1LaunchPlan, AgentFailureCause> {
    agent_process_driver::check_prompt_bounds(invocation)?;
    agent_process_driver::require_native_profile(
        invocation,
        AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
        compatibility_profile_for_version(invocation.adapter().version()).is_some(),
        || AgentFailureCause::start_failure("launch preparation", "unavailable"),
    )?;
    agent_process_driver::verify_session_binding(
        invocation
            .diagnostic_session()
            .verify_claude_code_native_session_path_binding(),
        "claude diagnostic session binding",
    )?;

    // Claude stages a native prompt file and a native session bridge under the protocol
    // directory; Pi's corresponding preparation uses a persisted session and injected
    // input extension.
    let expected_cwd = invocation
        .process()
        .protocol_cwd()
        .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"))?;
    let session_id = invocation
        .diagnostic_session()
        .claude_code_native_session_id()
        .ok_or(AgentFailureCause::start_failure(
            "launch preparation",
            "unavailable",
        ))?
        .to_owned();
    let native_session_bridge =
        ClaudeCodeNativeSessionBridge::prepare(invocation, &expected_cwd, &session_id)?;
    let mut system_prompt_file = tempfile::Builder::new()
        .prefix(SYSTEM_PROMPT_FILE_PREFIX)
        .tempfile_in(invocation.staging().result_endpoint_directory())
        .map_err(|error| AgentFailureCause::start_failure("claude prompt temp file", error))?;
    system_prompt_file
        .write_all(invocation.prompt().system_prompt().as_bytes())
        .and_then(|()| system_prompt_file.flush())
        .and_then(|()| {
            system_prompt_file
                .as_file()
                .set_permissions(std::fs::Permissions::from_mode(0o400))
        })
        .map_err(|error| AgentFailureCause::start_failure("claude prompt write", error))?;

    let (configuration, _) = invocation
        .adapter()
        .native_configuration()
        .claude_code()
        .ok_or_else(|| AgentFailureCause::start_failure("harness profile", "mismatch"))?;
    let input = initial_user_frame(invocation)?;
    let result_channel = if invocation.value_mode().kind() == AgentValueKind::Result {
        Some(
            PreparedResultChannel::prepare(
                invocation.staging().result_endpoint_directory(),
                invocation.limits().result_validation_deadline(),
                clock,
            )
            .map_err(|error| AgentFailureCause::start_failure("result tool", error))?,
        )
    } else {
        None
    };
    let arguments = match &result_channel {
        Some(channel) => result_mode_arguments(
            &configuration.model,
            configuration.effort.as_str(),
            &session_id,
            system_prompt_file.path(),
            channel.configuration_path(),
        ),
        None => normal_mode_arguments(
            &configuration.model,
            configuration.effort.as_str(),
            &session_id,
            system_prompt_file.path(),
        ),
    };
    Ok(ClaudeCodeStreamJsonV1LaunchPlan {
        arguments,
        input,
        native_session_bridge,
        result_channel,
        _system_prompt_file: system_prompt_file,
    })
}

struct ClaudeCodeNativeSessionBridge {
    ambient_project_directory: OwnedFd,
    transcript_target: PathBuf,
    transcript_link_name: OsString,
    links: Vec<OwnedAmbientSessionLink>,
    // Dropped after `Drop::drop` has removed the links, so it finds the
    // directories it removes empty of them.
    _created_directories: CreatedAmbientDirectories,
}

/// The directories of the ambient project path the bridge created, outermost
/// first. On drop it removes each one that is empty, innermost first, so the
/// invocation leaves Claude Code's configuration directory as it found it
/// unless the program itself wrote there (REQ-GRANT-37); a directory that was
/// already present, or that holds anything, is kept.
struct CreatedAmbientDirectories(Vec<PathBuf>);

impl CreatedAmbientDirectories {
    /// Creates `project` and any missing ancestor `0700`, recording the ones
    /// absent beforehand. A directory another invocation creates or removes
    /// meanwhile is safe either way: a removal only takes an empty directory,
    /// and the recursive creation recreates a parent removed under it.
    fn create(project: &Path) -> io::Result<Self> {
        let mut absent = Vec::new();
        let mut directory = Some(project);
        while let Some(path) = directory {
            match fs::symlink_metadata(path) {
                Ok(_) => break,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    absent.push(path.to_path_buf());
                    directory = path.parent();
                }
                Err(error) => return Err(error),
            }
        }
        absent.reverse();
        let created = Self(absent);
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder.create(project)?;
        Ok(created)
    }
}

impl Drop for CreatedAmbientDirectories {
    fn drop(&mut self) {
        for directory in self.0.iter().rev() {
            if fs::remove_dir(directory).is_err() {
                break;
            }
        }
    }
}

struct OwnedAmbientSessionLink {
    name: OsString,
    device: libc::dev_t,
    inode: libc::ino_t,
}

impl ClaudeCodeNativeSessionBridge {
    fn prepare(
        invocation: &AgentInvocation,
        expected_cwd: &Path,
        session_id: &str,
    ) -> Result<Self, AgentFailureCause> {
        invocation
            .diagnostic_session()
            .verify_claude_code_native_session_path_binding()
            .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"))?;
        let transcript = invocation
            .diagnostic_session()
            .claude_code_native_transcript_path()
            .ok_or(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ))?;
        let resources = invocation
            .diagnostic_session()
            .claude_code_native_resources_directory()
            .ok_or(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ))?;
        if !transcript.is_absolute() || !resources.is_absolute() {
            return Err(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ));
        }

        if !matches!(
            fs::symlink_metadata(&transcript),
            Err(error) if error.kind() == io::ErrorKind::NotFound
        ) {
            return Err(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ));
        }

        let config = claude_code_config_directory(invocation, expected_cwd)?;
        let project =
            config
                .join("projects")
                .join(native_project_slug(expected_cwd.to_str().ok_or(
                    AgentFailureCause::start_failure("launch preparation", "unavailable"),
                )?));
        let created_directories = CreatedAmbientDirectories::create(&project)
            .map_err(|error| AgentFailureCause::start_failure("claude project directory", error))?;
        let project = fs::canonicalize(project).map_err(|error| {
            AgentFailureCause::start_failure("claude project canonicalization", error)
        })?;
        let ambient_project_directory = open_directory_path(&project)
            .map_err(|error| AgentFailureCause::start_failure("claude project open", error))?;
        let transcript_link_name = OsString::from(format!("{session_id}.jsonl"));
        let mut bridge = Self {
            ambient_project_directory,
            transcript_target: transcript.clone(),
            transcript_link_name: transcript_link_name.clone(),
            links: Vec::with_capacity(2),
            _created_directories: created_directories,
        };
        bridge.create_link(&transcript, transcript_link_name)?;
        bridge.create_link(&resources, OsString::from(session_id))?;
        Ok(bridge)
    }

    fn transcript_capture_verified(&self) -> bool {
        let Some(link) = self
            .links
            .iter()
            .find(|link| link.name == self.transcript_link_name)
        else {
            return false;
        };
        self.owns_link(link)
            && fs::metadata(&self.transcript_target)
                .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
    }

    fn owns_link(&self, link: &OwnedAmbientSessionLink) -> bool {
        statat(
            &self.ambient_project_directory,
            &link.name,
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .is_ok_and(|metadata| {
            FileType::from_raw_mode(metadata.st_mode) == FileType::Symlink
                && metadata.st_dev == link.device
                && metadata.st_ino == link.inode
        })
    }

    fn create_link(&mut self, target: &Path, name: OsString) -> Result<(), AgentFailureCause> {
        symlinkat(target, &self.ambient_project_directory, &name)
            .map_err(|error| AgentFailureCause::start_failure("claude session symlink", error))?;
        let metadata = statat(
            &self.ambient_project_directory,
            &name,
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|error| AgentFailureCause::start_failure("claude session link stat", error))?;
        if FileType::from_raw_mode(metadata.st_mode) != FileType::Symlink {
            return Err(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ));
        }
        self.links.push(OwnedAmbientSessionLink {
            name,
            device: metadata.st_dev,
            inode: metadata.st_ino,
        });
        Ok(())
    }
}

impl Drop for ClaudeCodeNativeSessionBridge {
    fn drop(&mut self) {
        for link in self.links.iter().rev() {
            if self.owns_link(link) {
                let _ = unlinkat(
                    &self.ambient_project_directory,
                    &link.name,
                    AtFlags::empty(),
                );
            }
        }
    }
}

fn claude_code_config_directory(
    invocation: &AgentInvocation,
    expected_cwd: &Path,
) -> Result<PathBuf, AgentFailureCause> {
    let environment = invocation.process().environment().variables();
    let configured = environment
        .get(std::ffi::OsStr::new("CLAUDE_CONFIG_DIR"))
        .map(PathBuf::from);
    let directory = if let Some(configured) = configured {
        configured
    } else {
        let home = environment
            .get(std::ffi::OsStr::new("HOME"))
            .map(PathBuf::from)
            .ok_or(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ))?;
        home.join(".claude")
    };
    if directory.is_absolute() {
        Ok(directory)
    } else {
        Ok(expected_cwd.join(directory))
    }
}

pub(super) fn native_project_slug(cwd: &str) -> String {
    const MAXIMUM_SLUG_CODE_UNITS: usize = 200;
    let code_units = cwd.encode_utf16().collect::<Vec<_>>();
    let mut slug = String::with_capacity(code_units.len());
    for code_unit in &code_units {
        if let Ok(ascii) = u8::try_from(*code_unit)
            && ascii.is_ascii_alphanumeric()
        {
            slug.push(char::from(ascii));
        } else {
            slug.push('-');
        }
    }
    if code_units.len() <= MAXIMUM_SLUG_CODE_UNITS {
        return slug;
    }

    let mut hash = 0_i32;
    for code_unit in code_units {
        hash = hash.wrapping_mul(31).wrapping_add(i32::from(code_unit));
    }
    format!(
        "{}-{}",
        &slug[..MAXIMUM_SLUG_CODE_UNITS],
        base36(hash.unsigned_abs())
    )
}

fn base36(mut value: u32) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if value == 0 {
        return "0".to_owned();
    }
    let mut reversed = Vec::new();
    while value > 0 {
        let index = usize::try_from(value % 36).unwrap_or_default();
        reversed.push(char::from(DIGITS[index]));
        value /= 36;
    }
    reversed.iter().rev().collect()
}

fn initial_user_frame(invocation: &AgentInvocation) -> Result<Vec<u8>, AgentFailureCause> {
    if invocation.attachments().is_empty() {
        return initial_user_text_frame(invocation.prompt().message())
            .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"));
    }

    let validated = agent_process_driver::validate_staged_attachments(
        invocation.attachments(),
        invocation.staging().result_endpoint_directory(),
        invocation.limits().maximum_attachments().get(),
        invocation.limits().maximum_attachment_bytes().get(),
        |_| AgentFailureCause::start_failure("launch preparation", "unavailable"),
    )?;

    let mut content = Vec::new();
    content
        .try_reserve_exact(validated.len().saturating_add(1))
        .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"))?;
    content.push(json!({
        "type": "text",
        "text": invocation.prompt().message(),
    }));
    for (attachment, identity, _) in &validated {
        content.push(attachment_reference_content_block(attachment, identity)?);
    }

    let empty_frame_bytes = user_content_frame(Vec::new())
        .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"))?
        .len();
    let mut block_bytes = content
        .iter()
        .map(serialized_content_block_bytes)
        .collect::<Result<Vec<_>, _>>()?;
    let mut frame_bytes = block_bytes
        .iter()
        .try_fold(empty_frame_bytes, |total, bytes| total.checked_add(*bytes))
        .and_then(|total| total.checked_add(content.len().saturating_sub(1)))
        .ok_or(AgentFailureCause::start_failure(
            "launch preparation",
            "unavailable",
        ))?;
    if frame_bytes > MAXIMUM_INLINE_ATTACHMENT_FRAME_BYTES {
        return Err(AgentFailureCause::start_failure(
            "launch preparation",
            "unavailable",
        ));
    }

    for (index, (attachment, identity, expected_bytes)) in validated.iter().enumerate() {
        let content_index = index.saturating_add(1);
        let reference_bytes =
            block_bytes
                .get(content_index)
                .copied()
                .ok_or(AgentFailureCause::start_failure(
                    "launch preparation",
                    "unavailable",
                ))?;
        let frame_without_reference =
            frame_bytes
                .checked_sub(reference_bytes)
                .ok_or(AgentFailureCause::start_failure(
                    "launch preparation",
                    "unavailable",
                ))?;
        let replacement_budget = MAXIMUM_INLINE_ATTACHMENT_FRAME_BYTES
            .checked_sub(frame_without_reference)
            .ok_or(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ))?;
        if *expected_bytes > u64::try_from(replacement_budget).unwrap_or(u64::MAX) {
            continue;
        }
        let Some(inline) = attachment_inline_content_block(attachment, identity, *expected_bytes)?
        else {
            continue;
        };
        let inline_bytes = serialized_content_block_bytes(&inline)?;
        let candidate_frame_bytes = frame_without_reference.checked_add(inline_bytes).ok_or(
            AgentFailureCause::start_failure("launch preparation", "unavailable"),
        )?;
        if candidate_frame_bytes <= MAXIMUM_INLINE_ATTACHMENT_FRAME_BYTES {
            let content_slot =
                content
                    .get_mut(content_index)
                    .ok_or(AgentFailureCause::start_failure(
                        "launch preparation",
                        "unavailable",
                    ))?;
            *content_slot = inline;
            let block_bytes_slot =
                block_bytes
                    .get_mut(content_index)
                    .ok_or(AgentFailureCause::start_failure(
                        "launch preparation",
                        "unavailable",
                    ))?;
            *block_bytes_slot = inline_bytes;
            frame_bytes = candidate_frame_bytes;
        }
    }

    let frame = user_content_frame(content)
        .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"))?;
    if frame.len() > MAXIMUM_INLINE_ATTACHMENT_FRAME_BYTES {
        return Err(AgentFailureCause::start_failure(
            "launch preparation",
            "unavailable",
        ));
    }
    Ok(frame)
}

fn serialized_content_block_bytes(block: &Value) -> Result<usize, AgentFailureCause> {
    serde_json::to_vec(block)
        .map(|bytes| bytes.len())
        .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"))
}

fn attachment_inline_content_block(
    attachment: &StagedAgentAttachment,
    identity: &str,
    expected_bytes: u64,
) -> Result<Option<Value>, AgentFailureCause> {
    let media_type = attachment.media_type();
    let (_, text_media) = agent_process_driver::attachment_media_type(media_type);
    let native_media = if media_type.eq_ignore_ascii_case("image/png") {
        Some(("image", "image/png"))
    } else if media_type.eq_ignore_ascii_case("application/pdf") {
        Some(("document", "application/pdf"))
    } else {
        None
    };
    if !text_media && native_media.is_none() {
        return Ok(None);
    }

    let bytes = read_staged_attachment(attachment, expected_bytes)?;
    if text_media && let Ok(text) = std::str::from_utf8(&bytes) {
        return Ok(Some(agent_process_driver::attachment_text_content(
            identity, media_type, text,
        )));
    }
    Ok(native_media.map(|(block_type, native_media_type)| {
        json!({
            "type": block_type,
            "source": {
                "type": "base64",
                "media_type": native_media_type,
                "data": BASE64.encode(bytes),
            },
        })
    }))
}

fn attachment_reference_content_block(
    attachment: &StagedAgentAttachment,
    identity: &str,
) -> Result<Value, AgentFailureCause> {
    agent_process_driver::staged_attachment_reference(attachment, identity, || {
        AgentFailureCause::start_failure("launch preparation", "unavailable")
    })
}

fn read_staged_attachment(
    attachment: &StagedAgentAttachment,
    expected_bytes: u64,
) -> Result<Vec<u8>, AgentFailureCause> {
    agent_process_driver::read_staged_attachment(
        attachment,
        expected_bytes,
        |error| AgentFailureCause::start_failure("attachment capacity", error),
        |error| AgentFailureCause::start_failure("attachment read", error),
        |error| AgentFailureCause::start_failure("attachment read", error),
        |len| {
            AgentFailureCause::start_failure(
                "attachment length",
                format!("expected {expected_bytes} bytes, read {len}"),
            )
        },
    )
}

type LaunchedClaudeCodeProcess = StdioProcess;

impl agent_process_driver::StdioLaunchPlan for ClaudeCodeStreamJsonV1LaunchPlan {
    fn arguments(&self) -> &[OsString] {
        &self.arguments
    }
    fn environment(&self, invocation: &AgentInvocation) -> Vec<(OsString, OsString)> {
        let mut environment = agent_process_driver::invocation_environment(invocation);
        environment.remove(OsStr::new("CLAUDE_CODE_PROJECT_DIR_NAME"));
        for (name, value) in FIXED_INVOCATION_ENVIRONMENT {
            environment.insert(OsString::from(name), OsString::from(value));
        }
        environment.into_iter().collect()
    }
    fn verify_binding(&self, invocation: &AgentInvocation) -> Result<(), AgentFailureCause> {
        agent_process_driver::verify_session_binding(
            invocation
                .diagnostic_session()
                .verify_claude_code_native_session_path_binding(),
            "claude diagnostic session binding",
        )
    }
    fn spawn_stage(&self) -> &'static str {
        "claude process spawn"
    }
    fn release_stage(&self) -> &'static str {
        "claude process release"
    }
    fn guard_failure(&self) -> AgentFailureCause {
        AgentFailureCause::start_failure("launch preparation", "unavailable")
    }
}

type QuietBoundWait = Pin<Box<dyn Future<Output = ()> + Send>>;

/// The adapter's end of the result tool in Result mode (INV-HARNESS-23).
struct ResultTool<'a, Clock, Worker> {
    channel: PreparedResultChannel,
    hello: ResultChannelHello,
    submissions: ResultSubmissions,
    validator: &'a AuthoritativeResultValidator<Clock, Worker>,
    /// Whether the channel's server may still deliver an event.
    listening: bool,
}

enum ClaudeExtra {
    Initial(InitialInputProgress),
    Channel(ResultChannelEvent),
    QuietBound,
}

/// Whether start was acknowledged: what the reader yields before it is held, and dropped if
/// the start check fails.
enum Start {
    Pending(Vec<AgentObservation>),
    Acknowledged,
    Refused,
}

struct ClaudeProtocol<'a, Clock, Worker> {
    invocation: &'a AgentInvocation,
    started: &'a AgentStartCallback,
    reader: StreamReader,
    result_tool: Option<ResultTool<'a, Clock, Worker>>,
    /// Held open from the prompt frame until Claude Code exits or the invocation is
    /// cancelled; nothing more is written to it (INV-HARNESS-24).
    standard_input: Option<UnixStream>,
    initial_input: Pin<Box<dyn Future<Output = InitialInputProgress> + Send + 'a>>,
    initial_input_pending: bool,
    start: Start,
    /// The quiet bound, or after its interrupt the force that follows it.
    quiet_bound: Option<QuietBoundWait>,
    /// The deciding `result` held when the quiet bound interrupted Claude Code. What the
    /// interrupt makes it report afterwards, the `result` of the turn it aborts among it, is
    /// activity and decides nothing (REQ-AGENT-88).
    held_at_bound: Option<Option<DecidingResult>>,
    failure: Option<AgentFailureCause>,
}

impl<Clock, Worker> agent_process_driver::Protocol<Clock> for ClaudeProtocol<'_, Clock, Worker>
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
{
    type Extra = ClaudeExtra;

    async fn on_start(&mut self, state: &mut agent_process_driver::State<Clock>) {
        self.quiet_bound = Some(quiet_bound_wait(&mut state.clock, QUIET_BOUND));
    }

    fn extra_enabled(&self, _state: &agent_process_driver::State<Clock>) -> bool {
        self.initial_input_pending
            || self.quiet_bound.is_some()
            || self.result_tool.as_ref().is_some_and(|tool| tool.listening)
    }

    async fn extra(&mut self) -> ClaudeExtra {
        let listening = self.result_tool.as_ref().is_some_and(|tool| tool.listening);
        tokio::select! {
            biased;
            progress = &mut self.initial_input, if self.initial_input_pending => ClaudeExtra::Initial(progress),
            event = receive_channel_event(&mut self.result_tool), if listening => ClaudeExtra::Channel(event),
            () = wait_for_quiet_bound(&mut self.quiet_bound), if self.quiet_bound.is_some() => ClaudeExtra::QuietBound,
        }
    }

    async fn on_extra(
        &mut self,
        event: ClaudeExtra,
        state: &mut agent_process_driver::State<Clock>,
    ) {
        match event {
            ClaudeExtra::Initial(progress) => {
                self.initial_input_pending = false;
                match progress {
                    InitialInputProgress::Ready(input) => self.standard_input = Some(input),
                    InitialInputProgress::Cancelled(reason) => self.cancel(reason, state),
                    InitialInputProgress::Failed => self.fail(
                        AgentFailureCause::HarnessSetupFailed {
                            stage: AgentHarnessSetupStage::Initialization,
                        },
                        state,
                    ),
                }
            }
            ClaudeExtra::Channel(event) => self.on_channel_event(event, state).await,
            ClaudeExtra::QuietBound => self.on_quiet_bound(state).await,
        }
    }

    async fn on_cancel(
        &mut self,
        _reason: CancellationReason,
        state: &mut agent_process_driver::State<Clock>,
    ) {
        state.parser_enabled = false;
        self.quiet_bound = None;
        self.standard_input.take();
    }

    async fn on_stdout(&mut self, bytes: &[u8], state: &mut agent_process_driver::State<Clock>) {
        self.restart_quiet_bound(&mut state.clock);
        let mut observations = Vec::new();
        self.reader
            .push(bytes, |observation| observations.push(observation));
        if let Some(reason) = state.cancellation.cancellation_reason() {
            self.cancel(reason, state);
            return;
        }
        if matches!(self.start, Start::Pending(_))
            && let Some(initialization) = self.reader.initialization()
        {
            // Claude Code writes `system/init` only after the relay has answered `initialize`,
            // which follows the adapter's answer to its hello, so a missing hello here means
            // the result tool never connected (REQ-AGENT-44, REQ-AGENT-89).
            let acknowledged = initialization.claude_code_version.as_deref()
                == Some(self.invocation.adapter().version())
                && self
                    .result_tool
                    .as_ref()
                    .is_none_or(|tool| tool.hello.recorded());
            if !acknowledged {
                self.start = Start::Refused;
                self.fail(
                    AgentFailureCause::start_failure("launch preparation", "unavailable"),
                    state,
                );
                return;
            }
            if let Start::Pending(mut held) =
                std::mem::replace(&mut self.start, Start::Acknowledged)
            {
                held.append(&mut observations);
                observations = held;
            }
        }
        self.deliver(observations, state).await;
        if state.parser_enabled
            && self.reader.deciding_result()
                == Some(&DecidingResult::Successful(ResultText::OverLimit))
        {
            self.fail(AgentFailureCause::CapturedValueTooLarge, state);
        }
    }

    /// A read error ends the stream like its end does; the rule decides what was read.
    fn classify_read_failure(&mut self) {}

    fn cancellation_precedes_read_failure(&self) -> bool {
        true
    }

    async fn on_wait_error(&mut self, state: &mut agent_process_driver::State<Clock>) {
        if state.cancelled.is_none() {
            self.failure
                .get_or_insert(AgentFailureCause::HarnessProtocolFailed);
        }
        state.force_group();
    }

    fn needs_group(&self, state: &agent_process_driver::State<Clock>) -> bool {
        self.initial_input_pending || !state.group_quiescent
    }

    fn probe_group(&self, state: &agent_process_driver::State<Clock>) -> bool {
        state.output_closed
            && (state.completion.is_some() || state.wait_failed)
            && !state.group_quiescent
    }

    /// Once Claude Code has exited and its output has closed, what is left in its group is a
    /// leftover, the relay or a stray child: it is forced and decides nothing
    /// (INV-HARNESS-24).
    fn on_group_live(&mut self, state: &mut agent_process_driver::State<Clock>) {
        if !state.termination_requested {
            state.force_group();
        }
    }

    async fn finish(
        mut self,
        mut state: agent_process_driver::State<Clock>,
        supervisor_quiesced: bool,
    ) -> AgentOutcome {
        if state.cancelled.is_none() {
            state.cancelled = state.cancellation.cancellation_reason();
        }
        let mut observations = Vec::new();
        self.reader
            .finish(|observation| observations.push(observation));
        let accepted = self
            .result_tool
            .as_ref()
            .and_then(|tool| tool.submissions.accepted().cloned());
        if let Some(tool) = self.result_tool.take()
            && let Err(error) = tool.channel.shutdown().await
        {
            observations.push(warning(format!(
                "the result tool's socket could not be removed: {error}"
            )));
        }
        if state.wait_failed || !supervisor_quiesced {
            self.failure
                .get_or_insert(AgentFailureCause::HarnessProtocolFailed);
        }
        let started = matches!(self.start, Start::Acknowledged);
        if started && state.cancelled.is_none() && self.failure.is_none() {
            observations.extend([
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::HarnessCompleted,
                },
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::HarnessQuiescent,
                },
            ]);
            match emit_observations(
                self.invocation.observations(),
                self.started,
                observations,
                &state.cancellation,
            )
            .await
            {
                ObservationProgress::Completed => {}
                ObservationProgress::Cancelled(reason) => state.cancelled = Some(reason),
                ObservationProgress::Failed => {
                    self.failure = Some(AgentFailureCause::HarnessProtocolFailed);
                }
            }
        }
        let (ending, deciding_result) = match self.held_at_bound {
            Some(held) => (Ending::StoppedAtBound, held),
            None => (
                Ending::Exited {
                    success: state.completion.is_some_and(|status| status.success()),
                },
                self.reader.deciding_result().cloned(),
            ),
        };
        decide(StepFacts {
            cancellation: state.cancelled,
            own_failure: self.failure,
            started,
            value_kind: self.invocation.value_mode().kind(),
            deciding_result,
            accepted,
            ending,
        })
    }
}

impl<Clock, Worker> ClaudeProtocol<'_, Clock, Worker>
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
{
    /// Delivers observations once start is acknowledged; until then they are held, and once
    /// start is refused they are dropped.
    async fn deliver(
        &mut self,
        mut observations: Vec<AgentObservation>,
        state: &mut agent_process_driver::State<Clock>,
    ) {
        match &mut self.start {
            Start::Pending(held) => held.append(&mut observations),
            Start::Refused => {}
            Start::Acknowledged => {
                match emit_observations(
                    self.invocation.observations(),
                    self.started,
                    observations,
                    &state.cancellation,
                )
                .await
                {
                    ObservationProgress::Completed => {}
                    ObservationProgress::Cancelled(reason) => self.cancel(reason, state),
                    ObservationProgress::Failed => {
                        self.fail(AgentFailureCause::HarnessProtocolFailed, state);
                    }
                }
            }
        }
    }

    fn cancel(
        &mut self,
        reason: CancellationReason,
        state: &mut agent_process_driver::State<Clock>,
    ) {
        state.cancelled.get_or_insert(reason);
        state.parser_enabled = false;
        self.quiet_bound = None;
        self.standard_input.take();
    }

    /// Records the adapter's own failure, unless cancelled, and forces the group.
    fn fail(&mut self, cause: AgentFailureCause, state: &mut agent_process_driver::State<Clock>) {
        if state.cancelled.is_none() {
            self.failure.get_or_insert(cause);
        }
        state.parser_enabled = false;
        state.force_group();
    }

    /// Restarts the quiet bound in full, unless it has already interrupted Claude Code or the
    /// invocation was cancelled (INV-HARNESS-24).
    fn restart_quiet_bound(&mut self, clock: &mut Clock) {
        if self.quiet_bound.is_some() && self.held_at_bound.is_none() {
            self.quiet_bound = Some(quiet_bound_wait(clock, QUIET_BOUND));
        }
    }

    /// At the quiet bound Claude Code is interrupted, and 30 seconds later its group is
    /// forced and its output no longer read, so the step ends even when a process outside
    /// the group holds the pipe (REQ-AGENT-88).
    async fn on_quiet_bound(&mut self, state: &mut agent_process_driver::State<Clock>) {
        if self.held_at_bound.is_some() {
            self.quiet_bound = None;
            state.force_group();
            state.output_closed = true;
            return;
        }
        self.held_at_bound = Some(self.reader.deciding_result().cloned());
        self.quiet_bound = Some(quiet_bound_wait(&mut state.clock, QUIET_BOUND_FORCE_DELAY));
        self.deliver(
            vec![warning(String::from(
                "Claude Code wrote nothing and submitted nothing for 2 hours; stopping it",
            ))],
            state,
        )
        .await;
        agent_process_driver::signal_interrupt(state.process_group);
    }

    /// A hello is answered; a submission is judged and answered (REQ-AGENT-84,
    /// REQ-AGENT-85). Both restart the quiet bound.
    async fn on_channel_event(
        &mut self,
        event: ResultChannelEvent,
        state: &mut agent_process_driver::State<Clock>,
    ) {
        let incoming = match event {
            ResultChannelEvent::Request(incoming) => incoming,
            ResultChannelEvent::ProtocolFailure => {
                self.deliver(
                    vec![warning(String::from(
                        "a result tool request could not be read",
                    ))],
                    state,
                )
                .await;
                return;
            }
            ResultChannelEvent::Closed => {
                if let Some(tool) = self.result_tool.as_mut() {
                    tool.listening = false;
                }
                return;
            }
        };
        self.restart_quiet_bound(&mut state.clock);
        let Some(tool) = self.result_tool.as_mut() else {
            return;
        };
        if !state.parser_enabled {
            answer(
                incoming,
                ResultChannelAnswer::Failed {
                    cause: ResultChannelFailure::Channel,
                },
                &state.cancellation,
            )
            .await;
            return;
        }
        let judgement = tool
            .submissions
            .judge(
                incoming.request(),
                tool.validator,
                self.invocation.cancellation(),
            )
            .await;
        // Acceptance is the validator's decision: an answer that cannot be delivered undoes
        // nothing held.
        match judgement {
            SubmissionJudgement::Answer {
                answer: reply,
                observation,
            } => {
                let accepted = reply == ResultChannelAnswer::Accepted;
                answer(incoming, reply, &state.cancellation).await;
                let observation = observation.or_else(|| {
                    accepted.then(|| AgentObservation::Diagnostic {
                        level: AgentDiagnosticLevel::Information,
                        message: Arc::from("result accepted"),
                    })
                });
                if let Some(observation) = observation {
                    self.deliver(vec![observation], state).await;
                }
            }
            SubmissionJudgement::Fatal {
                answer: reply,
                cause,
            } => {
                answer(incoming, reply, &state.cancellation).await;
                self.fail(cause, state);
            }
            SubmissionJudgement::Cancelled(reason) => self.cancel(reason, state),
        }
    }
}

fn warning(message: String) -> AgentObservation {
    AgentObservation::Diagnostic {
        level: AgentDiagnosticLevel::Warning,
        message: Arc::from(message),
    }
}

fn quiet_bound_wait<Clock: CoordinatorClock>(clock: &mut Clock, after: Duration) -> QuietBoundWait {
    let deadline = clock.now().add(after);
    let deadline_clock = clock.clone();
    Box::pin(async move {
        deadline_clock.wait_until(deadline).await;
    })
}

async fn wait_for_quiet_bound(wait: &mut Option<QuietBoundWait>) {
    match wait {
        Some(wait) => wait.await,
        None => pending().await,
    }
}

async fn receive_channel_event<Clock, Worker>(
    result_tool: &mut Option<ResultTool<'_, Clock, Worker>>,
) -> ResultChannelEvent {
    match result_tool {
        Some(tool) => tool.channel.receive().await,
        None => pending().await,
    }
}

/// Answers one request; the relay may already be gone, which changes nothing.
async fn answer(
    incoming: IncomingResultRequest<ResultChannelRequest, ResultChannelAnswer>,
    reply: ResultChannelAnswer,
    cancellation: &CancellationSource,
) {
    tokio::select! {
        biased;
        _ = cancellation.wait_for_cancellation() => {}
        _ = incoming.respond(reply) => {}
    }
}

struct ClaudeDriverInput<'a, Clock, Worker> {
    bytes: &'a [u8],
    result_tool: Option<ResultTool<'a, Clock, Worker>>,
    clock: Clock,
}

async fn drive_process<Clock, Worker>(
    invocation: &AgentInvocation,
    started: &AgentStartCallback,
    process: LaunchedClaudeCodeProcess,
    process_directives: mpsc::UnboundedReceiver<AgentProcessDirective>,
    driver_input: ClaudeDriverInput<'_, Clock, Worker>,
) -> AgentOutcome
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
{
    let ClaudeDriverInput {
        bytes,
        result_tool,
        clock,
    } = driver_input;
    let (standard_input, output) = process.split();
    let cancellation = invocation.cancellation().clone();
    let initial_cancellation = cancellation.clone();
    let initial_input = Box::pin(async move {
        initialize_standard_input(standard_input, bytes, &initial_cancellation).await
    });
    let protocol = ClaudeProtocol {
        invocation,
        started,
        reader: StreamReader::new(
            invocation.value_mode().kind(),
            invocation.limits().maximum_response_bytes(),
        ),
        result_tool,
        standard_input: None,
        initial_input,
        initial_input_pending: true,
        start: Start::Pending(Vec::new()),
        quiet_bound: None,
        held_at_bound: None,
        failure: None,
    };
    agent_process_driver::drive_signalled(
        output,
        cancellation,
        clock,
        protocol,
        process_directives,
        None,
    )
    .await
}

enum InitialInputProgress {
    Ready(UnixStream),
    Cancelled(CancellationReason),
    Failed,
}

async fn initialize_standard_input(
    mut standard_input: UnixStream,
    bytes: &[u8],
    cancellation: &CancellationSource,
) -> InitialInputProgress {
    if let Some(reason) = cancellation.cancellation_reason() {
        return InitialInputProgress::Cancelled(reason);
    }
    // A full socket can mean the harness is still starting, so the prompt's delivery has no
    // deadline of its own. Standard input stays open after it until Claude Code exits:
    // Claude Code ends the session by its own idle exit (INV-HARNESS-24).
    let result = tokio::select! {
        biased;
        reason = cancellation.wait_for_cancellation() => return InitialInputProgress::Cancelled(reason),
        result = standard_input.write_all(bytes) => result,
    };
    if let Some(reason) = cancellation.cancellation_reason() {
        return InitialInputProgress::Cancelled(reason);
    }
    match result {
        Ok(()) => InitialInputProgress::Ready(standard_input),
        Err(_) => InitialInputProgress::Failed,
    }
}

enum ObservationProgress {
    Completed,
    Cancelled(CancellationReason),
    Failed,
}

async fn emit_observations(
    sink: &OrderedAgentObservationSink,
    started: &AgentStartCallback,
    observations: Vec<AgentObservation>,
    cancellation: &CancellationSource,
) -> ObservationProgress {
    for observation in observations {
        if let Some(reason) = cancellation.cancellation_reason() {
            return ObservationProgress::Cancelled(reason);
        }
        let reports_start = matches!(
            observation,
            AgentObservation::Lifecycle {
                milestone: AgentLifecycleMilestone::HarnessStarted,
            }
        );
        if reports_start && started.report().is_err() {
            return cancellation
                .cancellation_reason()
                .map_or(ObservationProgress::Failed, ObservationProgress::Cancelled);
        }
        let progress = emit_observation(sink, observation, cancellation).await;
        if !matches!(progress, ObservationProgress::Completed) {
            return progress;
        }
    }
    ObservationProgress::Completed
}

async fn emit_observation(
    sink: &OrderedAgentObservationSink,
    observation: AgentObservation,
    cancellation: &CancellationSource,
) -> ObservationProgress {
    let emitted = sink.emit(observation);
    tokio::pin!(emitted);
    let cancelled = cancellation.wait_for_cancellation();
    tokio::pin!(cancelled);
    tokio::select! {
        biased;
        reason = &mut cancelled => ObservationProgress::Cancelled(reason),
        result = &mut emitted => {
            if result.is_ok() {
                ObservationProgress::Completed
            } else {
                ObservationProgress::Failed
            }
        }
    }
}
