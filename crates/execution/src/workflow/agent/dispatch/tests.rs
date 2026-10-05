use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::workflow::admission::{CancellationSource, EnvironmentSnapshot};
use crate::workflow::agent::scripted::scripted_agent_dispatcher;
use crate::workflow::agent::{
    AdmittedAgentAdapter, AgentCompatibilityProfile, AgentInvocationIdentity,
    AgentInvocationLimits, AgentInvocationStaging, AgentProcessContext, AgentPrompt,
    AgentValueMode, CompletedAgentInvocation, NoopAgentObservationSink, PositiveDuration,
    WorkflowRunId, agent_start_channel,
};
use crate::workflow::agent_diagnostics::AgentDiagnosticSession;
use crate::workflow::claude_code::ClaudeCodeStreamJsonV1ProtocolLimits;
use crate::workflow::claude_code::{ClaudeCodeConfig, ClaudeCodeEffort};
use crate::workflow::codex::CodexAppServerV1ProtocolLimits;
use crate::workflow::codex::CodexConfig;
use crate::workflow::execution_root::AdmittedExecutionRoot;
use crate::workflow::pi::PiJsonV1ProtocolLimits;
use crate::workflow::pi::{PiConfig, Thinking};
use crate::workflow::process_group::ProcessGuardRegistry;
use crate::workflow::runtime::{ActionId, TransitionSequence};

fn invocation<Configuration, ProtocolLimits>(
    temporary: &tempfile::TempDir,
    profile: AgentCompatibilityProfile,
    executable: &str,
    version: &str,
    configuration: Configuration,
    protocol_limits: ProtocolLimits,
) -> AgentInvocation
where
    Configuration: crate::workflow::agent::IntoNativeHarness<ProtocolLimits>,
{
    let root = temporary.path().join(format!("root-{profile:?}"));
    std::fs::create_dir(&root).unwrap();
    let cwd = AdmittedExecutionRoot::admit(&root)
        .unwrap()
        .select_working_directory(None)
        .unwrap();
    let limits = AgentInvocationLimits::new(
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        NonZeroU64::new(4096).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(512).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        protocol_limits,
    );
    let diagnostic_session_path = temporary.path().join(format!("diagnostics-{profile:?}"));
    let diagnostic_session = match profile {
        AgentCompatibilityProfile::PiJsonV1 => {
            AgentDiagnosticSession::fixture(diagnostic_session_path)
        }
        AgentCompatibilityProfile::ClaudeCodeStreamJsonV1 => {
            AgentDiagnosticSession::claude_code_fixture(diagnostic_session_path)
        }
        AgentCompatibilityProfile::CodexAppServerV1 => {
            AgentDiagnosticSession::codex_fixture(diagnostic_session_path)
        }
    };
    AgentInvocation::new(
        AgentInvocationIdentity::new(
            WorkflowRunId::from(Arc::from("run")),
            Arc::from("agent"),
            ActionId {
                transition_sequence: TransitionSequence::default(),
            },
        ),
        AdmittedAgentAdapter::new(
            profile,
            executable.into(),
            Arc::from(version),
            configuration,
        ),
        AgentProcessContext::new(cwd, EnvironmentSnapshot::default()),
        AgentInvocationStaging::new(temporary.path().join("result-endpoint")),
        diagnostic_session,
        AgentPrompt::new(Arc::from("system"), Arc::from("message")),
        Arc::from([]),
        AgentValueMode::None,
        limits,
        CancellationSource::new(),
        ProcessGuardRegistry::default(),
        NoopAgentObservationSink,
    )
}

#[tokio::test]
async fn scripted_dispatch_preserves_each_native_harness_identity_and_returns_its_outcome() {
    let temporary = tempfile::tempdir().unwrap();
    let invocations = [
        invocation(
            &temporary,
            AgentCompatibilityProfile::PiJsonV1,
            "/validated/pi",
            "0.84.2",
            PiConfig {
                model: "openai/gpt-5".into(),
                thinking: Thinking::Minimal,
            },
            PiJsonV1ProtocolLimits::profile(),
        ),
        invocation(
            &temporary,
            AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
            "/validated/claude",
            "2.1.284",
            ClaudeCodeConfig {
                model: "claude-opus-4-1".into(),
                effort: ClaudeCodeEffort::High,
            },
            ClaudeCodeStreamJsonV1ProtocolLimits::profile(),
        ),
        invocation(
            &temporary,
            AgentCompatibilityProfile::CodexAppServerV1,
            "/validated/codex",
            "0.147.23",
            CodexConfig {
                model: "gpt-5.4".into(),
                effort: "xhigh".into(),
            },
            CodexAppServerV1ProtocolLimits::profile(),
        ),
    ];
    for invocation in invocations {
        let profile = invocation.adapter().profile();
        let (dispatcher, mut control) = scripted_agent_dispatcher();
        let (callback, started) = agent_start_channel();
        let task = tokio::spawn(async move { dispatcher.invoke(invocation, callback).await });
        let observed = control.wait_until_started().await.unwrap();
        assert_eq!(observed.profile(), profile);
        observed.control().start().await.unwrap();
        started.receive().await.unwrap();
        control.complete().await.unwrap();
        assert_eq!(
            task.await.unwrap(),
            AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
        );
    }
}
