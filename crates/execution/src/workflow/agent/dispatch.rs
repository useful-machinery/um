use std::io;
use std::num::NonZeroU64;
use std::sync::Arc;

use super::{AgentInvocation, AgentOutcome, AgentStartCallback, NativeHarness};
use crate::workflow::claude_code::adapter::ClaudeCodeStreamJsonV1Adapter;
use crate::workflow::codex::adapter::CodexAppServerV1Adapter;
use crate::workflow::diagnostic::StepDiagnosticLog;
use crate::workflow::pi::adapter::PiJsonV1Adapter;

pub use super::AgentAdapter as AgentInvocationDispatcher;

#[derive(Clone)]
pub struct ProductionAgentDispatcher<Clock, Observer> {
    pi: PiJsonV1Adapter<Clock, Observer>,
    claude_code: ClaudeCodeStreamJsonV1Adapter<Clock, Observer>,
    codex: CodexAppServerV1Adapter<Clock, Observer>,
}

pub fn production_agent_dispatcher<Clock, Observer>(
    diagnostics: StepDiagnosticLog,
    maximum_diagnostic_stream_bytes: NonZeroU64,
    clock: Clock,
    observer: Observer,
    client_version: &str,
) -> io::Result<ProductionAgentDispatcher<Clock, Observer>>
where
    Clock: Clone,
    Observer: Clone,
{
    let pi = PiJsonV1Adapter::new_default(
        diagnostics.clone(),
        maximum_diagnostic_stream_bytes,
        clock.clone(),
        observer.clone(),
    )?;
    let claude_code = ClaudeCodeStreamJsonV1Adapter::new_default(
        diagnostics.clone(),
        maximum_diagnostic_stream_bytes,
        clock.clone(),
        observer.clone(),
    )?;
    let codex = CodexAppServerV1Adapter::new(
        diagnostics,
        maximum_diagnostic_stream_bytes,
        clock,
        observer,
        Arc::from(client_version),
    )?;
    Ok(ProductionAgentDispatcher {
        pi,
        claude_code,
        codex,
    })
}

impl<Clock, Observer> AgentInvocationDispatcher for ProductionAgentDispatcher<Clock, Observer>
where
    Clock: crate::workflow::coordinator::CoordinatorClock,
    Observer: crate::workflow::observation::ExecutionObserver<Clock::Instant>,
{
    async fn invoke(
        &self,
        invocation: AgentInvocation,
        started: AgentStartCallback,
    ) -> AgentOutcome {
        match invocation.adapter().native_configuration() {
            NativeHarness::Pi(..) => self.pi.invoke(invocation, started).await,
            NativeHarness::ClaudeCode(..) => self.claude_code.invoke(invocation, started).await,
            NativeHarness::Codex(..) => self.codex.invoke(invocation, started).await,
        }
    }
}

#[cfg(test)]
mod tests;
