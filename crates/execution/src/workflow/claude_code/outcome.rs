//! How a Claude Code step ends (REQ-AGENT-86, INV-HARNESS-5, INV-HARNESS-22): a pure function
//! of what the adapter holds once the process has exited and its group is gone.

use super::reader::{DecidingResult, ResultText};
use crate::workflow::admission::CancellationReason;
use crate::workflow::agent::{
    AgentFailureCause, AgentHarnessFailureDetail, AgentOutcome, AgentValueKind,
    BoundedAgentResponse, CapturedJson, CompletedAgentInvocation, failed_agent_outcome,
};

/// How Claude Code's process ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Ending {
    Exited {
        success: bool,
    },
    /// The adapter stopped it at the quiet bound; how it exited is not read (REQ-AGENT-88).
    StoppedAtBound,
}

/// Everything a Claude Code step is decided by.
#[derive(Clone, Debug)]
pub(super) struct StepFacts {
    pub(super) cancellation: Option<CancellationReason>,
    /// A failure of the adapter's own: the prompt could not be written, the validator's
    /// deadline or worker, an observation that could not be delivered, a child that could
    /// not be waited for.
    pub(super) own_failure: Option<AgentFailureCause>,
    /// Whether start was acknowledged: a `system/init` naming the expected version was read
    /// and, in Result mode, the result channel's hello was recorded before it.
    pub(super) started: bool,
    pub(super) value_kind: AgentValueKind,
    pub(super) deciding_result: Option<DecidingResult>,
    /// The last submission the validator accepted.
    pub(super) accepted: Option<CapturedJson>,
    pub(super) ending: Ending,
}

/// The outcome by the rule, first match wins: a cancellation; the adapter's own failure, a
/// response above its limit among them; a start that never happened; a failed deciding
/// `result`; an unsuccessful exit or no successful deciding `result`; and then the value.
pub(super) fn decide(facts: StepFacts) -> AgentOutcome {
    if let Some(reason) = facts.cancellation {
        return AgentOutcome::Cancelled { reason };
    }
    if let Some(cause) = facts.own_failure {
        return failed_agent_outcome(cause);
    }
    if facts.deciding_result == Some(DecidingResult::Successful(ResultText::OverLimit)) {
        return failed_agent_outcome(AgentFailureCause::CapturedValueTooLarge);
    }
    if !facts.started {
        return failed_agent_outcome(AgentFailureCause::start_failure(
            "launch preparation",
            "unavailable",
        ));
    }
    let text = match facts.deciding_result {
        Some(DecidingResult::Failed(detail)) => {
            return failed_agent_outcome(AgentFailureCause::HarnessFailed { detail });
        }
        Some(DecidingResult::Successful(text))
            if facts.ending != (Ending::Exited { success: false }) =>
        {
            text
        }
        Some(DecidingResult::Successful(_)) | None => {
            return failed_agent_outcome(AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnsuccessfulExit,
            });
        }
    };
    match facts.value_kind {
        AgentValueKind::None => AgentOutcome::Completed(CompletedAgentInvocation::NoValue),
        AgentValueKind::Response => match text {
            ResultText::Text(text) => AgentOutcome::Completed(CompletedAgentInvocation::Response(
                BoundedAgentResponse::from_bounded(text),
            )),
            ResultText::NotKept | ResultText::Missing | ResultText::OverLimit => {
                failed_agent_outcome(AgentFailureCause::MissingResponse)
            }
        },
        AgentValueKind::Result => facts.accepted.map_or_else(
            || failed_agent_outcome(AgentFailureCause::MissingResult),
            |result| AgentOutcome::Completed(CompletedAgentInvocation::Result(result)),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use serde_json::{Value, json};

    use super::super::reader::StreamReader;
    use super::*;
    use crate::workflow::agent::{AgentHarnessSetupStage, AgentObservation};

    #[derive(Clone, Copy, Debug)]
    enum Results {
        None,
        EmptyOnly,
        Successful,
        Failed,
        FailedThenSuccessful,
        SuccessfulThenFailed,
        SuccessfulThenEmpty,
    }

    /// What the row's `result`s decide, as the rule reads them: `Some(Ok(text))` for a
    /// successful deciding `result` carrying `text`, `Some(Err(()))` for a failed one.
    fn deciding(results: Results) -> Option<Result<&'static str, ()>> {
        match results {
            Results::None | Results::EmptyOnly => None,
            Results::Successful | Results::SuccessfulThenEmpty => Some(Ok("answer")),
            Results::FailedThenSuccessful => Some(Ok("recovered")),
            Results::Failed | Results::SuccessfulThenFailed => Some(Err(())),
        }
    }

    fn successful(text: &str) -> Value {
        json!({"type": "result", "subtype": "success", "is_error": false,
            "terminal_reason": "completed", "num_turns": 2, "result": text})
    }

    fn failed() -> Value {
        json!({"type": "result", "subtype": "error_during_execution", "is_error": true,
            "terminal_reason": "rate_limit", "num_turns": 1, "result": "failure text"})
    }

    /// A turn Claude Code opens and closes without calling the model.
    fn empty() -> Value {
        json!({"type": "result", "subtype": "success", "is_error": false, "num_turns": 0,
            "result": ""})
    }

    fn frames(results: Results) -> Vec<Value> {
        match results {
            Results::None => Vec::new(),
            Results::EmptyOnly => vec![empty()],
            Results::Successful => vec![successful("answer")],
            Results::Failed => vec![failed()],
            Results::FailedThenSuccessful => vec![failed(), successful("recovered")],
            Results::SuccessfulThenFailed => vec![successful("answer"), failed()],
            Results::SuccessfulThenEmpty => vec![successful("answer"), empty()],
        }
    }

    /// The deciding `result` a reader of `kind` takes from `frames`.
    fn read(kind: AgentValueKind, frames: &[Value]) -> Option<DecidingResult> {
        let mut reader = StreamReader::new(kind, NonZeroU64::new(16).unwrap());
        let mut bytes = Vec::new();
        for frame in frames {
            serde_json::to_writer(&mut bytes, frame).unwrap();
            bytes.push(b'\n');
        }
        reader.push(&bytes, drop::<AgentObservation>);
        reader.finish(drop::<AgentObservation>);
        reader.deciding_result().cloned()
    }

    fn value(value: i64) -> CapturedJson {
        CapturedJson::fixture(Arc::new(json!(value)))
    }

    fn harness_failed(detail: AgentHarnessFailureDetail) -> AgentOutcome {
        failed_agent_outcome(AgentFailureCause::HarnessFailed { detail })
    }

    fn response(text: &str) -> AgentOutcome {
        AgentOutcome::Completed(CompletedAgentInvocation::Response(
            BoundedAgentResponse::from_bounded(Arc::from(text)),
        ))
    }

    // REQ-AGENT-86, REQ-AGENT-26, REQ-AGENT-27, REQ-AGENT-28, REQ-AGENT-70, REQ-AGENT-88,
    // INV-HARNESS-5
    #[test]
    fn the_rule_decides_every_combination() {
        let own_failure = AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::Initialization,
        };
        let start_failure = AgentFailureCause::start_failure("launch preparation", "unavailable");
        let mut rows = 0;
        for kind in [
            AgentValueKind::None,
            AgentValueKind::Response,
            AgentValueKind::Result,
        ] {
            for started in [false, true] {
                for own in [None, Some(own_failure.clone())] {
                    for cancellation in [None, Some(CancellationReason::UserRequest)] {
                        for results in [
                            Results::None,
                            Results::EmptyOnly,
                            Results::Successful,
                            Results::Failed,
                            Results::FailedThenSuccessful,
                            Results::SuccessfulThenFailed,
                            Results::SuccessfulThenEmpty,
                        ] {
                            for submissions in 0..=2 {
                                for ending in [
                                    Ending::Exited { success: true },
                                    Ending::Exited { success: false },
                                    Ending::StoppedAtBound,
                                ] {
                                    let accepted = (submissions > 0).then(|| value(submissions));
                                    let expected = if let Some(reason) = cancellation {
                                        AgentOutcome::Cancelled { reason }
                                    } else if let Some(cause) = own.clone() {
                                        failed_agent_outcome(cause)
                                    } else if !started {
                                        failed_agent_outcome(start_failure.clone())
                                    } else {
                                        match deciding(results) {
                                            Some(Err(())) => harness_failed(
                                                AgentHarnessFailureDetail::ModelError,
                                            ),
                                            None => harness_failed(
                                                AgentHarnessFailureDetail::UnsuccessfulExit,
                                            ),
                                            Some(Ok(_))
                                                if ending
                                                    == (Ending::Exited { success: false }) =>
                                            {
                                                harness_failed(
                                                    AgentHarnessFailureDetail::UnsuccessfulExit,
                                                )
                                            }
                                            Some(Ok(text)) => match kind {
                                                AgentValueKind::None => AgentOutcome::Completed(
                                                    CompletedAgentInvocation::NoValue,
                                                ),
                                                AgentValueKind::Response => response(text),
                                                AgentValueKind::Result => match accepted.clone() {
                                                    Some(result) => AgentOutcome::Completed(
                                                        CompletedAgentInvocation::Result(result),
                                                    ),
                                                    None => failed_agent_outcome(
                                                        AgentFailureCause::MissingResult,
                                                    ),
                                                },
                                            },
                                        }
                                    };
                                    let facts = StepFacts {
                                        cancellation,
                                        own_failure: own.clone(),
                                        started,
                                        value_kind: kind,
                                        deciding_result: read(kind, &frames(results)),
                                        accepted,
                                        ending,
                                    };
                                    let row = format!("{facts:?}");
                                    assert_eq!(decide(facts), expected, "{row}");
                                    rows += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(rows, 3 * 2 * 2 * 2 * 7 * 3 * 3);

        // The failed deciding `result` carries its classification, whatever was submitted.
        let outcome = decide(StepFacts {
            cancellation: None,
            own_failure: None,
            started: true,
            value_kind: AgentValueKind::Result,
            deciding_result: read(AgentValueKind::Result, &[successful("x"), failed()]),
            accepted: Some(value(7)),
            ending: Ending::Exited { success: true },
        });
        let AgentOutcome::Failed(failure) = outcome else {
            panic!("expected a failure, got {outcome:?}");
        };
        let AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::Classified { diagnostic, .. },
        } = failure.cause()
        else {
            panic!("expected a classified harness failure, got {failure:?}");
        };
        assert_eq!(diagnostic.harness_error, Some("rate_limited"));
        assert_eq!(diagnostic.http_status, None);
        assert_eq!(diagnostic.resets_at, None);

        // A Response step's value is the deciding `result`'s text: none is
        // `missing_response`, an empty one is a valid empty response, and one above the
        // limit is `captured_value_too_large` ahead of every later cause.
        let respond = |frames: &[Value], started: bool| {
            decide(StepFacts {
                cancellation: None,
                own_failure: None,
                started,
                value_kind: AgentValueKind::Response,
                deciding_result: read(AgentValueKind::Response, frames),
                accepted: None,
                ending: Ending::Exited { success: true },
            })
        };
        let mut textless = successful("");
        textless.as_object_mut().unwrap().remove("result");
        assert_eq!(
            respond(&[textless], true),
            failed_agent_outcome(AgentFailureCause::MissingResponse)
        );
        assert_eq!(
            respond(&[successful("answer"), successful("")], true),
            response("")
        );
        for started in [false, true] {
            assert_eq!(
                respond(&[successful("seventeen bytes!!")], started),
                failed_agent_outcome(AgentFailureCause::CapturedValueTooLarge)
            );
        }
    }
}
