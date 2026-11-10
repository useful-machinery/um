//! The condition sources `um workflow run resume` evaluates an `external` wait's rule
//! against (REQ-WFEXEC-65, REQ-CONTINUE-74): each `(source, event)` pair this CLI observes
//! and how. Admission refuses an `external` wait on any other pair (REQ-WFEXEC-59).

mod github;

use um_execution::{ConditionError, ConditionSource, ExternalEvent, WaitRecord};

type Observe = fn(&WaitRecord) -> Result<Vec<ExternalEvent>, ConditionError>;

/// The conditions this CLI evaluates: `github` / `pull_request.queue_settled`.
pub(super) struct Conditions {
    conditions: Vec<(&'static str, &'static str, Observe)>,
}

impl Default for Conditions {
    fn default() -> Self {
        Self {
            conditions: vec![(
                github::SOURCE,
                github::QUEUE_SETTLED,
                github::observe_queue_settled,
            )],
        }
    }
}

impl Conditions {
    /// The `(source, event)` pairs admission checks an `external` wait's rule against.
    pub(super) fn supported(&self) -> impl Iterator<Item = (&'static str, &'static str)> + '_ {
        self.conditions
            .iter()
            .map(|(source, event, _)| (*source, *event))
    }

    fn condition(&self, source: &str, event: &str) -> Option<Observe> {
        self.conditions
            .iter()
            .find(|(supported_source, supported_event, _)| {
                (*supported_source, *supported_event) == (source, event)
            })
            .map(|(_, _, observe)| *observe)
    }
}

impl ConditionSource for Conditions {
    fn supports(&self, source: &str, event: &str) -> bool {
        self.condition(source, event).is_some()
    }

    /// The events each of the rule's `event` names observes now, in the rule's order; a name
    /// this CLI does not evaluate is `wait_condition_unsupported`.
    fn observe(&self, wait: &WaitRecord) -> Result<Vec<ExternalEvent>, ConditionError> {
        let mut events = Vec::new();
        for event in &wait.rule.event {
            let observe =
                self.condition(&wait.rule.source, event)
                    .ok_or_else(|| ConditionError {
                        code: ConditionError::UNSUPPORTED,
                        message: format!(
                            "This CLI evaluates no condition for source {:?} and event {event:?}.",
                            wait.rule.source
                        ),
                    })?;
            events.extend(observe(wait)?);
        }
        Ok(events)
    }
}
