//! Project schedules and their occurrences over the generated schedule models.
//!
//! As for Linear triggers, requests go through the generated configuration with a
//! hand-written client: the generated one drops error headers, and its models drop
//! unknown members rather than fail, so every response is decoded closed.
use std::fmt;

use reqwest::StatusCode;

use super::generated::{apis, models};
use super::http_client::zeroize_generated_bearer_access_token;
use super::resource::{
    LifecycleAction, ProblemVocabulary, ResourceFailure, decode_closed_json, invalid,
    mutation_request, receive_closed_json_response, receive_confirmed_json_response,
    receive_json_response, receive_response, resource_configuration, validate_mutation_key,
};
use super::{HttpTransportPolicy, problem};

pub type Schedule = models::Schedule;
pub type ScheduleList = models::ScheduleList;
pub type ScheduleTombstone = models::ScheduleTombstone;
pub type ScheduleOccurrence = models::ScheduleOccurrence;
pub type ScheduleOccurrenceList = models::ScheduleOccurrenceList;
pub type ScheduleOccurrenceState = models::schedule_occurrence::State;
pub type CreateScheduleRequest = models::CreateScheduleRequest;
pub type UpdateScheduleRequest = models::UpdateScheduleRequest;

#[derive(Clone, serde::Serialize)]
#[serde(untagged)]
pub enum ScheduleRead {
    Active(Box<Schedule>),
    Deleted(ScheduleTombstone),
}

pub struct ScheduleOccurrenceFilters<'a> {
    pub limit: Option<i32>,
    pub cursor: Option<&'a str>,
    pub state: Option<&'a str>,
}

const SCHEDULE: ProblemVocabulary = ProblemVocabulary {
    bad_request: &[
        problem::BAD_REQUEST,
        "https://api.usefulmachinery.com/problems/timing-invalid",
        "https://api.usefulmachinery.com/problems/timing-too-frequent",
        "https://api.usefulmachinery.com/problems/input-kind-unschedulable",
    ],
    unprocessable: &["https://api.usefulmachinery.com/problems/workflow-source-invalid"],
    conflict: &[
        "https://api.usefulmachinery.com/problems/source-connection-conflict",
        "https://api.usefulmachinery.com/problems/idempotency-conflict",
        problem::QUANTITY_LIMIT_REACHED,
        "https://api.usefulmachinery.com/problems/workflow-source-unavailable",
    ],
};

pub struct ScheduleApi {
    configuration: apis::configuration::Configuration,
}

impl ScheduleApi {
    pub fn new(
        api_url: &str,
        token: &str,
        policy: HttpTransportPolicy,
    ) -> Result<Self, ScheduleApiError> {
        let configuration = resource_configuration(
            api_url,
            token,
            policy,
            ScheduleApiError::InvalidEndpoint,
            ScheduleApiError::BuildClient,
        )?;
        Ok(Self { configuration })
    }

    pub fn create(
        &self,
        org: &str,
        project: &str,
        key: &str,
        request: CreateScheduleRequest,
    ) -> Result<Schedule, ResourceFailure> {
        let endpoint = self.endpoint(org, project, &[]);
        let request = mutation_request(&self.configuration, reqwest::Method::POST, &endpoint, key)
            .json(&request);
        let schedule: Schedule =
            receive_confirmed_json_response(request, StatusCode::CREATED, key, &SCHEDULE)?;
        validate_schedule(&schedule, project, None)?;
        Ok(schedule)
    }

    pub fn update(
        &self,
        org: &str,
        project: &str,
        id: &str,
        key: &str,
        request: UpdateScheduleRequest,
    ) -> Result<Schedule, ResourceFailure> {
        let endpoint = self.endpoint(org, project, &[id]);
        let request = mutation_request(&self.configuration, reqwest::Method::PATCH, &endpoint, key)
            .json(&request);
        let schedule: Schedule =
            receive_confirmed_json_response(request, StatusCode::OK, key, &SCHEDULE)?;
        validate_schedule(&schedule, project, Some(id))?;
        Ok(schedule)
    }

    pub fn schedule(
        &self,
        org: &str,
        project: &str,
        id: &str,
    ) -> Result<ScheduleRead, ResourceFailure> {
        // The generated untagged union's active model has defaulted required fields;
        // it would consume a tombstone before reaching the tombstone variant.
        let endpoint = self.endpoint(org, project, &[id]);
        let body = receive_json_response(
            super::generated_api_request(&self.configuration, reqwest::Method::GET, &endpoint),
            StatusCode::OK,
            &SCHEDULE,
        )?;
        let value: serde_json::Value = serde_json::from_slice(&body).map_err(|_| invalid())?;
        if value.get("deleted").is_some() {
            if value.as_object().is_none_or(|m| m.len() != 4)
                || value.get("deleted") != Some(&serde_json::Value::Bool(true))
            {
                return Err(invalid());
            }
            let tombstone: ScheduleTombstone =
                serde_json::from_value(value).map_err(|_| invalid())?;
            if tombstone.id != id
                || tombstone.project_id != project
                || tombstone.deleted_at.is_empty()
            {
                return Err(invalid());
            }
            Ok(ScheduleRead::Deleted(tombstone))
        } else {
            // Generated models can drop unknown members rather than fail; the closed
            // decode never presents a partial schedule.
            let active: Schedule = decode_closed_json(&body)?;
            validate_schedule(&active, project, Some(id))?;
            Ok(ScheduleRead::Active(Box::new(active)))
        }
    }

    pub fn schedules(
        &self,
        org: &str,
        project: &str,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<ScheduleList, ResourceFailure> {
        let endpoint = self.endpoint(org, project, &[]);
        let mut request =
            super::generated_api_request(&self.configuration, reqwest::Method::GET, &endpoint);
        if let Some(limit) = limit {
            request = request.query(&[("limit", limit)]);
        }
        if let Some(cursor) = cursor {
            request = request.query(&[("cursor", cursor)]);
        }
        let page: ScheduleList = receive_closed_json_response(request, StatusCode::OK, &SCHEDULE)?;
        for item in &page.items {
            validate_schedule(item, project, None)?;
        }
        Ok(page)
    }

    /// Enables or disables the schedule and returns it, or deletes it and returns `None`.
    pub fn lifecycle(
        &self,
        org: &str,
        project: &str,
        id: &str,
        key: &str,
        action: LifecycleAction,
    ) -> Result<Option<Schedule>, ResourceFailure> {
        let suffix = match action {
            LifecycleAction::Enable => "enable",
            LifecycleAction::Disable => "disable",
            LifecycleAction::Delete => {
                let endpoint = self.endpoint(org, project, &[id]);
                let request =
                    mutation_request(&self.configuration, reqwest::Method::DELETE, &endpoint, key);
                let (body, headers) = receive_response(request, StatusCode::NO_CONTENT, &SCHEDULE)?;
                validate_mutation_key(&headers, key)?;
                if !body.is_empty() {
                    return Err(invalid());
                }
                return Ok(None);
            }
        };
        let endpoint = self.endpoint(org, project, &[id, suffix]);
        let request = mutation_request(&self.configuration, reqwest::Method::POST, &endpoint, key);
        let schedule: Schedule =
            receive_confirmed_json_response(request, StatusCode::OK, key, &SCHEDULE)?;
        validate_schedule(&schedule, project, Some(id))?;
        Ok(Some(schedule))
    }

    pub fn occurrences(
        &self,
        org: &str,
        project: &str,
        schedule: &str,
        filters: ScheduleOccurrenceFilters<'_>,
    ) -> Result<ScheduleOccurrenceList, ResourceFailure> {
        let endpoint = self.endpoint(org, project, &[schedule, "occurrences"]);
        let mut request =
            super::generated_api_request(&self.configuration, reqwest::Method::GET, &endpoint);
        if let Some(limit) = filters.limit {
            request = request.query(&[("limit", limit)]);
        }
        for (name, value) in [("cursor", filters.cursor), ("state", filters.state)] {
            if let Some(value) = value {
                request = request.query(&[(name, value)]);
            }
        }
        let page: ScheduleOccurrenceList =
            receive_closed_json_response(request, StatusCode::OK, &SCHEDULE)?;
        for item in &page.items {
            validate_occurrence(item, schedule, None)?;
        }
        Ok(page)
    }

    pub fn occurrence(
        &self,
        org: &str,
        project: &str,
        schedule: &str,
        id: &str,
    ) -> Result<ScheduleOccurrence, ResourceFailure> {
        let endpoint = self.endpoint(org, project, &[schedule, "occurrences", id]);
        let request =
            super::generated_api_request(&self.configuration, reqwest::Method::GET, &endpoint);
        let occurrence: ScheduleOccurrence =
            receive_closed_json_response(request, StatusCode::OK, &SCHEDULE)?;
        validate_occurrence(&occurrence, schedule, Some(id))?;
        Ok(occurrence)
    }

    /// `{base}/v1/organizations/{org}/projects/{project}/schedules[/{segment}…]`.
    fn endpoint(&self, org: &str, project: &str, segments: &[&str]) -> String {
        let mut endpoint = format!(
            "{}/v1/organizations/{}/projects/{}/schedules",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(org),
            apis::urlencode(project),
        );
        for segment in segments {
            endpoint.push('/');
            endpoint.push_str(&apis::urlencode(segment));
        }
        endpoint
    }
}

impl Drop for ScheduleApi {
    fn drop(&mut self) {
        zeroize_generated_bearer_access_token(&mut self.configuration);
    }
}

fn validate_schedule(
    value: &Schedule,
    project: &str,
    id: Option<&str>,
) -> Result<(), ResourceFailure> {
    if !um_support::valid_typed_id(&value.id, "sch_")
        || value.project_id != project
        || id.is_some_and(|id| value.id != id)
        || value.version < 1
        || value.grant_id.is_empty()
        || value.created_at.is_empty()
        || value.updated_at.is_empty()
        // REQ-SCHEDULE-13: a disabled schedule is never due.
        || (!value.enabled && value.next_due_at.is_some())
        || value.upcoming.len() > 3
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_occurrence(
    value: &ScheduleOccurrence,
    schedule: &str,
    id: Option<&str>,
) -> Result<(), ResourceFailure> {
    if !um_support::valid_typed_id(&value.id, "occ_")
        || value.schedule_id != schedule
        || id.is_some_and(|id| value.id != id)
        || value.schedule_version < 1
        || value.missed_count < 0
        || value.due_at.is_empty()
        || value.created_at.is_empty()
        || value.expires_at.is_empty()
        // REQ-SCHEDULE-29, REQ-SCHEDULE-36: a created run and an overlap skip name their runs.
        || (value.state == ScheduleOccurrenceState::RunCreated) != value.run_id.is_some()
        || (value.state == ScheduleOccurrenceState::SkippedActive
            && value.blocking_run_id.is_none())
    {
        return Err(invalid());
    }
    Ok(())
}

#[derive(Debug)]
pub enum ScheduleApiError {
    InvalidEndpoint,
    BuildClient(reqwest::Error),
}
impl fmt::Display for ScheduleApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint => write!(
                f,
                "invalid schedule API endpoint; check the deployment URL and HTTP policy"
            ),
            Self::BuildClient(error) => write!(f, "prepare schedule networking: {error}"),
        }
    }
}
impl std::error::Error for ScheduleApiError {}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::expect_used,
    reason = "schedule client unit tests use Rust test assertions and fixture extraction"
)]
mod tests {
    use reqwest::StatusCode;
    use reqwest::header::{HeaderMap, HeaderValue};

    use super::super::resource::{ResourceFailure, classify_response, retry_after};
    use super::{SCHEDULE, Schedule, ScheduleOccurrence, validate_occurrence, validate_schedule};

    const PROJECT: &str = "prj_01k0z6r1w8f4jy2m7q9v3x5abc";
    const SCHEDULE_ID: &str = "sch_01k0z6r1w8f4jy2m7q9v3x5abc";

    fn problem(status: u16, kind: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "type": format!("https://api.usefulmachinery.com/problems/{kind}"),
            "title": "Refused",
            "status": status,
        }))
        .expect("problem fixture")
    }

    fn classify(status: StatusCode, body: &[u8], headers: &HeaderMap) -> ResourceFailure {
        classify_response(status, body, retry_after(headers), &SCHEDULE)
    }

    fn schedule() -> Schedule {
        serde_json::from_value(serde_json::json!({
            "id": SCHEDULE_ID, "projectId": PROJECT, "version": 1, "enabled": true,
            "name": "nightly", "timing": {"cron": "0 3 * * *", "timeZone": "UTC"},
            "target": {"workflowPath": "workflows/build.yaml",
                       "executionPrincipalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abc"},
            "inputs": {}, "integrationContext": {},
            "grantId": "grt_01k0z6r1w8f4jy2m7q9v3x5abc",
            "nextDueAt": "2026-10-25T03:00:00Z",
            "upcoming": ["2026-10-25T03:00:00Z", "2026-10-26T03:00:00Z", "2026-10-27T03:00:00Z"],
            "lastOccurrence": null,
            "createdAt": "2026-10-24T00:00:00Z", "updatedAt": "2026-10-24T00:00:00Z"
        }))
        .expect("schedule fixture")
    }

    fn occurrence(state: &str) -> ScheduleOccurrence {
        serde_json::from_value(serde_json::json!({
            "id": "occ_01k0z6r1w8f4jy2m7q9v3x5abc", "scheduleId": SCHEDULE_ID,
            "scheduleVersion": 1, "grantId": "grt_01k0z6r1w8f4jy2m7q9v3x5abc",
            "dueAt": "2026-10-25T03:00:00Z", "missedCount": 0,
            "createdAt": "2026-10-25T03:00:00Z", "expiresAt": "2026-10-25T04:00:00Z",
            "state": state, "attemptNumber": 1
        }))
        .expect("occurrence fixture")
    }

    // REQ-SCHEDULE-43
    #[test]
    fn schedule_refusals_classify_with_the_schedule_vocabulary() {
        let none = HeaderMap::new();
        assert_eq!(
            classify(
                StatusCode::BAD_REQUEST,
                &problem(400, "timing-too-frequent"),
                &none
            ),
            ResourceFailure::InvalidInput {
                problem: Some("timing-too-frequent".to_owned())
            }
        );
        assert_eq!(
            classify(StatusCode::BAD_REQUEST, &problem(400, "bad-request"), &none),
            ResourceFailure::InvalidInput { problem: None }
        );
        for kind in ["source-connection-conflict", "idempotency-conflict"] {
            assert_eq!(
                classify(StatusCode::CONFLICT, &problem(409, kind), &none),
                ResourceFailure::Conflict
            );
        }
        let mut headers = HeaderMap::new();
        headers.insert("Retry-After", HeaderValue::from_static("30"));
        assert_eq!(
            classify(
                StatusCode::TOO_MANY_REQUESTS,
                &problem(429, "rate-limited"),
                &headers
            ),
            ResourceFailure::RateLimited {
                retry_after: Some(30)
            }
        );
        assert!(matches!(
            classify(
                StatusCode::BAD_REQUEST,
                &problem(400, "timing-later"),
                &none
            ),
            ResourceFailure::InvalidResponse { .. }
        ));
    }

    // REQ-SCHEDULE-43
    #[test]
    fn schedule_reads_outside_the_contract_are_refused() {
        assert_eq!(
            validate_schedule(&schedule(), PROJECT, Some(SCHEDULE_ID)),
            Ok(())
        );
        let mut other_project = schedule();
        other_project.project_id = "prj_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned();
        let mut untyped = schedule();
        untyped.id = "trg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned();
        let mut disabled_but_due = schedule();
        disabled_but_due.enabled = false;
        for refused in [other_project, untyped, disabled_but_due] {
            assert!(matches!(
                validate_schedule(&refused, PROJECT, None),
                Err(ResourceFailure::InvalidResponse { .. })
            ));
        }
    }

    // REQ-SCHEDULE-43
    #[test]
    fn occurrence_reads_outside_the_contract_are_refused() {
        let mut created = occurrence("run_created");
        assert!(validate_occurrence(&created, SCHEDULE_ID, None).is_err());
        created.run_id = Some("run_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned());
        assert_eq!(validate_occurrence(&created, SCHEDULE_ID, None), Ok(()));
        let mut skipped = occurrence("skipped_active");
        assert!(validate_occurrence(&skipped, SCHEDULE_ID, None).is_err());
        skipped.blocking_run_id = Some("run_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned());
        assert_eq!(validate_occurrence(&skipped, SCHEDULE_ID, None), Ok(()));
    }
}
