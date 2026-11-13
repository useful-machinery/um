//! The run mirror's Linear target (REQ-LOCALRUN-192): `MirrorTarget` over Linear's GraphQL
//! API with the operator's `LINEAR_API_KEY`, sent bare in `Authorization` as Linear's personal
//! keys are, through `um_api::provider_post_json`. `UM_LINEAR_API_URL` overrides the base URL
//! for development and tests, and admits `http`. Every request is one attempt: a comment is
//! not idempotent, and a failed write is recorded for a person to see. `um workflow run event`
//! reads an issue by its Linear id through the same target (REQ-LOCALRUN-208).

use std::cell::RefCell;
use std::time::Duration;

use serde_json::{Value, json};
use um_api::{HttpTransportPolicy, provider_post_json};
use um_execution::{MirrorFailure, MirrorTarget};
use zeroize::Zeroizing;

const LINEAR_API: &str = "https://api.linear.app";
const API_VARIABLE: &str = "UM_LINEAR_API_URL";
const KEY_VARIABLE: &str = "LINEAR_API_KEY";
const TIMEOUT: Duration = Duration::from_secs(30);
/// The most of Linear's error message a diagnostic carries.
const MAXIMUM_MESSAGE_CHARACTERS: usize = 512;

const ISSUE_STATE: (&str, &str) = (
    "IssueState",
    "query IssueState($id: String!) { issue(id: $id) { id identifier updatedAt state { id name } team { id states(first: 100) { nodes { id name } } } } }",
);
const CREATE_COMMENT: (&str, &str) = (
    "CreateIssueComment",
    "mutation CreateIssueComment($input: CommentCreateInput!) { commentCreate(input: $input) { success } }",
);
const UPDATE_STATE: (&str, &str) = (
    "UpdateIssueState",
    "mutation UpdateIssueState($id: String!, $input: IssueUpdateInput!) { issueUpdate(id: $id, input: $input) { success } }",
);

/// One `POST` the target makes: the GraphQL endpoint, the bare key and the JSON body.
pub(super) struct Request<'a> {
    pub(super) url: &'a str,
    pub(super) authorization: &'a str,
    pub(super) body: Vec<u8>,
}

/// An answer's status and body.
pub(super) struct Response {
    pub(super) status: u16,
    pub(super) body: Vec<u8>,
}

type Requester = Box<dyn Fn(&Request<'_>) -> Result<Response, String>>;

/// An issue as `IssueState` reads it.
#[derive(Clone)]
struct Issue {
    identifier: String,
    id: String,
    state: String,
    state_id: String,
    /// The team's id and the issue's `updatedAt`, which only `observe` needs.
    team: Option<String>,
    updated_at: Option<String>,
    /// The team's states by `(id, name)`.
    states: Vec<(String, String)>,
}

/// An issue as `um workflow run event` reads it fresh (REQ-LOCALRUN-208): its team id, its
/// state's id and name, and its `updatedAt`, none when Linear's answer lacks them.
pub(in super::super) struct IssueObservation {
    pub(in super::super) team: Option<String>,
    pub(in super::super) state: String,
    pub(in super::super) state_name: String,
    pub(in super::super) updated_at: Option<String>,
}

/// Linear, reached at `endpoint` with `key`. Every write is keyed by the issue's id, which
/// `issue(id:)` resolves from the identifier; the issue read is kept, so a hook reads the
/// issue once however many writes it makes (REQ-LOCALRUN-192).
pub(in super::super) struct LinearTarget {
    endpoint: String,
    key: Option<Zeroizing<String>>,
    requester: Requester,
    issue: RefCell<Option<Issue>>,
}

impl LinearTarget {
    /// The target the environment configures: `UM_LINEAR_API_URL` or Linear's API, and
    /// `LINEAR_API_KEY`.
    pub(in super::super) fn from_environment() -> Self {
        let overridden = std::env::var(API_VARIABLE)
            .ok()
            .filter(|url| !url.is_empty());
        let policy = if overridden.is_some() {
            HttpTransportPolicy::AllowInsecureHttp
        } else {
            HttpTransportPolicy::HttpsOnly
        };
        let base = overridden.unwrap_or_else(|| LINEAR_API.to_owned());
        Self::new(
            &base,
            std::env::var(KEY_VARIABLE).ok(),
            Box::new(move |request| {
                provider_post_json(
                    request.url,
                    request.authorization,
                    &request.body,
                    policy,
                    TIMEOUT,
                )
                .map(|response| Response {
                    status: response.status,
                    body: response.body,
                })
                .map_err(|error| error.to_string())
            }),
        )
    }

    /// The target at `<base>/graphql` with `key`, an empty one being none.
    fn new(base: &str, key: Option<String>, requester: Requester) -> Self {
        Self {
            endpoint: format!("{}/graphql", base.trim_end_matches('/')),
            key: key.filter(|key| !key.is_empty()).map(Zeroizing::new),
            requester,
            issue: RefCell::new(None),
        }
    }

    /// Whether the target holds a key: `mirror_credentials_unavailable` when
    /// `LINEAR_API_KEY` is unset or empty (REQ-LOCALRUN-192). Every request takes its key
    /// from here, so a check before a write and the write agree (REQ-LOCALRUN-204).
    pub(super) fn credentials(&self) -> Result<(), MirrorFailure> {
        self.key().map(|_| ())
    }

    fn key(&self) -> Result<&str, MirrorFailure> {
        self.key.as_deref().map(String::as_str).ok_or_else(|| {
            MirrorFailure::credentials_unavailable(format!(
                "{KEY_VARIABLE} is unset or empty, so Linear cannot be written."
            ))
        })
    }

    /// The `data` of one GraphQL `operation` with `variables`, or the classified failure.
    fn post(
        &self,
        (operation, query): (&str, &str),
        variables: Value,
    ) -> Result<Value, MirrorFailure> {
        let key = self.key()?;
        let body = serde_json::to_vec(&json!({
            "operationName": operation,
            "query": query,
            "variables": variables,
        }))
        .map_err(|error| {
            MirrorFailure::unavailable(format!("{operation} could not be encoded: {error}."))
        })?;
        let response = (self.requester)(&Request {
            url: &self.endpoint,
            authorization: key,
            body,
        })
        .map_err(|error| {
            MirrorFailure::unavailable(format!(
                "Linear could not be reached for {operation}: {error}."
            ))
        })?;
        let answer = serde_json::from_slice::<Value>(&response.body).ok();
        if let Some(message) = answer.as_ref().and_then(error_message) {
            return Err(MirrorFailure::write_rejected(format!(
                "Linear rejected {operation}: {message}"
            )));
        }
        if (400..500).contains(&response.status) {
            return Err(MirrorFailure::write_rejected(format!(
                "Linear rejected {operation} with HTTP {}.",
                response.status
            )));
        }
        if !(200..300).contains(&response.status) {
            return Err(MirrorFailure::unavailable(format!(
                "Linear answered {operation} with HTTP {}.",
                response.status
            )));
        }
        answer
            .and_then(|mut answer| answer.get_mut("data").map(Value::take))
            .filter(Value::is_object)
            .ok_or_else(|| {
                MirrorFailure::unavailable(format!(
                    "Linear answered {operation} without a JSON `data` object."
                ))
            })
    }

    /// The issue `identifier` names, read once per target.
    fn issue(&self, identifier: &str) -> Result<Issue, MirrorFailure> {
        if let Some(issue) = self
            .issue
            .borrow()
            .as_ref()
            .filter(|issue| issue.identifier == identifier)
        {
            return Ok(issue.clone());
        }
        let issue = self.read(identifier, "identifier")?;
        *self.issue.borrow_mut() = Some(issue.clone());
        Ok(issue)
    }

    /// The issue whose Linear id is `issue_id`, read fresh with one `IssueState`
    /// (REQ-LOCALRUN-208); a null or different issue is `mirror_issue_unavailable`.
    pub(in super::super) fn observe(
        &self,
        issue_id: &str,
    ) -> Result<IssueObservation, MirrorFailure> {
        let issue = self.read(issue_id, "id")?;
        Ok(IssueObservation {
            team: issue.team,
            state: issue.state_id,
            state_name: issue.state,
            updated_at: issue.updated_at,
        })
    }

    /// One `IssueState` for `requested`, which the answer's `field` must repeat.
    fn read(&self, requested: &str, field: &str) -> Result<Issue, MirrorFailure> {
        let data = self.post(ISSUE_STATE, json!({"id": requested}))?;
        let issue = match data.get("issue") {
            None | Some(Value::Null) => {
                return Err(MirrorFailure::issue_unavailable(format!(
                    "Linear has no issue {requested} the key can read."
                )));
            }
            Some(issue) => issue,
        };
        let text = |value: Option<&Value>| value.and_then(Value::as_str).map(str::to_owned);
        if text(issue.get(field)).as_deref() != Some(requested) {
            return Err(MirrorFailure::issue_unavailable(format!(
                "Linear answered another issue than {requested}."
            )));
        }
        let states = issue
            .pointer("/team/states/nodes")
            .and_then(Value::as_array)
            .map(|nodes| {
                nodes
                    .iter()
                    .filter_map(|node| Some((text(node.get("id"))?, text(node.get("name"))?)))
                    .collect::<Vec<_>>()
            });
        let (Some(id), Some(state), Some(state_id), Some(states)) = (
            text(issue.get("id")),
            text(issue.pointer("/state/name")),
            text(issue.pointer("/state/id")),
            states,
        ) else {
            return Err(MirrorFailure::unavailable(format!(
                "Linear's document for {requested} has no id, state name, state id or team states."
            )));
        };
        Ok(Issue {
            identifier: text(issue.get("identifier")).unwrap_or_default(),
            id,
            state,
            state_id,
            team: text(issue.pointer("/team/id")),
            updated_at: text(issue.get("updatedAt")),
            states,
        })
    }

    /// Moves the issue whose Linear id is `issue_id` to the state whose id is `state_id`.
    fn update_state(&self, issue_id: &str, state_id: &str) -> Result<(), MirrorFailure> {
        let data = self.post(
            UPDATE_STATE,
            json!({"id": issue_id, "input": {"stateId": state_id}}),
        )?;
        Self::confirmed(&data, "issueUpdate")
    }

    /// Requires `data.<field>.success` to be true.
    fn confirmed(data: &Value, field: &str) -> Result<(), MirrorFailure> {
        match data.get(field).and_then(|payload| payload.get("success")) {
            Some(Value::Bool(true)) => Ok(()),
            Some(Value::Bool(false)) => Err(MirrorFailure::write_rejected(format!(
                "Linear answered {field} with success false."
            ))),
            _ => Err(MirrorFailure::unavailable(format!(
                "Linear's answer to {field} has no boolean success."
            ))),
        }
    }
}

/// The first GraphQL error's message, bounded, when the answer carries `errors`.
fn error_message(answer: &Value) -> Option<String> {
    let errors = answer
        .get("errors")?
        .as_array()
        .filter(|errors| !errors.is_empty())?;
    let message = errors[0]
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("an error without a message");
    Some(message.chars().take(MAXIMUM_MESSAGE_CHARACTERS).collect())
}

impl MirrorTarget for LinearTarget {
    fn issue_state(&self, issue: &str) -> Result<String, MirrorFailure> {
        self.issue(issue).map(|issue| issue.state)
    }

    fn issue_state_id(&self, issue: &str) -> Result<String, MirrorFailure> {
        self.issue(issue).map(|issue| issue.state_id)
    }

    fn comment(&self, issue: &str, body: &str) -> Result<(), MirrorFailure> {
        // `CommentCreateInput.issueId` is keyed by the issue's id, as every write is; the
        // identifier is only ever sent to `issue(id:)`.
        let read = self.issue(issue)?;
        let data = self.post(
            CREATE_COMMENT,
            json!({"input": {"issueId": read.id, "body": body}}),
        )?;
        Self::confirmed(&data, "commentCreate")
    }

    fn set_state(&self, issue: &str, state: &str) -> Result<(), MirrorFailure> {
        let read = self.issue(issue)?;
        let state_id = read
            .states
            .iter()
            .find(|(_, name)| name == state)
            .map(|(id, _)| id.clone())
            .ok_or_else(|| {
                MirrorFailure::state_unknown(format!(
                    "The team of {issue} has no state named {state:?}."
                ))
            })?;
        self.update_state(&read.id, &state_id)
    }

    /// The resolved id is sent as it stands: the team's states are not consulted, so a state
    /// renamed since Cloud resolved it is still found (REQ-WFDEF-32).
    fn set_state_id(&self, issue: &str, state_id: &str) -> Result<(), MirrorFailure> {
        let read = self.issue(issue)?;
        self.update_state(&read.id, state_id)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;

    const BASE: &str = "http://linear.test";
    const KEY: &str = "lin_api_secret";

    /// What the requester saw: each request's URL, authorization and decoded body.
    type Seen = Rc<RefCell<Vec<(String, String, Value)>>>;

    fn issue(state: &str) -> Value {
        json!({"data": {"issue": {
            "id": "uuid-143",
            "identifier": "AUTO-143",
            "state": {"id": format!("id-{state}"), "name": state},
            "team": {"states": {"nodes": [
                {"id": "id-Engineering", "name": "Engineering"},
                {"id": "id-Backlog", "name": "Backlog"},
                {"id": "id-Done", "name": "Done"},
            ]}},
        }}})
    }

    /// A target whose requester answers each operation from `answers` by its name.
    fn linear(key: Option<&str>, answers: Vec<(&'static str, u16, Value)>) -> (LinearTarget, Seen) {
        let seen = Seen::default();
        let recorded = Rc::clone(&seen);
        let target = LinearTarget::new(
            BASE,
            key.map(str::to_owned),
            Box::new(move |request| {
                let body: Value = serde_json::from_slice(&request.body).unwrap();
                recorded.borrow_mut().push((
                    request.url.to_owned(),
                    request.authorization.to_owned(),
                    body.clone(),
                ));
                let (_, status, answer) = answers
                    .iter()
                    .find(|(operation, _, _)| body["operationName"] == *operation)
                    .unwrap_or_else(|| panic!("unexpected request {body}"));
                Ok(Response {
                    status: *status,
                    body: serde_json::to_vec(answer).unwrap(),
                })
            }),
        );
        (target, seen)
    }

    fn operations(seen: &Seen) -> Vec<String> {
        seen.borrow()
            .iter()
            .map(|(_, _, body)| body["operationName"].as_str().unwrap().to_owned())
            .collect()
    }

    // REQ-LOCALRUN-192: the issue is read by its identifier with the bare key.
    #[test]
    fn issue_state_reads_the_issue_by_its_identifier_with_the_bare_key() {
        let (target, seen) = linear(Some(KEY), vec![("IssueState", 200, issue("Engineering"))]);
        assert_eq!(target.issue_state("AUTO-143").unwrap(), "Engineering");
        let seen = seen.borrow();
        assert_eq!(seen.len(), 1);
        let (url, authorization, body) = &seen[0];
        assert_eq!(url, "http://linear.test/graphql");
        assert_eq!(authorization, KEY);
        assert_eq!(body["query"], ISSUE_STATE.1);
        assert_eq!(body["variables"], json!({"id": "AUTO-143"}));
    }

    // REQ-LOCALRUN-192: an issue Linear does not answer, or another one, is unavailable.
    #[test]
    fn a_missing_or_other_issue_is_issue_unavailable() {
        let (target, _) = linear(
            Some(KEY),
            vec![("IssueState", 200, json!({"data": {"issue": null}}))],
        );
        let failure = target.issue_state("AUTO-143").unwrap_err();
        assert_eq!(failure.code, MirrorFailure::ISSUE_UNAVAILABLE);

        let (target, _) = linear(Some(KEY), vec![("IssueState", 200, issue("Engineering"))]);
        let failure = target.issue_state("AUTO-144").unwrap_err();
        assert_eq!(failure.code, MirrorFailure::ISSUE_UNAVAILABLE);

        // A comment on an issue the key cannot read posts nothing.
        let (target, seen) = linear(
            Some(KEY),
            vec![("IssueState", 200, json!({"data": {"issue": null}}))],
        );
        let failure = target.comment("AUTO-143", "x").unwrap_err();
        assert_eq!(failure.code, MirrorFailure::ISSUE_UNAVAILABLE);
        assert_eq!(operations(&seen), ["IssueState"]);
    }

    // REQ-LOCALRUN-192: a state write moves the issue to the team state of the bound name,
    // reading the issue once even after the guard read it.
    #[test]
    fn set_state_updates_the_issue_to_the_team_state_of_that_name() {
        let success = json!({"data": {"issueUpdate": {"success": true}}});
        let (target, seen) = linear(
            Some(KEY),
            vec![
                ("IssueState", 200, issue("Engineering")),
                ("UpdateIssueState", 200, success.clone()),
            ],
        );
        target.set_state("AUTO-143", "Done").unwrap();
        assert_eq!(operations(&seen), ["IssueState", "UpdateIssueState"]);
        let update = seen.borrow()[1].2.clone();
        assert_eq!(update["query"], UPDATE_STATE.1);
        assert_eq!(
            update["variables"],
            json!({"id": "uuid-143", "input": {"stateId": "id-Done"}})
        );

        let (target, seen) = linear(
            Some(KEY),
            vec![
                ("IssueState", 200, issue("Engineering")),
                ("UpdateIssueState", 200, success),
            ],
        );
        assert_eq!(target.issue_state("AUTO-143").unwrap(), "Engineering");
        target.set_state("AUTO-143", "Backlog").unwrap();
        assert_eq!(operations(&seen), ["IssueState", "UpdateIssueState"]);

        let (target, seen) = linear(Some(KEY), vec![("IssueState", 200, issue("Engineering"))]);
        let failure = target.set_state("AUTO-143", "Canceled").unwrap_err();
        assert_eq!(failure.code, MirrorFailure::STATE_UNKNOWN);
        assert_eq!(operations(&seen), ["IssueState"]);
    }

    // REQ-LOCALRUN-192, REQ-WFDEF-32: by id, the issue's state id is the one Linear answers
    // and a state write sends the given id without matching any team state's name.
    #[test]
    fn by_id_the_state_is_read_and_written_by_its_id() {
        let success = json!({"data": {"issueUpdate": {"success": true}}});
        let (target, seen) = linear(
            Some(KEY),
            vec![
                ("IssueState", 200, issue("Engineering")),
                ("UpdateIssueState", 200, success),
            ],
        );
        assert_eq!(target.issue_state_id("AUTO-143").unwrap(), "id-Engineering");
        target.set_state_id("AUTO-143", "id-Shipped").unwrap();
        assert_eq!(operations(&seen), ["IssueState", "UpdateIssueState"]);
        assert_eq!(
            seen.borrow()[1].2["variables"],
            json!({"id": "uuid-143", "input": {"stateId": "id-Shipped"}})
        );
    }

    // REQ-LOCALRUN-208: `observe` reads the issue by its Linear id, answering its team,
    // state and `updatedAt`, and an answer for another id is `mirror_issue_unavailable`.
    #[test]
    fn observe_reads_the_issue_by_its_linear_id() {
        let mut answer = issue("Canceled");
        answer["data"]["issue"]["team"]["id"] = json!("team-auto");
        answer["data"]["issue"]["updatedAt"] = json!("2026-11-10T12:00:00.000Z");
        let (target, seen) = linear(Some(KEY), vec![("IssueState", 200, answer)]);
        let observed = target.observe("uuid-143").unwrap();
        assert_eq!(observed.team.as_deref(), Some("team-auto"));
        assert_eq!(observed.state, "id-Canceled");
        assert_eq!(observed.state_name, "Canceled");
        assert_eq!(
            observed.updated_at.as_deref(),
            Some("2026-11-10T12:00:00.000Z")
        );
        assert_eq!(seen.borrow()[0].2["variables"], json!({"id": "uuid-143"}));

        let (target, _) = linear(Some(KEY), vec![("IssueState", 200, issue("Canceled"))]);
        let failure = target.observe("uuid-144").err().unwrap();
        assert_eq!(failure.code, MirrorFailure::ISSUE_UNAVAILABLE);
    }

    // REQ-LOCALRUN-192: a comment reads the issue once for its id and is one `commentCreate`
    // keyed by that id, never the identifier, with the body; an earlier read serves it.
    #[test]
    fn comment_reads_the_issue_once_and_creates_one_comment_keyed_by_its_id() {
        let success = json!({"data": {"commentCreate": {"success": true}}});
        let (target, seen) = linear(
            Some(KEY),
            vec![
                ("IssueState", 200, issue("Engineering")),
                ("CreateIssueComment", 200, success.clone()),
            ],
        );
        target.comment("AUTO-143", "design: planned").unwrap();
        assert_eq!(operations(&seen), ["IssueState", "CreateIssueComment"]);
        let create = seen.borrow()[1].2.clone();
        assert_eq!(create["query"], CREATE_COMMENT.1);
        assert_eq!(
            create["variables"],
            json!({"input": {"issueId": "uuid-143", "body": "design: planned"}})
        );

        let (target, seen) = linear(
            Some(KEY),
            vec![
                ("IssueState", 200, issue("Engineering")),
                ("CreateIssueComment", 200, success),
            ],
        );
        assert_eq!(target.issue_state("AUTO-143").unwrap(), "Engineering");
        target.comment("AUTO-143", "x").unwrap();
        target.comment("AUTO-143", "y").unwrap();
        assert_eq!(
            operations(&seen),
            ["IssueState", "CreateIssueComment", "CreateIssueComment"]
        );
    }

    // REQ-LOCALRUN-192: Linear's refusal is `mirror_write_rejected` with its message and
    // never the key; an answer that cannot be read is `mirror_unavailable`.
    #[test]
    fn refusals_and_unreadable_answers_are_classified() {
        let refused = json!({"errors": [{"message": "Entity not found: Issue"}]});
        for (status, answer, cause) in [
            (400, refused.clone(), "Entity not found: Issue"),
            (200, refused.clone(), "Entity not found: Issue"),
            (500, refused, "Entity not found: Issue"),
            (401, json!({}), "HTTP 401"),
            (
                200,
                json!({"data": {"commentCreate": {"success": false}}}),
                "success false",
            ),
        ] {
            let (target, _) = linear(
                Some(KEY),
                vec![
                    ("IssueState", 200, issue("Engineering")),
                    ("CreateIssueComment", status, answer),
                ],
            );
            let failure = target.comment("AUTO-143", "x").unwrap_err();
            assert_eq!(failure.code, MirrorFailure::WRITE_REJECTED, "{failure}");
            assert!(failure.diagnostic.contains(cause), "{failure}");
            assert!(!failure.diagnostic.contains(KEY), "{failure}");
        }

        for (status, answer) in [
            (502, json!("bad gateway")),
            (200, json!(["not", "an", "object"])),
            (200, json!({"data": {"commentCreate": {}}})),
        ] {
            let (target, _) = linear(
                Some(KEY),
                vec![
                    ("IssueState", 200, issue("Engineering")),
                    ("CreateIssueComment", status, answer),
                ],
            );
            let failure = target.comment("AUTO-143", "x").unwrap_err();
            assert_eq!(failure.code, MirrorFailure::UNAVAILABLE, "{failure}");
        }
        let unreachable = LinearTarget::new(
            BASE,
            Some(KEY.to_owned()),
            Box::new(|_| Err("connection refused".to_owned())),
        );
        let failure = unreachable.comment("AUTO-143", "x").unwrap_err();
        assert_eq!(failure.code, MirrorFailure::UNAVAILABLE);
        assert!(
            failure.diagnostic.contains("connection refused"),
            "{failure}"
        );
        let not_json = LinearTarget::new(
            BASE,
            Some(KEY.to_owned()),
            Box::new(|_| {
                Ok(Response {
                    status: 200,
                    body: b"<html>".to_vec(),
                })
            }),
        );
        let failure = not_json.comment("AUTO-143", "x").unwrap_err();
        assert_eq!(failure.code, MirrorFailure::UNAVAILABLE);
    }

    // REQ-LOCALRUN-192, REQ-LOCALRUN-204: without a key the check before a write and every
    // write fail alike and nothing is sent; with one the check passes.
    #[test]
    fn an_unset_or_empty_key_fails_before_any_request() {
        for key in [None, Some("")] {
            let (target, seen) = linear(key, Vec::new());
            for failure in [
                target.credentials().unwrap_err(),
                target.comment("AUTO-143", "x").unwrap_err(),
                target.issue_state("AUTO-143").unwrap_err(),
                target.set_state("AUTO-143", "Done").unwrap_err(),
                target.issue_state_id("AUTO-143").unwrap_err(),
                target.set_state_id("AUTO-143", "id-Done").unwrap_err(),
            ] {
                assert_eq!(failure.code, MirrorFailure::CREDENTIALS_UNAVAILABLE);
            }
            assert!(seen.borrow().is_empty());
        }
        assert_eq!(linear(Some("key"), Vec::new()).0.credentials(), Ok(()));
    }
}
