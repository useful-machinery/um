//! `github` / `pull_request.queue_settled` (REQ-WFEXEC-65): whether the pull request an
//! `external` wait's rule names has left the merge queue. The pull request is read from the
//! GitHub REST API with the operator's GitHub token and its queue entry from Mergify's, the
//! repository's merge queue, with `MERGIFY_TOKEN` when the CLI's environment holds one and else
//! with that GitHub token, through `um_api::provider_get`. `UM_GITHUB_API_URL` and
//! `UM_MERGIFY_API_URL` override the two base URLs for development and tests, and admit
//! `http`.

use std::process::Command;
use std::time::Duration;

use serde_json::{Map, Value};
use time::format_description::well_known::Rfc3339;
use um_api::{HttpTransportPolicy, provider_get};
use um_execution::{ConditionError, ExternalEvent, WaitRecord};

pub(super) const SOURCE: &str = "github";
pub(super) const QUEUE_SETTLED: &str = "pull_request.queue_settled";

const GITHUB_API: &str = "https://api.github.com";
const MERGIFY_API: &str = "https://api.mergify.com";
const GITHUB_API_VARIABLE: &str = "UM_GITHUB_API_URL";
const MERGIFY_API_VARIABLE: &str = "UM_MERGIFY_API_URL";
const TOKEN_VARIABLE: &str = "GITHUB_TOKEN";
const MERGIFY_TOKEN_VARIABLE: &str = "MERGIFY_TOKEN";
const PULL_REQUEST_PREFIX: &str = "https://github.com/";
const TIMEOUT: Duration = Duration::from_secs(30);
/// Mergify's error code for a pull request its per-pull-request endpoint does not hold.
const NOT_QUEUED: &str = "PULL_REQUEST_NOT_QUEUED";
const GITHUB_ACCEPT: &str = "application/vnd.github+json";
const MERGIFY_ACCEPT: &str = "application/json";

/// One `GET` the condition makes, with the bearer token it carries.
pub(super) struct Request<'a> {
    pub(super) url: String,
    pub(super) accept: &'static str,
    pub(super) token: &'a str,
}

/// An answer's status and body.
pub(super) struct Response {
    pub(super) status: u16,
    pub(super) body: Vec<u8>,
}

type Requester<'a> = &'a dyn Fn(&Request<'_>) -> Result<Response, String>;

/// The two APIs' base URLs, and whether a development override named either, which admits
/// `http`.
pub(super) struct Endpoints {
    github: String,
    mergify: String,
    overridden: bool,
}

impl Endpoints {
    fn from_environment() -> Self {
        let mut overridden = false;
        let mut base = |variable: &str, default: &str| {
            let url = std::env::var(variable).ok().filter(|url| !url.is_empty());
            overridden |= url.is_some();
            url.unwrap_or_else(|| default.to_owned())
                .trim_end_matches('/')
                .to_owned()
        };
        Self {
            github: base(GITHUB_API_VARIABLE, GITHUB_API),
            mergify: base(MERGIFY_API_VARIABLE, MERGIFY_API),
            overridden,
        }
    }

    fn transport_policy(&self) -> HttpTransportPolicy {
        if self.overridden {
            HttpTransportPolicy::AllowInsecureHttp
        } else {
            HttpTransportPolicy::HttpsOnly
        }
    }
}

/// A pull request as `https://github.com/<owner>/<repository>/pull/<number>` names it.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct PullRequest {
    url: String,
    owner: String,
    repository: String,
    number: u64,
}

impl PullRequest {
    fn parse(url: &str) -> Result<Self, ConditionError> {
        let name = |segment: &str| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
        };
        let parsed = url
            .strip_prefix(PULL_REQUEST_PREFIX)
            .map(|path| path.split('/').collect::<Vec<_>>())
            .and_then(|segments| match segments.as_slice() {
                [owner, repository, "pull", number]
                    if name(owner)
                        && name(repository)
                        && number.bytes().all(|byte| byte.is_ascii_digit()) =>
                {
                    let number = number.parse::<u64>().ok().filter(|number| *number > 0)?;
                    Some(Self {
                        url: url.to_owned(),
                        owner: (*owner).to_owned(),
                        repository: (*repository).to_owned(),
                        number,
                    })
                }
                _ => None,
            });
        parsed.ok_or_else(|| {
            ConditionError::unavailable(format!(
                "{url:?} is not a GitHub pull request URL (https://github.com/OWNER/REPOSITORY/pull/NUMBER)."
            ))
        })
    }

    /// The pull request the wait's rule names in its `event.pullRequest == "<url>"` conjunct.
    fn of(wait: &WaitRecord) -> Result<Self, ConditionError> {
        let url = um_execution::event_literal(&wait.rule.where_, &["pullRequest"])
            .ok()
            .flatten()
            .ok_or_else(|| {
                ConditionError::unavailable(
                    "The wait's rule names no pull request: its `where` has no `event.pullRequest == \"<url>\"` conjunct.",
                )
            })?;
        Self::parse(&url)
    }
}

/// Observes the queue of the pull request the wait's rule names, with the operator's tokens
/// and the configured APIs.
pub(super) fn observe_queue_settled(
    wait: &WaitRecord,
) -> Result<Vec<ExternalEvent>, ConditionError> {
    let pull_request = PullRequest::of(wait)?;
    let token = token()?;
    let mergify = mergify_token(std::env::var(MERGIFY_TOKEN_VARIABLE).ok())?;
    let endpoints = Endpoints::from_environment();
    let policy = endpoints.transport_policy();
    settle(
        &pull_request,
        &endpoints,
        &token,
        mergify.as_deref(),
        &|request| {
            provider_get(&request.url, request.accept, request.token, policy, TIMEOUT)
                .map(|response| Response {
                    status: response.status,
                    body: response.body,
                })
                .map_err(|error| error.to_string())
        },
    )
}

/// `GITHUB_TOKEN` when set and non-empty, else the trimmed output of `gh auth token`.
fn token() -> Result<String, ConditionError> {
    if let Some(token) = std::env::var(TOKEN_VARIABLE)
        .ok()
        .filter(|token| !token.is_empty())
    {
        return Ok(token);
    }
    let output = Command::new("gh")
        .args(["auth", "token"])
        .output()
        .map_err(|error| {
            ConditionError::unavailable(format!(
                "No GitHub token: {TOKEN_VARIABLE} is unset and `gh auth token` could not run: {error}."
            ))
        })?;
    let token = String::from_utf8(output.stdout)
        .ok()
        .map(|token| token.trim().to_owned())
        .filter(|token| {
            output.status.success() && !token.is_empty() && !token.contains(char::is_whitespace)
        });
    token.ok_or_else(|| {
        ConditionError::unavailable(format!(
            "No GitHub token: {TOKEN_VARIABLE} is unset and `gh auth token` answered none; run `gh auth login` or set {TOKEN_VARIABLE}."
        ))
    })
}

/// `MERGIFY_TOKEN`'s trimmed value when it is set and not blank, else none, the GitHub token
/// then serving Mergify too. A value with whitespace inside is refused rather than replaced,
/// so a run's Mergify reads keep one identity.
fn mergify_token(value: Option<String>) -> Result<Option<String>, ConditionError> {
    let Some(token) = value
        .map(|value| value.trim().to_owned())
        .filter(|token| !token.is_empty())
    else {
        return Ok(None);
    };
    if token.contains(char::is_whitespace) {
        return Err(ConditionError::unavailable(format!(
            "{MERGIFY_TOKEN_VARIABLE} holds whitespace inside its value; set it to one Mergify application key or unset it."
        )));
    }
    Ok(Some(token))
}

/// The pull request's queue state: `merged` when GitHub reports it merged, `dequeued` when it
/// is not and Mergify answers that its queue does not hold it, no event while Mergify holds
/// it, and an error for any other answer. GitHub is read with `token` and Mergify with
/// `mergify`, else with `token`.
fn settle(
    pull_request: &PullRequest,
    endpoints: &Endpoints,
    token: &str,
    mergify: Option<&str>,
    requester: Requester<'_>,
) -> Result<Vec<ExternalEvent>, ConditionError> {
    let PullRequest {
        owner,
        repository,
        number,
        ..
    } = pull_request;
    let get = |service: &str, url: String, accept, token| {
        requester(&Request { url, accept, token }).map_err(|error| {
            ConditionError::unavailable(format!(
                "{service} could not be read for {}: {error}.",
                pull_request.url
            ))
        })
    };
    let object = |service: &str, body: &[u8]| {
        serde_json::from_slice::<Value>(body)
            .ok()
            .and_then(|value| match value {
                Value::Object(object) => Some(object),
                _ => None,
            })
            .ok_or_else(|| {
                ConditionError::unavailable(format!(
                    "{service} answered {} with a body that is not a JSON object.",
                    pull_request.url
                ))
            })
    };
    let refused = |service: &str, status: u16| {
        ConditionError::unavailable(format!(
            "{service} answered HTTP {status} for {}.",
            pull_request.url
        ))
    };

    let github = get(
        "GitHub",
        format!(
            "{}/repos/{owner}/{repository}/pulls/{number}",
            endpoints.github
        ),
        GITHUB_ACCEPT,
        token,
    )?;
    if github.status != 200 {
        return Err(refused("GitHub", github.status));
    }
    let document = object("GitHub", &github.body)?;
    let head_sha = document
        .get("head")
        .and_then(|head| head.get("sha"))
        .and_then(Value::as_str);
    let (Some(merged), Some(head_sha)) =
        (document.get("merged").and_then(Value::as_bool), head_sha)
    else {
        return Err(ConditionError::unavailable(format!(
            "GitHub's document for {} has no boolean `merged` or no `head.sha`.",
            pull_request.url
        )));
    };
    if merged {
        return Ok(vec![event(pull_request, "merged", head_sha)]);
    }

    let queue = get(
        "Mergify",
        format!(
            "{}/v1/repos/{owner}/{repository}/merge-queue/pull/{number}",
            endpoints.mergify
        ),
        MERGIFY_ACCEPT,
        mergify.unwrap_or(token),
    )?;
    match queue.status {
        200 => {
            object("Mergify", &queue.body)?;
            Ok(Vec::new())
        }
        404 if object("Mergify", &queue.body).is_ok_and(|body| {
            body.get("error_code").and_then(Value::as_str) == Some(NOT_QUEUED)
        }) =>
        {
            Ok(vec![event(pull_request, "dequeued", head_sha)])
        }
        status @ (401 | 403) => {
            let credential = if mergify.is_some() {
                MERGIFY_TOKEN_VARIABLE.to_owned()
            } else {
                format!("the GitHub token ({MERGIFY_TOKEN_VARIABLE} is unset)")
            };
            Err(ConditionError::unavailable(format!(
                "Mergify answered HTTP {status} for {} to {credential}: it refused the credential or the credential lacks permission for the repository; set {MERGIFY_TOKEN_VARIABLE} to a Mergify application key that can read it.",
                pull_request.url
            )))
        }
        status => Err(refused("Mergify", status)),
    }
}

fn event(pull_request: &PullRequest, state: &str, head_sha: &str) -> ExternalEvent {
    let mut payload = Map::new();
    payload.insert(
        "pullRequest".to_owned(),
        Value::String(pull_request.url.clone()),
    );
    payload.insert("state".to_owned(), Value::String(state.to_owned()));
    payload.insert("headSha".to_owned(), Value::String(head_sha.to_owned()));
    ExternalEvent {
        source: SOURCE.to_owned(),
        event: QUEUE_SETTLED.to_owned(),
        at: um_support::utc_now().format(&Rfc3339).unwrap_or_default(),
        payload,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use serde_json::json;
    use um_execution::{WaitKind, WaitRule};

    use super::*;

    const URL: &str = "https://github.com/useful-machinery/platform/pull/3958";
    const PULL: &str = "http://github.test/repos/useful-machinery/platform/pulls/3958";
    const QUEUE: &str =
        "http://mergify.test/v1/repos/useful-machinery/platform/merge-queue/pull/3958";

    fn endpoints() -> Endpoints {
        Endpoints {
            github: "http://github.test".to_owned(),
            mergify: "http://mergify.test".to_owned(),
            overridden: true,
        }
    }

    fn pull_request() -> PullRequest {
        PullRequest::parse(URL).unwrap()
    }

    fn wait(where_: &str) -> WaitRecord {
        WaitRecord {
            kind: WaitKind::External,
            since: "2026-11-07T21:41:58Z".to_owned(),
            step: vec![json!("merge")],
            then: "merge".to_owned(),
            on_hand_back: None,
            rule: WaitRule {
                source: SOURCE.to_owned(),
                event: vec![QUEUE_SETTLED.to_owned()],
                where_: where_.to_owned(),
            },
            payload: Map::new(),
        }
    }

    /// A settled result and what was requested, in order.
    type Settled<T> = (Result<Vec<ExternalEvent>, ConditionError>, Vec<T>);

    /// Settles the pull request against scripted answers by URL with the GitHub token `test`
    /// and `mergify` as the Mergify token, returning the result and each request's URL and
    /// token in order.
    fn settled_with(
        answers: &[(&str, u16, Value)],
        mergify: Option<&str>,
    ) -> Settled<(String, String)> {
        let requested = RefCell::new(Vec::new());
        let result = settle(&pull_request(), &endpoints(), "test", mergify, &|request| {
            requested
                .borrow_mut()
                .push((request.url.clone(), request.token.to_owned()));
            let (_, status, body) = answers
                .iter()
                .find(|(url, _, _)| *url == request.url)
                .unwrap_or_else(|| panic!("unexpected request {}", request.url));
            Ok(Response {
                status: *status,
                body: serde_json::to_vec(body).unwrap(),
            })
        });
        (result, requested.into_inner())
    }

    /// `settled_with` without a Mergify token, returning the URLs requested in order.
    fn settled(answers: &[(&str, u16, Value)]) -> Settled<String> {
        let (result, requested) = settled_with(answers, None);
        (result, requested.into_iter().map(|(url, _)| url).collect())
    }

    fn pull(merged: bool) -> (&'static str, u16, Value) {
        (
            PULL,
            200,
            json!({"merged": merged, "state": "open", "head": {"sha": "4f2a"}}),
        )
    }

    fn states(events: &[ExternalEvent]) -> Vec<(&str, &str, Value)> {
        events
            .iter()
            .map(|event| {
                (
                    event.source.as_str(),
                    event.event.as_str(),
                    Value::Object(event.payload.clone()),
                )
            })
            .collect()
    }

    // REQ-CONTINUE-74, REQ-WFEXEC-65
    #[test]
    fn a_merged_pull_request_settles_without_asking_the_queue() {
        let (result, requested) = settled(&[pull(true)]);
        assert_eq!(
            states(&result.unwrap()),
            [(
                SOURCE,
                QUEUE_SETTLED,
                json!({"pullRequest": URL, "state": "merged", "headSha": "4f2a"})
            )]
        );
        assert_eq!(requested, [PULL]);
    }

    // REQ-CONTINUE-74, REQ-WFEXEC-65
    #[test]
    fn an_open_pull_request_is_queued_or_dequeued_as_mergify_answers() {
        let (result, requested) = settled(&[pull(false), (QUEUE, 200, json!({"number": 3958}))]);
        assert_eq!(result.unwrap(), []);
        assert_eq!(requested, [PULL, QUEUE]);

        let (result, _) = settled(&[
            pull(false),
            (QUEUE, 404, json!({"error_code": "PULL_REQUEST_NOT_QUEUED"})),
        ]);
        assert_eq!(
            states(&result.unwrap()),
            [(
                SOURCE,
                QUEUE_SETTLED,
                json!({"pullRequest": URL, "state": "dequeued", "headSha": "4f2a"})
            )]
        );
    }

    // REQ-CONTINUE-74: an answer the condition cannot read is an error, never a held wait.
    #[test]
    fn an_unreadable_answer_is_unavailable() {
        for (answers, cause) in [
            (vec![(PULL, 401, json!({}))], "401"),
            (vec![pull(false), (QUEUE, 401, json!({}))], "401"),
            (
                vec![
                    pull(false),
                    (QUEUE, 404, json!({"error_code": "NOT_FOUND"})),
                ],
                "404",
            ),
            (vec![(PULL, 200, json!(["merged"]))], "JSON object"),
            (vec![(PULL, 200, json!({"state": "open"}))], "merged"),
        ] {
            let error = settled(&answers).0.unwrap_err();
            assert_eq!(error.code, ConditionError::UNAVAILABLE, "{answers:?}");
            assert!(error.message.contains(cause), "{error}");
        }
        let error = settle(&pull_request(), &endpoints(), "test", None, &|_| {
            Err("connection refused".to_owned())
        })
        .unwrap_err();
        assert_eq!(error.code, ConditionError::UNAVAILABLE);
        assert!(error.message.contains("connection refused"), "{error}");
    }

    // REQ-WFEXEC-65: GitHub is read with the GitHub token and Mergify with `MERGIFY_TOKEN`
    // when the CLI holds one, else with the GitHub token.
    #[test]
    fn mergify_is_read_with_its_own_token_when_one_is_held() {
        let answers = [pull(false), (QUEUE, 200, json!({"number": 3958}))];
        for (mergify, sent) in [(Some("mergify-key"), "mergify-key"), (None, "test")] {
            let (result, requested) = settled_with(&answers, mergify);
            assert_eq!(result.unwrap(), []);
            assert_eq!(
                requested,
                [
                    (PULL.to_owned(), "test".to_owned()),
                    (QUEUE.to_owned(), sent.to_owned())
                ]
            );
        }
    }

    // REQ-WFEXEC-65, REQ-CONTINUE-74: Mergify refusing the credential is unavailable, never an
    // event, and the error names which credential was sent without its value.
    #[test]
    fn a_refused_mergify_credential_is_unavailable_and_named() {
        for status in [401, 403] {
            let answers = [pull(false), (QUEUE, status, json!({}))];
            for (mergify, named) in [
                (Some("mergify-key"), MERGIFY_TOKEN_VARIABLE),
                (None, "GitHub token"),
            ] {
                let error = settled_with(&answers, mergify).0.unwrap_err();
                assert_eq!(error.code, ConditionError::UNAVAILABLE);
                assert!(error.message.contains(&status.to_string()), "{error}");
                assert!(error.message.contains(named), "{error}");
                assert!(!error.message.contains("mergify-key"), "{error}");
                assert!(!error.message.contains("test"), "{error}");
            }
        }
    }

    // REQ-WFEXEC-65: an unset or blank `MERGIFY_TOKEN` falls back to the GitHub token; one with
    // whitespace inside is refused rather than replaced.
    #[test]
    fn the_mergify_token_is_a_held_non_empty_value() {
        for unset in [None, Some(""), Some("  \n")] {
            assert_eq!(
                mergify_token(unset.map(str::to_owned)).unwrap(),
                None,
                "{unset:?}"
            );
        }
        assert_eq!(
            mergify_token(Some(" mergify-key\n".to_owned())).unwrap(),
            Some("mergify-key".to_owned())
        );
        let error = mergify_token(Some("mergify key".to_owned())).unwrap_err();
        assert_eq!(error.code, ConditionError::UNAVAILABLE);
        assert!(error.message.contains(MERGIFY_TOKEN_VARIABLE), "{error}");
        assert!(!error.message.contains("mergify key"), "{error}");
    }

    // REQ-WFEXEC-65: the pull request comes from the rule's `event.pullRequest` conjunct.
    #[test]
    fn the_pull_request_is_the_rule_s_quoted_github_url() {
        assert_eq!(
            PullRequest::of(&wait(&format!(
                "event.state == \"merged\" and event.pullRequest == \"{URL}\""
            )))
            .unwrap(),
            PullRequest {
                url: URL.to_owned(),
                owner: "useful-machinery".to_owned(),
                repository: "platform".to_owned(),
                number: 3958,
            }
        );
        for where_ in [
            "event.state == \"merged\"",
            "event.pullRequest == wait.then",
            "event.pullRequest != \"x\"",
        ] {
            let error = PullRequest::of(&wait(where_)).unwrap_err();
            assert_eq!(error.code, ConditionError::UNAVAILABLE, "{where_}");
            assert!(error.message.contains("event.pullRequest"), "{error}");
        }
        for url in [
            "https://gitlab.com/useful-machinery/platform/pull/3958",
            "https://github.com/useful-machinery/platform/issues/3958",
            "https://github.com/useful-machinery/platform/pull/3958/files",
            "https://github.com/useful-machinery/platform/pull/0",
            "https://github.com/useful-machinery/platform/pull/x",
        ] {
            assert_eq!(
                PullRequest::parse(url).unwrap_err().code,
                ConditionError::UNAVAILABLE,
                "{url}"
            );
        }
    }
}
