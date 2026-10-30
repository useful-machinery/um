//! Validate the closed request grammar before handing a typed request to the generated transport.
//! Generated untagged unions accept extra properties and may default missing fields.
use std::path::Path;

use anyhow::{Result, anyhow};
use serde_json::{Map, Value};
use um_api::{CreateLinearTriggerRequest, UpdateLinearTriggerRequest};

use super::super::resource;

fn object<'a>(
    value: &'a Value,
    required: &[&str],
    allowed: &[&str],
) -> Result<&'a Map<String, Value>> {
    let map = value
        .as_object()
        .ok_or_else(|| anyhow!("expected an object"))?;
    if map.keys().any(|key| !allowed.contains(&key.as_str()))
        || required.iter().any(|key| !map.contains_key(*key))
        || map.iter().any(|(key, v)| v.is_null() && key != "value")
    {
        return Err(anyhow!("unknown, missing, or null configuration property"));
    }
    Ok(map)
}

fn literal_or_field(value: &Value, snapshot: bool, text: bool) -> Result<()> {
    let map = value
        .as_object()
        .ok_or_else(|| anyhow!("invalid mapping source"))?;
    let kind = map
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing mapping type"))?;
    match kind {
        "literal" if !snapshot => {
            object(value, &["type", "value"], &["type", "value"])?;
            if text && !map["value"].is_string() {
                return Err(anyhow!("text literal must be a string"));
            }
        }
        "field" if !snapshot => {
            object(value, &["type", "field"], &["type", "field"])?;
            if !map["field"].is_string() {
                return Err(anyhow!("invalid mapping field"));
            }
        }
        "snapshot" if snapshot => {
            object(value, &["type"], &["type"])?;
        }
        _ => return Err(anyhow!("invalid mapping source for kind")),
    }
    Ok(())
}

/// Advisory copy of the shared lifecycle catalog; the Gateway owns it and answers 400 on drift.
const LIFECYCLE_EVENT_TYPES: [&str; 19] = [
    "run.queued",
    "run.started",
    "run.succeeded",
    "run.failed",
    "run.cancelled",
    "run.interrupted",
    "run.rejected",
    "step.started",
    "step.succeeded",
    "step.failed",
    "step.skipped",
    "step.cancelled",
    "step.blocked",
    "step.not_run",
    "publication.published",
    "publication.no_changes",
    "publication.already_merged",
    "publication.failed",
    "publication.unavailable",
];

fn nonempty_string(value: &Value) -> bool {
    value.as_str().is_some_and(|s| !s.is_empty())
}

/// Mirrors REQ-TRIGGER-104 except the `publication.*` rule, which the Gateway checks against the
/// merged candidate because a PATCH may omit `publication`.
fn lifecycle(choice: &Value) -> Result<()> {
    let fields = object(choice, &["enabled"], &["enabled", "teamMappings"])?;
    match fields["enabled"].as_bool() {
        Some(false) if fields.len() == 1 => return Ok(()),
        Some(true) => {}
        _ => return Err(anyhow!("invalid Linear lifecycle policy")),
    }
    let teams = fields
        .get("teamMappings")
        .and_then(Value::as_array)
        .filter(|teams| !teams.is_empty())
        .ok_or_else(|| anyhow!("an enabled Linear lifecycle requires team mappings"))?;
    let mut team_ids = std::collections::HashSet::new();
    for team in teams {
        let mapping = object(team, &["teamId", "rules"], &["teamId", "rules"])?;
        if !nonempty_string(&mapping["teamId"]) || !team_ids.insert(&mapping["teamId"]) {
            return Err(anyhow!(
                "Linear lifecycle team IDs must be nonempty and unique"
            ));
        }
        let rules = mapping["rules"]
            .as_array()
            .filter(|rules| !rules.is_empty())
            .ok_or_else(|| anyhow!("a Linear lifecycle team mapping requires rules"))?;
        // Each (eventType, node) pair once; node `None` is the all-node rule.
        let mut selected: Vec<(&Value, Option<&Value>, &Value)> = Vec::new();
        for rule in rules {
            let fields = object(
                rule,
                &["eventType", "stateId"],
                &["eventType", "node", "stateId"],
            )?;
            let event = fields["eventType"].as_str().unwrap_or("");
            if !LIFECYCLE_EVENT_TYPES.contains(&event) || !nonempty_string(&fields["stateId"]) {
                return Err(anyhow!("invalid Linear lifecycle rule"));
            }
            let node = fields.get("node");
            if let Some(node) = node {
                let selector = object(node, &["scope", "role", "id"], &["scope", "role", "id"])?;
                if !event.starts_with("step.")
                    || !selector["scope"]
                        .as_array()
                        .is_some_and(|scope| scope.iter().all(Value::is_string))
                    || !["step", "finalizer"].contains(&selector["role"].as_str().unwrap_or(""))
                    || !selector["id"].is_string()
                {
                    return Err(anyhow!("invalid Linear lifecycle node selector"));
                }
            }
            let state = &fields["stateId"];
            let conflict = selected.iter().any(|(other, other_node, other_state)| {
                *other == &fields["eventType"]
                    && (*other_node == node
                        || (other_node.is_none() != node.is_none() && *other_state != state))
            });
            if conflict {
                return Err(anyhow!("conflicting Linear lifecycle rules"));
            }
            selected.push((&fields["eventType"], node, state));
        }
    }
    Ok(())
}

fn validate(value: &Value, update: bool) -> Result<()> {
    let allowed = if update {
        &[
            "source",
            "target",
            "conditions",
            "inputs",
            "integrationContext",
            "publication",
            "linearLifecycle",
        ][..]
    } else {
        &[
            "enabled",
            "source",
            "target",
            "conditions",
            "inputs",
            "integrationContext",
            "publication",
            "linearLifecycle",
        ][..]
    };
    let required = if update { &[][..] } else { &allowed[..6] };
    let map = object(value, required, allowed)?;
    if update && map.is_empty() {
        return Err(anyhow!("update requires a configuration field"));
    }
    if map
        .get("enabled")
        .is_some_and(|enabled| !enabled.is_boolean())
    {
        return Err(anyhow!("enabled must be a boolean"));
    }
    if let Some(source) = map.get("source") {
        let fields = object(
            source,
            &["type", "connectionId"],
            &["type", "connectionId", "includeSelfEvents"],
        )?;
        if fields["type"] != "linear"
            || !fields["connectionId"].is_string()
            || fields
                .get("includeSelfEvents")
                .is_some_and(|v| !v.is_boolean())
        {
            return Err(anyhow!("invalid trigger source"));
        }
    }
    if let Some(target) = map.get("target") {
        let fields = object(
            target,
            &["workflowPath", "executionPrincipalId"],
            &["workflowPath", "executionPrincipalId"],
        )?;
        if !fields.values().all(Value::is_string) {
            return Err(anyhow!("invalid trigger target"));
        }
    }
    if let Some(conditions) = map.get("conditions") {
        let items = conditions
            .as_array()
            .ok_or_else(|| anyhow!("conditions must be an array"))?;
        if !(1..=20).contains(&items.len()) {
            return Err(anyhow!("conditions must contain 1 to 20 entries"));
        }
        for item in items {
            let fields = object(
                item,
                &["field", "operator", "value"],
                &["field", "operator", "value"],
            )?;
            let value = &fields["value"];
            if !(value.is_null()
                || value.as_str().is_some_and(|s| !s.is_empty())
                || value.as_array().is_some_and(|items| {
                    (1..=100).contains(&items.len())
                        && items
                            .iter()
                            .all(|v| v.is_null() || v.as_str().is_some_and(|s| !s.is_empty()))
                }))
            {
                return Err(anyhow!("invalid condition value"));
            }
        }
    }
    if let Some(inputs) = map.get("inputs") {
        let items = inputs
            .as_object()
            .ok_or_else(|| anyhow!("inputs must be an object"))?;
        if items.len() > 256 {
            return Err(anyhow!("too many inputs"));
        }
        for mapping in items.values() {
            let fields = object(mapping, &["kind", "source"], &["kind", "source"])?;
            match fields["kind"].as_str() {
                Some("file") => literal_or_field(&fields["source"], true, false)?,
                Some("text") => literal_or_field(&fields["source"], false, true)?,
                Some("json") => literal_or_field(&fields["source"], false, false)?,
                _ => return Err(anyhow!("invalid input kind")),
            }
        }
    }
    if let Some(context) = map.get("integrationContext") {
        let items = context
            .as_object()
            .ok_or_else(|| anyhow!("integrationContext must be an object"))?;
        if items.len() > 32 {
            return Err(anyhow!("too many integration context entries"));
        }
        for (key, source) in items {
            if key.is_empty() || key.len() > 64 || key.contains('\0') {
                return Err(anyhow!("invalid integration context key"));
            }
            literal_or_field(source, false, true)?;
        }
    }
    if let Some(choice) = map.get("linearLifecycle") {
        lifecycle(choice)?;
    }
    if let Some(publication) = map.get("publication") {
        let fields = object(publication, &["exportName"], &["exportName"])?;
        if !fields["exportName"].is_string() {
            return Err(anyhow!("invalid publication"));
        }
    }
    Ok(())
}

fn parse(bytes: &[u8], expected: Option<i32>) -> Result<Value> {
    resource::parse_configuration(bytes, expected, validate)
}

/// Reads a create configuration, or with its `expected` version an update's fields.
pub(super) fn read(path: &Path, expected: Option<i32>, stdin_claimed: bool) -> Result<Value> {
    parse(
        &resource::read_configuration(path, stdin_claimed)?,
        expected,
    )
}

pub(super) fn create(value: Value) -> Result<CreateLinearTriggerRequest> {
    serde_json::from_value(value).map_err(|_| anyhow!("invalid trigger configuration"))
}
pub(super) fn update(value: Value) -> Result<UpdateLinearTriggerRequest> {
    serde_json::from_value(value).map_err(|_| anyhow!("invalid trigger configuration"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn closed_configuration_and_null_values() {
        let base = br#"{"enabled":false,"source":{"type":"linear","connectionId":"lcn_1"},"target":{"workflowPath":".um/a.yaml","executionPrincipalId":"prn_1"},"conditions":[{"field":"event.action","operator":"eq","value":"update"}],"inputs":{"ticket":{"kind":"file","source":{"type":"snapshot"}},"data":{"kind":"json","source":{"type":"literal","value":null}}},"integrationContext":{"key":{"type":"field","field":"current.title"}}}"#;
        let mut value = parse(base, None).unwrap();
        value["inputs"]["message"] =
            serde_json::json!({"kind":"text","source":{"type":"literal","value":"ready"}});
        value["inputs"]["title"] =
            serde_json::json!({"kind":"text","source":{"type":"field","field":"current.title"}});
        value["inputs"]["metadata"] =
            serde_json::json!({"kind":"json","source":{"type":"field","field":"current.comments"}});
        value["integrationContext"]["fixed"] =
            serde_json::json!({"type":"literal","value":"ready"});
        value["conditions"][0]["value"] = Value::Null;
        value["publication"] = serde_json::json!({"exportName":"published"});
        assert!(create(value).is_ok());
        for bad in [
            br#"{"unknown":1}"#.as_slice(),
            br#"{"inputs":{"x":{"kind":"file","source":{"type":"snapshot","field":"oops"}}}}"#,
        ] {
            assert!(parse(bad, Some(1)).is_err());
        }
        assert!(parse(br#"{"inputs":{}}"#, Some(5)).is_ok());
        assert!(parse(br#"{"inputs":null}"#, Some(5)).is_err());
        assert!(parse(br#"{"expectedVersion":2}"#, Some(5)).is_err());
        assert!(
            update(parse(br#"{"publication":{"exportName":"published"}}"#, Some(5)).unwrap())
                .is_ok()
        );
        let mut disabled = parse(base, None).unwrap();
        disabled["linearLifecycle"] = serde_json::json!({"enabled":false});
        assert!(create(disabled).is_ok());
        assert!(
            update(parse(br#"{"linearLifecycle":{"enabled":false}}"#, Some(1)).unwrap()).is_ok()
        );
        for bad in [
            r#"{}"#,
            r#"{"enabled":null}"#,
            r#"{"enabled":true}"#,
            r#"{"enabled":false,"extra":0}"#,
        ] {
            let patch = format!(r#"{{"linearLifecycle":{bad}}}"#);
            assert!(parse(patch.as_bytes(), Some(1)).is_err());
        }
        assert!(create(parse(base, None).unwrap()).is_ok());
        for bad in [
            br#"{"inputs":{"x":{"kind":"text","source":{"type":"field","field":"current.comments"}}}}"#.as_slice(),
            br#"{"inputs":{"x":{"kind":"text","source":{"type":"literal","value":null}}}}"#,
            br#"{"integrationContext":{"x":{"type":"snapshot"}}}"#,
        ] {
            if let Ok(value) = parse(bad, Some(1)) { assert!(update(value).is_err()); }
        }
    }

    // REQ-TRIGGER-104: the local mirror accepts the enabled policy and rejects its invalid forms.
    #[test]
    fn enabled_lifecycle_policy_shape() {
        let policy = serde_json::json!({"enabled":true,"teamMappings":[{"teamId":"LINEAR_TEAM_ID","rules":[
            {"eventType":"run.started","stateId":"WORKING_STATE_ID"},
            {"eventType":"step.succeeded","node":{"scope":[],"role":"step","id":"review"},"stateId":"REVIEWED_STATE_ID"},
            {"eventType":"publication.published","stateId":"IN_REVIEW_STATE_ID"},
            {"eventType":"publication.no_changes","stateId":"TRIAGE_STATE_ID"},
            {"eventType":"run.failed","stateId":"TRIAGE_STATE_ID"}]}]});
        let mut config = serde_json::json!({"enabled":false,"source":{"type":"linear","connectionId":"lcn_1"},"target":{"workflowPath":".um/a.yaml","executionPrincipalId":"prn_1"},"conditions":[{"field":"event.action","operator":"eq","value":"update"}],"inputs":{},"integrationContext":{},"publication":{"exportName":"published"}});
        config["linearLifecycle"] = policy.clone();
        let created = create(parse(config.to_string().as_bytes(), None).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_value(created).unwrap()["linearLifecycle"],
            policy
        );
        let patch = |policy: &Value| {
            let body = serde_json::json!({"linearLifecycle": policy}).to_string();
            parse(body.as_bytes(), Some(1))
        };
        let updated = update(patch(&policy).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_value(updated).unwrap()["linearLifecycle"],
            policy
        );
        let rule = |extra: Value| {
            let mut rule = serde_json::json!({"eventType":"step.failed","stateId":"S"});
            rule.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            rule
        };
        let node = serde_json::json!({"scope":["c"],"role":"finalizer","id":"n"});
        let team = |rules: Vec<Value>| serde_json::json!({"teamId":"T","rules":rules});
        let enabled = |teams: Vec<Value>| serde_json::json!({"enabled":true,"teamMappings":teams});
        // An all-node rule may sit beside a node-bearing rule for the same type and destination.
        let shared = rule(serde_json::json!({"node":node}));
        assert!(
            patch(&enabled(vec![team(vec![
                rule(serde_json::json!({})),
                shared.clone()
            ])]))
            .is_ok()
        );
        for bad in [
            serde_json::json!({"enabled":false,"teamMappings":[]}),
            enabled(vec![]),
            enabled(vec![team(vec![])]),
            enabled(vec![team(vec![rule(
                serde_json::json!({"eventType":"webhook.test"}),
            )])]),
            enabled(vec![team(vec![rule(
                serde_json::json!({"eventType":"run.failed","node":node}),
            )])]),
            enabled(vec![team(vec![rule(
                serde_json::json!({"node":{"scope":[],"role":"step"}}),
            )])]),
            enabled(vec![team(vec![rule(serde_json::json!({"node":null}))])]),
            enabled(vec![team(vec![rule(serde_json::json!({"stateId":""}))])]),
            enabled(vec![team(vec![rule(serde_json::json!({"extra":1}))])]),
            enabled(vec![
                team(vec![rule(serde_json::json!({}))]),
                team(vec![rule(serde_json::json!({}))]),
            ]),
            enabled(vec![team(vec![shared.clone(), shared.clone()])]),
            enabled(vec![team(vec![
                shared,
                rule(serde_json::json!({"stateId":"X"})),
            ])]),
        ] {
            assert!(patch(&bad).is_err(), "{bad}");
        }
    }
}
