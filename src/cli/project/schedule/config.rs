//! Validate the closed schedule request grammar (REQ-SCHEDULE-3, REQ-SCHEDULE-15) before
//! handing a typed request to the generated transport, whose models default missing
//! members and ignore unknown ones. Cloud's other rules are left to Cloud.
use std::path::Path;

use anyhow::{Result, anyhow};
use serde_json::{Map, Value};
use um_api::{CreateScheduleRequest, UpdateScheduleRequest};

use super::super::resource;

const MEMBERS: [&str; 7] = [
    "name",
    "enabled",
    "timing",
    "target",
    "inputs",
    "integrationContext",
    "publication",
];

/// An object holding only `allowed` members, every `required` one among them.
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
    {
        return Err(anyhow!("unknown or missing configuration property"));
    }
    Ok(map)
}

fn strings(value: &Value, members: &[&str], what: &str) -> Result<()> {
    if !object(value, members, members)?
        .values()
        .all(Value::is_string)
    {
        return Err(anyhow!("invalid schedule {what}"));
    }
    Ok(())
}

fn validate(value: &Value, update: bool) -> Result<()> {
    // `enabled` changes only through enable and disable.
    let allowed: Vec<&str> = MEMBERS
        .into_iter()
        .filter(|member| !update || *member != "enabled")
        .collect();
    let required = if update { &[][..] } else { &MEMBERS[..6] };
    let map = object(value, required, &allowed)?;
    if update && map.is_empty() {
        return Err(anyhow!("update requires a configuration field"));
    }
    for (key, member) in map {
        match key.as_str() {
            "name" if member.is_string() => {}
            "enabled" if member.is_boolean() => {}
            "timing" => strings(member, &["cron", "timeZone"], "timing")?,
            "target" => strings(member, &["workflowPath", "executionPrincipalId"], "target")?,
            // A json input's literal may be any value, null included.
            "inputs" if member.is_object() => {}
            "integrationContext" => {
                if !member
                    .as_object()
                    .is_some_and(|entries| entries.values().all(Value::is_string))
                {
                    return Err(anyhow!("integrationContext must map keys to strings"));
                }
            }
            "publication" if update && member.is_null() => {}
            "publication" => strings(member, &["exportName"], "publication")?,
            _ => return Err(anyhow!("invalid or null configuration property {key}")),
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

pub(super) fn create(value: Value) -> Result<CreateScheduleRequest> {
    serde_json::from_value(value).map_err(|_| anyhow!("invalid schedule configuration"))
}
pub(super) fn update(value: Value) -> Result<UpdateScheduleRequest> {
    serde_json::from_value(value).map_err(|_| anyhow!("invalid schedule configuration"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn closed_configuration_and_null_values() {
        let base = br#"{"name":"nightly","enabled":false,"timing":{"cron":"0 3 * * *","timeZone":"UTC"},"target":{"workflowPath":".um/a.yaml","executionPrincipalId":"prn_1"},"inputs":{"data":null,"count":2},"integrationContext":{"team":"platform"}}"#;
        let mut value = parse(base, None).unwrap();
        assert!(value["inputs"]["data"].is_null());
        value["publication"] = serde_json::json!({"exportName":"published"});
        assert!(create(parse(&serde_json::to_vec(&value).unwrap(), None).unwrap()).is_ok());
        for bad in [
            br#"{"name":"nightly"}"#.as_slice(),
            br#"{"name":"nightly","enabled":false,"timing":{"cron":"0 3 * * *","timeZone":"UTC"},"target":{"workflowPath":".um/a.yaml","executionPrincipalId":"prn_1"},"inputs":{},"integrationContext":{},"publication":null}"#,
            br#"{"name":"nightly","enabled":false,"timing":{"cron":"0 3 * * *","timeZone":"UTC","jitter":1},"target":{"workflowPath":".um/a.yaml","executionPrincipalId":"prn_1"},"inputs":{},"integrationContext":{}}"#,
        ] {
            assert!(parse(bad, None).is_err());
        }
        let removed = update(parse(br#"{"publication":null}"#, Some(5)).unwrap()).unwrap();
        assert_eq!(removed.publication, Some(None));
        assert_eq!(
            serde_json::to_value(&removed).unwrap(),
            serde_json::json!({"expectedVersion":5,"publication":null})
        );
        assert!(parse(br#"{"inputs":{"x":null}}"#, Some(5)).is_ok());
        for bad in [
            br#"{}"#.as_slice(),
            br#"{"inputs":null}"#,
            br#"{"name":null}"#,
            br#"{"enabled":true}"#,
            br#"{"expectedVersion":2}"#,
            br#"{"integrationContext":{"team":1}}"#,
            br#"{"target":{"workflowPath":".um/a.yaml"}}"#,
            br#"{"publication":{"exportName":"published","target":{}}}"#,
        ] {
            assert!(parse(bad, Some(5)).is_err());
        }
    }
}
