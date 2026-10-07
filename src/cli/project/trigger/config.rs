//! Validate the closed request grammar before handing a typed request to the generated transport.
//! Generated untagged unions accept extra properties and may default missing fields.
use std::io::{self, Read};
use std::path::Path;

use anyhow::{Result, anyhow};
use serde_json::{Map, Value};
use um_api::{CreateLinearTriggerRequest, UpdateLinearTriggerRequest};

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

fn validate(value: &Value, update: bool) -> Result<()> {
    let allowed = if update {
        &[
            "source",
            "target",
            "conditions",
            "inputs",
            "integrationContext",
            "publication",
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
    if let Some(publication) = map.get("publication") {
        let fields = object(publication, &["exportName"], &["exportName"])?;
        if !fields["exportName"].is_string() {
            return Err(anyhow!("invalid publication"));
        }
    }
    Ok(())
}

fn parse(bytes: &[u8], update: bool, expected: Option<i32>) -> Result<Value> {
    let mut value: Value =
        serde_json::from_slice(bytes).map_err(|_| anyhow!("invalid configuration JSON"))?;
    validate(&value, update)?;
    if update {
        let version = expected.ok_or_else(|| anyhow!("expected version required"))?;
        if version < 1 {
            return Err(anyhow!("expected version must be positive"));
        }
        value
            .as_object_mut()
            .ok_or_else(|| anyhow!("invalid configuration"))?
            .insert("expectedVersion".into(), version.into());
    }
    Ok(value)
}

pub(super) fn read(
    path: &Path,
    update: bool,
    expected: Option<i32>,
    stdin_claimed: bool,
) -> Result<Value> {
    if path == Path::new("-") && stdin_claimed {
        return Err(anyhow!(
            "configuration and authentication cannot both read stdin"
        ));
    }
    let mut bytes = Vec::new();
    if path == Path::new("-") {
        io::stdin().take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
    } else {
        std::fs::File::open(path)?
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)?;
    }
    if bytes.len() > 1024 * 1024 {
        return Err(anyhow!("configuration exceeds 1 MiB"));
    }
    parse(&bytes, update, expected)
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
        let mut value = parse(base, false, None).unwrap();
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
            assert!(parse(bad, true, Some(1)).is_err());
        }
        assert!(parse(br#"{"inputs":{}}"#, true, Some(5)).is_ok());
        assert!(parse(br#"{"inputs":null}"#, true, Some(5)).is_err());
        assert!(parse(br#"{"expectedVersion":2}"#, true, Some(5)).is_err());
        assert!(
            update(
                parse(
                    br#"{"publication":{"exportName":"published"}}"#,
                    true,
                    Some(5)
                )
                .unwrap()
            )
            .is_ok()
        );
        assert!(create(parse(base, false, None).unwrap()).is_ok());
        for bad in [
            br#"{"inputs":{"x":{"kind":"text","source":{"type":"field","field":"current.comments"}}}}"#.as_slice(),
            br#"{"inputs":{"x":{"kind":"text","source":{"type":"literal","value":null}}}}"#,
            br#"{"integrationContext":{"x":{"type":"snapshot"}}}"#,
        ] {
            if let Ok(value) = parse(bad, true, Some(1)) { assert!(update(value).is_err()); }
        }
    }
}
