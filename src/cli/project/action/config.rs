//! Closed CLI configuration parsing before authentication or any network access.
use anyhow::{Result, anyhow};
use serde_json::{Map, Value};
use std::{
    io::{self, Read},
    path::Path,
};
use um_api::{CreateGoogleDocsActionRequest, PatchGoogleDocsActionRequest};

fn object<'a>(
    value: &'a Value,
    allowed: &[&str],
    required: &[&str],
) -> Result<&'a Map<String, Value>> {
    let fields = value
        .as_object()
        .ok_or_else(|| anyhow!("expected a configuration object"))?;
    if fields.keys().any(|key| !allowed.contains(&key.as_str()))
        || required.iter().any(|key| !fields.contains_key(*key))
    {
        return Err(anyhow!("unknown or missing configuration field"));
    }
    Ok(fields)
}
fn non_null(value: &Value) -> Result<()> {
    if value.is_null() {
        Err(anyhow!("null is not valid for this field"))
    } else {
        Ok(())
    }
}
fn validate(value: &Value, patch: bool) -> Result<()> {
    let allowed = if patch {
        &[
            "displayName",
            "workflowPath",
            "description",
            "sortOrder",
            "inputBindings",
            "inputPresentation",
            "publication",
        ][..]
    } else {
        &[
            "displayName",
            "workflowPath",
            "enabled",
            "description",
            "sortOrder",
            "inputBindings",
            "inputPresentation",
            "publication",
        ][..]
    };
    let required = if patch {
        &[][..]
    } else {
        &["displayName", "workflowPath", "enabled"][..]
    };
    let fields = object(value, allowed, required)?;
    if patch && fields.is_empty() {
        return Err(anyhow!("update requires a configuration field"));
    }
    for (key, field) in fields {
        if key != "publication" {
            non_null(field)?;
        }
        match key.as_str() {
            "inputBindings" => {
                let bindings = field
                    .as_object()
                    .ok_or_else(|| anyhow!("input bindings must be an object"))?;
                for binding in bindings.values() {
                    let tag = binding
                        .get("type")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow!("invalid input binding"))?;
                    match tag {
                        "user" => {
                            object(binding, &["type"], &["type"])?;
                        }
                        "document" => {
                            object(binding, &["type", "field"], &["type", "field"])?;
                            if !matches!(binding["field"].as_str(), Some("id" | "url")) {
                                return Err(anyhow!("invalid document binding"));
                            }
                        }
                        "literal" => {
                            let parts = object(
                                binding,
                                &["type", "kind", "value"],
                                &["type", "kind", "value"],
                            )?;
                            match parts["kind"].as_str() {
                                Some("text") if parts["value"].is_string() => (),
                                Some("json") => (),
                                _ => return Err(anyhow!("invalid literal binding")),
                            }
                        }
                        _ => return Err(anyhow!("invalid input binding")),
                    }
                }
            }
            "inputPresentation" => {
                let presentation = field
                    .as_object()
                    .ok_or_else(|| anyhow!("input presentation must be an object"))?;
                for entry in presentation.values() {
                    let fields = object(entry, &["label", "help"], &[])?;
                    if fields.values().any(|value| !value.is_string()) {
                        return Err(anyhow!("invalid input presentation"));
                    }
                }
            }
            "publication" if !field.is_null() => {
                let fields = object(field, &["exportName"], &["exportName"])?;
                if !fields["exportName"].is_string() {
                    return Err(anyhow!("invalid publication"));
                }
            }
            _ => (),
        }
    }
    Ok(())
}

// The domain parser accepts 2 MiB of source JSON before normalization. Accept
// whitespace-heavy files locally; the typed request removes that whitespace
// before the Gateway's separate 1 MiB HTTP body limit applies.
const MAX_SOURCE_BYTES: u64 = 2 * 1024 * 1024;

pub(super) fn read(
    path: &Path,
    patch: bool,
    version: Option<i32>,
    credential_stdin: bool,
) -> Result<Value> {
    if path == Path::new("-") && credential_stdin {
        return Err(anyhow!(
            "configuration and authentication cannot both read standard input"
        ));
    }
    let mut source: Box<dyn Read> = if path == Path::new("-") {
        Box::new(io::stdin())
    } else {
        Box::new(
            super::super::super::open_regular_file_nonblocking(path)
                .map_err(|_| anyhow!("config file must be a readable regular file"))?,
        )
    };
    let mut bytes = Vec::new();
    source
        .by_ref()
        .take(MAX_SOURCE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| anyhow!("cannot read configuration"))?;
    if bytes.len() as u64 > MAX_SOURCE_BYTES {
        return Err(anyhow!("configuration exceeds 2 MiB"));
    }
    let mut value: Value = um_support::strict_json_from_slice(&bytes)
        .map_err(|_| anyhow!("configuration must be strict JSON without duplicate fields"))?;
    validate(&value, patch)?;
    if patch {
        let version = version
            .filter(|version| *version > 0)
            .ok_or_else(|| anyhow!("expected version must be positive"))?;
        value
            .as_object_mut()
            .ok_or_else(|| anyhow!("invalid configuration"))?
            .insert("expectedVersion".to_owned(), version.into());
        let _: PatchGoogleDocsActionRequest = serde_json::from_value(value.clone())
            .map_err(|_| anyhow!("invalid action configuration"))?;
    } else {
        let _: CreateGoogleDocsActionRequest = serde_json::from_value(value.clone())
            .map_err(|_| anyhow!("invalid action configuration"))?;
    }
    Ok(value)
}

pub(super) fn create(value: Value) -> Result<CreateGoogleDocsActionRequest> {
    serde_json::from_value(value).map_err(|_| anyhow!("invalid action configuration"))
}
pub(super) fn update(value: Value) -> Result<PatchGoogleDocsActionRequest> {
    serde_json::from_value(value).map_err(|_| anyhow!("invalid action configuration"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn closed_patch_and_null_literal() {
        let value = serde_json::json!({"inputBindings":{"x":{"type":"literal","kind":"json","value":null}},"publication":null});
        validate(&value, true).unwrap();
        let mut value = value;
        value["expectedVersion"] = 3.into();
        let wire = serde_json::to_value(update(value).unwrap()).unwrap();
        assert!(wire["inputBindings"]["x"].get("value").is_some());
        assert!(wire.get("publication").is_some());
        assert_eq!(wire["expectedVersion"], 3);
        for bad in [
            serde_json::json!({"enabled":true}),
            serde_json::json!({"publication":{"exportName":"x", "extra":1}}),
            serde_json::json!({"inputBindings":{"x":{"type":"user","value":null}}}),
        ] {
            assert!(validate(&bad, true).is_err());
        }
    }
}
