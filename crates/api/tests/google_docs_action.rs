#![allow(
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "generated action client wire-contract fixture uses assertions and decoding"
)]

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use um_api::{GoogleDocsBinding, GoogleDocsPresentation, PatchGoogleDocsActionRequest};

// The released decoder of `um` 0.73.0 (source revision bd12fae18c8c37e9ad05511df47ea61d22a58375)
// and 0.72.1 (d84ce7c3212633b3f4d8b5cfb1304f83e15d9ab9), copied rather than imported so the
// model in this tree may move on: `GoogleDocsAction` and `GoogleDocsActionList` from
// `cli/crates/api/src/generated/models/google_docs_action.rs` and `google_docs_action_list.rs`,
// the publication model from `create_google_docs_action_request_publication.rs`, and the
// closed round trip of `ActionApi::json` in `cli/crates/api/src/google_docs_actions.rs`.
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
struct ReleasedGoogleDocsAction {
    #[serde(rename = "id")]
    id: String,
    #[serde(rename = "projectId")]
    project_id: String,
    #[serde(rename = "version")]
    version: i32,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(rename = "updatedAt")]
    updated_at: String,
    #[serde(rename = "displayName")]
    display_name: String,
    #[serde(rename = "workflowPath")]
    workflow_path: String,
    #[serde(rename = "enabled")]
    enabled: bool,
    #[serde(rename = "description")]
    description: String,
    #[serde(rename = "sortOrder")]
    sort_order: i32,
    #[serde(rename = "inputBindings")]
    input_bindings: std::collections::HashMap<String, GoogleDocsBinding>,
    #[serde(rename = "inputPresentation")]
    input_presentation: std::collections::HashMap<String, GoogleDocsPresentation>,
    #[serde(rename = "publication", deserialize_with = "Option::deserialize")]
    publication: Option<Box<ReleasedPublication>>,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
struct ReleasedPublication {
    #[serde(rename = "exportName")]
    export_name: String,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
struct ReleasedGoogleDocsActionList {
    #[serde(rename = "items")]
    items: Vec<ReleasedGoogleDocsAction>,
    #[serde(rename = "nextCursor", deserialize_with = "Option::deserialize")]
    next_cursor: Option<String>,
}

/// `ActionApi::json` of those releases: decode the answer, serialize the model again, and
/// refuse the answer unless the two are equal.
fn released_decode<T: DeserializeOwned + Serialize>(raw: &serde_json::Value) -> Option<T> {
    let value: T = serde_json::from_value(raw.clone()).ok()?;
    (serde_json::to_value(&value).ok()? == *raw).then_some(value)
}

const RELEASED_PAGE: &str = include_str!("fixtures/list-google-docs-actions-200.json");
const REPRESENTATION_2_PAGE: &str =
    include_str!("fixtures/list-google-docs-actions-representation-2-200.json");

/// REQ-DOCSACTION-123, REQ-COMPAT-20: a published `um` reads Cloud's default action answer,
/// whether or not an action names a definition, and refuses one that carries `definitionId`.
#[test]
fn released_decoder_reads_default_representation_and_refuses_representation_2() {
    let page: serde_json::Value = serde_json::from_str(RELEASED_PAGE).unwrap();
    let decoded = released_decode::<ReleasedGoogleDocsActionList>(&page).unwrap();
    assert_eq!(decoded.items.len(), 2);
    for item in page["items"].as_array().unwrap() {
        assert!(released_decode::<ReleasedGoogleDocsAction>(item).is_some());
    }
    let later: serde_json::Value = serde_json::from_str(REPRESENTATION_2_PAGE).unwrap();
    let items = later["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    for item in items {
        assert!(released_decode::<ReleasedGoogleDocsAction>(item).is_none());
    }
}

#[test]
fn action_client_preserves_literal_null_and_publication_clear() {
    let literal: GoogleDocsBinding = serde_json::from_value(serde_json::json!({
        "type":"literal", "kind":"json", "value":null
    }))
    .unwrap();
    let absent: GoogleDocsBinding =
        serde_json::from_value(serde_json::json!({"type":"user"})).unwrap();
    assert_eq!(literal.value, Some(None));
    assert_eq!(absent.value, None);
    assert_eq!(
        serde_json::to_value(literal).unwrap()["value"],
        serde_json::Value::Null
    );
    assert!(serde_json::to_value(absent).unwrap().get("value").is_none());

    let clear: PatchGoogleDocsActionRequest = serde_json::from_value(serde_json::json!({
        "expectedVersion":1, "publication":null
    }))
    .unwrap();
    let unchanged: PatchGoogleDocsActionRequest =
        serde_json::from_value(serde_json::json!({"expectedVersion":1})).unwrap();
    assert_eq!(clear.publication, Some(None));
    assert_eq!(unchanged.publication, None);
    assert!(
        serde_json::to_value(clear)
            .unwrap()
            .get("publication")
            .is_some()
    );
    assert!(
        serde_json::to_value(unchanged)
            .unwrap()
            .get("publication")
            .is_none()
    );
}
