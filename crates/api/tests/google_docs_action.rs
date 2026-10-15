#![allow(
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "generated action client wire-contract fixture uses assertions and decoding"
)]

use um_api::{GoogleDocsBinding, PatchGoogleDocsActionRequest};

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
