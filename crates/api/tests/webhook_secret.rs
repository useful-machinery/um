#![allow(
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "webhook secret redaction test uses Rust test assertions and fixture decoding"
)]

use um_api::{WebhookSubscription, WebhookSubscriptionList};

#[test]
fn subscription_and_page_debug_do_not_disclose_show_once_secret() {
    let secret = "show-once-signing-material";
    let subscription: WebhookSubscription = serde_json::from_value(serde_json::json!({
        "id": "whs_01k0z6r1w8f4jy2m7q9v3x5abc", "projectId": "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
        "url": "https://receiver.example.test/hook", "state": "enabled", "version": 1,
        "eventTypes": ["run.failed"], "contextKeys": [], "createdAt": "2026-01-01T00:00:00Z",
        "updatedAt": "2026-01-01T00:00:00Z", "secret": secret
    }))
    .unwrap();
    assert!(!format!("{subscription:?}").contains(secret));
    assert_eq!(
        serde_json::to_value(&subscription).unwrap()["secret"],
        secret
    );
    let page = WebhookSubscriptionList {
        items: vec![subscription],
        next_cursor: None,
    };
    assert!(!format!("{page:?}").contains(secret));
}
