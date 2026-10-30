/// The wire name of a serialized enum value: its JSON string, or none for any other shape.
pub fn wire_name(value: impl serde::Serialize) -> Option<String> {
    serde_json::to_value(value)
        .ok()?
        .as_str()
        .map(str::to_owned)
}
