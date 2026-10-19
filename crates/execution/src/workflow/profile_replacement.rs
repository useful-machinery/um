//! Invocation-time agent profile replacement.
//!
//! An invoker may replace a workflow's declared agent profiles as a whole, by name,
//! with the closed document defined by Local Workflow Run CLI section 3.6. The
//! document is decoded with the same strict YAML rules as a workflow definition and
//! validated structurally against its own canonical schema before semantic validation
//! applies each replacement in place of the declared profile.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use jsonschema::Validator;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

use super::document::{AgentProfile, HarnessDefinition};
use super::validated::ValidatedHarness;
use super::{DecodeFailure, strict_yaml};

/// Largest accepted replacement document, matching the retained-input Text bound.
pub const MAXIMUM_AGENT_PROFILE_REPLACEMENT_BYTES: u64 = 1024 * 1024;

pub const AGENT_PROFILE_REPLACEMENT_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/agent-profile-replacement-v1.schema.json"
));

static STRUCTURAL_VALIDATOR: OnceLock<Result<Validator, ()>> = OnceLock::new();

/// The exact bytes of an invoker-supplied replacement document before decoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentProfileReplacementDocument {
    bytes: Arc<[u8]>,
}

impl AgentProfileReplacementDocument {
    pub fn new(bytes: impl Into<Arc<[u8]>>) -> Self {
        Self {
            bytes: bytes.into(),
        }
    }

    pub fn bytes(&self) -> &Arc<[u8]> {
        &self.bytes
    }
}

/// The decoded, structurally valid replacement map keyed by declared profile name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecodedAgentProfileReplacement {
    pub(crate) profiles: BTreeMap<String, AgentProfile>,
}

/// A replacement after semantic validation against the selected workflow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedAgentProfileReplacement {
    pub document: AgentProfileReplacementDocument,
    /// Lowercase hexadecimal SHA-256 digest of the exact document bytes.
    pub digest: String,
    /// The effective closed harness configuration pinned for each replaced profile.
    pub profiles: BTreeMap<String, ValidatedHarness>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplacementDto {
    #[serde(rename = "schemaVersion")]
    schema_version: u8,
    #[serde(rename = "agentProfiles")]
    agent_profiles: BTreeMap<String, ProfileDto>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileDto {
    harness: HarnessDto,
}

#[derive(Deserialize)]
#[serde(tag = "kind")]
enum HarnessDto {
    #[serde(rename = "pi")]
    Pi { config: Value },
    #[serde(rename = "claude_code")]
    ClaudeCode { config: Value },
    #[serde(rename = "codex")]
    Codex { config: Value },
}

pub(crate) fn decode(bytes: &[u8]) -> Result<DecodedAgentProfileReplacement, DecodeFailure> {
    let parsed = strict_yaml::parse(bytes)?;
    let validator = structural_validator().ok_or_else(DecodeFailure::structural_contract)?;
    if !validator.is_valid(&parsed.value) {
        return Err(DecodeFailure::structural_contract());
    }
    let dto = serde_json::from_value::<ReplacementDto>(parsed.value)
        .map_err(|_| DecodeFailure::structural_contract())?;
    if dto.schema_version != 1 || dto.agent_profiles.is_empty() {
        return Err(DecodeFailure::structural_contract());
    }
    let profiles = dto
        .agent_profiles
        .into_iter()
        .map(|(name, profile)| {
            let harness = match profile.harness {
                HarnessDto::Pi { config } => HarnessDefinition::Pi { config },
                HarnessDto::ClaudeCode { config } => HarnessDefinition::ClaudeCode { config },
                HarnessDto::Codex { config } => HarnessDefinition::Codex { config },
            };
            (name, AgentProfile { harness })
        })
        .collect();
    Ok(DecodedAgentProfileReplacement { profiles })
}

fn structural_validator() -> Option<&'static Validator> {
    STRUCTURAL_VALIDATOR
        .get_or_init(|| {
            let schema =
                serde_json::from_str::<Value>(AGENT_PROFILE_REPLACEMENT_SCHEMA).map_err(|_| ())?;
            jsonschema::draft202012::new(&schema).map_err(|_| ())
        })
        .as_ref()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::super::DecodeFailureKind;
    use super::*;

    // REQ-AGENT-54
    #[test]
    fn decodes_a_closed_document_and_rejects_structural_deviations() {
        let valid = b"schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: claude_code\n      config:\n        model: example/model\n        effort: high\n";
        let decoded = decode(valid).unwrap();
        assert_eq!(decoded.profiles.len(), 1);
        assert!(matches!(
            decoded.profiles["coding"].harness,
            HarnessDefinition::ClaudeCode { .. }
        ));

        let rejected = [
            // Empty map.
            b"schemaVersion: 1\nagentProfiles: {}\n".as_slice(),
            // Missing schema version.
            b"agentProfiles:\n  coding:\n    harness:\n      kind: pi\n      config:\n        model: m\n        thinking: low\n",
            // Additional top-level key.
            b"schemaVersion: 1\nsteps: {}\nagentProfiles:\n  coding:\n    harness:\n      kind: pi\n      config:\n        model: m\n        thinking: low\n",
            // Inline agent instead of harness.
            b"schemaVersion: 1\nagentProfiles:\n  coding:\n    agent:\n      profile: x\n",
            // Unknown harness kind.
            b"schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: other\n      config: {}\n",
            // Pi effort instead of thinking.
            b"schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: pi\n      config:\n        model: m\n        effort: low\n",
            // Non-identifier profile name.
            b"schemaVersion: 1\nagentProfiles:\n  Coding:\n    harness:\n      kind: pi\n      config:\n        model: m\n        thinking: low\n",
        ];
        for document in rejected {
            assert_eq!(
                decode(document).unwrap_err().kind(),
                DecodeFailureKind::StructuralContract,
                "{}",
                String::from_utf8_lossy(document)
            );
        }
        assert_eq!(
            decode(b"schemaVersion: 1\nagentProfiles: [\n")
                .unwrap_err()
                .kind(),
            DecodeFailureKind::MalformedYaml
        );
    }
}
