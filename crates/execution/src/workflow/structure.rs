//! The field and the rule a structurally invalid document breaks (REQ-WFFORMAT-103).
//!
//! A document the embedded schema refuses is reduced to one violation: the first error
//! the validator reports, followed through a `oneOf` or `anyOf` into the one form the
//! value is closest to. The violation keeps field names and name-shaped keys from the
//! document and expectations from the schema; it never holds a value from the document.

use std::borrow::Cow;

use jsonschema::Validator;
use jsonschema::error::{TypeKind, ValidationError, ValidationErrorKind};
use jsonschema::paths::{Location, LocationSegment};
use serde::Serialize;
use serde_json::Value;

/// The closed set of rules a structural violation names.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StructureRule {
    UnknownField,
    MissingField,
    Pattern,
    Enum,
    Type,
    Length,
    Count,
    Range,
    Duplicate,
    Forbidden,
    Form,
    Invalid,
}

impl StructureRule {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::UnknownField => "unknown_field",
            Self::MissingField => "missing_field",
            Self::Pattern => "pattern",
            Self::Enum => "enum",
            Self::Type => "type",
            Self::Length => "length",
            Self::Count => "count",
            Self::Range => "range",
            Self::Duplicate => "duplicate",
            Self::Forbidden => "forbidden",
            Self::Form => "form",
            Self::Invalid => "invalid",
        }
    }
}

/// Where a document breaks its schema and which rule it breaks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StructureViolation {
    field: Option<String>,
    rule: StructureRule,
    expected: Option<Value>,
}

impl StructureViolation {
    /// A failure no other rule names, at no field.
    pub(crate) fn invalid() -> Self {
        Self {
            field: None,
            rule: StructureRule::Invalid,
            expected: None,
        }
    }

    /// The path of the failing field from the document root, absent at the root.
    pub(crate) fn field(&self) -> Option<&str> {
        self.field.as_deref()
    }

    pub(crate) fn rule(&self) -> StructureRule {
        self.rule
    }

    /// The pattern, the allowed values or the allowed JSON types, from the schema.
    pub(crate) fn expected(&self) -> Option<&Value> {
        self.expected.as_ref()
    }
}

/// Checks `document` against `validator`, naming one violation when it is refused.
///
/// The same document always yields the same violation.
pub(crate) fn check_structure(
    validator: &Validator,
    document: &Value,
) -> Result<(), StructureViolation> {
    if validator.is_valid(document) {
        return Ok(());
    }
    Err(validator
        .iter_errors(document)
        .next()
        .map_or_else(StructureViolation::invalid, |error| {
            Finding::of(&error, document).into_violation()
        }))
}

/// A violation whose field path is still being built.
struct Finding {
    path: FieldPath,
    rule: StructureRule,
    expected: Option<Value>,
}

impl Finding {
    fn of(error: &ValidationError<'_>, document: &Value) -> Self {
        let at = |rule, expected| Self {
            path: FieldPath::of(error.instance_path(), document),
            rule,
            expected,
        };
        match error.kind() {
            ValidationErrorKind::AdditionalProperties { unexpected }
            | ValidationErrorKind::UnevaluatedProperties { unexpected } => {
                let mut finding = at(StructureRule::UnknownField, None);
                finding
                    .path
                    .push_key(unexpected.first().map_or("", String::as_str));
                finding
            }
            ValidationErrorKind::Required { property } => {
                let mut finding = at(StructureRule::MissingField, None);
                finding.path.push_key(property.as_str().unwrap_or(""));
                finding
            }
            ValidationErrorKind::Pattern { pattern } => {
                at(StructureRule::Pattern, Some(Value::String(pattern.clone())))
            }
            ValidationErrorKind::Enum { options } => at(StructureRule::Enum, Some(options.clone())),
            ValidationErrorKind::Constant { expected_value } => at(
                StructureRule::Enum,
                Some(Value::Array(vec![expected_value.clone()])),
            ),
            ValidationErrorKind::Type { kind } => at(
                StructureRule::Type,
                Some(Value::Array(
                    type_names(kind).into_iter().map(Value::from).collect(),
                )),
            ),
            ValidationErrorKind::MinLength { .. } | ValidationErrorKind::MaxLength { .. } => {
                at(StructureRule::Length, None)
            }
            ValidationErrorKind::MinItems { .. }
            | ValidationErrorKind::MaxItems { .. }
            | ValidationErrorKind::MinProperties { .. }
            | ValidationErrorKind::MaxProperties { .. } => at(StructureRule::Count, None),
            ValidationErrorKind::Minimum { .. }
            | ValidationErrorKind::Maximum { .. }
            | ValidationErrorKind::ExclusiveMinimum { .. }
            | ValidationErrorKind::ExclusiveMaximum { .. } => at(StructureRule::Range, None),
            ValidationErrorKind::UniqueItems => at(StructureRule::Duplicate, None),
            ValidationErrorKind::Not { .. } | ValidationErrorKind::FalseSchema => {
                at(StructureRule::Forbidden, None)
            }
            ValidationErrorKind::PropertyNames { error: key_error } => {
                // The key's own error sits at the object; the key is the field.
                let mut finding = Self::of(key_error, document);
                if finding.rule == StructureRule::Invalid {
                    return finding;
                }
                finding.path = FieldPath::of(error.instance_path(), document);
                finding
                    .path
                    .push_key(key_error.instance().as_str().unwrap_or(""));
                finding
            }
            ValidationErrorKind::OneOfNotValid { context }
            | ValidationErrorKind::AnyOf { context } => Self::of_union(error, context, document),
            ValidationErrorKind::OneOfMultipleValid { .. } => at(StructureRule::Form, None),
            _ => Self::invalid(),
        }
    }

    fn invalid() -> Self {
        Self {
            path: FieldPath::default(),
            rule: StructureRule::Invalid,
            expected: None,
        }
    }

    /// Follows a union into the one form the value is closest to, or names the union.
    fn of_union(
        error: &ValidationError<'_>,
        forms: &[Vec<ValidationError<'_>>],
        document: &Value,
    ) -> Self {
        let here = error.instance_path();
        let form_failure = || Self {
            path: FieldPath::of(here, document),
            rule: StructureRule::Form,
            expected: None,
        };

        // A form for another JSON type is not one the value tried to take.
        let (typed, mistyped): (Vec<_>, Vec<_>) = forms
            .iter()
            .map(Vec::as_slice)
            .partition(|form| !form.iter().any(|error| is_type_failure_at(error, here)));
        if typed.is_empty() {
            return Self::of_types(here, &mistyped, document).unwrap_or_else(form_failure);
        }

        // The forms are told apart by constants on the value's own fields.
        let matching = typed
            .iter()
            .copied()
            .filter(|form| {
                !form
                    .iter()
                    .any(|error| constant_failure_below(error, here).is_some())
            })
            .collect::<Vec<_>>();
        let closest = match matching.as_slice() {
            [form] => Some(*form),
            [] => {
                if let Some(finding) = Self::of_discriminator(here, &typed, document) {
                    return finding;
                }
                // Every form contradicts the value somewhere: the closest is the one the
                // value contradicts least, a field it lacks not counting as a contradiction.
                Self::fewest_errors(&typed, |error| {
                    !matches!(error.kind(), ValidationErrorKind::Required { .. })
                })
            }
            _ => Self::fewest_errors(&matching, |_| true),
        };
        closest
            .and_then(|form| Self::descend(form, document))
            .unwrap_or_else(form_failure)
    }

    /// Every form wants a different JSON type: name the types the union allows.
    fn of_types(
        here: &Location,
        forms: &[&[ValidationError<'_>]],
        document: &Value,
    ) -> Option<Self> {
        let mut allowed = Vec::new();
        for form in forms {
            let error = form.iter().find(|error| is_type_failure_at(error, here))?;
            let ValidationErrorKind::Type { kind } = error.kind() else {
                return None;
            };
            for name in type_names(kind) {
                let name = Value::from(name);
                if !allowed.contains(&name) {
                    allowed.push(name);
                }
            }
        }
        Some(Self {
            path: FieldPath::of(here, document),
            rule: StructureRule::Type,
            expected: Some(Value::Array(allowed)),
        })
    }

    /// Every form fails a constant on the same field: name that field and the constants.
    fn of_discriminator(
        here: &Location,
        forms: &[&[ValidationError<'_>]],
        document: &Value,
    ) -> Option<Self> {
        let (first, rest) = forms.split_first()?;
        first.iter().find_map(|candidate| {
            let (field, _) = constant_failure_below(candidate, here)?;
            let mut allowed = Vec::new();
            for form in std::iter::once(first).chain(rest) {
                let expected = form.iter().find_map(|error| {
                    constant_failure_below(error, here)
                        .filter(|(other, _)| *other == field)
                        .map(|(_, expected)| expected)
                })?;
                if !allowed.contains(expected) {
                    allowed.push(expected.clone());
                }
            }
            Some(Self {
                path: FieldPath::of(candidate.instance_path(), document),
                rule: StructureRule::Enum,
                expected: Some(Value::Array(allowed)),
            })
        })
    }

    /// The form with strictly fewer counted errors than every other, if one has.
    fn fewest_errors<'f, 'e>(
        forms: &[&'f [ValidationError<'e>]],
        counts: impl Fn(&ValidationError<'e>) -> bool,
    ) -> Option<&'f [ValidationError<'e>]> {
        let count =
            |form: &[ValidationError<'e>]| form.iter().filter(|error| counts(error)).count();
        let fewest = forms.iter().map(|form| count(form)).min()?;
        match forms
            .iter()
            .filter(|form| count(form) == fewest)
            .collect::<Vec<_>>()
            .as_slice()
        {
            [form] => Some(form),
            _ => None,
        }
    }

    fn descend(form: &[ValidationError<'_>], document: &Value) -> Option<Self> {
        form.first().map(|error| Self::of(error, document))
    }

    fn into_violation(self) -> StructureViolation {
        StructureViolation {
            field: self.path.into_field(),
            rule: self.rule,
            expected: self.expected,
        }
    }
}

fn is_type_failure_at(error: &ValidationError<'_>, here: &Location) -> bool {
    matches!(error.kind(), ValidationErrorKind::Type { .. }) && error.instance_path() == here
}

/// The field and the constant of a `const` failure on a field directly below `here`.
fn constant_failure_below<'e>(
    error: &'e ValidationError<'_>,
    here: &Location,
) -> Option<(&'e str, &'e Value)> {
    let ValidationErrorKind::Constant { expected_value } = error.kind() else {
        return None;
    };
    let field = error.instance_path().as_str();
    let child = field
        .strip_prefix(here.as_str())?
        .strip_prefix('/')
        .filter(|child| !child.is_empty() && !child.contains('/'))?;
    Some((child, expected_value))
}

fn type_names(kind: &TypeKind) -> Vec<&'static str> {
    match kind {
        TypeKind::Single(single) => vec![single.as_str()],
        TypeKind::Multiple(set) => set.iter().map(|single| single.as_str()).collect(),
    }
}

/// A field path from the document root: field names and keys joined by `.`, a sequence
/// element written `[n]` after its field, and a key that is not name-shaped written `?`.
#[derive(Default)]
struct FieldPath(String);

impl FieldPath {
    fn of(location: &Location, document: &Value) -> Self {
        let mut path = Self::default();
        let mut node = Some(document);
        for segment in location {
            let key = match segment {
                LocationSegment::Property(key) => key,
                LocationSegment::Index(index) => Cow::Owned(index.to_string()),
            };
            match node {
                Some(Value::Array(items)) => {
                    let index = key.parse::<usize>().ok();
                    node = index.and_then(|index| items.get(index));
                    path.push_index(index);
                }
                _ => {
                    node = node.and_then(|value| value.get(key.as_ref()));
                    path.push_key(&key);
                }
            }
        }
        path
    }

    fn push_key(&mut self, key: &str) {
        if !self.0.is_empty() {
            self.0.push('.');
        }
        self.0.push_str(if is_name_shaped(key) { key } else { "?" });
    }

    fn push_index(&mut self, index: Option<usize>) {
        match index {
            Some(index) => self.0.push_str(&format!("[{index}]")),
            None => self.0.push_str("[?]"),
        }
    }

    fn into_field(self) -> Option<String> {
        (!self.0.is_empty()).then_some(self.0)
    }
}

/// A key the path may carry: 1 to 64 ASCII letters, digits, `_` or `-`.
fn is_name_shaped(key: &str) -> bool {
    (1..=64).contains(&key.len())
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}
