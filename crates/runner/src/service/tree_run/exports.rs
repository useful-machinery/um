//! Reading the root's text exports a tree run's finish lists (REQ-RUNNERENROLL-131): the
//! result the run's current attempt published, found through `state.json`, lists each
//! export in `result.json`'s `exports`, in the order of the names by their bytes, which is
//! the order the engine wrote them. Each export that result lists `available` with kind
//! `text` is listed, the first `MAXIMUM_TREE_EXPORTS` that can be, with its text when it is
//! within the one-text bound and the texts carried with it within the finish's total, and
//! otherwise by the size its result records, its carrier unread. A carrier is read through
//! the engine's reader of a run directory, never past the one-text bound. What cannot be
//! read is left out, and the class of the first fault is kept for the run event; nothing of
//! a path, an operating-system message or an export's name or text goes into the class.

use std::path::Path;

use serde_json::{Map, Value};
use useful_execution::read_run_file_bounded;
use useful_runner_protocol::{
    MAXIMUM_TREE_EXPORT_TEXT_BYTES, MAXIMUM_TREE_EXPORTS, MAXIMUM_TREE_EXPORTS_TEXT_BYTES,
    TreeExport,
};

use super::records::{published_attempt, read_json};

/// No published result of the current attempt, or one whose `result.json` or its
/// `exports` cannot be read: nothing is listed.
const RESULT_UNAVAILABLE: &str = "export_result_unavailable";
/// An available text export whose name, `sizeBytes` or `path` is not the engine's.
const ENTRY_INVALID: &str = "export_entry_invalid";
/// A carrier that cannot be opened or read, is not a regular file, is behind a link or is
/// larger than the one-text bound.
const CARRIER_UNAVAILABLE: &str = "export_carrier_unavailable";
/// A carrier whose bytes are not of the size its result records.
const SIZE_MISMATCH: &str = "export_size_mismatch";
/// A carrier whose bytes are not UTF-8.
const NOT_UTF8: &str = "export_not_utf8";
/// A listable export past the first `MAXIMUM_TREE_EXPORTS`.
const LIST_TRUNCATED: &str = "export_list_truncated";
/// The class recorded when the blocking worker reading the exports did not return.
pub(in crate::service) const EXPORT_READER_LOST: &str = "export_reader_lost";

/// The exports a finish lists and the class of the first export that could not be listed.
#[derive(Debug, Default)]
pub(super) struct RootExports {
    pub(super) exports: Vec<TreeExport>,
    pub(super) failure: Option<&'static str>,
}

impl RootExports {
    /// Keeps `class` unless an earlier fault set one.
    fn fail(&mut self, class: &'static str) {
        self.failure.get_or_insert(class);
    }
}

/// Reads the text exports of the result the current attempt of the run at `run_directory`
/// published; see the module documentation. It cannot fail: a fault leaves its export out
/// and its class on the answer.
pub(super) fn read_exports(run_directory: &Path) -> RootExports {
    let mut read = RootExports::default();
    let Some((result_directory, entries)) = published_exports(run_directory) else {
        read.fail(RESULT_UNAVAILABLE);
        return read;
    };
    let mut carried = 0_u64;
    for (name, entry) in &entries {
        if entry["state"] != "available" || entry["kind"] != "text" {
            continue;
        }
        match listed(run_directory, &result_directory, name, entry, carried) {
            Ok(_) if read.exports.len() == MAXIMUM_TREE_EXPORTS => {
                read.fail(LIST_TRUNCATED);
                break;
            }
            Ok(export) => {
                carried += export.text().map_or(0, |text| text.len() as u64);
                read.exports.push(export);
            }
            Err(class) => read.fail(class),
        }
    }
    read
}

/// The `relativeDirectory` of the result the current attempt published and the `exports`
/// object of its `result.json`; `None` when either cannot be read.
fn published_exports(run_directory: &Path) -> Option<(String, Map<String, Value>)> {
    let state = read_json(run_directory, "state.json")?;
    let directory = published_attempt(&state)?["result"]["relativeDirectory"]
        .as_str()?
        .to_owned();
    let mut result = read_json(run_directory, &format!("{directory}/result.json"))?;
    match result["exports"].take() {
        Value::Object(exports) => Some((directory, exports)),
        _ => None,
    }
}

/// The export `name` of `entry`, an available text, as the finish lists it once `carried`
/// bytes of text are carried: by its recorded size, unread, past either bound, and
/// otherwise with the text its carrier holds; `Err` with the class of the fault that leaves
/// it out.
fn listed(
    run_directory: &Path,
    result_directory: &str,
    name: &str,
    entry: &Value,
    carried: u64,
) -> Result<TreeExport, &'static str> {
    let size = entry["sizeBytes"].as_u64().ok_or(ENTRY_INVALID)?;
    let path = entry["path"]
        .as_str()
        .filter(|path| is_carrier_path(path))
        .ok_or(ENTRY_INVALID)?;
    let by_size = TreeExport::by_size(name, size).map_err(|_| ENTRY_INVALID)?;
    let text_bound = MAXIMUM_TREE_EXPORT_TEXT_BYTES as u64;
    if size > text_bound || carried + size > MAXIMUM_TREE_EXPORTS_TEXT_BYTES as u64 {
        return Ok(by_size);
    }
    let bytes = read_run_file_bounded(
        run_directory,
        &format!("{result_directory}/{path}"),
        text_bound,
    )
    .map_err(|_| CARRIER_UNAVAILABLE)?;
    if bytes.len() as u64 != size {
        return Err(SIZE_MISMATCH);
    }
    let text = String::from_utf8(bytes).map_err(|_| NOT_UTF8)?;
    TreeExport::with_text(name, text).map_err(|_| ENTRY_INVALID)
}

/// Whether `path` is a carrier path the engine writes, `exports/` and four ASCII digits.
fn is_carrier_path(path: &str) -> bool {
    path.strip_prefix("exports/").is_some_and(|ordinal| {
        ordinal.len() == 4 && ordinal.bytes().all(|byte| byte.is_ascii_digit())
    })
}
