//! Guard test: keylens must never issue server-blocking commands.
//!
//! `KEYS` on a production Redis with millions of keys blocks the server for seconds. It is
//! the single fastest way for a browsing tool to cause an outage, and it is *easy* to add
//! by accident when a `SCAN` loop feels tedious. Same for `FLUSHALL`/`FLUSHDB`, which have
//! no place in a read-only tool at all.
//!
//! The same reasoning bans unbounded *collection* reads. `HGETALL` on a two-million-field
//! hash blocks just as hard as `KEYS`, and a key browser is exactly the thing that will
//! meet that hash. Use `HSCAN`/`SSCAN`, or an explicit range.
//!
//! This walks the workspace source rather than trusting review.

use std::fs;
use std::path::{Path, PathBuf};

/// Commands with no legitimate use in a read-only browser, anywhere, ever.
const BANNED: &[&str] = &[
    "\"KEYS\"",
    "\"FLUSHALL\"",
    "\"FLUSHDB\"",
    "\"MONITOR\"",
    "\"DEBUG\"",
];

/// Whole-collection reads, permitted **only** in the one file that measures a key before
/// reading it.
///
/// Not every Redis-compatible server implements `HSCAN`/`SSCAN`/`GETRANGE` — Recached, for
/// one, does not. Refusing to render a five-field hash on those servers would be silly, so
/// `value.rs` calls `HLEN`/`SCARD`/`STRLEN` first and only reads the whole thing when it's
/// small. The bound survives; it's measured client-side instead of requested server-side.
const SIZE_GATED: &[&str] = &["\"HGETALL\"", "\"SMEMBERS\""];

/// The only file allowed to use [`SIZE_GATED`] commands.
const SIZE_GATED_FILE: &str = "value.rs";

/// The guard those commands must sit behind. If this helper is ever removed or renamed,
/// the exception above stops being justified and this test fails.
const SIZE_GATE_FN: &str = "async fn size_ok(";

fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is crates/keylens
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if path.is_dir() {
            if name == "target" || name == "node_modules" || name.starts_with('.') {
                continue;
            }
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            // This test file necessarily contains the banned literals.
            if path
                .file_name()
                .is_some_and(|f| f == "no_dangerous_commands.rs")
            {
                continue;
            }
            out.push(path);
        }
    }
}

#[test]
fn no_keyspace_blocking_commands_in_source() {
    let root = workspace_root();
    let mut files = Vec::new();
    rust_sources(&root.join("crates"), &mut files);

    assert!(
        !files.is_empty(),
        "found no Rust sources under {}",
        root.display()
    );

    let mut violations = Vec::new();
    for file in &files {
        let Ok(text) = fs::read_to_string(file) else {
            continue;
        };
        for (lineno, line) in text.lines().enumerate() {
            // Doc comments may legitimately discuss why these are banned.
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") || trimmed.starts_with("*") {
                continue;
            }
            let in_gated_file = file.file_name().is_some_and(|f| f == SIZE_GATED_FILE);
            let checks: Vec<&&str> = BANNED
                .iter()
                .chain(SIZE_GATED.iter().filter(|_| !in_gated_file))
                .collect();

            for banned in checks {
                if line.contains(*banned) {
                    violations.push(format!(
                        "{}:{}: {}",
                        file.strip_prefix(&root).unwrap_or(file).display(),
                        lineno + 1,
                        line.trim()
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "banned command literals found -- use cursor-paged SCAN instead:\n{}",
        violations.join("\n")
    );
}

#[test]
fn whole_collection_reads_stay_behind_the_size_gate() {
    // The exception carved out for `value.rs` is only defensible while the size check
    // exists. If someone deletes it, the whole-collection reads become the unbounded reads
    // this suite is here to prevent.
    let root = workspace_root();
    let mut files = Vec::new();
    rust_sources(&root.join("crates"), &mut files);

    let gated: Vec<&PathBuf> = files
        .iter()
        .filter(|f| f.file_name().is_some_and(|n| n == SIZE_GATED_FILE))
        .collect();
    assert_eq!(gated.len(), 1, "expected exactly one {SIZE_GATED_FILE}");

    let text = fs::read_to_string(gated[0]).expect("read value.rs");
    assert!(
        text.contains(SIZE_GATE_FN),
        "{SIZE_GATED_FILE} uses whole-collection reads but no longer defines `{SIZE_GATE_FN}`"
    );

    // Every whole-collection call must be reachable only from a size-checked branch.
    for cmd in SIZE_GATED {
        if text.contains(cmd) {
            assert!(
                text.contains("size_ok(self, key"),
                "{cmd} appears in {SIZE_GATED_FILE} without a size_ok() guard"
            );
        }
    }
}
