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

/// Commands that must never appear as a literal command name in shipped code.
const BANNED: &[&str] = &[
    "\"KEYS\"",
    "\"FLUSHALL\"",
    "\"FLUSHDB\"",
    "\"MONITOR\"",
    "\"DEBUG\"",
    // Unbounded collection reads -- use the cursor or range variants.
    "\"HGETALL\"",
    "\"SMEMBERS\"",
    "\"LRANGE_ALL\"",
];

fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is crates/keylens
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else { return };
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
            if path.file_name().is_some_and(|f| f == "no_dangerous_commands.rs") {
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

    assert!(!files.is_empty(), "found no Rust sources under {}", root.display());

    let mut violations = Vec::new();
    for file in &files {
        let Ok(text) = fs::read_to_string(file) else { continue };
        for (lineno, line) in text.lines().enumerate() {
            // Doc comments may legitimately discuss why these are banned.
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") || trimmed.starts_with("*") {
                continue;
            }
            for banned in BANNED {
                if line.contains(banned) {
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
