//! The key tree.
//!
//! Redis keyspaces are flat, but nobody thinks of them that way -- `bull:emails:failed`
//! reads as a path. Splitting on `:` and rendering a tree is the difference between
//! scrolling 40,000 keys and opening two folders.
//!
//! This model is deliberately pure: keys go in, rows come out, no I/O. It is the piece
//! most likely to have an off-by-one, so it is the piece with the most tests.

use std::collections::{BTreeMap, HashSet};

use keylens_conn::Kind;

pub const SEPARATOR: char = ':';

#[derive(Debug, Default)]
struct Node {
    children: BTreeMap<String, Node>,
    /// A key terminates exactly here. A node can be both a key and a branch: with
    /// `stats` and `stats:daily` both present, `stats` is both.
    is_key: bool,
    kind: Option<Kind>,
    /// Keys in this subtree, including `self` if `is_key`.
    subtree_keys: usize,
}

/// One rendered line.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub depth: usize,
    /// The segment shown, not the whole key.
    pub label: String,
    /// Full key path from the root, joined with `:`.
    pub path: String,
    pub is_key: bool,
    pub is_branch: bool,
    pub expanded: bool,
    pub subtree_keys: usize,
    pub kind: Option<Kind>,
}

#[derive(Debug)]
pub struct KeyTree {
    root: Node,
    expanded: HashSet<String>,
    total: usize,
    compact: bool,
}

impl Default for KeyTree {
    fn default() -> Self {
        Self {
            root: Node::default(),
            expanded: HashSet::new(),
            total: 0,
            compact: true,
        }
    }
}

impl KeyTree {
    pub fn new() -> Self {
        Self::default()
    }

    /// Collapse runs of single-child branches into one row (`2453:logs` rather than
    /// `2453` ▸ `logs`).
    ///
    /// This is not cosmetic. BullMQ stores a job at `bull:q:<id>` and its log at
    /// `bull:q:<id>:logs`, so without compaction every job id becomes a folder holding one
    /// thing, and browsing a queue means expanding thousands of them.
    pub fn set_compact(&mut self, compact: bool) {
        self.compact = compact;
    }

    pub fn compact(&self) -> bool {
        self.compact
    }

    /// Total keys inserted.
    pub fn len(&self) -> usize {
        self.total
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Drop all keys but keep expansion state, so a refresh doesn't collapse the view
    /// the user just arranged.
    pub fn clear_keys(&mut self) {
        self.root = Node::default();
        self.total = 0;
    }

    pub fn reset(&mut self) {
        self.clear_keys();
        self.expanded.clear();
    }

    pub fn insert(&mut self, key: &str) {
        self.insert_with_kind(key, None);
    }

    pub fn insert_with_kind(&mut self, key: &str, kind: Option<Kind>) {
        if key.is_empty() {
            return;
        }

        // Walk once to check for a duplicate before mutating counts, otherwise re-scanning
        // the same page inflates every ancestor's subtree count.
        if self.contains(key) {
            return;
        }

        let mut node = &mut self.root;
        node.subtree_keys += 1;
        for segment in key.split(SEPARATOR) {
            node = node.children.entry(segment.to_string()).or_default();
            node.subtree_keys += 1;
        }
        node.is_key = true;
        node.kind = kind;
        self.total += 1;
    }

    pub fn contains(&self, key: &str) -> bool {
        self.find(key).is_some_and(|n| n.is_key)
    }

    /// Attach a type to an already-inserted key. Types arrive after the key list, since
    /// `SCAN` returns names and typing them is a separate round trip.
    pub fn set_kind(&mut self, key: &str, kind: Kind) {
        let mut node = &mut self.root;
        for segment in key.split(SEPARATOR) {
            match node.children.get_mut(segment) {
                Some(child) => node = child,
                None => return,
            }
        }
        node.kind = Some(kind);
    }

    fn find(&self, path: &str) -> Option<&Node> {
        let mut node = &self.root;
        for segment in path.split(SEPARATOR) {
            node = node.children.get(segment)?;
        }
        Some(node)
    }

    pub fn is_expanded(&self, path: &str) -> bool {
        self.expanded.contains(path)
    }

    /// Returns the new expanded state, or `None` if the path isn't a branch.
    pub fn toggle(&mut self, path: &str) -> Option<bool> {
        let is_branch = self.find(path).is_some_and(|n| !n.children.is_empty());
        if !is_branch {
            return None;
        }
        if self.expanded.contains(path) {
            self.expanded.remove(path);
            Some(false)
        } else {
            self.expanded.insert(path.to_string());
            Some(true)
        }
    }

    pub fn expand(&mut self, path: &str) {
        if self.find(path).is_some_and(|n| !n.children.is_empty()) {
            self.expanded.insert(path.to_string());
        }
    }

    /// Expand every ancestor of a path so the path itself becomes visible.
    pub fn reveal(&mut self, path: &str) {
        let segments: Vec<&str> = path.split(SEPARATOR).collect();
        for i in 1..segments.len() {
            self.expanded.insert(segments[..i].join(":"));
        }
    }

    pub fn collapse_all(&mut self) {
        self.expanded.clear();
    }

    /// Expand every branch. Bounded by the caller -- on a huge tree this produces a huge
    /// row list, so the UI only offers it when the key count is modest.
    pub fn expand_all(&mut self) {
        let mut paths = Vec::new();
        collect_branches(&self.root, String::new(), &mut paths);
        self.expanded.extend(paths);
    }

    /// Flatten to the lines currently visible, honouring collapse state.
    ///
    /// Ordering is branches before leaves, each lexicographic -- the same convention as a
    /// file browser, so `bull:` sorts above `session:abc` regardless of alphabet.
    pub fn rows(&self) -> Vec<Row> {
        let mut out = Vec::new();
        self.walk(&self.root, String::new(), 0, &mut out);
        out
    }

    fn walk(&self, node: &Node, prefix: String, depth: usize, out: &mut Vec<Row>) {
        // Fold first, classify second. Order matters: a chain like `zzz` -> `child` folds
        // down to a single leaf row, so it must sort with the leaves. Partitioning on the
        // pre-fold node would sort it as a folder and put it above real leaves.
        let mut folded: Vec<(String, String, &Node)> = Vec::new();
        for (segment, child) in &node.children {
            let mut path = if prefix.is_empty() {
                segment.clone()
            } else {
                format!("{prefix}{SEPARATOR}{segment}")
            };
            let mut label = segment.clone();
            let mut target = child;

            // Fold `a` -> `b` -> `c` into one `a:b:c` row while each link is a pure
            // pass-through. A node that is itself a key stops the fold: it has a value to
            // select, so it needs its own row.
            if self.compact {
                while !target.is_key && target.children.len() == 1 {
                    let (seg, only) = target.children.iter().next().expect("len == 1");
                    label.push(SEPARATOR);
                    label.push_str(seg);
                    path.push(SEPARATOR);
                    path.push_str(seg);
                    target = only;
                }
            }

            folded.push((label, path, target));
        }

        // `partition` keeps the map's lexicographic order within each group.
        let (branches, leaves): (Vec<_>, Vec<_>) = folded
            .into_iter()
            .partition(|(_, _, n)| !n.children.is_empty());

        for (label, path, target) in branches.into_iter().chain(leaves) {
            let is_branch = !target.children.is_empty();
            let expanded = is_branch && self.expanded.contains(&path);

            out.push(Row {
                depth,
                label,
                path: path.clone(),
                is_key: target.is_key,
                is_branch,
                expanded,
                subtree_keys: target.subtree_keys,
                kind: target.kind,
            });

            if expanded {
                self.walk(target, path, depth + 1, out);
            }
        }
    }
}

fn collect_branches(node: &Node, prefix: String, out: &mut Vec<String>) {
    for (segment, child) in &node.children {
        if child.children.is_empty() {
            continue;
        }
        let path = if prefix.is_empty() {
            segment.clone()
        } else {
            format!("{prefix}{SEPARATOR}{segment}")
        };
        out.push(path.clone());
        collect_branches(child, path, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(keys: &[&str]) -> KeyTree {
        let mut t = KeyTree::new();
        for k in keys {
            t.insert(k);
        }
        t
    }

    fn paths(rows: &[Row]) -> Vec<&str> {
        rows.iter().map(|r| r.path.as_str()).collect()
    }

    #[test]
    fn collapsed_by_default_shows_only_top_level() {
        let t = tree(&["bull:emails:1", "bull:emails:2", "cache:user:9"]);
        // Chains fold, so the top level is one row per distinct branch point.
        assert_eq!(paths(&t.rows()), vec!["bull:emails", "cache:user:9"]);
    }

    #[test]
    fn expanding_reveals_one_level_at_a_time() {
        let mut t = tree(&["bull:emails:1", "bull:emails:2"]);
        assert_eq!(paths(&t.rows()), vec!["bull:emails"]);

        t.toggle("bull:emails");
        assert_eq!(
            paths(&t.rows()),
            vec!["bull:emails", "bull:emails:1", "bull:emails:2"]
        );
    }

    #[test]
    fn subtree_counts_roll_up() {
        let t = tree(&["bull:emails:1", "bull:emails:2", "bull:jobs:3"]);
        let rows = t.rows();
        assert_eq!(rows[0].path, "bull");
        assert_eq!(rows[0].subtree_keys, 3);
    }

    #[test]
    fn a_chain_that_folds_to_a_leaf_sorts_with_the_leaves() {
        // `zzz` is a branch in the raw tree but folds to a single leaf row. Sorting on the
        // pre-fold shape would hoist it above `aaa` as though it were a folder.
        let t = tree(&["aaa", "zzz:child"]);
        assert_eq!(paths(&t.rows()), vec!["aaa", "zzz:child"]);
    }

    #[test]
    fn reinserting_a_key_does_not_inflate_counts() {
        // SCAN can return the same key twice across cursor pages -- that is documented
        // Redis behaviour, not a bug. Counting it twice makes every total wrong.
        let mut t = tree(&["bull:emails:1"]);
        t.insert("bull:emails:1");
        assert_eq!(t.len(), 1);
        assert_eq!(t.rows()[0].subtree_keys, 1);
    }

    #[test]
    fn a_node_can_be_both_key_and_branch() {
        let mut t = tree(&["stats", "stats:daily"]);
        t.toggle("stats");
        let rows = t.rows();
        assert_eq!(rows[0].path, "stats");
        assert!(rows[0].is_key, "`stats` is itself a key");
        assert!(rows[0].is_branch, "`stats` also has children");
        assert_eq!(rows[1].path, "stats:daily");
    }

    #[test]
    fn branches_sort_before_leaves() {
        // `zzz` stays a real branch (two children), `aaa` is a leaf: folder-first ordering
        // wins over alphabetical.
        let t = tree(&["aaa", "zzz:one", "zzz:two"]);
        assert_eq!(paths(&t.rows()), vec!["zzz", "aaa"]);
    }

    #[test]
    fn toggle_returns_none_for_leaves() {
        let mut t = tree(&["solo"]);
        assert_eq!(t.toggle("solo"), None);
        assert_eq!(t.toggle("nonexistent"), None);
    }

    #[test]
    fn reveal_expands_ancestors_but_not_the_target() {
        let mut t = tree(&["a:b:c:d", "a:b:x", "a:b:c:e"]);
        t.reveal("a:b:c");
        assert!(t.is_expanded("a:b"));
        assert!(
            !t.is_expanded("a:b:c"),
            "reveal makes the target visible, not opened"
        );
        assert!(paths(&t.rows()).contains(&"a:b:c"));
    }

    #[test]
    fn refresh_keeps_expansion_state() {
        // Re-scanning must not collapse the view the user just arranged.
        let mut t = tree(&["bull:emails:1", "bull:emails:2"]);
        t.toggle("bull:emails");
        t.clear_keys();
        assert!(t.is_empty());

        t.insert("bull:emails:3");
        t.insert("bull:emails:4");
        assert_eq!(
            paths(&t.rows()),
            vec!["bull:emails", "bull:emails:3", "bull:emails:4"],
            "the expanded branch should still be open after a rescan"
        );
    }

    #[test]
    fn keys_without_separators_are_top_level_leaves() {
        let t = tree(&["mykey"]);
        let rows = t.rows();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].is_key);
        assert!(!rows[0].is_branch);
    }

    #[test]
    fn empty_key_is_ignored() {
        let mut t = KeyTree::new();
        t.insert("");
        assert!(t.is_empty());
    }

    #[test]
    fn set_kind_attaches_to_an_existing_key() {
        let mut t = tree(&["bull:emails:meta"]);
        t.set_kind("bull:emails:meta", Kind::Hash);
        t.expand_all();
        let row = t
            .rows()
            .into_iter()
            .find(|r| r.path == "bull:emails:meta")
            .unwrap();
        assert_eq!(row.kind, Some(Kind::Hash));
    }

    #[test]
    fn single_child_chains_are_folded_into_one_row() {
        // Without this, `a` and `b` are two rows you must expand to reach anything.
        let t = tree(&["a:b:c", "a:b:d"]);
        let rows = t.rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].label, "a:b");
        assert_eq!(rows[0].path, "a:b");
        assert!(rows[0].is_branch);
        assert_eq!(rows[0].subtree_keys, 2);
    }

    #[test]
    fn folding_stops_at_a_node_that_is_itself_a_key() {
        // The BullMQ shape: `bull:q:42` is a job hash and `bull:q:42:logs` is its log
        // list. Folding through `42` would hide the job itself, which is the thing you
        // actually want to open.
        let mut t = tree(&["bull:q:42", "bull:q:42:logs", "bull:q:43"]);
        assert_eq!(t.rows()[0].path, "bull:q");

        t.toggle("bull:q");
        let rows = t.rows();
        assert_eq!(rows[1].path, "bull:q:42");
        assert!(rows[1].is_key, "the job hash keeps its own row");
        assert!(rows[1].is_branch, "and still has its logs child");

        t.toggle("bull:q:42");
        assert_eq!(t.rows()[2].path, "bull:q:42:logs");
    }

    #[test]
    fn folded_branches_still_toggle_by_their_full_path() {
        let mut t = tree(&["a:b:c", "a:b:d"]);
        assert_eq!(t.toggle("a:b"), Some(true));
        assert_eq!(paths(&t.rows()), vec!["a:b", "a:b:c", "a:b:d"]);
    }

    #[test]
    fn compaction_can_be_turned_off() {
        let mut t = tree(&["a:b:c", "a:b:d"]);
        t.set_compact(false);
        let rows = t.rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].label, "a");
    }

    #[test]
    fn expand_all_reaches_folded_branches() {
        let mut t = tree(&["a:b:c", "a:b:d:e"]);
        t.expand_all();
        // Both children fold down to leaves, so they sort alphabetically rather than
        // `d` being hoisted as a folder.
        assert_eq!(paths(&t.rows()), vec!["a:b", "a:b:c", "a:b:d:e"]);
    }

    #[test]
    fn expand_all_then_collapse_all() {
        let mut t = tree(&["a:b:c", "a:d"]);
        t.expand_all();
        // `a:b` folds into its only child, so the expanded view is three rows, not four.
        assert_eq!(paths(&t.rows()), vec!["a", "a:b:c", "a:d"]);
        t.collapse_all();
        assert_eq!(paths(&t.rows()), vec!["a"]);
    }
}
