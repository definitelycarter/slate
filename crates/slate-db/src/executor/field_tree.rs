use std::collections::HashMap;

use bson::raw::{RawBsonRef, RawDocument};

/// A pre-built tree of dot-notation field paths.
///
/// Given `["foo.bar.baz", "foo.bar.bux", "name"]`, builds:
/// ```text
/// { "foo": Branch({ "bar": Branch({ "baz": Leaf("foo.bar.baz"), "bux": Leaf("foo.bar.bux") }) }),
///   "name": Leaf("name") }
/// ```
///
/// Built once, reused across all documents. No per-document allocations.
///
/// Each Leaf stores the original full dotted path so that consumers
/// (filter, sort, index ops) can use it without reconstructing the string.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FieldTree {
    /// Take the entire field value. Carries the original full path.
    Leaf(String),
    /// Recurse into sub-fields.
    Branch(HashMap<String, FieldTree>),
}

impl FieldTree {
    /// Build a tree from a list of dot-notation paths.
    pub(crate) fn from_paths(paths: &[String]) -> HashMap<String, FieldTree> {
        let mut root: HashMap<String, FieldTree> = HashMap::new();
        for path in paths {
            insert_path(&mut root, path, path);
        }
        root
    }
}

/// Walk a document once, visiting every field that matches the tree.
///
/// **Expands arrays**: if a Leaf field is an array, the visitor is called
/// once per element. If a Branch field is an array of documents, each
/// document element is recursed into. Use this for Distinct, index ops,
/// and anywhere you need individual scalar values.
pub(crate) fn walk<'a, F>(doc: &'a RawDocument, tree: &HashMap<String, FieldTree>, mut visitor: F)
where
    F: FnMut(&str, RawBsonRef<'a>),
{
    walk_inner(doc, tree, &mut visitor, true);
}

fn walk_inner<'a, F>(
    doc: &'a RawDocument,
    tree: &HashMap<String, FieldTree>,
    visitor: &mut F,
    expand_leaf_arrays: bool,
) where
    F: FnMut(&str, RawBsonRef<'a>),
{
    for entry in doc.iter() {
        let (key, value) = match entry {
            Ok(kv) => kv,
            Err(_) => continue,
        };
        match tree.get(key.as_str()) {
            Some(FieldTree::Leaf(full_path)) => {
                if expand_leaf_arrays
                    && let RawBsonRef::Array(arr) = value
                {
                    for v in arr.into_iter().flatten() {
                        visitor(full_path, v);
                    }
                    continue;
                }
                visitor(full_path, value);
            }
            Some(FieldTree::Branch(children)) => match value {
                RawBsonRef::Document(sub_doc) => {
                    walk_inner(sub_doc, children, visitor, expand_leaf_arrays);
                }
                RawBsonRef::Array(arr) => {
                    // Array traversal: iterate elements, recurse into documents.
                    for elem in arr {
                        if let Ok(RawBsonRef::Document(sub_doc)) = elem {
                            walk_inner(sub_doc, children, visitor, expand_leaf_arrays);
                        }
                    }
                }
                _ => {}
            },
            None => {}
        }
    }
}

fn insert_path(map: &mut HashMap<String, FieldTree>, full_path: &str, remaining: &str) {
    match remaining.split_once('.') {
        None => {
            // Leaf — takes the whole field. Overrides any existing Branch
            // (if someone asks for both "foo" and "foo.bar", "foo" wins).
            map.insert(
                remaining.to_string(),
                FieldTree::Leaf(full_path.to_string()),
            );
        }
        Some((top, rest)) => {
            if rest == "[]" {
                // `tags.[]` — the field is an array; treat as Leaf.
                // The walker expands arrays at Leaf level.
                map.insert(top.to_string(), FieldTree::Leaf(full_path.to_string()));
            } else {
                // Strip `[].` — e.g. `tags.[].name` becomes recursion into `name`
                // under the `tags` branch. The walker handles Branch + Array
                // by iterating array elements and recursing into documents.
                let rest = rest.strip_prefix("[].").unwrap_or(rest);
                let entry = map
                    .entry(top.to_string())
                    .or_insert_with(|| FieldTree::Branch(HashMap::new()));
                if let FieldTree::Branch(children) = entry {
                    insert_path(children, full_path, rest);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bson::rawdoc;

    use super::*;

    #[test]
    fn flat_fields() {
        let paths = vec!["name".into(), "status".into()];
        let tree = FieldTree::from_paths(&paths);
        assert_eq!(tree.get("name"), Some(&FieldTree::Leaf("name".into())));
        assert_eq!(tree.get("status"), Some(&FieldTree::Leaf("status".into())));
        assert_eq!(tree.get("missing"), None);
    }

    #[test]
    fn nested_fields() {
        let paths = vec!["foo.bar.baz".into(), "foo.bar.bux".into()];
        let tree = FieldTree::from_paths(&paths);
        match tree.get("foo") {
            Some(FieldTree::Branch(foo)) => match foo.get("bar") {
                Some(FieldTree::Branch(bar)) => {
                    assert_eq!(bar.get("baz"), Some(&FieldTree::Leaf("foo.bar.baz".into())));
                    assert_eq!(bar.get("bux"), Some(&FieldTree::Leaf("foo.bar.bux".into())));
                }
                other => panic!("expected Branch for bar, got {:?}", other),
            },
            other => panic!("expected Branch for foo, got {:?}", other),
        }
    }

    #[test]
    fn leaf_overrides_branch() {
        let paths = vec!["foo.bar".into(), "foo".into()];
        let tree = FieldTree::from_paths(&paths);
        assert_eq!(tree.get("foo"), Some(&FieldTree::Leaf("foo".into())));
    }

    #[test]
    fn branch_does_not_override_leaf() {
        let paths = vec!["foo".into(), "foo.bar".into()];
        let tree = FieldTree::from_paths(&paths);
        assert_eq!(tree.get("foo"), Some(&FieldTree::Leaf("foo".into())));
    }

    #[test]
    fn mixed_flat_and_nested() {
        let paths = vec!["name".into(), "address.city".into(), "address.zip".into()];
        let tree = FieldTree::from_paths(&paths);
        assert_eq!(tree.get("name"), Some(&FieldTree::Leaf("name".into())));
        match tree.get("address") {
            Some(FieldTree::Branch(addr)) => {
                assert_eq!(
                    addr.get("city"),
                    Some(&FieldTree::Leaf("address.city".into()))
                );
                assert_eq!(
                    addr.get("zip"),
                    Some(&FieldTree::Leaf("address.zip".into()))
                );
            }
            other => panic!("expected Branch for address, got {:?}", other),
        }
    }

    #[test]
    fn walk_flat() {
        let doc = rawdoc! { "name": "Alice", "status": "active", "extra": 42 };
        let paths = vec!["name".into(), "status".into()];
        let tree = FieldTree::from_paths(&paths);

        let mut visited = Vec::new();
        walk(&doc, &tree, |path, val| {
            visited.push((path.to_string(), format!("{val:?}")));
        });

        assert_eq!(visited.len(), 2);
        assert!(visited.iter().any(|(p, _)| p == "name"));
        assert!(visited.iter().any(|(p, _)| p == "status"));
    }

    #[test]
    fn walk_nested() {
        let doc = rawdoc! {
            "name": "Alice",
            "address": { "city": "NYC", "zip": "10001", "state": "NY" }
        };
        let paths = vec!["name".into(), "address.city".into(), "address.zip".into()];
        let tree = FieldTree::from_paths(&paths);

        let mut visited = Vec::new();
        walk(&doc, &tree, |path, _val| {
            visited.push(path.to_string());
        });

        assert_eq!(visited.len(), 3);
        assert!(visited.contains(&"name".to_string()));
        assert!(visited.contains(&"address.city".to_string()));
        assert!(visited.contains(&"address.zip".to_string()));
    }

    #[test]
    fn walk_missing_field() {
        let doc = rawdoc! { "name": "Alice" };
        let paths = vec!["name".into(), "missing".into()];
        let tree = FieldTree::from_paths(&paths);

        let mut count = 0;
        walk(&doc, &tree, |_, _| count += 1);
        assert_eq!(count, 1); // only "name" visited
    }

    #[test]
    fn walk_leaf_array_expands() {
        let doc = rawdoc! { "tags": ["a", "b", "c"] };
        let paths = vec!["tags".into()];
        let tree = FieldTree::from_paths(&paths);

        let mut visited = Vec::new();
        walk(&doc, &tree, |path, val| {
            visited.push((path.to_string(), format!("{val:?}")));
        });

        assert_eq!(visited.len(), 3);
        assert!(visited.iter().all(|(p, _)| p == "tags"));
    }

    #[test]
    fn walk_branch_array_of_docs() {
        let doc = rawdoc! {
            "items": [
                { "name": "A", "price": 10 },
                { "name": "B", "price": 20 }
            ]
        };
        let paths = vec!["items.name".into()];
        let tree = FieldTree::from_paths(&paths);

        let mut visited = Vec::new();
        walk(&doc, &tree, |path, _val| {
            visited.push(path.to_string());
        });

        assert_eq!(visited.len(), 2);
        assert!(visited.iter().all(|p| p == "items.name"));
    }

    #[test]
    fn walk_nested_not_a_document() {
        let doc = rawdoc! { "foo": "not_a_doc" };
        let paths = vec!["foo.bar".into()];
        let tree = FieldTree::from_paths(&paths);

        let mut count = 0;
        walk(&doc, &tree, |_, _| count += 1);
        assert_eq!(count, 0); // "foo" isn't a document, so "foo.bar" not visited
    }
}
