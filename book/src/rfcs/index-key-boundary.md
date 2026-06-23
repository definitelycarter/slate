# RFC: Index Key Value/Doc-Id Boundary (variable-width)

> **Status: implemented.** Extracted from the roadmap; the
> [roadmap](../roadmap.md) tracks status at a glance.

An `i` index key is `i\0{collection}\0{field}\0{value_bytes}{doc_id_lp}` with no
delimiter between the value and the length-prefixed doc_id. For **fixed-width**
value types the decoder derives the value length from the entry's type byte
(`index_value_len`), so the boundary is unambiguous — this fixed numeric/date index
scans, which previously crashed with `malformed value in index key` when a sortable
number's bytes happened to resemble a length-prefixed doc_id header.

**Variable-width** values (strings) used to locate the boundary by scanning backwards
for a parseable trailing doc_id (`split_trailing_doc_id`). That scan was ambiguous —
the value bytes plus the doc_id could admit more than one valid split, so a string
index could silently mis-decode a few entries (observed: a `status = "active"` index
scan undercounting a full scan by 3 on a 52k corpus). An undercount is a *false
negative*, which the residual recheck cannot repair.

## The fix (landed)

The boundary is now deterministic, not guessed. A string (variable-width) index key
carries a trailing **`u32` value-length suffix**:
`i\0{collection}\0{field}\0{value_bytes}{doc_id_lp}{value_len:u32}`. The decoder
reads `value_len` directly; `split_trailing_doc_id` is gone. The suffix sits *after*
the doc_id, so it never affects prefix scans or key ordering, and `u32` (not `u16`)
means an indexed string over 64 KiB can't truncate the recorded length. **Fixed-width
keys are byte-for-byte unchanged** — they keep deriving their length from the type
byte. Decoding an `i` key now always requires the entry's metadata type byte, so the
key-only `Key::decode_index` / `split_trailing_doc_id` path was removed; index entries
are read only via `IndexRecord` / `IndexEntry`, which carry the metadata.

This is an index-key encoding change (chosen approach 2 of the two below), so it is
**versioned** (a marker in `_sys_`) with a re-index migration. The two candidate
encodings were: (1) store the value length in the entry metadata, or (2) length-suffix
the value so it reads back-to-front — (2) was taken because it leaves the metadata/TTL
layout untouched and keeps the fixed-width path identical.

**Migration.** On open the engine compares the stored index-encoding version and, when
behind, **rebuilds every collection's `i` entries from the records** in one atomic
transaction, then stamps the version. Records are the unambiguous source of truth, so
the rebuild can't inherit the boundary bug it repairs; a failure rolls back and retries
on the next open (never half-migrated). Unique (`u`) entries are untouched — their
value runs to the end of the key with no doc_id suffix, so the boundary fix doesn't
affect them.
