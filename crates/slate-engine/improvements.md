# slate-engine Performance Improvements

Remaining optimization opportunities for the encoding / key layer.

## Minor Wins

- ~~**Reusable key buffers in encoding.**~~ *(done — 2026-06-22)* Added a
  borrowed-parts index-key encoder — `Key::encode_index_key_into(&mut Vec<u8>, …)`
  and its allocating wrapper `Key::encode_index_key(…)` — and switched the
  per-index-entry hot path (`IndexRecord::encode`) to it, replacing the old
  `Key::Index(…).encode_index(value_bytes)`. The real win turned out to be
  eliminating the per-entry `doc_id.clone()` that building the `Key::Index` enum
  forced (a heap alloc, since write-path doc_ids are `Cow::Owned`), not buffer
  amortization: each `IndexRecord` owns its key `Vec`, so a single reused buffer
  can't back a whole batch. The now-superseded `Key::encode_index` was removed.

  Measured on the `engine` bench, interleaved before/after to control for machine
  drift (the untouched `index_scan` path is the noise baseline):

  | bench | before | after | Δ |
  |-------|-------:|------:|---:|
  | put/100 | 155.3 µs | 148.9 µs | −3.9% |
  | put_nx/100 | 154.6 µs | 148.8 µs | −3.6% |
  | put_overwrite/100 | 250.3 µs | 242.6 µs | −3.0% |
  | put_overwrite/1000 | 2.982 ms | 2.882 ms | −2.7% |
  | delete/100 | 127.1 µs | 123.3 µs | −2.8% |
  | index_scan_eq/1000 (untouched control) | 28.24 µs | 27.60 µs | −2.1% |

  The win is **real and reproducible** — write benches moved negative across two
  runs with opposite-sign machine drift — but **modest** (~2–3% raw, ~1–2% net of
  the ~2% drift the control bench shows), below the repo's 5% "material" bar. Kept
  anyway because it removes a production `clone()` (which AGENTS.md discourages) and
  is a net call-site simplification, not added complexity. The `*_into` variant
  remains available for any future caller that can amortize a buffer across a batch.

## Correctness hardening (compound indexes)

- **Pin the single/compound prefix-isolation invariant. (SHIPPED)** A single-field
  full scan on `status` seeks `[i\0{coll}\0status\0, i\0{coll}\0status\x01)`. A
  compound index `status\x01created_at` stores keys at
  `i\0{coll}\0status\x01created_at\0…`, which sort *at/after* that exclusive upper
  bound and are correctly excluded — safe only because **`FIELD_SEP` (0x01) > `SEP`
  (0x00)**. Now guarded by: (1) a `const _: () = assert!(FIELD_SEP > SEP, …)`
  static assertion + comment in `encoding/key.rs`; (2) a regression test
  (`tests/kv.rs::single_and_compound_index_prefixes_do_not_leak`) — one collection
  holding both `create_index("status")` and
  `create_compound_index(["status","created_at"])`, asserting neither scan sweeps in
  the other's entries.

- **Specialize the single-field decode hot path. (SHIPPED — value-side offsets, no
  enum.)** The single/compound decoder fork is gone. Index-entry decode is now a
  single value-side-offsets path: the per-component value/doc_id boundaries are
  precomputed on the *value* side at write time — metadata is
  `[t1…tN][end_1…end_N : u32 LE][ttl?]`, where `end_k` is the cumulative value
  length through component `k` — and the key drops its trailing var-width length
  suffixes (the offsets replace them). `IndexEntry::from_raw(key, metadata,
  field_prefix_len, n)` just bounds-checks and stores four fields (no offset
  resolution loop, flat in N); accessors read each `end_k` O(1) from the metadata.
  A throwaway decode/encode micro-bench (since removed) confirmed this matches the
  single-field fast path at N=1 and beats the old general compound decoder, flat in
  N. Guardrails: `traits.rs` `from_raw_decodes_production_entry` /
  `from_raw_rejects_short_metadata` (scan-path decoder + its header bounds check),
  the `index_record.rs` `from_pair` round-trip tests, the prefix-isolation test, and
  `tests/kv.rs::string_index_full_scan_is_byte_sorted` (the raw value-concat sort
  property now that suffixes are gone). Deleted: the `IndexEntry` `Single`/`Compound`
  enum, `from_single_raw`/`from_compound_raw`, `compound_value_offsets`,
  `index_value_len`, `index_value_is_var_width`, `fixed_value_len`, and the
  `ValueOffsets`/`FixedLenCache` aliases.
