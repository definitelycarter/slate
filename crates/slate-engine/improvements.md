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
