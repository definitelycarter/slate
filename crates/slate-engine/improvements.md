# slate-engine Performance Improvements

Remaining optimization opportunities for the encoding / key layer.

## Minor Wins

- **Reusable key buffers in encoding.** `Key::encode()` and `Key::encode_index()`
  (`src/encoding/key.rs`) allocate a fresh `Vec<u8>` per call, on the hot per-record
  and per-index-entry write/scan paths. `*_into(&mut Vec<u8>)` variants — or a
  `KeyEncoder` holding a reusable buffer — would let callers amortize the allocation
  across a batch (~6–10% fewer allocs) at the cost of a slightly more verbose API.
  Measure with the write/scan benches before settling on the API shape.
