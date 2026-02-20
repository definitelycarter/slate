# slate-store / slate-db Performance Improvements

Remaining optimization opportunities.

## Minor Wins

- **Reusable key buffers in encoding:** `record_key()`, `index_key()` allocate per call; `_into(&mut Vec<u8>)` variants reduce allocs ~6-10% but add code complexity. Consider a `KeyEncoder` struct for cleaner API.
- **Duplicate encode_value calls:** Cache encoded index values across scan + key construction
