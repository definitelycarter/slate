//! Feature-gated `tracing` shims for `slate-db` (mirrors the executor's).
//!
//! With the `trace` feature off (default), these expand to nothing and `tracing`
//! is not a dependency — zero cost, wasm-safe. With it on, they forward to
//! `tracing` and the host wires its own subscriber.

/// Emit a debug-level event. Expands to a `tracing::debug!` call when `trace` is
/// on, and to nothing when off.
macro_rules! trace_event {
    ($($arg:tt)+) => {
        #[cfg(feature = "trace")]
        ::tracing::debug!($($arg)+);
    };
}

pub(crate) use trace_event;
