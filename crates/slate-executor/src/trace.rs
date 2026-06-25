//! Feature-gated `tracing` shims.
//!
//! The executor instruments its seams with the macros here. With the `trace`
//! feature **off** (the default), each macro expands to nothing — `tracing` is
//! not even a dependency, so the default build (and wasm) carries zero cost and
//! no extra code. With `trace` **on**, they forward to `tracing`, and the host
//! application wires its own subscriber to consume the spans/events.
//!
//! Keeping the call sites behind these macros (rather than `#[cfg(...)]` at every
//! site) keeps the instrumented code readable: one `trace_event!("scan", ...)`
//! line reads the same whether the feature is on or off.

/// Enter a span for the duration of the current scope. Expands to a guard binding
/// when `trace` is on, and to nothing when off.
///
/// Usage: `trace_scope!("execute", node = node_name);`
macro_rules! trace_scope {
    ($name:expr $(, $field:ident = $value:expr)* $(,)?) => {
        #[cfg(feature = "trace")]
        let _slate_trace_span = ::tracing::trace_span!($name $(, $field = $value)*).entered();
    };
}

/// Emit a trace-level event. Expands to a `tracing::trace!` call when `trace` is
/// on, and to nothing (the expressions are not evaluated) when off.
///
/// Usage: `trace_event!("scan opened", collection = name);`
macro_rules! trace_event {
    ($($arg:tt)+) => {
        #[cfg(feature = "trace")]
        ::tracing::trace!($($arg)+);
    };
}

pub(crate) use trace_event;
pub(crate) use trace_scope;
