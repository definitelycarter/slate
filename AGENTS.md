## Code rules

These apply whenever you write Rust in this repo, not just at commit time.

- Follow Rust idioms and best practices.
- Avoid `clone()` in production code — provide justification if proposing it (acceptable in tests).
- Avoid `unwrap()`, `expect()`, and other panic-prone error handling in production code (acceptable in tests).
- Avoid `.ok()` to silently discard errors in production code — propagate with `?` or `map_err` instead (acceptable in tests and in `sort_by` closures where returning `Result` is not possible).
- Add tests for new functionality.

## Workflow rules

- Do not automatically commit or push — wait for explicit user approval.
- Run `cargo fmt` and `cargo test` before proposing a commit.

## Commands

```bash
cargo build   # build everything
cargo test    # run all tests
cargo fmt     # format
cargo bench   # full bench suite (slow — usually run a single harness instead)
```

## Skills

Workflow recipes live in `.claude/skills/`. Invoke by name when the matching activity comes up.

- **commit** — pre-flight gates, forbidden-pattern audit, message style, staging, `Co-Authored-By` trailer. Use when the user asks to commit.
- **bench** — affected-bench map by source area, before/after baseline workflow, noise thresholds, README sync. Use when running, adding, or interpreting benchmarks.
- **docs** — affected-doc map (architecture, roadmap, README), roadmap hygiene, "update in same commit" rule. Use at the end of a feature or refactor before committing.

## Docs layout

- `README.md` — top-level overview, quoted benchmark numbers.
- `book/src/architecture.md` — crate tiers and how they fit together.
- `book/src/querying.md` — query surface, examples.
- `book/src/benchmarks.md` — methodology and detailed results.
- `book/src/roadmap.md` — planned and completed work.
- `book/src/swift.md` — Swift/Apple bindings.
- `crates/<crate>/improvements.md` — crate-local perf TODOs (informal).
