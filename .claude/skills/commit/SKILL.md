---
name: commit
description: Use when the user asks to commit changes in this repo. Runs format/test/lint gates, audits for forbidden patterns, drafts a message in repo style, and stages specific files. Never auto-commits without explicit user approval.
---

# Committing changes in slate

Only commit when the user explicitly asks for it. Never amend; always create a new commit. Always include a `Co-Authored-By` trailer (see step 5).

## 1. Survey the change

Run in parallel:

- `git status` (no `-uall` flag)
- `git diff` (staged + unstaged)
- `git log --oneline -10` (to match existing message style)

Read the diff fully before drafting anything. The message describes *why*, not just *what*, so you need to understand the change.

## 2. Pre-flight gates

These must all pass before proposing a commit. Run in parallel where possible.

- `cargo fmt` — applies formatting; if it changes files, include them in the commit
- `cargo test` — all tests must pass
- New functionality must ship with tests. If the diff adds public behavior without a test, stop and surface that to the user before committing.

## 3. Production-code audit

Grep the diff (not the whole repo) for forbidden patterns in non-test code:

- `unwrap()`, `expect(` — propagate errors with `?` or `map_err` instead
- `.ok()` discarding errors — `sort_by` closures are the only exception
- `clone()` — must have a justification in the message or be removed

Test code (`#[cfg(test)]` blocks, `tests/` dirs, `*_test.rs`, `*_tests.rs`) is exempt. If you find a violation in production code, fix it or flag it before committing — do not commit through it.

## 4. Performance check (when applicable)

If the diff touches a perf-sensitive area (planner, executor, encoding, kv, storage backends, mutation paths, vm), follow the **bench skill** — it has the affected-bench map, the before/after workflow, and the noise threshold.

Two rules that intersect with committing specifically:

- **Material regressions (> ~10%) must be surfaced to the user before committing** — never commit through one silently.
- **README numbers stay in sync.** If a bench result quoted in `README.md` moved materially, update it in this same commit (precedent: `52d64db`). When a number moved materially, include the before/after in the commit body (e.g. `scan 20k: 142ms → 98ms (-31%)`).

## 4b. Docs check (when applicable)

If this commit lands a feature, ships a roadmap item, changes user-visible behavior, or renames things referenced in docs, follow the **docs skill** — it has the affected-doc map and roadmap hygiene rules. Doc updates land in the **same commit** as the code change.

## 5. Draft the message

Match the style of recent commits (`git log --oneline -20`). Conventions:

- **Subject**: imperative mood, under ~72 chars, no trailing period. Start with a verb: `Add`, `Fix`, `Update`, `Refactor`, `Use`, `Move`, `Feature-gate`, `Reject`, etc.
- **Body** (only when the change needs explanation): blank line, then bullets or short paragraphs covering *why* and any non-obvious mechanics. Wrap at ~72 chars.
- Subject-only is fine for small, self-explanatory changes (see `52d64db`, `c1a9a46`).
- Multi-area changes get bullets per area (see `bb8e5dd` for the pattern).
- **Always** end with a blank line followed by a `Co-Authored-By` trailer naming the model currently running this session. Use the model's display name as given in the environment context (e.g. `Opus 4.7 (1M context)`, `Sonnet 4.6`, `Haiku 4.5`). Format:

  ```
  Co-Authored-By: Claude <display-name> <noreply@anthropic.com>
  ```

  Do **not** hard-code a specific model — read the current one from the environment each time.

## 6. Stage and commit

Stage files by name, not `git add .` or `-A` — protects against accidentally including `.env`, scratch files, or unrelated worktrees.

```bash
git add path/to/file1.rs path/to/file2.rs
```

For multi-line messages, use a HEREDOC:

```bash
git commit -m "$(cat <<'EOF'
Subject line under 72 chars

Body paragraph or bullets explaining why. Wrap at ~72 chars.

Co-Authored-By: Claude <model-name> <noreply@anthropic.com>
EOF
)"
```

For subject-only commits, use `-m` twice so the trailer lands in its own paragraph:

```bash
git commit -m "Subject line" -m "Co-Authored-By: Claude <model-name> <noreply@anthropic.com>"
```

After commit, run `git status` to confirm a clean tree (or to verify the intended unstaged files were left out).

## 7. Do not push

Never `git push` unless the user explicitly says so. AGENTS.md is explicit on this.

## Failure modes to avoid

- Committing through a failing `cargo test` "because the failure looks unrelated" — confirm with the user first.
- Amending a previous commit instead of creating a new one.
- Using `--no-verify` to skip hooks.
- Staging files you didn't read in the diff.
- Drafting a message before reading the actual changes (results in generic "Update X" messages that don't explain why).
