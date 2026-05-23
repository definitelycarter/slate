---
name: docs
description: Use at the end of a feature or refactor before committing. Identifies which docs are affected by the change (architecture, roadmap, README, crate improvements), updates them in the same commit, and keeps the roadmap honest about what's done and what's planned.
---

# Updating docs in slate

The rule: **docs change in the same commit as the code that made them stale.** Doc drift compounds, and the commit log has plenty of evidence that this works in practice (`35af299`, `a689fa9`, `a90a1c7`, `c1a9a46`).

This skill is for end-of-feature doc updates. It is not for writing new standalone documents — see "What not to write" at the bottom.

## 1. Affected-doc map

Pick the docs to touch based on what the change actually altered. Widen when uncertain.

| Code area / change type                        | Update these docs                                           |
|------------------------------------------------|-------------------------------------------------------------|
| Anything that lands a roadmap item             | `book/src/roadmap.md` (mark done, see step 3)               |
| Anything that adds *new* planned work          | `book/src/roadmap.md` (add a section)                       |
| Crate tier boundaries / new crate / new trait  | `book/src/architecture.md`                                  |
| Query/SQL/expression surface changes           | `book/src/querying.md`                                      |
| New benchmark or moved numbers                 | `book/src/benchmarks.md` **and** `README.md` if quoted there |
| Swift / uniffi binding changes                 | `book/src/swift.md`                                         |
| Crate-local perf TODO done                     | `crates/<crate>/improvements.md` (strike or remove)         |
| User-visible API or feature                    | `README.md` (overview / feature list)                       |

If a change touches multiple rows, take the union.

## 2. The check

Before drafting the commit, scan each doc above that *might* be affected for stale lines: code samples that won't compile, names that were renamed, claims about behavior that changed, missing entries for new features. The skill is doing this scan, not relying on memory.

```bash
# Quick grep for renamed symbols
rg '<old-name>' book/ README.md
```

If you can't find drift but the change is substantial (new feature, new crate, behavior change), the answer is almost never "no docs need updating" — look again.

## 3. Roadmap hygiene

When a roadmap item lands:

- **Move the section** out of "planned" into a "Done" subsection at the same depth, or
- **Append "— Done"** to the section heading (current convention — see `## Dynamic Primary Key Path — Done`, `## Backup — Done (Hot Backup)`).
- **Keep the design notes.** Don't delete the body — it's the historical record of *why* the feature looks the way it does. Future readers (and future you) need it.
- **Add follow-ups discovered during implementation** as new sections, even if they're tiny. Better to capture than to forget.

When adding *new* planned work, match the existing section template: heading, `### Concept`, `### Motivation`, optional `### Benefits` / `### Performance note`. See the `## Collect Node` section for the canonical shape.

## 4. README and benchmark numbers

`README.md` and `book/src/benchmarks.md` quote specific numbers. The **bench skill** owns the "update in same commit" rule for these — when bench output moves materially, both files update with the new numbers and the commit body includes the before/after. Don't re-state that rule here; just remember to run the bench skill alongside this one when perf-sensitive code changed.

## 5. Commit-time integration

Doc updates go in the **same commit** as the code change that motivated them. Two commits ("code change" then "update docs") is the wrong pattern — it leaves `main` in a stale-doc state at the intermediate commit, and the docs commit ends up without context.

The commit message body should mention doc updates explicitly when they're substantive:

```
Update docs: <area> reflects <change>
```

(See `a90a1c7`, `35af299` for precedent.)

## 6. What not to write

The CLAUDE.md / AGENTS.md guidance is to **not create unrequested `.md` files**. Specifically:

- Don't write planning docs, decision logs, "thoughts" files, or `NOTES.md` unless the user asks.
- Don't create per-feature design docs as separate files — design notes belong inside the relevant `roadmap.md` section.
- Don't write tutorial content speculatively — wait until the user signals it's wanted.
- Don't duplicate `book/src/` content into top-level `*.md` files.

If you find yourself wanting to create a new `.md` file, the answer is almost always "add a section to an existing doc" instead.

## 7. Failure modes to avoid

- Committing code that adds a feature without updating `README.md` or the architecture doc.
- Marking a roadmap item done by **deleting** the section instead of marking it done in place.
- Updating docs in a follow-up commit (leaves an intermediate stale state).
- Writing speculative planning files outside `book/src/roadmap.md`.
- Forgetting to scan `crates/<crate>/improvements.md` when a local perf TODO has been fixed.
