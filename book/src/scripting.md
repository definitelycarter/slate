# Scripting — Triggers & Validators

Collections can attach **validator** and **trigger** scripts that run during writes. Slate's scripting runtime is pluggable (`dyn ScriptRuntime`, injected via `DatabaseBuilder::with_scripting`); the Lua backend ships by default on native builds. See [Architecture](./architecture.md) for how the runtime is wired.

Mutations (insert, update, replace, delete, upsert) can have **triggers** and **validators** attached to a collection. These are resolved at plan time from a `HookSnapshot` — a frozen view of all registered hooks captured when the transaction begins. The planner wraps the mutation pipeline with typed nodes; the executor just runs them.

### Node Types

**`Node::Validate`** — a gate. Runs each validator function against the document. If any validator returns `{ ok: false, reason: "..." }`, the mutation is rejected with an error. On success, the document passes through unchanged. Validators are pure functions — they receive `{ doc: <the document> }` and have no access to the database.

**`Node::Trigger`** — a tap. Fires each trigger function as a side effect, passing the document through unchanged. Triggers receive `{ action: "<event>", doc: <the document> }` and have read-write access to the database via a scoped context (`ctx.get`, `ctx.put`, `ctx.delete`).

**`Plan::Trigger`** — identical behavior to `Node::Trigger`, but wraps an entire `Plan` instead of a `Node`. Used for after-mutation triggers that need to see the result of the mutation.

### Lifecycle Events

Each mutation type fires before/after trigger pairs:

| Mutation | Before | After |
|----------|--------|-------|
| Insert | `inserting` | `inserted` |
| Update | `updating` | `updated` |
| Replace | `updating` | `updated` |
| Delete | `deleting` | `deleted` |

Before-triggers see the **source** document (old state for updates, new doc for inserts). After-triggers see the **result** document (the doc as written to storage).

### Plan Trees

**Insert with validators and triggers:**

```
Plan::Trigger { action: "inserted", hooks }
  └── Plan::Insert { collection }
        └── Node::Trigger { action: "inserting", hooks }
              └── Node::Validate { validators }
                    └── Node::Values([...docs])
```

**Update with triggers (no validation — mutation applies to existing docs):**

```
Plan::Trigger { action: "updated", hooks }
  └── Plan::Update { collection, mutation }
        └── Node::Trigger { action: "updating", hooks }
              └── Node::Validate { validators }
                    └── Filter → KeyLookup → IndexScan   (or Filter → Scan)
```

**Delete (no validation — nothing being written):**

```
Plan::Trigger { action: "deleted", hooks }
  └── Plan::Delete { collection }
        └── Node::Trigger { action: "deleting", hooks }
              └── Filter → Scan
```

The delete node yields full documents (not `None`) so the after-trigger can see what was deleted.

**Upsert — special case:**

Upsert's trigger actions are runtime-conditional: `inserting`/`inserted` for new docs, `updating`/`updated` for existing docs. Because the action depends on whether the document already exists, triggers are fired internally by the upsert node rather than as separate plan wrappers. Validators still apply as a `Node::Validate` in the source pipeline.

### Hook Resolution

Hooks are not discovered at execution time. The planner resolves them from the `HookSnapshot`:

1. `HookSnapshot` is captured at `begin()` time via `ArcSwap` (lock-free load)
2. Planner calls `snapshot.validators_for(cf, collection)` and `snapshot.triggers_for(cf, collection)`
3. If hooks exist, the planner wraps the pipeline with the appropriate nodes
4. If no hooks exist, no wrapper nodes are added — zero overhead for collections without scripts

When a transaction commits after modifying hooks (`register_trigger`, `register_validator`, `drop_trigger`, etc.), the `HookRegistry` is swapped with a fresh snapshot so subsequent transactions see the updated hooks.

