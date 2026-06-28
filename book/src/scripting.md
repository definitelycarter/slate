# Triggers & Validators

Collections can attach **validators** (constraints) and **triggers** (side effects) that run during writes.

Both are **native Rust functions** — registered in a database-scoped bag and bound per collection (the [native-functions](./rfcs/native-functions.md) model, shared with UDFs), never code stored in the database. A validator is `Fn(&ValidatorCtx) -> Result<Verdict, ValidatorError>`; a trigger is `Fn(&TriggerCtx) -> Result<(), TriggerError>`. There is no embedded VM. See [Architecture](./architecture.md) for how each is wired.

Either way the *shape* is the same: mutations (insert, update, replace, delete, upsert) have their hooks **resolved at plan time** from a `HookSnapshot` — a frozen view captured when the transaction begins — and the planner wraps the mutation pipeline with typed nodes the executor runs.

### Node Types

**`Node::Validate`** — a gate. Runs each bound native validator against the document. A validator is `Fn(&ValidatorCtx) -> Result<Verdict, ValidatorError>`; if any returns `Verdict::Reject(reason)` — or errors, or panics — the mutation is rejected and the write is aborted (fail-safe). On success, the document passes through unchanged. Validators are `Pure`: `ValidatorCtx` exposes only the candidate document (`ctx.doc()`), so a validator structurally cannot touch the database. The node carries `(validator_name, native_function)` bindings; the executor resolves each against the live validator bag once per query — a dangling binding (bound but unregistered) aborts the write up front.

**`Node::Trigger`** — a tap. Fires each bound native trigger as a side effect, passing the document through unchanged. A trigger is `Fn(&TriggerCtx) -> Result<(), TriggerError>`: `ctx.action()` is the firing event, `ctx.doc()` the candidate document, and it has read-write access to *other* documents via `ctx.get`/`ctx.put`/`ctx.delete`/`ctx.merge` (the last a field-merge upsert: overlay a document's fields onto the row with the same pk, inserting it if absent). That access is **confined to the firing column family** — the context names a collection, never a cf, so cross-cf access is structurally impossible. An error or panic aborts the write (fail-safe). Like validators, the node carries `(trigger_name, native_function)` bindings, resolved against the live trigger bag once per query — a dangling binding aborts the write up front.

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
Plan::Trigger { action: "inserted", triggers }
  └── Plan::Insert { collection }
        └── Node::Trigger { action: "inserting", triggers }
              └── Node::Validate { validators }
                    └── Node::Values([...docs])
```

**Update with triggers (no validation — mutation applies to existing docs):**

```
Plan::Trigger { action: "updated", triggers }
  └── Plan::Update { collection, mutation }
        └── Node::Trigger { action: "updating", triggers }
              └── Node::Validate { validators }
                    └── Filter → KeyLookup → IndexScan   (or Filter → Scan)
```

**Delete (no validation — nothing being written):**

```
Plan::Trigger { action: "deleted", triggers }
  └── Plan::Delete { collection }
        └── Node::Trigger { action: "deleting", triggers }
              └── Filter → Scan
```

The delete node yields full documents (not `None`) so the after-trigger can see what was deleted.

**Upsert — special case:**

Upsert's trigger actions are runtime-conditional: `inserting`/`inserted` for new docs, `updating`/`updated` for existing docs. Because the action depends on whether the document already exists, triggers are fired internally by the upsert node rather than as separate plan wrappers. Validators still apply as a `Node::Validate` in the source pipeline.

### Hook Resolution

Hooks are not discovered at execution time. The planner resolves them from the `HookSnapshot`:

1. `HookSnapshot` is captured at `begin()` time via `ArcSwap` (lock-free load).
2. The planner calls `snapshot.validators_for(cf, collection)` and `snapshot.triggers_for(cf, collection)`. Each yields its `(name, native_function)` bindings — the code resolves from the live bag at exec time.
3. If hooks exist, the planner wraps the pipeline with the appropriate nodes.
4. If no hooks exist, no wrapper nodes are added — zero overhead for collections without hooks.

When a transaction commits after modifying hooks (`triggers().create`, `validators().create`, `validators().register`, `triggers().remove`, etc.), the `HookRegistry` is swapped with a fresh snapshot so subsequent transactions see the updated bindings.

