# slate-db Improvements

Informal crate-local TODOs for the db layer.

## Deferred Refactors

- **Unify the `create_*_index` family.** The create-index methods on the db
  `Transaction` have proliferated — `create_index`, `create_unique_index`,
  `create_compound_index`, `create_unique_compound_index`, and now
  `create_vector_index`. These should collapse into a single create-index API
  (e.g. one method taking an index-spec enum) rather than a method per index
  flavor. A user-requested future refactor — deferred for now (do not do it as
  part of the vector-index work).
