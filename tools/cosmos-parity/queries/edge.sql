# Edge behavior — valid queries whose result hinges on undefined handling and
# JOIN semantics. Parity means the SAME result (usually omission / empty), not
# an error. Run against the products dataset.

# ── Missing property is omitted, not an error ─────────────────────
SELECT c.missing FROM c
SELECT c.id, c.missing FROM c
SELECT VALUE c.missing FROM c
SELECT c.missing.deeper FROM c
SELECT VALUE IS_DEFINED(c.missing) FROM c
SELECT VALUE IS_DEFINED(c.price) FROM c

# ── Array indexing ────────────────────────────────────────────────
SELECT VALUE c.tags[0] FROM c
SELECT VALUE c.tags[99] FROM c

# ── Arithmetic / functions on undefined or wrong types → undefined ─
SELECT VALUE c.missing + 1 FROM c
SELECT VALUE c.price + c.name FROM c
SELECT VALUE UPPER(c.price) FROM c
SELECT VALUE 1 / 0
SELECT VALUE 5 % 0

# ── Cross-type comparison in WHERE → not true → row dropped ────────
SELECT VALUE c.id FROM c WHERE c.price > 'x'
SELECT VALUE c.id FROM c WHERE c.name > 100

# ── JOIN edge cases ───────────────────────────────────────────────
# Join on a non-array (string) value: inner-join semantics → no rows
SELECT VALUE x FROM c JOIN x IN c.name
# Join on a missing property → no rows
SELECT VALUE x FROM c JOIN x IN c.missing
# Field access on a scalar array element → undefined → dropped
SELECT VALUE t.foo FROM c JOIN t IN c.tags
# Double join over the same array → cross product
SELECT c.id FROM c JOIN a IN c.tags JOIN b IN c.tags
