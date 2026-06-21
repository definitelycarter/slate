# Families: nested object arrays (parents, children), nested arrays
# (children[].pets), mixed schema, empty arrays. Exercises deep nesting and the
# known-divergence cases.

# ── Projection / nested access ────────────────────────────────────
SELECT VALUE c.id FROM c
SELECT c.id, c.lastName FROM c
SELECT VALUE c.address.city FROM c
SELECT VALUE c.address FROM c

# ── WHERE ─────────────────────────────────────────────────────────
SELECT VALUE c.id FROM c WHERE c.isRegistered = true
SELECT VALUE c.id FROM c WHERE c.address.state = 'WA'
SELECT VALUE c.id FROM c WHERE ARRAY_LENGTH(c.children) > 1
SELECT VALUE c.id FROM c WHERE ARRAY_CONTAINS(c.tags, 'a')

# ── ORDER BY (order-sensitive) ────────────────────────────────────
SELECT VALUE c.id FROM c ORDER BY c.creationDate ASC
SELECT VALUE c.id FROM c ORDER BY c.creationDate DESC OFFSET 1 LIMIT 1

# ── JOIN ──────────────────────────────────────────────────────────
SELECT VALUE ch.grade FROM c JOIN ch IN c.children
SELECT c.id, ch.firstName FROM c JOIN ch IN c.children WHERE ch.grade > 4
SELECT c.id, p.firstName FROM c JOIN p IN c.parents

# ── Aggregates over a join / group ────────────────────────────────
SELECT VALUE COUNT(1) FROM c
SELECT VALUE AVG(ch.grade) FROM c JOIN ch IN c.children
SELECT c.address.state AS state, COUNT(1) AS n FROM c GROUP BY c.address.state

# ── Subqueries ────────────────────────────────────────────────────
SELECT c.id, (SELECT VALUE COUNT(1) FROM ch IN c.children) AS kids FROM c
SELECT VALUE c.id FROM c WHERE EXISTS (SELECT VALUE p FROM p IN c.parents WHERE p.firstName = 'Ben')
SELECT c.id, ARRAY(SELECT VALUE ch.firstName FROM ch IN c.children) AS names FROM c
SELECT c.id, (SELECT VALUE COUNT(1) FROM ch IN c.children WHERE ch.grade > 4) AS older FROM c

# ── Former divergences, now in parity (kept as regression markers) ─
# Unqualified identifier: now BOTH error (slate rejects it; Cosmos: 400). Goldened
# as error.
SELECT id FROM c
# FROM <outer-alias> item-scoped subquery: slate now matches Cosmos (both len=1).
# Goldened as a result.
SELECT c.id, (SELECT COUNT(1) AS len FROM t) AS x FROM c JOIN t IN c.tags
