# Negative tests — queries that SHOULD be rejected. Parity here means both
# engines error (the harness counts "both error" as a match). A row where slate
# accepts what Cosmos rejects (or vice versa) is a real divergence.
# Run against the products dataset.

# ── Malformed syntax ──────────────────────────────────────────────
SELECT FROM c
SELECT * FROM
SELECT VALUE FROM c
SELECT c. FROM c
SELECT * FROM c WHERE
SELECT * FROM c WHERE c.price =
SELECT * FROM c ORDER BY
SELECT * FROM c GROUP BY
SELECT VALUE (1 + 2 FROM c
SELECT VALUE 1 +

# ── SELECT * without a single source ──────────────────────────────
SELECT *

# ── Unqualified identifiers (Cosmos: must be fully qualified) ──────
SELECT id FROM c
SELECT VALUE name FROM c
SELECT * FROM c WHERE foo = 1

# ── Aggregate mixed with an ungrouped column, no GROUP BY ──────────
SELECT c.category, COUNT(1) FROM c

# ── Reserved word as an alias (TOP) ───────────────────────────────
SELECT c.price AS top FROM c

# ── Unknown function / wrong arity ────────────────────────────────
SELECT VALUE NOPE(1)
SELECT VALUE ABS()
SELECT VALUE ABS(1, 2)
SELECT VALUE UPPER()
