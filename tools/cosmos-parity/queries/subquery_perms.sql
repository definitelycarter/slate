# Subquery / JOIN permutations — vetting the "(SELECT … FROM t) … JOIN t IN …"
# family against Cosmos. Run against products (c.id, c.tags: 3,2,3,2,3 tags).
# Axes: subquery source (outer-alias t / IN t / IN c.tags), projection shape
# (VALUE / list / AS), and qualified vs unqualified outer columns.

# ── Source = outer alias `t` (item-scoped: t is one tag), projection shapes ──
SELECT c.id, (SELECT VALUE COUNT(1) FROM t) AS x FROM c JOIN t IN c.tags
SELECT c.id, (SELECT COUNT(1) FROM t) AS x FROM c JOIN t IN c.tags
SELECT c.id, (SELECT COUNT(1) AS len FROM t) AS x FROM c JOIN t IN c.tags

# ── Source = IN <outer scalar alias> (iterate the single bound tag string) ──
SELECT c.id, (SELECT VALUE COUNT(1) FROM x IN t) AS x FROM c JOIN t IN c.tags

# ── Source = IN c.tags (correlated to the outer container — the "intended" count) ──
SELECT c.id, (SELECT VALUE COUNT(1) FROM x IN c.tags) AS x FROM c
SELECT c.id, (SELECT COUNT(1) FROM x IN c.tags) AS x FROM c

# ── Projecting the join alias directly (baselines) ────────────────
SELECT c.id, t FROM c JOIN t IN c.tags
SELECT VALUE t FROM c JOIN t IN c.tags

# ── Array subquery over the nested array (no join) ────────────────
SELECT c.id, ARRAY(SELECT VALUE t FROM t IN c.tags) AS allTags FROM c

# ── Unqualified outer columns (the literal forms that "feel wrong") ──
SELECT _id, (SELECT COUNT(1) FROM t) AS tags FROM c JOIN t IN c.tags
SELECT id, (SELECT COUNT(1) FROM t) AS tags FROM c JOIN t IN c.tags
