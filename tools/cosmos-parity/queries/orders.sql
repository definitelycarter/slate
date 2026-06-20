# Orders (from the bundled SampleDB): arrays of line-item objects + nested
# shipping address — the richest dataset for JOIN / subquery / aggregate.

# ── Nested-object access ──────────────────────────────────────────
SELECT VALUE c.shippingAddress.state FROM c
SELECT c.id, c.shippingAddress.city AS city FROM c
SELECT VALUE c.id FROM c WHERE c.shippingAddress.state = 'WA'

# ── JOIN (unwind items) ───────────────────────────────────────────
SELECT VALUE i.productName FROM c JOIN i IN c.items
SELECT c.id, i.productName, i.quantity FROM c JOIN i IN c.items
SELECT VALUE i.productName FROM c JOIN i IN c.items WHERE i.price > 100
SELECT c.id FROM c JOIN i IN c.items WHERE i.quantity > 1

# ── Aggregates / GROUP BY ─────────────────────────────────────────
SELECT VALUE COUNT(1) FROM c
SELECT VALUE SUM(c.totalAmount) FROM c
SELECT VALUE AVG(c.totalAmount) FROM c
SELECT VALUE MAX(c.totalAmount) FROM c
SELECT c.status AS status, COUNT(1) AS n FROM c GROUP BY c.status

# ── Subqueries over the items array ───────────────────────────────
SELECT c.id, (SELECT VALUE COUNT(1) FROM i IN c.items) AS lineItems FROM c
SELECT c.id, (SELECT VALUE SUM(i.quantity) FROM i IN c.items) AS totalQty FROM c
SELECT VALUE c.id FROM c WHERE EXISTS (SELECT VALUE i FROM i IN c.items WHERE i.quantity > 1)
SELECT c.id, ARRAY(SELECT VALUE i.productName FROM i IN c.items) AS products FROM c
SELECT c.id, ARRAY(SELECT VALUE i.productName FROM i IN c.items WHERE i.price > 100) AS pricey FROM c

# ── ORDER BY (order-sensitive) ────────────────────────────────────
SELECT VALUE c.id FROM c ORDER BY c.totalAmount DESC
SELECT VALUE c.status FROM c ORDER BY c.orderDate ASC
