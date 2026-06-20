# Products (from the bundled SampleDB): floats, booleans, tag arrays, categories.

# ── Projection ────────────────────────────────────────────────────
SELECT * FROM c
SELECT VALUE c.name FROM c
SELECT c.id, c.price FROM c
SELECT c.name AS product, c.price AS cost FROM c
SELECT VALUE { 'n': c.name, 'cheap': c.price < 50 } FROM c

# ── WHERE / operators ─────────────────────────────────────────────
SELECT VALUE c.name FROM c WHERE c.inStock = true
SELECT VALUE c.name FROM c WHERE c.inStock = false
SELECT VALUE c.name FROM c WHERE c.price > 100
SELECT VALUE c.name FROM c WHERE c.price >= 29.99 AND c.price <= 50
SELECT VALUE c.name FROM c WHERE c.price BETWEEN 25 AND 60
SELECT VALUE c.name FROM c WHERE c.category IN ('Electronics', 'Stationery')
SELECT VALUE c.name FROM c WHERE c.category != 'Furniture'
SELECT VALUE c.name FROM c WHERE NOT (c.category = 'Furniture')
SELECT VALUE c.name FROM c WHERE c.name LIKE 'Premium%'
SELECT VALUE c.name FROM c WHERE c.name LIKE '%Mouse'
SELECT VALUE c.name FROM c WHERE c.description LIKE '%office%'

# ── Functions over data ───────────────────────────────────────────
SELECT VALUE UPPER(c.category) FROM c
SELECT VALUE LENGTH(c.name) FROM c
SELECT VALUE CONCAT(c.category, ': ', c.name) FROM c
SELECT VALUE c.name FROM c WHERE STARTSWITH(c.name, 'Premium')
SELECT VALUE c.name FROM c WHERE CONTAINS(c.description, 'laptop')
SELECT VALUE ARRAY_LENGTH(c.tags) FROM c
SELECT VALUE c.name FROM c WHERE ARRAY_CONTAINS(c.tags, 'office')
SELECT VALUE IIF(c.inStock, 'available', 'sold out') FROM c

# ── ORDER BY / paging (order-sensitive) ───────────────────────────
SELECT VALUE c.name FROM c ORDER BY c.price ASC
SELECT VALUE c.name FROM c ORDER BY c.price DESC
SELECT VALUE c.name FROM c ORDER BY c.category ASC, c.price DESC
SELECT VALUE c.name FROM c ORDER BY c.price DESC OFFSET 1 LIMIT 2

# ── Aggregates / GROUP BY ─────────────────────────────────────────
SELECT VALUE COUNT(1) FROM c
SELECT VALUE SUM(c.price) FROM c
SELECT VALUE AVG(c.price) FROM c
SELECT VALUE MIN(c.price) FROM c
SELECT VALUE MAX(c.price) FROM c
SELECT c.category AS category, COUNT(1) AS n FROM c GROUP BY c.category
SELECT c.category AS category, MAX(c.price) AS maxPrice FROM c GROUP BY c.category

# ── Subqueries ────────────────────────────────────────────────────
SELECT c.name, (SELECT VALUE COUNT(1) FROM t IN c.tags) AS tagCount FROM c
SELECT VALUE c.name FROM c WHERE EXISTS (SELECT VALUE t FROM t IN c.tags WHERE t = 'office')
SELECT c.name, ARRAY(SELECT VALUE UPPER(t) FROM t IN c.tags) AS upperTags FROM c
