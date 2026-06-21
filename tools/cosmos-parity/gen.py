#!/usr/bin/env python3
"""Generate a large query matrix by composing grammar fragments.

Because the harness has a differential oracle (real Cosmos), we only need to
generate *queries* — Cosmos defines correctness. This emits subqueries in every
supported position (SELECT / WHERE / JOIN source), functions and aggregates
*inside* subqueries, nesting (a subquery whose source is a subquery), and
identifier / FROM-form variants.

Writes queries/gen_<dataset>.sql for each dataset below; `run.py` auto-includes
them. Re-run after editing. Tune VOLUME knobs to scale toward 1000+.
"""

import os

HERE = os.path.dirname(os.path.abspath(__file__))

# Per-dataset schema hints. scalars: (path, type); scalar_arrays: (path, elemtype);
# obj_arrays: (path, [(subfield, type), ...]).
SCHEMA = {
    "products": {
        "scalars": [("c.price", "num"), ("c.name", "str"), ("c.category", "str")],
        "scalar_arrays": [("c.tags", "str")],
        "obj_arrays": [],
    },
    "orders": {
        "scalars": [("c.totalAmount", "num"), ("c.status", "str")],
        "scalar_arrays": [],
        "obj_arrays": [("c.items", [("quantity", "num"), ("price", "num"), ("productName", "str")])],
    },
    "families": {
        "scalars": [("c.lastName", "str"), ("c.creationDate", "num")],
        "scalar_arrays": [("c.tags", "str")],
        "obj_arrays": [
            ("c.children", [("grade", "num"), ("firstName", "str")]),
            ("c.parents", [("firstName", "str")]),
        ],
    },
}

NUM_FUNCS = ["ABS", "FLOOR", "CEILING", "SQUARE", "SIGN"]
STR_FUNCS = ["UPPER", "LOWER", "REVERSE", "LENGTH"]
AGGS = ["COUNT", "SUM", "AVG", "MIN", "MAX"]


def scalar_exprs(base, typ):
    out = [base]
    out += [f"{fn}({base})" for fn in (NUM_FUNCS if typ == "num" else STR_FUNCS)]
    if typ == "num":
        out += [f"{base} * 2", f"{base} + 1", f"{base} - 1"]
    return out


def preds(base, typ):
    if typ == "num":
        return [f"{base} > 1", f"{base} >= 0", f"{base} != 0"]
    return [f"STARTSWITH({base}, 'a')", f"CONTAINS({base}, 'e')", f"{base} != 'zzz'"]


def agg_arg(elem_alias, elem):
    """An aggregate argument over an array element (scalar or object)."""
    if isinstance(elem, str):  # scalar array element
        return ["1", elem_alias] if elem == "str" else ["1", elem_alias]
    # object array: aggregate over a numeric subfield, plus COUNT(1)
    nums = [f"{elem_alias}.{f}" for f, t in elem if t == "num"]
    return ["1"] + nums


def inner_scalar(elem_alias, elem):
    if isinstance(elem, str):
        return [elem_alias, f"UPPER({elem_alias})"] if elem == "str" else [elem_alias]
    return [f"{elem_alias}.{f}" for f, _ in elem]


def inner_pred(elem_alias, elem):
    if isinstance(elem, str):
        return preds(elem_alias, elem)
    # object: pick first field
    f, t = elem[0]
    return preds(f"{elem_alias}.{f}", t)


def gen_for_array(arr_path, elem, alias):
    """All subquery-position permutations for one array source."""
    q = []
    args = agg_arg(alias, elem)
    iscals = inner_scalar(alias, elem)
    ipreds = inner_pred(alias, elem)

    # 1) Scalar subquery in SELECT: every aggregate × arg × (±filter).
    for agg in AGGS:
        for arg in args:
            if agg == "COUNT" and arg != "1":
                continue
            q.append(f"SELECT c.id, (SELECT VALUE {agg}({arg}) FROM {alias} IN {arr_path}) AS v FROM c")
            q.append(f"SELECT c.id, (SELECT VALUE {agg}({arg}) FROM {alias} IN {arr_path} WHERE {ipreds[0]}) AS v FROM c")

    # 2) ARRAY subquery in SELECT (with function-in-subquery), ±filter.
    for proj in iscals:
        q.append(f"SELECT c.id, ARRAY(SELECT VALUE {proj} FROM {alias} IN {arr_path}) AS a FROM c")
        q.append(f"SELECT c.id, ARRAY(SELECT VALUE {proj} FROM {alias} IN {arr_path} WHERE {ipreds[0]}) AS a FROM c")

    # 3) EXISTS in WHERE × predicate.
    for p in ipreds:
        q.append(f"SELECT VALUE c.id FROM c WHERE EXISTS(SELECT VALUE {alias} FROM {alias} IN {arr_path} WHERE {p})")

    # 4) Scalar subquery compared in WHERE.
    for p in ipreds:
        q.append(f"SELECT VALUE c.id FROM c WHERE (SELECT VALUE COUNT(1) FROM {alias} IN {arr_path} WHERE {p}) >= 1")
    q.append(f"SELECT VALUE c.id FROM c WHERE (SELECT VALUE COUNT(1) FROM {alias} IN {arr_path}) > 0")

    # 5) Multi-value subquery as a JOIN source (functions/filters inside).
    inner = f"SELECT VALUE {alias} FROM {alias} IN {arr_path} WHERE {ipreds[0]}"
    q.append(f"SELECT VALUE j FROM c JOIN j IN ({inner})")
    q.append(f"SELECT c.id, j FROM c JOIN j IN ({inner})")

    # 6) Function wrapping a subquery.
    q.append(f"SELECT c.id, ARRAY_LENGTH(ARRAY(SELECT VALUE {iscals[0]} FROM {alias} IN {arr_path})) AS n FROM c")
    q.append(f"SELECT VALUE ARRAY_LENGTH(ARRAY(SELECT VALUE {alias} FROM {alias} IN {arr_path})) FROM c")

    # 7) Nested subquery: a subquery whose source is itself an ARRAY subquery.
    q.append(
        f"SELECT c.id, (SELECT VALUE COUNT(1) FROM y IN (SELECT VALUE {iscals[0]} FROM {alias} IN {arr_path})) AS n FROM c"
    )

    # 8) Plain JOIN baselines (+ function/filter on the unwound element).
    q.append(f"SELECT VALUE {alias} FROM c JOIN {alias} IN {arr_path}")
    q.append(f"SELECT VALUE {alias} FROM c JOIN {alias} IN {arr_path} WHERE {ipreds[0]}")
    for proj in iscals:
        q.append(f"SELECT VALUE {proj} FROM c JOIN {alias} IN {arr_path}")
    return q


def gen_scalars(scalars):
    """Function-in-projection and function-in-WHERE over scalar fields."""
    q = []
    for path, typ in scalars:
        for e in scalar_exprs(path, typ):
            q.append(f"SELECT VALUE {e} FROM c")
        for p in preds(path, typ):
            q.append(f"SELECT VALUE c.id FROM c WHERE {p}")
        q.append(f"SELECT VALUE c.id FROM c ORDER BY {path} ASC")
        q.append(f"SELECT VALUE c.id FROM c ORDER BY {path} DESC OFFSET 1 LIMIT 2")
    return q


def gen_identifier_forms(scalars):
    """Qualified / unqualified / nonexistent identifiers and FROM forms."""
    f0 = scalars[0][0].split(".")[1]
    q = [
        f"SELECT VALUE c.{f0} FROM c",          # qualified (baseline)
        f"SELECT VALUE {f0} FROM c",            # unqualified
        f"SELECT VALUE d.{f0} FROM c",          # nonexistent alias
        f"SELECT VALUE c.{f0} FROM c AS x",     # AS alias, wrong ref
        f"SELECT VALUE x.{f0} FROM c AS x",     # AS alias, right ref
        f"SELECT VALUE p.{f0} FROM coll p",     # named container + alias
        f"SELECT VALUE c.nonexistent FROM c",   # missing column
        f"SELECT VALUE c.a.b.c FROM c",         # deep missing path
    ]
    return q


def aliases():
    # cycle distinct element aliases so multi-array datasets don't collide
    yield from ["t", "i", "ch", "p", "e", "u", "w"]


def main():
    total = 0
    for ds, schema in SCHEMA.items():
        q = []
        q += gen_scalars(schema["scalars"])
        q += gen_identifier_forms(schema["scalars"])
        ali = aliases()
        for arr, _ in schema["scalar_arrays"]:
            q += gen_for_array(arr, "str", next(ali))
        for arr, fields in schema["obj_arrays"]:
            q += gen_for_array(arr, fields, next(ali))
        # de-dup, stable order
        seen, uniq = set(), []
        for s in q:
            if s not in seen:
                seen.add(s)
                uniq.append(s)
        out = os.path.join(HERE, "queries", f"gen_{ds}.sql")
        with open(out, "w") as f:
            f.write(f"# GENERATED by gen.py — do not edit. {len(uniq)} queries.\n")
            f.write("\n".join(uniq) + "\n")
        print(f"{ds}: {len(uniq)} queries -> {os.path.relpath(out, HERE)}")
        total += len(uniq)
    print(f"total generated: {total}")


if __name__ == "__main__":
    main()
