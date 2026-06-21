#!/usr/bin/env python3
"""Capture committed goldens for slate's own curated query matrices.

For each (label, dataset, query) in the curated matrices, run the query on the
**Cosmos emulator** and write a committed golden holding the **raw** Cosmos
output:

  goldens/<label>/<NNN>.json  ->  {"query": ..., "dataset": ..., "result": [...]}

or, when Cosmos rejects the query (parity = both engines error),

  goldens/<label>/<NNN>.json  ->  {"query": ..., "dataset": ..., "error": true}

Results are stored **raw** — system fields (`_rid`/`_etag`/`_ts`/…) and all.
Normalization happens exactly once, in Rust, at replay time
(`crates/slate-db/tests/cosmos_golden.rs`), so there is a single normalization
implementation (cosmos-parity SKILL §4/§8). We never normalize at capture time.

Two sidecar files keep the capture honest and auditable:

  goldens/_excluded.json   queries we refuse to golden because the emulator's
                           output is known-WRONG (SKILL §5 emulator-bug list).
                           Pinning a known-wrong oracle as a golden is forbidden.
                           The live run.py still exercises these.

  goldens/_known_gaps.json queries where the emulator is RIGHT but slate diverges
                           (a real slate gap). We DO golden the correct Cosmos
                           output, but the replay test treats these as expected
                           divergences (it reports them instead of asserting
                           equality) so the suite stays green while the gap is
                           pinned and surfaced.

Generated matrices (gen_*.sql) are NOT goldened: a randomly generated query has no
fixed oracle, so those keep using the live emulator via run.py.

Usage:
  python3 tools/cosmos-parity/capture_goldens.py            # recreate emulator, capture
  python3 tools/cosmos-parity/capture_goldens.py --no-recreate  # reuse running container
  python3 tools/cosmos-parity/capture_goldens.py --triage   # report only, write nothing
"""

import json
import os
import sys

import run  # sibling: reuse the emulator control + normalization, no duplication

HERE = os.path.dirname(os.path.abspath(__file__))
GOLDENS = os.path.join(HERE, "goldens")

# Curated matrices only — the hand-written queries/*.sql. Generated gen_*.sql are
# deliberately excluded (no fixed oracle for a random query; they stay on run.py).
# (label, dataset, query file). `functions` is FROM-less; its dataset is unused
# but still loaded so the in-process slate side has a collection to query.
CURATED = [
    ("functions", "datasets/products.json", "queries/functions.sql"),
    ("products", "datasets/products.json", "queries/products.sql"),
    ("orders", "datasets/orders.json", "queries/orders.sql"),
    ("families", "datasets/families.json", "queries/families.sql"),
    ("edge", "datasets/products.json", "queries/edge.sql"),
    ("negative", "datasets/products.json", "queries/negative.sql"),
    ("subquery_perms", "datasets/products.json", "queries/subquery_perms.sql"),
]


def load_sidecar(name):
    """Return {query: entry} for a goldens/<name> sidecar, or {} if absent.

    A sidecar is a self-documenting object: {"description": ..., "queries": [...]}
    where each entry carries at least a "query" key. The object form lets an empty
    list still record *why* it is empty (e.g. no emulator-bug families appear in
    the curated matrices)."""
    p = os.path.join(GOLDENS, name)
    if not os.path.exists(p):
        return {}
    with open(p) as f:
        return {e["query"]: e for e in json.load(f).get("queries", [])}


def write_golden(label, idx, payload):
    d = os.path.join(GOLDENS, label)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, f"{idx:03d}.json"), "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")


def clear_label(label):
    d = os.path.join(GOLDENS, label)
    if not os.path.isdir(d):
        return
    for fn in os.listdir(d):
        if fn.endswith(".json"):
            os.remove(os.path.join(d, fn))


def main():
    triage_only = "--triage" in sys.argv
    if "--no-recreate" not in sys.argv:
        run.recreate_and_wait()

    excluded = load_sidecar("_excluded.json")
    known_gaps = load_sidecar("_known_gaps.json")

    n_golden = n_excluded = n_gap = 0
    unclassified = []  # divergences that are neither excluded nor a known gap

    for label, fixtures, queries_rel in CURATED:
        fixtures_abs = run.path(fixtures)
        queries_abs = run.path(queries_rel)
        docs = json.load(open(fixtures_abs))
        container = f"cap_{label}"
        run.seed(container, docs)

        slate = run.slate_results(fixtures_abs, queries_abs)
        queries = run.load_queries(queries_abs)

        if not triage_only:
            clear_label(label)

        for idx, sql in enumerate(queries, start=1):
            ordered = "order by" in sql.lower()
            c_ok, c_val = run.cosmos_query(sql, container)
            s_rec = slate.get(sql, {"ok": False, "error": "not run"})
            s_ok = s_rec.get("ok", False)

            dataset_name = os.path.basename(fixtures)

            if sql in excluded:
                n_excluded += 1
                continue

            # The golden encodes Cosmos's RAW output for this query.
            if c_ok:
                payload = {"query": sql, "dataset": dataset_name, "result": c_val}
            else:
                payload = {"query": sql, "dataset": dataset_name, "error": True}

            # Does slate agree (after the SAME normalization run.py uses)?
            if s_ok and c_ok:
                agree = run.canon(run.normalize(s_rec["items"], ordered)) == run.canon(
                    run.normalize(c_val, ordered)
                )
            elif not s_ok and not c_ok:
                agree = True
            else:
                agree = False

            if agree:
                n_golden += 1
                if not triage_only:
                    write_golden(label, idx, payload)
            elif sql in known_gaps:
                n_gap += 1
                if not triage_only:
                    write_golden(label, idx, payload)
            else:
                unclassified.append((label, sql, s_ok, s_rec, c_ok, c_val))

    print("\n" + "=" * 60)
    verb = "would capture" if triage_only else "captured"
    print(
        f"{verb}: {n_golden} goldens (+{n_gap} known-gap), "
        f"{n_excluded} excluded (emulator bug)"
    )
    if unclassified:
        print(
            f"\n⚠️  {len(unclassified)} UNCLASSIFIED divergence(s) — triage each as an "
            f"emulator bug (-> _excluded.json) or a real slate gap (-> _known_gaps.json):"
        )
        for label, sql, s_ok, s_rec, c_ok, c_val in unclassified:
            s = s_rec.get("items", s_rec.get("error")) if s_ok else s_rec.get("error")
            s_str = s if isinstance(s, str) else run.canon(s)
            c_str = c_val if isinstance(c_val, str) else run.canon(c_val)
            print(f"\n• [{label}] {sql}")
            print(f"  slate : {'ok ' if s_ok else 'ERR'} {s_str[:200]}")
            print(f"  cosmos: {'ok ' if c_ok else 'ERR'} {c_str[:200]}")
        if not triage_only:
            print(
                "\n(no golden written for the unclassified queries above — "
                "classify them, then re-run)"
            )
        sys.exit(1)


if __name__ == "__main__":
    main()
