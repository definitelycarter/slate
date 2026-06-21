#!/usr/bin/env python3
"""Differential test: run the same queries against slate and the Cosmos emulator.

Loads the SAME fixture documents into both engines, runs every query against
each, normalizes the results, and reports where they agree or diverge.

For a clean, idempotent run the emulator is restarted first (its data is
in-memory — no volume is mounted), then each dataset is loaded into its own
Cosmos container. The slate side runs entirely in-process via the
`parity_dump` example.

Prereqs:
  - The Cosmos emulator running as a container named `slate-cosmos`.
  - `cargo` available to build the slate side.

Usage:
  python3 tools/cosmos-parity/run.py
"""

import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))

CONTAINER = "slate-cosmos"
IMAGE = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-latest"
DB = "ParityDB"
SYS_PREFIX = "_"  # Cosmos system fields (_rid, _etag, _ts, ...) — our fixtures use none.

# (label, fixtures, queries). Each runs its queries against its own Cosmos
# container, loaded with the same docs the slate side gets. `functions` is
# FROM-less so its data is unused.
DATASETS = [
    ("functions", "datasets/products.json", "queries/functions.sql"),
    ("products", "datasets/products.json", "queries/products.sql"),
    ("orders", "datasets/orders.json", "queries/orders.sql"),
    ("families", "datasets/families.json", "queries/families.sql"),
    ("edge", "datasets/products.json", "queries/edge.sql"),
    ("negative", "datasets/products.json", "queries/negative.sql"),
    ("subquery_perms", "datasets/products.json", "queries/subquery_perms.sql"),
]

# Auto-include the generated matrices (gen.py) when present.
for _name, _fx in [("products", "datasets/products.json"),
                   ("orders", "datasets/orders.json"),
                   ("families", "datasets/families.json")]:
    _gf = f"queries/gen_{_name}.sql"
    if os.path.exists(os.path.join(HERE, _gf)):
        DATASETS.append((f"gen_{_name}", _fx, _gf))


def path(rel):
    return os.path.join(HERE, rel)


# ── emulator control ─────────────────────────────────────────────

def shell(*args):
    """Run a cosmoshell command inside the emulator; return (exit, combined output)."""
    p = subprocess.run(
        ["docker", "exec", CONTAINER, "cosmoshell.sh", "--cs", "0", *args],
        capture_output=True, text=True,
    )
    return p.returncode, p.stdout + p.stderr


def recreate_and_wait():
    # The emulator persists data to its container layer, so a `restart` would
    # keep stale docs. Recreate the container for a guaranteed clean slate.
    print("recreating emulator for a clean slate…")
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, text=True)
    rc = subprocess.run(["docker", "run", "--detach", "--name", CONTAINER, IMAGE],
                        capture_output=True, text=True)
    if rc.returncode != 0:
        sys.exit(f"failed to start emulator: {rc.stderr}")
    for _ in range(90):
        p = subprocess.run(
            ["docker", "exec", CONTAINER, "sh", "-c",
             'curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/ready'],
            capture_output=True, text=True,
        )
        if p.stdout.strip() == "200":
            return
        time.sleep(2)
    sys.exit("emulator did not become ready after restart")


def seed(container, docs):
    rc, out = shell("-c", f"mkdb {DB}")
    shell("-c", "mkcon", container, "/id", f"--database={DB}")
    for d in docs:
        rc, out = shell("-c", "mkitem", "-container", container, f"--database={DB}",
                        json.dumps(d, separators=(",", ":")))
        if rc != 0:
            sys.exit(f"failed to seed {d.get('id')} into {container}: {out}")


def cosmos_query(sql, container):
    """Return (ok, items_or_errmsg)."""
    rc, out = shell("-c", f'query "{sql}" --database={DB} --container={container}')
    lines = out.splitlines()
    start = next((i for i, ln in enumerate(lines) if ln.strip() == "{"), None)
    if rc != 0 or start is None:
        msg = " ".join(l.strip() for l in lines if l.strip())
        return False, msg
    try:
        return True, json.loads("\n".join(lines[start:])).get("items", [])
    except json.JSONDecodeError as e:
        return False, f"parse error: {e}"


# ── slate side ───────────────────────────────────────────────────

def slate_results(fixtures, queries):
    """Build+run the slate dumper; return {sql: record}."""
    p = subprocess.run(
        ["cargo", "run", "-q", "-p", "slate-cli", "--example", "parity_dump",
         "--", fixtures, queries],
        capture_output=True, text=True, cwd=REPO,
    )
    if p.returncode != 0:
        sys.exit(f"slate dumper failed:\n{p.stderr}")
    out = {}
    for line in p.stdout.splitlines():
        if line.strip().startswith("{"):
            rec = json.loads(line)
            out[rec["sql"]] = rec
    return out


# ── normalization & comparison ───────────────────────────────────

def strip_system(v):
    if isinstance(v, dict):
        return {k: strip_system(x) for k, x in v.items() if not k.startswith(SYS_PREFIX)}
    if isinstance(v, list):
        return [strip_system(x) for x in v]
    return v


def numify(v):
    """Collapse whole-valued floats to ints. slate's BSON typing makes math
    functions return Double (`4.0`); Cosmos's unified JSON numbers serialize as
    `4`. Same value, different representation — compare by value."""
    if isinstance(v, bool):
        return v
    if isinstance(v, float) and v.is_integer():
        return int(v)
    if isinstance(v, list):
        return [numify(x) for x in v]
    if isinstance(v, dict):
        return {k: numify(x) for k, x in v.items()}
    return v


def canon(v):
    return json.dumps(numify(v), sort_keys=True, separators=(",", ":"))


def normalize(items, ordered):
    items = [strip_system(x) for x in items]
    return items if ordered else sorted(items, key=canon)


def load_queries(queries_path):
    with open(queries_path) as f:
        return [s.strip() for s in f if s.strip() and not s.strip().startswith("#")]


# ── driver ───────────────────────────────────────────────────────

def run_dataset(label, fixtures, queries_path):
    docs = json.load(open(fixtures))
    container = f"c_{label}"
    seed(container, docs)
    slate = slate_results(fixtures, queries_path)
    queries = load_queries(queries_path)

    # Cosmos round-trips dominate wall-clock; run them concurrently.
    with ThreadPoolExecutor(max_workers=8) as ex:
        cosmos = dict(zip(queries, ex.map(lambda s: cosmos_query(s, container), queries)))

    npass = 0
    mismatches = []
    for sql in queries:
        ordered = "order by" in sql.lower()
        s_rec = slate.get(sql, {"ok": False, "error": "not run"})
        c_ok, c_val = cosmos[sql]
        s_ok = s_rec.get("ok", False)

        if s_ok and c_ok:
            if canon(normalize(s_rec["items"], ordered)) == canon(normalize(c_val, ordered)):
                npass += 1
            else:
                mismatches.append((sql, "value mismatch", normalize(s_rec["items"], ordered),
                                   normalize(c_val, ordered)))
        elif not s_ok and not c_ok:
            npass += 1
        else:
            who = "slate ok / cosmos ERROR" if s_ok else "slate ERROR / cosmos ok"
            mismatches.append((sql, who, s_rec.get("items", s_rec.get("error")), c_val))

    n = len(queries)
    flag = "✅" if not mismatches else f"❌ {len(mismatches)} diverge"
    print(f"### {label:<16} {npass}/{n}  {flag}")
    return npass, n, mismatches


def main():
    recreate_and_wait()
    total_pass = total_q = 0
    all_div = []
    for label, fixtures, queries in DATASETS:
        npass, n, mism = run_dataset(label, path(fixtures), path(queries))
        total_pass += npass
        total_q += n
        all_div.extend(mism)

    print("\n" + "=" * 50)
    print(f"TOTAL: {total_pass}/{total_q} match   ({len(all_div)} divergences)")
    if all_div:
        print("\n── divergences ──────────────────────────────")
        for sql, kind, s, c in all_div:
            s_str = s if isinstance(s, str) else canon(s)
            c_str = c if isinstance(c, str) else canon(c)
            print(f"\n• {sql}\n  [{kind}]")
            print(f"  slate : {s_str[:240]}")
            print(f"  cosmos: {c_str[:240]}")


if __name__ == "__main__":
    main()
