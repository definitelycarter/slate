#!/usr/bin/env python3
"""Compare slate against the Azure-Samples query corpus.

Each scripts/<name>/ folder ships an authoritative result.json, so this needs no
emulator — Microsoft's documented output is the oracle. Run
`tools/cosmos-parity/samples_fetch.sh` first to fetch the corpus.

Usage:
  python3 tools/cosmos-parity/run_samples.py
"""

import json
import os
import subprocess
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
SCRIPTS = os.path.join(HERE, ".samples", "scripts")


def numify(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, float):
        # whole floats compare equal to ints; otherwise round off last-ULP
        # differences in transcendental results (e.g. LOG base).
        return int(v) if v.is_integer() else round(v, 10)
    if isinstance(v, list):
        return [numify(x) for x in v]
    if isinstance(v, dict):
        return {k: numify(x) for k, x in v.items()}
    return v


def strip(v):
    if isinstance(v, dict):
        return {k: strip(x) for k, x in v.items() if not k.startswith("_")}
    if isinstance(v, list):
        return [strip(x) for x in v]
    return v


def canon(v):
    return json.dumps(numify(strip(v)), sort_keys=True, separators=(",", ":"))


def norm(items, ordered):
    return items if ordered else sorted(items, key=canon)


def slate_run():
    p = subprocess.run(
        ["cargo", "run", "-q", "-p", "slate-cli", "--example", "parity_samples", "--", SCRIPTS],
        capture_output=True, text=True, cwd=REPO,
    )
    if p.returncode != 0:
        sys.exit(f"slate runner failed:\n{p.stderr}")
    return {json.loads(l)["dir"]: json.loads(l) for l in p.stdout.splitlines() if l.strip().startswith("{")}


def err_sig(e):
    if "parse error" in e:
        return "parse / grammar"
    if "unknown function" in e.lower() or "no such function" in e.lower():
        return "missing function"
    return "other"


def main():
    if not os.path.isdir(SCRIPTS):
        sys.exit("samples not found — run tools/cosmos-parity/samples_fetch.sh first")

    slate = slate_run()
    folders = sorted(
        d for d in os.listdir(SCRIPTS)
        if os.path.exists(os.path.join(SCRIPTS, d, "result.json"))
        and os.path.exists(os.path.join(SCRIPTS, d, "query.sql"))
    )

    match = 0
    errors = []
    mismatches = []
    for d in folders:
        exp = json.load(open(os.path.join(SCRIPTS, d, "result.json")))
        sql = open(os.path.join(SCRIPTS, d, "query.sql")).read()
        ordered = "order by" in sql.lower()
        rec = slate.get(d, {"ok": False, "error": "not run"})
        if not rec.get("ok"):
            errors.append((d, rec.get("error", "?")))
        elif canon(norm(rec["items"], ordered)) == canon(norm(exp, ordered)):
            match += 1
        else:
            mismatches.append((d, rec["items"], exp))

    n = len(folders)
    print(f"\nslate vs Azure-Samples result.json: {match}/{n} match  "
          f"({len(mismatches)} value-mismatch, {len(errors)} slate-error)")
    print("error categories:", dict(Counter(err_sig(e) for _, e in errors)))

    print("\n── slate errors (missing features) ──")
    for d, e in errors:
        print(f"  {d:30} {e[:80]}")

    print("\n── value mismatches ──")
    for d, s, x in mismatches:
        print(f"\n• {d}")
        print(f"  slate   : {canon(s)[:200]}")
        print(f"  expected: {canon(x)[:200]}")


if __name__ == "__main__":
    main()
