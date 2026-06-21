#!/usr/bin/env bash
# Fetch the Azure-Samples query corpus into a gitignored .samples/ dir. We don't
# vendor the third-party content; we fetch it on demand, but PINNED to a specific
# commit with NO git history — a single-snapshot checkout — so the parity baseline
# is reproducible and the corpus can't drift underfoot. Each scripts/<name>/ has
# query.sql, an authoritative result.json, and optional seed.json.
#
# To bump the corpus: set COMMIT to the new SHA, delete .samples/, re-run this,
# then re-run run_samples.py and update the match count in README.md and the
# cosmos-parity skill.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)/.samples"
REPO="https://github.com/Azure-Samples/cosmos-db-nosql-query-samples"
COMMIT="117c70bacdaadf40f17b72259d57a086dac3a96e"  # 2024-10-24; baseline 115/117

# Idempotent: skip the network round-trip if already at the pinned commit.
if [ -f "$DIR/.pinned-commit" ] && [ "$(cat "$DIR/.pinned-commit")" = "$COMMIT" ]; then
  echo "samples already pinned at ${COMMIT:0:12} — nothing to do"
  exit 0
fi

# Shallow fetch of the single pinned commit (GitHub allows fetch-by-SHA), then
# drop .git so what's left is just the snapshot — no history, no nested repo.
rm -rf "$DIR"
git init -q "$DIR"
git -C "$DIR" remote add origin "$REPO"
git -C "$DIR" fetch -q --depth 1 origin "$COMMIT"
git -C "$DIR" checkout -q FETCH_HEAD
rm -rf "$DIR/.git"
echo "$COMMIT" > "$DIR/.pinned-commit"

echo "samples ready: $DIR/scripts ($(ls "$DIR/scripts" | wc -l | tr -d ' ') folders) @ ${COMMIT:0:12}"
