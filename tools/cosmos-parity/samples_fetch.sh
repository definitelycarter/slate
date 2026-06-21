#!/usr/bin/env bash
# Fetch the Azure-Samples query corpus into a gitignored .samples/ dir (we don't
# vendor the third-party content). Each scripts/<name>/ has query.sql, an
# authoritative result.json, and optional seed.json.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)/.samples"
REPO="https://github.com/Azure-Samples/cosmos-db-nosql-query-samples"

if [ -d "$DIR/.git" ]; then
  git -C "$DIR" pull --ff-only
else
  git clone --depth 1 "$REPO" "$DIR"
fi

echo "samples ready: $DIR/scripts ($(ls "$DIR/scripts" | wc -l | tr -d ' ') folders)"
