#!/usr/bin/env bash
#
# Rebuild the slate-wasm playground bundle committed under
# book/theme/playground/pkg.
#
# The book embeds a prebuilt wasm bundle so that `mdbook build` — locally and in
# CI — needs no wasm toolchain. Run this whenever crates/slate-wasm changes, then
# commit the regenerated pkg/ alongside your code change. The `wasm32 build` CI
# job is the guard that this bundle can still be rebuilt from source.
#
# Requirements:
#   - wasm-pack            https://rustwasm.github.io/wasm-pack/installer/
#   - the wasm32 target    rustup target add wasm32-unknown-unknown
#
# Usage (from anywhere in the repo):
#   ./book/build-playground.sh

set -euo pipefail

# Resolve the repo root from this script's own location so it runs from anywhere.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUT="book/theme/playground/pkg"

if ! command -v wasm-pack >/dev/null 2>&1; then
  echo "error: wasm-pack not found — install it from" >&2
  echo "       https://rustwasm.github.io/wasm-pack/installer/" >&2
  exit 1
fi

echo "Building slate-wasm → $OUT (release, --target web)…"
# --out-dir is resolved relative to the crate directory (crates/slate-wasm).
wasm-pack build crates/slate-wasm --release --target web --out-dir "../../$OUT"

# wasm-pack writes a `.gitignore` containing `*` into the out-dir. The bundle is
# committed on purpose (so the book builds without a wasm toolchain), so drop it.
rm -f "$OUT/.gitignore"

echo
echo "Done. Committed bundle files:"
ls -lh "$OUT" | awk 'NR>1 {printf "  %-28s %s\n", $9, $5}'
echo
echo "Next: git add $OUT && commit the regenerated bundle."
