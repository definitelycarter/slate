# Query playground bundle

This directory powers the book's in-browser **query playground** — runnable SQL
cells that execute real slate queries client-side, with no backend.

```
playground/
├── playground.js   theme JS: loads the wasm, seeds the dataset, upgrades
│                   ```slate-sql / ```slate-find code blocks into Run cells
├── playground.css  styling for the cells, result tables, and error boxes
├── dataset.js      the shared sample corpus seeded into the in-memory db
└── pkg/            ← GENERATED — the slate-wasm bundle (do not hand-edit)
    ├── slate_wasm.js          ES-module glue (wasm-bindgen, --target web)
    ├── slate_wasm_bg.wasm     the compiled module (~2.8 MB; ~0.9 MB gzipped)
    ├── slate_wasm.d.ts        type declarations
    ├── slate_wasm_bg.wasm.d.ts
    └── package.json
```

## Why the bundle is committed

`pkg/` is a build artifact but it is **checked in on purpose** so that
`mdbook build` (locally and in CI) needs no Rust/wasm toolchain — the book is
just static files plus this prebuilt module. The raw `.wasm` is ~2.8 MB
(~0.9 MB over the wire, since GitHub Pages serves it gzipped); it is committed
uncompressed so the browser can stream-instantiate it directly with no
JS-side decompression step.

The `wasm32 build` CI job is the guard that the bundle can still be rebuilt
from source — if a change breaks the wasm build, that job fails.

## Rebuilding

Whenever `crates/slate-wasm` changes, regenerate the bundle and commit it
alongside the code change:

```bash
./book/build-playground.sh        # wraps `wasm-pack build … --target web`
git add book/theme/playground/pkg
```

Requires [`wasm-pack`](https://rustwasm.github.io/wasm-pack/installer/) and the
`wasm32-unknown-unknown` target (`rustup target add wasm32-unknown-unknown`).
