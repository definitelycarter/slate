# Query playground

This powers the book's in-browser **query playground** — runnable SQL cells that
execute real slate queries client-side via the `slate-wasm` build, with no
backend.

## Layout

The assets are split across two directories because mdbook treats them
differently:

```
book/
├── theme/playground/         INJECTED on every page (book.toml additional-*)
│   ├── playground.js         loads the wasm, seeds the dataset, upgrades
│   │                         ```slate-sql / ```slate-find blocks into Run cells
│   ├── playground.css        styling for cells, result tables, error boxes
│   └── README.md             (this file)
└── src/playground/           SERVED as static assets (copied verbatim by mdbook)
    ├── dataset.js            the shared sample corpus (ES module)
    └── pkg/                  ← GENERATED slate-wasm bundle (do not hand-edit)
        ├── slate_wasm.js          ES-module glue (wasm-bindgen, --target web)
        ├── slate_wasm_bg.wasm     compiled module (~2.8 MB; ~0.9 MB gzipped)
        ├── slate_wasm.d.ts
        ├── slate_wasm_bg.wasm.d.ts
        └── package.json
```

Why the split? mdbook injects `additional-js`/`additional-css` into every page
(with a content hash in the filename) but **only** copies those listed files —
not sibling directories. Arbitrary static assets (the `.wasm`, the dataset) are
served only if they live under `src/`. So the injected behavior lives in
`theme/playground/` and the served payload lives in `src/playground/`.
`playground.js` finds the payload at run time by resolving paths relative to its
own (`document.currentScript`) URL, so it works no matter what sub-path the book
is hosted under.

## Why the bundle is committed

`pkg/` is a build artifact but it is **checked in on purpose** so that
`mdbook build` (locally and in CI) needs no Rust/wasm toolchain — the book is
just static files plus this prebuilt module. The raw `.wasm` is ~2.8 MB
(~0.9 MB over the wire, since GitHub Pages serves it gzipped); it is committed
uncompressed so the browser can stream-instantiate it with no JS-side
decompression step.

The `wasm32 build` CI job is the guard that the bundle can still be rebuilt from
source — if a change breaks the wasm build, that job fails.

## Rebuilding

Whenever `crates/slate-wasm` changes, regenerate the bundle and commit it
alongside the code change:

```bash
./book/build-playground.sh        # wraps `wasm-pack build … --target web`
git add book/src/playground/pkg
```

Requires [`wasm-pack`](https://rustwasm.github.io/wasm-pack/installer/) and the
`wasm32-unknown-unknown` target (`rustup target add wasm32-unknown-unknown`).

## Serving

The playground only runs over HTTP (wasm fetch + ES-module import are blocked on
`file://`). Use `mdbook serve` for local preview; static blocks remain readable
if the wasm fails to load.
