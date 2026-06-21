// slate query playground.
//
// Upgrades ```slate-sql and ```slate-find fenced code blocks into editable,
// runnable cells backed by an in-browser slate-wasm database. Fully
// client-side: the wasm module is loaded once per page, an in-memory SlateDb is
// constructed, and a shared sample dataset is seeded before any cell runs.
//
// mdbook injects this as a classic <script> (no module scope) on every page, so
// we resolve the served payload — the wasm bundle and the dataset — relative to
// this script's own URL rather than a hard-coded path. If the wasm can't load
// (e.g. opened over file://, or the bundle is missing) the original code blocks
// are left untouched as static, readable examples.

(function () {
  "use strict";

  var current = document.currentScript;
  if (!current || !current.src) return;

  // We are served at `<root>/theme/playground/playground-<hash>.js`; the payload
  // lives at `<root>/playground/`. Strip the known suffix to find the root.
  var ROOT = current.src.replace(/theme\/playground\/[^/]*$/, "");
  var BUNDLE_URL = ROOT + "playground/pkg/slate_wasm.js";
  var DATASET_URL = ROOT + "playground/dataset.js";

  // Minimal fallback corpus, used only if dataset.js can't be loaded. The
  // canonical dataset lives in src/playground/dataset.js.
  var FALLBACK = {
    dataset: {
      products: [
        { _id: "prod-001", category: "Electronics", name: "Premium Laptop", price: 1299.99, inStock: true },
        { _id: "prod-002", category: "Electronics", name: "Wireless Mouse", price: 29.99, inStock: true },
        { _id: "prod-003", category: "Furniture", name: "Office Chair", price: 399.99, inStock: true },
        { _id: "prod-004", category: "Furniture", name: "LED Desk Lamp", price: 49.99, inStock: false },
        { _id: "prod-005", category: "Stationery", name: "Notebook Set", price: 24.99, inStock: true }
      ]
    },
    indexes: { products: ["category"] }
  };

  // ── One-time init: load wasm, build the db, seed the dataset ──────────────

  var envPromise = null;
  function getEnv() {
    if (!envPromise) envPromise = initEnv();
    return envPromise;
  }

  async function initEnv() {
    var mod = await import(BUNDLE_URL);
    await mod.default(); // fetches slate_wasm_bg.wasm relative to the glue URL
    var db = new mod.SlateDb();

    var spec = await loadDataset();
    var data = spec.dataset || {};
    var indexes = spec.indexes || {};
    var collections = Object.keys(data);

    collections.forEach(function (coll) {
      db.create_collection(coll);
      var docs = data[coll];
      if (docs && docs.length) db.insert_many(coll, docs);
      (indexes[coll] || []).forEach(function (field) {
        db.create_index(coll, field);
      });
    });

    return { db: db, collections: collections };
  }

  async function loadDataset() {
    try {
      var m = await import(DATASET_URL);
      if (m && m.DATASET) {
        return { dataset: m.DATASET, indexes: m.INDEXES || {} };
      }
    } catch (e) {
      console.warn("[slate playground] dataset.js unavailable, using fallback:", e);
    }
    return FALLBACK;
  }

  // ── Running queries ───────────────────────────────────────────────────────

  // The slate `FROM` clause only binds a row alias; the container is chosen
  // out-of-band. The playground bridges the two by reading the collection from
  // the first identifier after FROM — so `FROM products p` runs against the
  // `products` collection with rows bound to `p`.
  function collectionFromSql(sql) {
    var m = /\bfrom\s+([A-Za-z_][\w-]*)/i.exec(sql);
    return m ? m[1] : null;
  }

  function runSql(env, sql) {
    // FROM-less queries (e.g. `SELECT VALUE 1 + 1`) read no container; any
    // collection name works since slate skips the metadata lookup.
    var coll = collectionFromSql(sql) || env.collections[0] || "_";
    return Array.from(env.db.query(coll, sql));
  }

  // A find cell is a collection name on the first non-empty line followed by an
  // optional JSON filter document.
  function runFind(env, text) {
    var lines = text.split("\n");
    var i = 0;
    while (i < lines.length && lines[i].trim() === "") i++;
    var coll = (lines[i] || "").trim();
    if (!coll) throw new Error("first line must name a collection");
    var rest = lines.slice(i + 1).join("\n").trim();
    var filter = rest ? JSON.parse(rest) : {};
    return Array.from(env.db.find(coll, filter));
  }

  // ── Rendering ─────────────────────────────────────────────────────────────

  function el(tag, cls) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    return n;
  }

  function isPlainObject(v) {
    return v !== null && typeof v === "object" && !Array.isArray(v);
  }

  function formatCell(v) {
    if (v === undefined) return "";
    if (v === null) return "null";
    if (typeof v === "object") return JSON.stringify(v);
    return String(v);
  }

  function buildTable(rows) {
    var cols = [];
    var seen = Object.create(null);
    rows.forEach(function (r) {
      Object.keys(r).forEach(function (k) {
        if (!seen[k]) { seen[k] = true; cols.push(k); }
      });
    });

    var table = el("table", "slate-table");
    var thead = el("thead");
    var htr = el("tr");
    cols.forEach(function (c) {
      var th = el("th");
      th.textContent = c;
      htr.appendChild(th);
    });
    thead.appendChild(htr);
    table.appendChild(thead);

    var tbody = el("tbody");
    rows.forEach(function (r) {
      var tr = el("tr");
      cols.forEach(function (c) {
        var td = el("td");
        td.textContent = formatCell(r[c]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    var wrap = el("div", "slate-table-wrap");
    wrap.appendChild(table);
    return wrap;
  }

  function buildJson(rows) {
    var pre = el("pre", "slate-cell__json");
    pre.textContent = JSON.stringify(rows, null, 2);
    return pre;
  }

  function renderResults(out, rows) {
    out.textContent = "";
    var meta = el("div", "slate-cell__meta");
    meta.textContent = rows.length + (rows.length === 1 ? " row" : " rows");
    out.appendChild(meta);
    if (rows.length === 0) return;
    // Compact table when every row is a plain object; otherwise pretty JSON
    // (scalars from `SELECT VALUE`, arrays, mixed shapes).
    out.appendChild(rows.every(isPlainObject) ? buildTable(rows) : buildJson(rows));
  }

  function renderError(out, e) {
    out.textContent = "";
    var box = el("div", "slate-cell__error");
    box.textContent = (e && e.message) ? e.message : String(e);
    out.appendChild(box);
  }

  // ── Upgrading code blocks into cells ──────────────────────────────────────

  function buildCell(kind, source, env) {
    var cell = el("div", "slate-cell");

    var bar = el("div", "slate-cell__bar");
    var badge = el("span", "slate-cell__badge");
    badge.textContent = kind === "find" ? "find" : "sql";
    bar.appendChild(badge);

    var run = el("button", "slate-cell__run");
    run.type = "button";
    run.textContent = "Run ▸";
    bar.appendChild(run);

    var hint = el("span", "slate-cell__hint");
    var isMac = /Mac|iP(hone|ad|od)/.test((navigator && navigator.platform) || "");
    hint.textContent = (isMac ? "⌘" : "Ctrl") + "+Enter";
    bar.appendChild(hint);

    var input = el("textarea", "slate-cell__input");
    input.value = source;
    input.spellcheck = false;
    input.rows = Math.min(Math.max(source.split("\n").length, 2), 14);

    var output = el("div", "slate-cell__output");

    function execute() {
      try {
        var rows = kind === "find" ? runFind(env, input.value) : runSql(env, input.value);
        renderResults(output, rows);
      } catch (e) {
        renderError(output, e);
      }
    }

    run.addEventListener("click", execute);
    input.addEventListener("keydown", function (ev) {
      if ((ev.metaKey || ev.ctrlKey) && ev.key === "Enter") {
        ev.preventDefault();
        execute();
      }
    });

    cell.appendChild(bar);
    cell.appendChild(input);
    cell.appendChild(output);

    // Show initial results so the page is live on load.
    execute();
    return cell;
  }

  function upgradeBlocks(env) {
    var codes = document.querySelectorAll(
      "code.language-slate-sql, code.language-slate-find"
    );
    Array.prototype.forEach.call(codes, function (code) {
      var pre = code.closest("pre");
      if (!pre || pre.dataset.slateUpgraded) return;
      pre.dataset.slateUpgraded = "1";
      var kind = code.classList.contains("language-slate-find") ? "find" : "sql";
      // mdbook appends a trailing newline to fenced content.
      var source = code.textContent.replace(/\n$/, "");
      pre.replaceWith(buildCell(kind, source, env));
    });
  }

  function start() {
    // Only do the (relatively heavy) wasm load if the page actually has cells.
    if (!document.querySelector("code.language-slate-sql, code.language-slate-find")) {
      return;
    }
    getEnv()
      .then(upgradeBlocks)
      .catch(function (e) {
        // Leave the blocks as static, readable examples.
        console.warn("[slate playground] disabled (wasm unavailable):", e);
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
