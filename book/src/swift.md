# Swift / Apple Platforms

Slate can be embedded directly in macOS and iOS apps via [UniFFI](https://mozilla.github.io/uniffi-rs/). The `slate-uniffi` crate generates Swift bindings that wrap `slate-db` with a JSON-based API — no TCP server, no network round-trips.

## Storage Backends

Choose a backend at compile time via feature flags:

| Feature | Store | Use Case |
|---------|-------|----------|
| `memory` (default) | `MemoryStore` | Ephemeral data, caching |
| `redb` | `RedbStore` | Persistent, pure-Rust (no C deps) |
| `rocksdb` | `RocksStore` | Persistent, highest throughput |

For Apple platform distribution, `redb` is recommended — it's pure Rust with no C dependencies, compiles cleanly for all Apple targets, and produces smaller binaries. `rocksdb` requires a C toolchain and `libclang`.

## Build

### 1. Add Rust targets

```bash
# macOS (usually already installed)
rustup target add aarch64-apple-darwin

# iOS device
rustup target add aarch64-apple-ios

# iOS simulator (Apple Silicon)
rustup target add aarch64-apple-ios-sim
```

### 2. Build the library

```bash
# macOS
cargo build -p slate-uniffi --release --no-default-features --features redb

# iOS device
cargo build -p slate-uniffi --release --target aarch64-apple-ios \
  --no-default-features --features redb

# iOS simulator (Apple Silicon)
cargo build -p slate-uniffi --release --target aarch64-apple-ios-sim \
  --no-default-features --features redb
```

This produces `libslate_uniffi.a` in `target/<target>/release/`.

### 3. Generate Swift bindings

The bindings must be generated from a library built with the same features:

```bash
cargo run -p slate-uniffi --no-default-features --features redb \
  --bin uniffi-bindgen generate \
  --library target/release/libslate_uniffi.dylib \
  --language swift \
  --out-dir crates/slate-uniffi/bindings/swift
```

This generates three files:

| File | Purpose |
|------|---------|
| `slate_uniffi.swift` | Swift bindings |
| `slate_uniffiFFI.h` | C header for FFI |
| `slate_uniffiFFI.modulemap` | Module map |

Generated bindings are gitignored — regenerate them after building with your chosen feature.

### 4. Create an XCFramework

Bundle the static libraries for each target into an XCFramework:

```bash
xcodebuild -create-xcframework \
  -library target/aarch64-apple-ios/release/libslate_uniffi.a \
  -headers crates/slate-uniffi/bindings/swift/slate_uniffiFFI.h \
  -library target/aarch64-apple-ios-sim/release/libslate_uniffi.a \
  -headers crates/slate-uniffi/bindings/swift/slate_uniffiFFI.h \
  -output SlateFFI.xcframework
```

### 5. Add to Xcode project

1. Drag `SlateFFI.xcframework` into your Xcode project
2. Add `slate_uniffi.swift` to your Swift sources
3. Add the modulemap so Swift can find the C header

## Usage

### In-memory database

```swift
let db = SlateDatabase.memory()
```

### Persistent database (redb)

```swift
let path = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
    .appendingPathComponent("slate.redb").path
let db = try SlateDatabase.open(path: path)
```

### Collections and indexes

```swift
try db.createCollection(name: "users", indexes: ["status", "email"])
let collections = try db.listCollections()
try db.dropCollection(collection: "users")

try db.createIndex(collection: "users", field: "name")
try db.dropIndex(collection: "users", field: "name")
let indexes = try db.listIndexes(collection: "users")
```

### Insert

```swift
let id = try db.insertOne(collection: "users", docJson: """
  {"name": "Alice", "status": "active", "email": "alice@example.com"}
""")

let ids = try db.insertMany(collection: "users", docsJson: """
  [
    {"name": "Bob", "status": "active"},
    {"name": "Carol", "status": "rejected"}
  ]
""")
```

### Query

Documents and queries are passed as JSON strings. Results are returned as JSON strings.

```swift
// All documents
let all = try db.find(collection: "users", queryJson: "{}")

// With filter
let active = try db.find(collection: "users", queryJson: """
  {"filter": {"logical": "and", "children": [
    {"condition": {"field": "status", "operator": "eq", "value": "active"}}
  ]}}
""")

// With sort, skip, take
let page = try db.find(collection: "users", queryJson: """
  {"filter": {"logical": "and", "children": [
    {"condition": {"field": "status", "operator": "eq", "value": "active"}}
  ]}, "sort": [{"field": "name", "direction": "asc"}], "skip": 0, "take": 20}
""")

// With projection
let names = try db.find(collection: "users", queryJson: """
  {"columns": ["name", "email"]}
""")

// Find one
let user = try db.findOne(collection: "users", queryJson: """
  {"filter": {"logical": "and", "children": [
    {"condition": {"field": "email", "operator": "eq", "value": "alice@example.com"}}
  ]}}
""")

// Find by ID
let record = try db.findById(collection: "users", id: id, columns: nil)

// Count
let count = try db.count(collection: "users", filterJson: nil)
```

### Update

```swift
let filter = """
  {"logical": "and", "children": [
    {"condition": {"field": "_id", "operator": "eq", "value": "\(id)"}}
  ]}
"""

// Merge update (preserves unspecified fields)
let result = try db.updateOne(
    collection: "users",
    filterJson: filter,
    updateJson: """{"status": "archived"}""",
    upsert: false
)
// result.matched, result.modified, result.upsertedId

// Full replacement
let replaced = try db.replaceOne(
    collection: "users",
    filterJson: filter,
    docJson: """{"name": "Alice", "status": "inactive"}"""
)
```

### Delete

```swift
let deleted = try db.deleteOne(collection: "users", filterJson: filter)
// deleted == 1

let deletedMany = try db.deleteMany(collection: "users", filterJson: """
  {"logical": "and", "children": [
    {"condition": {"field": "status", "operator": "eq", "value": "rejected"}}
  ]}
""")
```

### Bulk operations

```swift
// Upsert (insert or full replace)
let upsertResult = try db.upsertMany(collection: "users", docsJson: """
  [{"_id": "user-1", "name": "Alice", "status": "active"}]
""")
// upsertResult.inserted, upsertResult.updated

// Merge (insert or partial update)
let mergeResult = try db.mergeMany(collection: "users", docsJson: """
  [{"_id": "user-1", "status": "archived"}]
""")
```

### Error handling

```swift
do {
    let id = try db.insertOne(collection: "users", docJson: """
      {"_id": "user-1", "name": "Alice"}
    """)
} catch SlateError.DuplicateKey(let message) {
    print("Duplicate: \(message)")
} catch SlateError.InvalidQuery(let message) {
    print("Bad query: \(message)")
} catch SlateError.Serialization(let message) {
    print("JSON error: \(message)")
}
```

## Design Notes

- **Auto-commit**: Each method runs in its own transaction. No multi-operation transactions across the FFI boundary. This matches the HTTP API pattern.
- **JSON strings**: Documents, queries, and filters are passed as JSON strings. Swift's `Codable` or `JSONSerialization` work naturally with this.
- **Thread safety**: `SlateDatabase` conforms to `Sendable` — safe to use from any thread or actor.

## Filter Operators

| Operator | Description |
|----------|-------------|
| `eq` | Equals |
| `gt` / `gte` | Greater than / greater or equal |
| `lt` / `lte` | Less than / less or equal |
| `icontains` | Case-insensitive substring |
| `istarts_with` | Case-insensitive prefix |
| `iends_with` | Case-insensitive suffix |
| `is_null` | Field is null (value: `true`) or not null (value: `false`) |
