/* tslint:disable */
/* eslint-disable */

export class SlateDb {
    free(): void;
    [Symbol.dispose](): void;
    count(collection: string, filter: any): number;
    create_collection(name: string): void;
    create_index(collection: string, field: string): void;
    delete_many(collection: string, filter: any): Array<any>;
    delete_one(collection: string, filter: any): Array<any>;
    drop_collection(name: string): void;
    drop_index(collection: string, field: string): void;
    find(collection: string, filter: any): Array<any>;
    find_one(collection: string, filter: any): any;
    insert_many(collection: string, docs: Array<any>): Array<any>;
    insert_one(collection: string, doc: any): Array<any>;
    list_collections(): Array<any>;
    list_indexes(collection: string): Array<any>;
    merge_many(collection: string, docs: Array<any>): Array<any>;
    constructor();
    /**
     * Run a CosmosDB-style SQL statement against `collection` and return the
     * result rows as a JS array.
     *
     * SQL is read-only and shares the query stack with `find`. The container is
     * chosen out-of-band here (the `FROM` clause only binds the row alias,
     * matching Cosmos), so — like `find` — the collection is an explicit
     * argument rather than part of the query text. Rows are converted at the
     * value level (see [`value_to_js`]) so scalar `SELECT VALUE` projections,
     * not just documents, round-trip cleanly.
     */
    query(collection: string, sql: string): Array<any>;
    replace_one(collection: string, filter: any, replacement: any): Array<any>;
    update_many(collection: string, filter: any, update: any): Array<any>;
    update_one(collection: string, filter: any, update: any): Array<any>;
    upsert_many(collection: string, docs: Array<any>): Array<any>;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_slatedb_free: (a: number, b: number) => void;
    readonly slatedb_count: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly slatedb_create_collection: (a: number, b: number, c: number) => [number, number];
    readonly slatedb_create_index: (a: number, b: number, c: number, d: number, e: number) => [number, number];
    readonly slatedb_delete_many: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly slatedb_delete_one: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly slatedb_drop_collection: (a: number, b: number, c: number) => [number, number];
    readonly slatedb_drop_index: (a: number, b: number, c: number, d: number, e: number) => [number, number];
    readonly slatedb_find: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly slatedb_find_one: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly slatedb_insert_many: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly slatedb_insert_one: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly slatedb_list_collections: (a: number) => [number, number, number];
    readonly slatedb_list_indexes: (a: number, b: number, c: number) => [number, number, number];
    readonly slatedb_merge_many: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly slatedb_new: () => [number, number, number];
    readonly slatedb_query: (a: number, b: number, c: number, d: number, e: number) => [number, number, number];
    readonly slatedb_replace_one: (a: number, b: number, c: number, d: any, e: any) => [number, number, number];
    readonly slatedb_update_many: (a: number, b: number, c: number, d: any, e: any) => [number, number, number];
    readonly slatedb_update_one: (a: number, b: number, c: number, d: any, e: any) => [number, number, number];
    readonly slatedb_upsert_many: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
