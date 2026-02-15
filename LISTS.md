# Lists — Design Notes

## Overview

Lists are saved views associated with a datasource. They define which columns to display, column ordering, sort configuration, and optional default filters. Think of them as the configuration backing a grid UI (ag-grid or similar).

## Implicit Datasource Fields

Every datasource automatically gets these fields injected by the API layer on creation. Users don't specify them — they're added before saving:

| Field | Type |
|-------|------|
| `id` | String |
| `name` | String |
| `status` | String |
| `last_updated_at` | Date |

These exist on every datasource alongside user-defined fields.

## List Types

### Standard Lists

- User-created via API
- **Must** have a `datasource_id`
- Columns must reference fields that exist on the associated datasource (including implicit fields)
- Can have an optional default `FilterGroup` (from `slate-query`)

### Special Lists

- System-defined (e.g. "Snoozed", "Rejected")
- No datasource — they query across all of a user's records by prefix
- Have a predefined filter on `status` (e.g. `status = "snoozed"`)
- **Not** user-creatable

## Data Model

### List

```json
{
  "id": "my-leads",
  "name": "My Leads",
  "datasource_id": "leads",
  "columns": [
    { "field": "name", "data_type": "String", "sort_direction": null, "sort_index": null },
    { "field": "email", "data_type": "String", "sort_direction": "Asc", "sort_index": 0 }
  ],
  "filter": null
}
```

### Column

Position is determined by index in the `columns` array — no separate position field.

On **create**, users send minimal column definitions:

```json
{ "field": "email" }
```

Optionally with sort config:

```json
{ "field": "email", "sort_direction": "Asc", "sort_index": 0 }
```

The API then:
1. Validates `datasource_id` exists
2. Validates each column's `field` exists on the datasource
3. Looks up `data_type` from the datasource's field definitions
4. Stores the enriched column

### Column Fields

| Field | Type | Description |
|-------|------|-------------|
| `field` | String | References a field on the datasource |
| `data_type` | FieldType | Inferred from datasource on create, not user-provided |
| `sort_direction` | Option | `"Asc"` / `"Desc"` / null |
| `sort_index` | Option | For multi-column sort ordering (separate from column position) |

More ag-grid config fields (pinned, width, hidden, etc.) can be added later.

### Filter

A list can have a default `FilterGroup` from `slate-query`:

```json
{
  "filter": {
    "logical": "And",
    "children": [
      { "Condition": { "field": "status", "operator": "Eq", "value": { "String": "active" } } }
    ]
  }
}
```

## API Endpoints

| Method   | Path                  | Description |
|----------|-----------------------|-------------|
| `POST`   | `/v1/lists`           | Create a list (validates datasource + columns) |
| `GET`    | `/v1/lists`           | List all lists |
| `GET`    | `/v1/lists/{id}`      | Get a list by ID |
| `DELETE` | `/v1/lists/{id}`      | Delete a list |

## Open Question: Storage

How to persist lists. Three options discussed:

1. **Serialize to Record fields** — same pattern as datasource catalog (`__ds__` prefix). Works but verbose mapping code for nested types like `FilterGroup` and `Vec<Column>`.

2. **JSON blob in a Record** — store the whole List as `Value::String(json)`. Simple but loses queryability.

3. **Dedicated ListCatalog in slate-db** — new catalog alongside existing `Catalog` with its own prefix and serde logic. Cleanest, mirrors existing pattern, but requires changes to `slate-db`, `slate-server`, and `slate-client`.

**To be decided.**
