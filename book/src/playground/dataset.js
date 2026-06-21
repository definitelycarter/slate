// Shared sample dataset for the query playground.
//
// Served as a static ES module under src/playground/ and dynamic-imported once
// by theme/playground/playground.js, which seeds it into the in-browser
// SlateDb before any cell runs. The shapes mirror tools/cosmos-parity/datasets
// (a flat `products` catalog and a nested `families` collection) so the same
// queries the parity suite exercises can be run live here.
//
// Keys use `_id` (slate's default primary-key path). Edit a query in any cell
// and re-run; reloading the page re-seeds a fresh copy.

export const DATASET = {
  // Flat documents: scalars, a boolean, and a string array (`tags`).
  products: [
    { _id: "prod-001", category: "Electronics", name: "Premium Laptop", price: 1299.99, inStock: true, tags: ["computer", "productivity", "business"] },
    { _id: "prod-002", category: "Electronics", name: "Wireless Mouse", price: 29.99, inStock: true, tags: ["accessory", "peripheral"] },
    { _id: "prod-003", category: "Furniture", name: "Ergonomic Office Chair", price: 399.99, inStock: true, tags: ["furniture", "office", "comfort"] },
    { _id: "prod-004", category: "Furniture", name: "LED Desk Lamp", price: 49.99, inStock: false, tags: ["lighting", "office"] },
    { _id: "prod-005", category: "Stationery", name: "Premium Notebook Set", price: 24.99, inStock: true, tags: ["stationery", "writing", "organization"] }
  ],

  // Nested documents: a sub-object (`address`) and arrays of objects
  // (`parents`, `children`, each child with its own `pets`) — for JOINs over
  // arrays and sub-path access.
  families: [
    {
      _id: "AndersenFamily",
      lastName: "Andersen",
      isRegistered: true,
      address: { state: "WA", county: "King", city: "Seattle" },
      parents: [{ firstName: "Thomas" }, { firstName: "Mary Kay" }],
      children: [
        { firstName: "Henriette Thaulow", gender: "female", grade: 5, pets: [{ givenName: "Fluffy" }] }
      ],
      tags: ["a", "b", "c"]
    },
    {
      _id: "WakefieldFamily",
      lastName: "Wakefield",
      isRegistered: false,
      address: { state: "NY", county: "Manhattan", city: "NY" },
      parents: [{ firstName: "Robin" }, { firstName: "Ben" }],
      children: [
        { firstName: "Jesse", gender: "female", grade: 1, pets: [{ givenName: "Goofy" }, { givenName: "Shadow" }] },
        { firstName: "Lisa", gender: "female", grade: 8 }
      ],
      tags: ["x", "y"]
    },
    {
      _id: "SmithFamily",
      lastName: "Smith",
      isRegistered: true,
      address: { state: "WA", county: "Pierce", city: "Tacoma" },
      parents: [{ firstName: "Joe" }],
      children: [],
      tags: []
    }
  ]
};

// Secondary indexes created after seeding. The playground builds these so that
// equality filters on the listed fields (e.g. `WHERE c.category = "..."`) are
// served by an index scan rather than a full collection scan.
export const INDEXES = {
  products: ["category"],
  families: ["lastName"]
};
