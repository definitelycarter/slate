# Function Reference

Slate implements CosmosDB's built-in functions for use in SQL queries (and, where
noted, behind the Mongo update operators). This chapter is the per-function
catalog; for the grammar that calls them, see [Querying](./querying.md).

Slate is embedded in this page as WebAssembly, so the examples are **live** —
every function below carries a `Run ▸` cell you can execute and edit, seeded with
the playground's `products` and `families` collections (see the
[Playground](./playground.md) for the data). Here's one to start:

```slate-sql
SELECT VALUE UPPER("hello world")
```

## Conventions

- **Case-insensitive.** `ABS(x)`, `abs(x)`, and `Abs(x)` are identical — names are
  upper-cased before dispatch.
- **Two failure modes.** A call with the *wrong number of arguments* is a **query
  error**. A call with the *wrong argument type* returns **`undefined`** — and an
  `undefined` value is omitted from its document (in a projection) or excludes the
  row (in a `WHERE`), matching CosmosDB. So `UPPER(123)` doesn't error; it simply
  yields nothing.
- **`null` vs `undefined`.** `null` is a real JSON value; `undefined` is the
  absence of one. The `IS_NULL` / `IS_DEFINED` family distinguishes them, and most
  functions treat `null` as a non-matching type (→ `undefined`).
- **Number model.** CosmosDB treats every number as a 64-bit double, so most math
  functions **widen integer inputs and return `Double`** (`CEILING(3)` → `3.0`).
  The exceptions are called out below: `ABS` preserves the input integer type, the
  `INT*` integer/bitwise functions return `Int64`, and `INC` is type-preserving.

**Categories:** [Math](#math) · [Integer & bitwise](#integer--bitwise) ·
[Type checking](#type-checking) · [String](#string) · [Array](#array) ·
[Conditional & object](#conditional--object) · [Mutation helpers](#mutation-helpers) ·
[Date & time](#date--time) · [Spatial](#spatial) · [Aggregates](#aggregate-functions)

## Math

### `ABS(num)`
Absolute value of a numeric expression.
- **Returns:** same numeric type as the input (`Int64` for integers, `Double` for doubles) — unlike the other math functions it does not widen to `Double`; non-numeric input yields undefined.
- **Example:** `ABS(-5)` → `5`

```slate-sql
SELECT VALUE ABS(-5)
```

### `ACOS(num)`
Arccosine of `num`, in radians.
- **Returns:** `Double`; undefined on non-numeric input.
- **Example:** `ACOS(-1)` → `3.141592653589793` (π)

```slate-sql
SELECT VALUE ACOS(-1)
```

### `ASIN(num)`
Arcsine of `num`, in radians.
- **Returns:** `Double`; undefined on non-numeric input.
- **Example:** `ASIN(1)` → `1.5707963267948966` (π/2)

```slate-sql
SELECT VALUE ASIN(1)
```

### `ATAN(num)`
Arctangent of `num`, in radians.
- **Returns:** `Double`; undefined on non-numeric input.
- **Example:** `ATAN(0)` → `0`

```slate-sql
SELECT VALUE ATAN(0)
```

### `ATN2(n1, n2)`
Angle in radians between the positive x-axis and the point `(n2, n1)` — the two-argument arctangent of `n1 / n2`.
- **Returns:** `Double`; undefined if either argument is non-numeric.
- **Example:** `ATN2(1, 1)` → `0.7853981633974483` (π/4)

```slate-sql
SELECT VALUE ATN2(1, 1)
```

### `CEILING(num)`
Smallest integer value greater than or equal to `num`.
- **Returns:** `Double` (integer inputs widen, e.g. `CEILING(0)` → `0.0`); undefined on non-numeric input.
- **Example:** `CEILING(123.45)` → `124`

```slate-sql
SELECT VALUE CEILING(123.45)
```

### `COS(num)`
Trigonometric cosine of `num`, in radians.
- **Returns:** `Double`; undefined on non-numeric input.
- **Example:** `COS(0)` → `1`

```slate-sql
SELECT VALUE COS(0)
```

### `COT(num)`
Trigonometric cotangent of `num` in radians (`1 / tan(num)`).
- **Returns:** `Double`; undefined on non-numeric input.
- **Example:** `COT(0.7853981633974483)` → `1.0000000000000002` (cotangent of π/4, to floating-point precision)

```slate-sql
SELECT VALUE COT(0.7853981633974483)
```

### `DEGREES(num)`
Converts `num` radians to degrees.
- **Returns:** `Double`; undefined on non-numeric input.
- **Example:** `DEGREES(3.141592653589793)` → `180`

```slate-sql
SELECT VALUE DEGREES(3.141592653589793)
```

### `EXP(num)`
`e` raised to the power `num`.
- **Returns:** `Double`; undefined on non-numeric input.
- **Example:** `EXP(10)` → `22026.465794806718`

```slate-sql
SELECT VALUE EXP(10)
```

### `FLOOR(num)`
Largest integer value less than or equal to `num`.
- **Returns:** `Double` (integer inputs widen); undefined on non-numeric input (including null).
- **Example:** `FLOOR(62.6)` → `62`

```slate-sql
SELECT VALUE FLOOR(62.6)
```

### `LOG(num[, base])`
The logarithm of `num` — natural log with one argument, or in the given `base` with a second.
- **Returns:** `Double`; undefined if any argument is non-numeric.
- **Example:** `LOG(5)` → `1.6094379124341003`

```slate-sql
SELECT VALUE LOG(5)
```

### `LOG10(num)`
The base-10 logarithm of `num`.
- **Returns:** `Double`; undefined on non-numeric input.
- **Example:** `LOG10(100)` → `2`

```slate-sql
SELECT VALUE LOG10(100)
```

### `PI()`
The constant value of π.
- **Returns:** `Double`; takes no arguments (any argument is an arity error).
- **Example:** `PI()` → `3.141592653589793`

```slate-sql
SELECT VALUE PI()
```

### `POWER(base, exp)`
`base` raised to the power `exp`.
- **Returns:** `Double`; undefined if either argument is non-numeric (including null).
- **Example:** `POWER(2, 2)` → `4`

```slate-sql
SELECT VALUE POWER(2, 2)
```

### `RADIANS(num)`
Converts `num` degrees to radians.
- **Returns:** `Double`; undefined on non-numeric input.
- **Example:** `RADIANS(180)` → `3.141592653589793` (π)

```slate-sql
SELECT VALUE RADIANS(180)
```

### `ROUND(num)`
`num` rounded to the closest integer, using midpoint rounding away from zero.
- **Returns:** `Double` (integer inputs widen); undefined on non-numeric input.
- **Example:** `ROUND(2.5)` → `3`

```slate-sql
SELECT VALUE ROUND(2.5)
```

### `SIGN(num)`
The sign of `num`: `-1`, `0`, or `+1` (zero returns `0`, not `+1`).
- **Returns:** `Double`; undefined on non-numeric input.
- **Example:** `SIGN(-2)` → `-1`

```slate-sql
SELECT VALUE SIGN(-2)
```

### `SIN(num)`
Trigonometric sine of `num`, in radians.
- **Returns:** `Double`; undefined on non-numeric input.
- **Example:** `SIN(0)` → `0`

```slate-sql
SELECT VALUE SIN(0)
```

### `SQRT(num)`
The square root of `num`.
- **Returns:** `Double` (IEEE correctly-rounded); undefined on non-numeric input.
- **Example:** `SQRT(17)` → `4.123105625617661`

```slate-sql
SELECT VALUE SQRT(17)
```

### `SQUARE(num)`
`num` multiplied by itself.
- **Returns:** `Double` (integer inputs widen); undefined on non-numeric input (including null).
- **Example:** `SQUARE(3)` → `9`

```slate-sql
SELECT VALUE SQUARE(3)
```

### `TAN(num)`
Trigonometric tangent of `num`, in radians.
- **Returns:** `Double`; undefined on non-numeric input.
- **Example:** `TAN(0)` → `0`

```slate-sql
SELECT VALUE TAN(0)
```

### `TRUNC(num)`
`num` truncated toward zero to the closest integer.
- **Returns:** `Double` (integer inputs widen); undefined on non-numeric input.
- **Example:** `TRUNC(2.37)` → `2`

```slate-sql
SELECT VALUE TRUNC(2.37)
```

## Integer & bitwise

These operate on integers and return `Int64`. A fractional or non-numeric argument
yields `undefined`.

### `INTADD(num1, num2)`
Integer sum of two integers (wrapping).
- **Returns:** `Int64`; a fractional or non-numeric argument yields undefined.
- **Example:** `INTADD(20, 10)` → `30`

```slate-sql
SELECT VALUE INTADD(20, 10)
```

### `INTSUB(num1, num2)`
Integer difference of two integers (wrapping).
- **Returns:** `Int64`; a fractional or non-numeric argument yields undefined.
- **Example:** `INTSUB(25, 50)` → `-25`

```slate-sql
SELECT VALUE INTSUB(25, 50)
```

### `INTMUL(num1, num2)`
Integer product of two integers (wrapping).
- **Returns:** `Int64`; a fractional or non-numeric argument yields undefined.
- **Example:** `INTMUL(5, 2)` → `10`

```slate-sql
SELECT VALUE INTMUL(5, 2)
```

### `INTDIV(num1, num2)`
Integer (truncating) division of two integers.
- **Returns:** `Int64`; division by zero (or `i64::MIN / -1` overflow, or a fractional/non-numeric arg) yields undefined.
- **Example:** `INTDIV(10, 2)` → `5`

```slate-sql
SELECT VALUE INTDIV(10, 2)
```

### `INTMOD(num1, num2)`
Integer remainder (truncated division) of two integers.
- **Returns:** `Int64`; modulo by zero (or a fractional/non-numeric arg) yields undefined.
- **Example:** `INTMOD(12, 5)` → `2`

```slate-sql
SELECT VALUE INTMOD(12, 5)
```

### `INTBITAND(num1, num2)`
Bitwise AND of two integers.
- **Returns:** `Int64`; a fractional or non-numeric argument yields undefined.
- **Example:** `INTBITAND(15, 25)` → `9`

```slate-sql
SELECT VALUE INTBITAND(15, 25)
```

### `INTBITOR(num1, num2)`
Bitwise inclusive OR of two integers.
- **Returns:** `Int64`; a fractional or non-numeric argument yields undefined.
- **Example:** `INTBITOR(56, 100)` → `124`

```slate-sql
SELECT VALUE INTBITOR(56, 100)
```

### `INTBITXOR(num1, num2)`
Bitwise exclusive OR of two integers.
- **Returns:** `Int64`; a fractional or non-numeric argument yields undefined.
- **Example:** `INTBITXOR(56, 100)` → `92`

```slate-sql
SELECT VALUE INTBITXOR(56, 100)
```

### `INTBITNOT(num)`
Bitwise complement of an integer.
- **Returns:** `Int64`; a fractional or non-numeric argument yields undefined.
- **Example:** `INTBITNOT(65)` → `-66`

```slate-sql
SELECT VALUE INTBITNOT(65)
```

### `INTBITLEFTSHIFT(num, shift)`
Left-shift `num` by `shift` bits.
- **Returns:** `Int64`; a fractional/non-numeric arg, a negative shift, or a shift of 64+ bits yields undefined.
- **Example:** `INTBITLEFTSHIFT(16, 4)` → `256`

```slate-sql
SELECT VALUE INTBITLEFTSHIFT(16, 4)
```

### `INTBITRIGHTSHIFT(num, shift)`
Arithmetic right-shift `num` by `shift` bits.
- **Returns:** `Int64`; a fractional/non-numeric arg, a negative shift, or a shift of 64+ bits yields undefined.
- **Example:** `INTBITRIGHTSHIFT(16, 4)` → `1`

```slate-sql
SELECT VALUE INTBITRIGHTSHIFT(16, 4)
```

### `NUMBERBIN(value[, bin_size])`
Round `value` down to the nearest multiple of `bin_size` (`floor(value / bin_size) * bin_size`), with `bin_size` defaulting to `1`.
- **Returns:** `Double`; a non-numeric argument or a `bin_size` of zero yields undefined.
- **Example:** `NUMBERBIN(37.752, 10)` → `30.0`

```slate-sql
SELECT VALUE NUMBERBIN(37.752, 10)
```

### `INC(current, delta)`
Type-preserving numeric increment that backs the Mongo `$inc` update operator (distinct from `+`, which widens to double per Cosmos's model).
- **Returns:** the promoted numeric type (i32+i32 → `Int32`, overflowing to `Int64`; mixed i32/i64 → `Int64`; anything + double → `Double`); a missing `current` starts from `0` of `delta`'s type, and a non-numeric operand yields undefined.
- **Example:** `INC(6, 1)` → `7`

```slate-sql
SELECT VALUE INC(6, 1)
```

## Type checking

Each takes one argument and always returns a `Boolean` (never `undefined`); a
missing/undefined value is `false`.

### `IS_ARRAY(expr)`
Whether the value is an array.
- **Returns:** `Boolean`.
- **Example:** `IS_ARRAY([25344, 82947])` → `true`

```slate-sql
SELECT VALUE IS_ARRAY([25344, 82947])
```

### `IS_BOOL(expr)`
Whether the value is a boolean.
- **Returns:** `Boolean`.
- **Example:** `IS_BOOL(true)` → `true`

```slate-sql
SELECT VALUE IS_BOOL(true)
```

### `IS_DEFINED(expr)`
Whether the property has a value (is not undefined).
- **Returns:** `Boolean`; `true` for any defined value, `false` only for undefined.
- **Example:** `IS_DEFINED(1)` → `true`

```slate-sql
SELECT VALUE IS_DEFINED(1)
```

### `IS_FINITE_NUMBER(expr)`
Whether the value is a finite number (not infinity or NaN); integers are always finite.
- **Returns:** `Boolean`.
- **Example:** `IS_FINITE_NUMBER(1234.567)` → `true`

```slate-sql
SELECT VALUE IS_FINITE_NUMBER(1234.567)
```

### `IS_INTEGER(expr)`
Whether the value represents a signed 64-bit integer (a value + range test, not type identity).
- **Returns:** `Boolean`; `Int32`/`Int64` always qualify, and a `Double` qualifies only if finite, with no fractional part, and within `i64` range — so `5.0` → `true`, `5.5` → `false`.
- **Example:** `IS_INTEGER(5523432)` → `true`

```slate-sql
SELECT VALUE IS_INTEGER(5523432)
```

### `IS_NULL(expr)`
Whether the value is JSON null.
- **Returns:** `Boolean`; undefined is not null and returns `false`.
- **Example:** `IS_NULL(null)` → `true`

```slate-sql
SELECT VALUE IS_NULL(null)
```

### `IS_NUMBER(expr)`
Whether the value is a number.
- **Returns:** `Boolean`.
- **Example:** `IS_NUMBER(1)` → `true`

```slate-sql
SELECT VALUE IS_NUMBER(1)
```

### `IS_OBJECT(expr)`
Whether the value is a JSON object (document).
- **Returns:** `Boolean`; arrays and missing/undefined return `false`.
- **Example:** `IS_OBJECT({ "name": "Tecozow coat" })` → `true`

```slate-sql
SELECT VALUE IS_OBJECT({ "name": "Tecozow coat" })
```

### `IS_PRIMITIVE(expr)`
Whether the value is a primitive: string, boolean, number, or null.
- **Returns:** `Boolean`; arrays, objects, and undefined return `false`.
- **Example:** `IS_PRIMITIVE("value")` → `true`

```slate-sql
SELECT VALUE IS_PRIMITIVE("value")
```

### `IS_STRING(expr)`
Whether the value is a string.
- **Returns:** `Boolean`.
- **Example:** `IS_STRING("value")` → `true`

```slate-sql
SELECT VALUE IS_STRING("value")
```

## String

Unless noted, a non-string argument yields `undefined`.

### `CONCAT(str, str, …)`
Concatenates two or more strings.
- **Returns:** String; undefined if any argument is not a string. Requires at least 2 arguments.
- **Example:** `CONCAT("a", "b", "c")` → `"abc"`

```slate-sql
SELECT VALUE CONCAT("a", "b", "c")
```

### `CONTAINS(str, substr[, ignoreCase])`
Reports whether the first string contains the second.
- **Returns:** Boolean; the optional third argument enables case-insensitive search.
- **Example:** `CONTAINS("Hello", "ELL", true)` → `true`

```slate-sql
SELECT VALUE CONTAINS("Hello", "ELL", true)
```

### `ENDSWITH(str, suffix[, ignoreCase])`
Reports whether a string ends with a suffix.
- **Returns:** Boolean; the optional third argument enables case-insensitive search.
- **Example:** `ENDSWITH("AdventureWorks", "Works")` → `true`

```slate-sql
SELECT VALUE ENDSWITH("AdventureWorks", "Works")
```

### `INDEX_OF(str, search[, start])`
Returns the zero-based character index of the first occurrence of `search` in `str`, or `-1` if not found.
- **Returns:** Int64; the optional third argument sets the character position to start searching from.
- **Example:** `INDEX_OF("AdventureWorks", "Works")` → `9`

```slate-sql
SELECT VALUE INDEX_OF("AdventureWorks", "Works")
```

### `LEFT(str, n)`
Returns the first `n` characters of a string.
- **Returns:** String; a negative `n` yields the empty string; `n` beyond the length returns the whole string.
- **Example:** `LEFT("AdventureWorks", 5)` → `"Adven"`

```slate-sql
SELECT VALUE LEFT("AdventureWorks", 5)
```

### `LENGTH(str)`
Returns the number of characters in a string.
- **Returns:** Int64.
- **Example:** `LENGTH("hello")` → `5`

```slate-sql
SELECT VALUE LENGTH("hello")
```

### `LOWER(str)`
Converts a string to lowercase.
- **Returns:** String.
- **Example:** `LOWER("aBc")` → `"abc"`

```slate-sql
SELECT VALUE LOWER("aBc")
```

### `LTRIM(str[, chars])`
Removes leading whitespace, or any character in the given set, from the start of a string.
- **Returns:** String; the optional second argument is treated as a *set* of characters, not a substring; an empty set trims nothing.
- **Example:** `LTRIM("AdventureWorks", "Adventure")` → `"Works"`

```slate-sql
SELECT VALUE LTRIM("AdventureWorks", "Adventure")
```

### `REGEXMATCH(str, pattern[, modifiers])`
Reports whether a string matches a regular expression.
- **Returns:** Boolean; undefined for an unknown modifier or invalid pattern. The optional modifiers string accepts `i`, `m`, `s`, and `x`.
- **Example:** `REGEXMATCH("abcd", "ABC", "i")` → `true`

```slate-sql
SELECT VALUE REGEXMATCH("abcd", "ABC", "i")
```

### `REPLACE(str, old, new)`
Replaces every occurrence of `old` with `new`.
- **Returns:** String.
- **Example:** `REPLACE("AdventureWorksLT", "LT", "LT2")` → `"AdventureWorksLT2"`

```slate-sql
SELECT VALUE REPLACE("AdventureWorksLT", "LT", "LT2")
```

### `REPLICATE(str, n)`
Returns a string repeated `n` times.
- **Returns:** String; undefined for a negative or non-finite count, or a result exceeding 10,000 characters.
- **Example:** `REPLICATE("Cosmic", 3)` → `"CosmicCosmicCosmic"`

```slate-sql
SELECT VALUE REPLICATE("Cosmic", 3)
```

### `REVERSE(str)`
Returns the characters of a string in reverse order.
- **Returns:** String.
- **Example:** `REVERSE("AdventureWorks")` → `"skroWerutnevdA"`

```slate-sql
SELECT VALUE REVERSE("AdventureWorks")
```

### `RIGHT(str, n)`
Returns the last `n` characters of a string.
- **Returns:** String; a negative `n` yields the empty string; `n` beyond the length returns the whole string.
- **Example:** `RIGHT("AdventureWorks", 5)` → `"Works"`

```slate-sql
SELECT VALUE RIGHT("AdventureWorks", 5)
```

### `RTRIM(str[, chars])`
Removes trailing whitespace, or any character in the given set, from the end of a string.
- **Returns:** String; the optional second argument is treated as a *set* of characters, not a substring; an empty set trims nothing.
- **Example:** `RTRIM("AdventureWorks", "Works")` → `"Adventure"`

```slate-sql
SELECT VALUE RTRIM("AdventureWorks", "Works")
```

### `STARTSWITH(str, prefix[, ignoreCase])`
Reports whether a string starts with a prefix.
- **Returns:** Boolean; the optional third argument enables case-insensitive search.
- **Example:** `STARTSWITH("Hello", "HE", true)` → `true`

```slate-sql
SELECT VALUE STARTSWITH("Hello", "HE", true)
```

### `STRINGEQUALS(str1, str2[, ignoreCase])`
Reports whether two strings are equal.
- **Returns:** Boolean; the optional third argument enables case-insensitive comparison.
- **Example:** `STRINGEQUALS("AdventureWorks", "adventureworks", true)` → `true`

```slate-sql
SELECT VALUE STRINGEQUALS("AdventureWorks", "adventureworks", true)
```

### `STRINGJOIN(array, separator)`
Concatenates the elements of an array into a string, placing the separator between each element.
- **Returns:** String; undefined unless the first argument is an array of all strings and the second is a string. An empty array yields the empty string.
- **Example:** `STRINGJOIN(["a", "b", "c"], "-")` → `"a-b-c"`

```slate-sql
SELECT VALUE STRINGJOIN(["a", "b", "c"], "-")
```

### `STRINGSPLIT(string, delimiter)`
Splits a string into an array of substrings on each occurrence of the delimiter.
- **Returns:** Array of strings; an empty delimiter returns the whole string as a single element (it does not split into characters).
- **Example:** `STRINGSPLIT("a,b,c", ",")` → `["a", "b", "c"]`

```slate-sql
SELECT VALUE STRINGSPLIT("a,b,c", ",")
```

### `STRINGTOARRAY(str)`
Parses a JSON array string into an array.
- **Returns:** Array; undefined for invalid JSON or JSON that isn't an array.
- **Example:** `STRINGTOARRAY("[\"a\", \"b\"]")` → `["a", "b"]`

```slate-sql
SELECT VALUE STRINGTOARRAY("[\"a\", \"b\"]")
```

### `STRINGTOBOOLEAN(str)`
Parses a string to a boolean (surrounding whitespace ignored).
- **Returns:** Boolean; undefined for anything other than the strings `"true"`/`"false"`.
- **Example:** `STRINGTOBOOLEAN("  false  ")` → `false`

```slate-sql
SELECT VALUE STRINGTOBOOLEAN("  false  ")
```

### `STRINGTONULL(str)`
Parses a string to null (surrounding whitespace ignored, case-sensitive).
- **Returns:** Null; undefined for anything other than the string `"null"`.
- **Example:** `STRINGTONULL("  null  ")` → `null`

```slate-sql
SELECT VALUE STRINGTONULL("  null  ")
```

### `STRINGTONUMBER(str)`
Parses a string to a number (leading/trailing whitespace ignored).
- **Returns:** Int64 for integer text, otherwise Double; undefined for a string that isn't a finite number.
- **Example:** `STRINGTONUMBER("3.14")` → `3.14`

```slate-sql
SELECT VALUE STRINGTONUMBER("3.14")
```

### `STRINGTOOBJECT(str)`
Parses a JSON object string into an object.
- **Returns:** Object; undefined for invalid JSON or JSON that isn't an object.
- **Example:** `STRINGTOOBJECT("{\"isAvailable\": true}")` → `{"isAvailable": true}`

```slate-sql
SELECT VALUE STRINGTOOBJECT("{\"isAvailable\": true}")
```

### `SUBSTRING(str, start, length)`
Returns a portion of a string starting at a zero-based character position for a given character count.
- **Returns:** String; a negative `length`, or a `start` at or past the end, yields the empty string.
- **Example:** `SUBSTRING("AdventureWorks", 9, 5)` → `"Works"`

```slate-sql
SELECT VALUE SUBSTRING("AdventureWorks", 9, 5)
```

### `TOSTRING(value)`
Returns a string representation of a scalar value (strings verbatim, numbers/booleans/null as JSON tokens, arrays/objects as compact JSON).
- **Returns:** String; `undefined` stays `undefined`.
- **Example:** `TOSTRING(125)` → `"125"`

```slate-sql
SELECT VALUE TOSTRING(125)
```

### `TRIM(str[, chars])`
Removes leading and trailing characters from a string.
- **Returns:** String; with one argument it strips whitespace; the optional second argument is a *set* of characters to strip from both ends, not a substring.
- **Example:** `TRIM("___AdventureWorks___", "_")` → `"AdventureWorks"`

```slate-sql
SELECT VALUE TRIM("___AdventureWorks___", "_")
```

### `UPPER(str)`
Converts a string to uppercase.
- **Returns:** String.
- **Example:** `UPPER("aBc")` → `"ABC"`

```slate-sql
SELECT VALUE UPPER("aBc")
```

## Array

A non-array argument yields `undefined`.

### `ARRAY_CONCAT(arr1, arr2, …)`
Concatenates two or more arrays into a single array.
- **Returns:** Array; requires at least 2 arguments (error otherwise).
- **Example:** `ARRAY_CONCAT(["backpacks", "daypacks"], ["hippacks"])` → `["backpacks", "daypacks", "hippacks"]`

```slate-sql
SELECT VALUE ARRAY_CONCAT(["backpacks", "daypacks"], ["hippacks"])
```

### `ARRAY_CONTAINS(arr, value[, partial])`
Tests whether an array contains a value, comparing elements with numeric coercion and optionally matching objects by subset.
- **Returns:** Boolean; with a third argument of `true`, objects match partially (an element matches when it contains all of `value`'s fields, recursively).
- **Example:** `ARRAY_CONTAINS(["a", 7], 7)` → `true`
- **Index-accelerated** when the array argument is a field path with a multikey (`.[]`) index and the needle is a scalar literal: `ARRAY_CONTAINS(c.tags, "x")` over a `tags.[]` index plans as a multikey `IndexScan`, not a full scan. The 3-arg `partial` form is not accelerated (it matches more than the indexed elements). See [Plan Scenarios](./plan-scenarios.md).

```slate-sql
SELECT VALUE ARRAY_CONTAINS(["a", 7], 7)
```

### `ARRAY_CONTAINS_ALL(arr, v1, v2, …)`
Tests whether an array contains every one of the given values.
- **Returns:** Boolean; a missing defined value gives `false`, but if all defined values are present and any argument is `undefined` the result is undefined (requires at least 2 args).
- **Example:** `ARRAY_CONTAINS_ALL([1, 2, 3, 4], 2, 3, 4, 5)` → `false`
- **Index-accelerated** over a multikey (`.[]`) index when every needle is a scalar literal: it plans as an `IndexMerge(And)` of per-value element scans (the intersection of their doc-id sets). See [Plan Scenarios](./plan-scenarios.md).

```slate-sql
SELECT VALUE ARRAY_CONTAINS_ALL([1, 2, 3, 4], 2, 3, 4, 5)
```

### `ARRAY_CONTAINS_ANY(arr, v1, v2, …)`
Tests whether an array contains any one of the given values.
- **Returns:** Boolean; a present value gives `true`, but if no defined value is present and any argument is `undefined` the result is undefined (requires at least 2 args).
- **Example:** `ARRAY_CONTAINS_ANY([1, 2, 3, 4], 2, 3, 4, 5)` → `true`
- **Index-accelerated** over a multikey (`.[]`) index when every needle is a scalar literal: it plans as an `IndexMerge(Or)` of per-value element scans (the union of their doc-id sets). See [Plan Scenarios](./plan-scenarios.md).

```slate-sql
SELECT VALUE ARRAY_CONTAINS_ANY([1, 2, 3, 4], 2, 3, 4, 5)
```

### `ARRAY_LENGTH(arr)`
Returns the number of elements in an array.
- **Returns:** Int64.
- **Example:** `ARRAY_LENGTH([1, "a"])` → `2`

```slate-sql
SELECT VALUE ARRAY_LENGTH([1, "a"])
```

### `ARRAY_SLICE(arr, start[, length])`
Returns a subset of an array starting at a zero-based index.
- **Returns:** Array; a negative `start` counts from the end and the optional `length` caps the count; a non-integer `start`/`length` yields undefined.
- **Example:** `ARRAY_SLICE(["Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf"], -2)` → `["Foxtrot", "Golf"]`

```slate-sql
SELECT VALUE ARRAY_SLICE(["Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf"], -2)
```

### `SETINTERSECT(arr1, arr2)`
Returns the set of values present in both arrays, without duplicates.
- **Returns:** Array; element order follows the second array.
- **Example:** `SETINTERSECT([1, 2, 3, 4], [3, 4, 5, 6])` → `[3, 4]`

```slate-sql
SELECT VALUE SETINTERSECT([1, 2, 3, 4], [3, 4, 5, 6])
```

### `SETUNION(arr1, arr2)`
Returns the set of all values from both arrays, without duplicates.
- **Returns:** Array; element order follows the first array, then new values from the second.
- **Example:** `SETUNION([1, 2, 3, 4], [3, 4, 5, 6])` → `[1, 2, 3, 4, 5, 6]`

```slate-sql
SELECT VALUE SETUNION([1, 2, 3, 4], [3, 4, 5, 6])
```

## Conditional & object

### `CHOOSE(index, v1, v2, …)`
Returns the value at the given one-based index in the value list.
- **Returns:** the selected value; an out-of-range or non-integer index yields undefined (requires at least 2 args).
- **Example:** `CHOOSE(1, "Vimero", "Hydration", "Pack")` → `"Vimero"`

```slate-sql
SELECT VALUE CHOOSE(1, "Vimero", "Hydration", "Pack")
```

### `IIF(cond, true_expr, false_expr)`
Returns `true_expr` when `cond` is the boolean `true`, otherwise `false_expr`.
- **Returns:** one of the two branch values; only the boolean `true` takes the true branch — any non-boolean condition, or `false`/`null`/undefined, takes the false branch (requires exactly 3 args).
- **Example:** `IIF(true, 123, 456)` → `123`

```slate-sql
SELECT VALUE IIF(true, 123, 456)
```

### `OBJECTTOARRAY(obj[, keyName, valueName])`
Converts an object's field/value pairs into an array of two-field elements.
- **Returns:** Array of `{ "k": <field>, "v": <value> }` documents by default; the optional second and third string arguments rename the key/value fields (1 to 3 args).
- **Example:** `OBJECTTOARRAY({ "a": "12345", "b": "67890" })` → `[{ "k": "a", "v": "12345" }, { "k": "b", "v": "67890" }]`

```slate-sql
SELECT VALUE OBJECTTOARRAY({ "a": "12345", "b": "67890" })
```

## Mutation helpers

These back the Mongo update operators; they are also callable directly. A non-array
target yields `undefined`.

### `LPUSH(arr, value)`
Prepends a value to the front of an array; backs `$push` (prepend variant).
- **Returns:** the updated array; a missing/undefined array becomes `[value]`, and an undefined `value` leaves the array unchanged (requires exactly 2 args).
- **Example:** `LPUSH([2, 3], 1)` → `[1, 2, 3]`

```slate-sql
SELECT VALUE LPUSH([2, 3], 1)
```

### `RPUSH(arr, value)`
Appends a value to the end of an array; backs `$push` (append variant).
- **Returns:** the updated array; a missing/undefined array becomes `[value]`, and an undefined `value` leaves the array unchanged (requires exactly 2 args).
- **Example:** `RPUSH([1, 2], 3)` → `[1, 2, 3]`

```slate-sql
SELECT VALUE RPUSH([1, 2], 3)
```

### `POP(arr)`
Removes the last element of an array; backs `$pop`.
- **Returns:** the updated array; an empty array stays empty (requires exactly 1 arg).
- **Example:** `POP([1, 2, 3])` → `[1, 2]`

```slate-sql
SELECT VALUE POP([1, 2, 3])
```

## Date & time

Cosmos models an instant three ways: a **DateTime** ISO 8601 string
(`YYYY-MM-DDTHH:MM:SS.fffffffZ`, UTC, 7 fractional digits = 100ns precision), a
**Timestamp** (Unix epoch milliseconds), and **Ticks** (100-nanosecond intervals
since the Unix epoch). Part arguments use the abbreviations `yyyy`, `mm`, `dd`,
`hh`, `mi`, `ss`, `ms`, `mcs`, `ns`.

### `DATETIMEADD(part, amount, dateTime)`
Adds an integer `amount` of the given date part to a DateTime string.
- **Returns:** DateTime string; year/month parts are calendar-aware, the rest are fixed durations; an unparseable date, bad part, or non-integer amount yields undefined.
- **Example:** `DATETIMEADD("mm", 1, "2020-07-03T00:00:00.0000000")` → `"2020-08-03T00:00:00.0000000Z"`

```slate-sql
SELECT VALUE DATETIMEADD("mm", 1, "2020-07-03T00:00:00.0000000")
```

### `DATETIMEBIN(dateTime, part[, binSize[, origin]])`
Rounds a DateTime down to the nearest bin of `binSize` units of `part`, measured from `origin` (default Unix epoch).
- **Returns:** DateTime string; `binSize` defaults to 1 and must be a positive integer; `part` must be a fixed-duration part; bad inputs yield undefined.
- **Example:** `DATETIMEBIN("2021-01-08T18:35:00.0000000", "hh", 5)` → `"2021-01-08T15:00:00.0000000Z"`

```slate-sql
SELECT VALUE DATETIMEBIN("2021-01-08T18:35:00.0000000", "hh", 5)
```

### `DATETIMEDIFF(part, startDate, endDate)`
Returns the count of `part` boundaries crossed from `startDate` to `endDate` (end minus start).
- **Returns:** Int64; year/month compare calendar components while fixed parts count boundaries crossed; an unparseable date or bad part yields undefined.
- **Example:** `DATETIMEDIFF("mm", "2018-03-05T05:00:00.0000000", "2019-02-04T16:00:00.0000000")` → `11`

```slate-sql
SELECT VALUE DATETIMEDIFF("mm", "2018-03-05T05:00:00.0000000", "2019-02-04T16:00:00.0000000")
```

### `DATETIMEFROMPARTS(year, month, day[, hour, minute, second, fractions])`
Builds a DateTime string from numeric component parts.
- **Returns:** DateTime string; hour/minute/second/fractions default to 0, `fractions` is in 100ns ticks (0–9999999); invalid or out-of-range components yield undefined.
- **Example:** `DATETIMEFROMPARTS(2017, 4, 20, 13, 15, 20, 3456789)` → `"2017-04-20T13:15:20.3456789Z"`

```slate-sql
SELECT VALUE DATETIMEFROMPARTS(2017, 4, 20, 13, 15, 20, 3456789)
```

### `DATETIMEPART(part, dateTime)`
Extracts a single date/time component from a DateTime string.
- **Returns:** Int64 component value; an unparseable date or unknown part yields undefined.
- **Example:** `DATETIMEPART("ms", "2016-05-29T08:30:00.1301617")` → `130`

```slate-sql
SELECT VALUE DATETIMEPART("ms", "2016-05-29T08:30:00.1301617")
```

### `DATETIMETOTICKS(dateTime)`
Converts a DateTime string to ticks (100-nanosecond intervals since the Unix epoch).
- **Returns:** Int64 ticks; an unparseable date yields undefined.
- **Example:** `DATETIMETOTICKS("1995-01-01T00:00:00.0000000")` → `7889184000000000`

```slate-sql
SELECT VALUE DATETIMETOTICKS("1995-01-01T00:00:00.0000000")
```

### `DATETIMETOTIMESTAMP(dateTime)`
Converts a DateTime string to a timestamp (Unix epoch milliseconds).
- **Returns:** Int64 milliseconds; an unparseable date yields undefined.
- **Example:** `DATETIMETOTIMESTAMP("2015-05-19T12:00:00.0000000")` → `1432036800000`

```slate-sql
SELECT VALUE DATETIMETOTIMESTAMP("2015-05-19T12:00:00.0000000")
```

### `TICKSTODATETIME(ticks)`
Converts ticks (100-nanosecond intervals since the Unix epoch) to a DateTime string.
- **Returns:** DateTime string; a non-integer argument yields undefined.
- **Example:** `TICKSTODATETIME(15973607943002652)` → `"2020-08-13T23:19:54.3002652Z"`

```slate-sql
SELECT VALUE TICKSTODATETIME(15973607943002652)
```

### `TIMESTAMPTODATETIME(timestamp)`
Converts a timestamp (Unix epoch milliseconds) to a DateTime string.
- **Returns:** DateTime string; a non-integer argument yields undefined.
- **Example:** `TIMESTAMPTODATETIME(0)` → `"1970-01-01T00:00:00.0000000Z"`

```slate-sql
SELECT VALUE TIMESTAMPTODATETIME(0)
```

## Spatial

GeoJSON geometries are ordinary documents — `{ "type": "Point", "coordinates": [lng, lat] }`, plus `LineString`/`Polygon`/`Multi*` — so no new type is needed; they reach functions as a `Document`. Invalid or non-geometry input yields `undefined`. The boolean predicates (`ST_ISVALID*`, `ST_WITHIN`, `ST_INTERSECTS`) match CosmosDB exactly; the metric functions (`ST_DISTANCE`, `ST_AREA`) compute on the WGS84 ellipsoid and land within ~cm / ~1 ppm of CosmosDB (which uses a proprietary spatial library). There is no spatial index yet — these are scalar functions.

### `ST_ISVALID(geometry)`
Whether a value is a valid GeoJSON geometry — coordinate ranges, ring closure, and minimum ring size.
- **Returns:** Boolean.
- **Example:** `ST_ISVALID({ "type": "Point", "coordinates": [31.9, -4.8] })` → `true`

```slate-sql
SELECT VALUE ST_ISVALID({ "type": "Point", "coordinates": [31.9, -4.8] })
```

### `ST_ISVALIDDETAILED(geometry)`
GeoJSON validity with an explanation when invalid.
- **Returns:** Document `{ "valid": bool, "reason"?: string }`.
- **Example:** `ST_ISVALIDDETAILED({ "type": "Point", "coordinates": [200, 5] })` → `{ "valid": false, "reason": "Longitude values must be between -180 and 180 degrees." }`

```slate-sql
SELECT VALUE ST_ISVALIDDETAILED({ "type": "Point", "coordinates": [200, 5] })
```

### `ST_WITHIN(geometry1, geometry2)`
Whether `geometry1` is contained within `geometry2` (planar lng/lat predicate).
- **Returns:** Boolean.
- **Example:** a point at `[0.5, 0.5]` inside a square spanning `[0,0]`–`[2,2]` → `true`

```slate-sql
SELECT VALUE ST_WITHIN(
  { "type": "Point", "coordinates": [0.5, 0.5] },
  { "type": "Polygon", "coordinates": [[[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]] }
)
```

### `ST_INTERSECTS(geometry1, geometry2)`
Whether two geometries spatially intersect (planar lng/lat predicate).
- **Returns:** Boolean.
- **Example:** two overlapping squares → `true`

```slate-sql
SELECT VALUE ST_INTERSECTS(
  { "type": "Polygon", "coordinates": [[[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]] },
  { "type": "Polygon", "coordinates": [[[1, 1], [3, 1], [3, 3], [1, 3], [1, 1]]] }
)
```

### `ST_DISTANCE(geometry1, geometry2)`
Distance in **meters** between two geometries, on the WGS84 ellipsoid (Vincenty geodesic).
- **Returns:** Double (meters); cross-type or invalid input → undefined.
- **Example:** two points 0.1° of latitude apart → `11118.351044593872` (≈ 11.1 km)

```slate-sql
SELECT VALUE ST_DISTANCE(
  { "type": "Point", "coordinates": [-122.3, 47.6] },
  { "type": "Point", "coordinates": [-122.3, 47.7] }
)
```

### `ST_AREA(geometry)`
Area in **square meters** of a polygonal geometry, on the WGS84 ellipsoid.
- **Returns:** Double (m²); non-polygonal or invalid input → undefined.
- **Example:** a ~0.1°×0.1° box near Seattle → `83530723.92923811` (m²)

```slate-sql
SELECT VALUE ST_AREA(
  { "type": "Polygon", "coordinates": [[[-122.3, 47.6], [-122.3, 47.7], [-122.2, 47.7], [-122.2, 47.6], [-122.3, 47.6]]] }
)
```

## Aggregate functions

Aggregates collapse the rows of a `GROUP BY` group (or, with no `GROUP BY`, the
whole result set) into a single value. Unlike the scalar functions above they are
evaluated by the query's aggregation stage, not the scalar dispatcher, and a query
that aggregates always emits one row per group — so `COUNT` over an empty group is
`0`.

### `COUNT(expr)`
Number of rows in the group where `expr` is *defined*. `COUNT(1)` counts every row.
- **Returns:** Int64; an empty group is `0`.
- **Example:** `COUNT(1)` over the five `products` → `5`

```slate-sql
SELECT VALUE COUNT(1) FROM products c
```

### `SUM(expr)`
Sum of the numeric values of `expr` in the group.
- **Returns:** Double; `undefined` values are skipped, but a single non-numeric *defined* value (string/bool/null) poisons the result to `undefined`, and a group with no qualifying values is `undefined`.
- **Example:** `SUM(c.price)` over `products` → `1804.95`

```slate-sql
SELECT VALUE SUM(c.price) FROM products c
```

### `AVG(expr)`
Arithmetic mean of the numeric values of `expr` in the group.
- **Returns:** Double; same skip/poison rules as `SUM`; an empty group is `undefined`.
- **Example:** `AVG(c.price)` over `products` → `360.99`

```slate-sql
SELECT VALUE AVG(c.price) FROM products c
```

### `MIN(expr)`
Smallest value of `expr` in the group, by Slate's total order across types.
- **Returns:** the winning value, with its type preserved; `undefined` values are skipped, there is **no** poison rule (mixed types are ranked by the total order), and an empty group is `undefined`.
- **Example:** `MIN(c.price)` over `products` → `24.99`

```slate-sql
SELECT VALUE MIN(c.price) FROM products c
```

### `MAX(expr)`
Largest value of `expr` in the group, by Slate's total order across types.
- **Returns:** the winning value, with its type preserved; same rules as `MIN`.
- **Example:** `MAX(c.price)` over `products` → `1299.99`

```slate-sql
SELECT VALUE MAX(c.price) FROM products c
```
