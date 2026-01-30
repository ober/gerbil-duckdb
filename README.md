# gerbil-duckdb

A Gerbil Scheme FFI binding for [DuckDB](https://duckdb.org/), the in-process analytical database. Provides safe, idiomatic Scheme access to DuckDB with automatic resource management.

## Project Layout

```
gerbil-duckdb/
  gerbil.pkg              # Package declaration (package: clan)
  build.ss                # Build script - compiles FFI + Gerbil module
  manifest.ss             # Version manifest
  db/
    _duckdb.scm           # Low-level FFI: C wrappers, Gambit c-lambda bindings
    _duckdb.ssi           # FFI interface declaration (extern exports)
    duckdb.ss             # High-level Gerbil API (what users import)
    duckdb-test.ss        # Test suite (11 test cases, :std/test)
```

The module is imported as `:clan/db/duckdb`.

## Prerequisites

- **Gerbil Scheme** (v0.18+)
- **DuckDB C library** (`libduckdb` + headers) installed on the system

## Building

The `build.ss` expects DuckDB headers and library at `/opt/homebrew/include` and `/opt/homebrew/lib` (macOS Homebrew defaults). For Linux, edit `build.ss` to point to your DuckDB install location:

```scheme
;; build.ss - adjust -I and -L paths for your platform
(defbuild-script
  `((gsc: "db/_duckdb"
          "-cc-options" "-I/usr/local/include"       ; adjust
          "-ld-options" "-L/usr/local/lib -lduckdb")  ; adjust
    (ssi: "db/_duckdb")
    "db/duckdb"))
```

Then build:

```shell
gerbil build
```

Run tests:

```shell
gerbil test db/...
```

## API Reference

All functions are exported from `:clan/db/duckdb`.

```scheme
(import :clan/db/duckdb)
```

### Error Handling

| Form | Description |
|------|-------------|
| `(duckdb-error? e)` | Predicate - is `e` a DuckDB error? |

All operations raise a `DuckDBError` exception on failure. Catch with:

```scheme
(try
  (duckdb-query conn "SELECT ...")
  (catch (duckdb-error? e)
    (displayln "DuckDB error: " (error-message e))))
```

### Database Lifecycle

| Function | Signature | Description |
|----------|-----------|-------------|
| `duckdb-open` | `(duckdb-open [path])` | Open a database. Omit `path` or pass `#f` for in-memory. |
| `duckdb-close` | `(duckdb-close db)` | Close database. Idempotent (safe to call twice). |
| `duckdb?` | `(duckdb? obj)` | Predicate - is `obj` a database handle? |

### Connection Lifecycle

| Function | Signature | Description |
|----------|-----------|-------------|
| `duckdb-connect` | `(duckdb-connect db)` | Create a connection from an open database. |
| `duckdb-disconnect` | `(duckdb-disconnect conn)` | Close connection. Idempotent. |
| `duckdb-conn?` | `(duckdb-conn? obj)` | Predicate. |

### Query Execution

| Function | Signature | Returns | Description |
|----------|-----------|---------|-------------|
| `duckdb-query` | `(duckdb-query conn sql)` | List of hash-tables | Execute SELECT, return rows as `(hash ("col" val) ...)`. |
| `duckdb-exec` | `(duckdb-exec conn sql)` | Integer | Execute DDL/DML, return rows-changed count. |
| `duckdb-columns` | `(duckdb-columns conn sql)` | List of strings | Return column names for a query. |
| `duckdb-exec*` | `(duckdb-exec* conn sql . params)` | List of hash-tables | One-shot parameterized query (prepare + bind + execute + finalize). |

### Prepared Statements

| Function | Signature | Description |
|----------|-----------|-------------|
| `duckdb-prepare` | `(duckdb-prepare conn sql)` | Create a prepared statement. Use `$1`, `$2`, ... for parameters. |
| `duckdb-bind` | `(duckdb-bind stmt index value)` | Bind value at 1-based index. Auto-dispatches on Scheme type. |
| `duckdb-execute` | `(duckdb-execute stmt)` | Execute prepared statement, return rows. |
| `duckdb-finalize` | `(duckdb-finalize stmt)` | Destroy prepared statement. Idempotent. |

**Type dispatch for `duckdb-bind`:**

| Scheme Type | SQL Binding |
|-------------|-------------|
| `boolean?` | BOOLEAN |
| `fixnum?` / `integer?` | BIGINT |
| `flonum?` | DOUBLE |
| `string?` | VARCHAR |
| `u8vector?` | BLOB |
| `void?` | NULL |

### Resource Management

```scheme
(call-with-database path proc)
```

Opens a database at `path` (`#f` for in-memory), creates a connection, calls `(proc conn)`, and guarantees cleanup via `try/finally`. Returns the result of `proc`.

### NULL Handling

SQL `NULL` values are returned as Gerbil `(void)`. Test with `(void? val)`.

## Usage Examples

### Basic: Create, Insert, Query

```scheme
(import :clan/db/duckdb)

(call-with-database #f
  (lambda (conn)
    ;; Create table
    (duckdb-exec conn "CREATE TABLE users (id INTEGER, name VARCHAR, score DOUBLE)")

    ;; Insert rows
    (duckdb-exec conn "INSERT INTO users VALUES (1, 'Alice', 95.5)")
    (duckdb-exec conn "INSERT INTO users VALUES (2, 'Bob', 87.3)")
    (duckdb-exec conn "INSERT INTO users VALUES (3, 'Charlie', 92.1)")

    ;; Query
    (def rows (duckdb-query conn "SELECT * FROM users ORDER BY score DESC"))
    (for-each
      (lambda (row)
        (displayln (hash-ref row "name") ": " (hash-ref row "score")))
      rows)))
;; Output:
;;   Alice: 95.5
;;   Charlie: 92.1
;;   Bob: 87.3
```

### Parameterized Queries

```scheme
(call-with-database #f
  (lambda (conn)
    (duckdb-exec conn "CREATE TABLE kv (key VARCHAR, value INTEGER)")

    ;; Using duckdb-exec* (one-shot convenience)
    (duckdb-exec* conn "INSERT INTO kv VALUES ($1, $2)" "alpha" 1)
    (duckdb-exec* conn "INSERT INTO kv VALUES ($1, $2)" "beta" 2)
    (duckdb-exec* conn "INSERT INTO kv VALUES ($1, $2)" "gamma" 3)

    ;; Query with parameter
    (def rows (duckdb-exec* conn "SELECT * FROM kv WHERE value > $1" 1))
    (displayln (length rows) " rows matched")))
```

### Manual Prepared Statement Lifecycle

```scheme
(call-with-database #f
  (lambda (conn)
    (duckdb-exec conn "CREATE TABLE points (x DOUBLE, y DOUBLE)")

    (def stmt (duckdb-prepare conn "INSERT INTO points VALUES ($1, $2)"))
    ;; Reuse the same statement with different bindings
    (for-each
      (lambda (pair)
        (duckdb-bind stmt 1 (car pair))
        (duckdb-bind stmt 2 (cdr pair))
        (duckdb-execute stmt))
      '((1.0 . 2.0) (3.0 . 4.0) (5.0 . 6.0)))
    (duckdb-finalize stmt)

    (duckdb-query conn "SELECT * FROM points")))
```

### Persistent (On-Disk) Database

```scheme
;; Data persists across runs
(call-with-database "/tmp/my.duckdb"
  (lambda (conn)
    (duckdb-exec conn "CREATE TABLE IF NOT EXISTS log (ts TIMESTAMP, msg VARCHAR)")
    (duckdb-exec* conn
      "INSERT INTO log VALUES (current_timestamp, $1)"
      "application started")
    (duckdb-query conn "SELECT * FROM log ORDER BY ts DESC LIMIT 10")))
```

### Aggregation

```scheme
(call-with-database #f
  (lambda (conn)
    (duckdb-exec conn "CREATE TABLE sales (product VARCHAR, amount DOUBLE)")
    (duckdb-exec conn "INSERT INTO sales VALUES
      ('Widget', 29.99), ('Widget', 15.50), ('Gadget', 42.00), ('Gadget', 38.75)")

    (def rows (duckdb-query conn
      "SELECT product, SUM(amount) as total, AVG(amount) as avg_price
       FROM sales GROUP BY product ORDER BY total DESC"))
    (for-each
      (lambda (row)
        (displayln (hash-ref row "product")
                   " total=" (hash-ref row "total")
                   " avg=" (hash-ref row "avg_price")))
      rows)))
```

### Reading Parquet Files

DuckDB can natively query Parquet files. This works through standard SQL:

```scheme
(call-with-database #f
  (lambda (conn)
    (def rows (duckdb-query conn "SELECT * FROM read_parquet('data/sales.parquet')"))
    (displayln "Rows: " (length rows))
    (for-each (lambda (r) (displayln r)) rows)))
```

### Converting Parquet to JSON

Two approaches, depending on whether you want a JSON file on disk or JSON data in Scheme.

**Approach 1: DuckDB writes the JSON file directly**

This is the fastest option for large files since DuckDB handles everything in C:

```scheme
(import :clan/db/duckdb)

(call-with-database #f
  (lambda (conn)
    (duckdb-exec conn
      "COPY (SELECT * FROM read_parquet('input.parquet'))
       TO 'output.json' (FORMAT JSON, ARRAY true)")))
```

`ARRAY true` produces a top-level JSON array `[{...}, {...}, ...]`. Without it, DuckDB writes newline-delimited JSON (one object per line).

You can filter or transform during the conversion:

```scheme
(call-with-database #f
  (lambda (conn)
    (duckdb-exec conn
      "COPY (
         SELECT id, name, amount
         FROM read_parquet('sales/*.parquet')
         WHERE amount > 100
         ORDER BY amount DESC
       ) TO 'large_sales.json' (FORMAT JSON, ARRAY true)")))
```

**Approach 2: Query into Scheme, serialize with `:std/text/json`**

This gives full control in Scheme -- you can filter, transform, or reshape data between the query and the JSON serialization:

```scheme
(import :clan/db/duckdb :std/text/json)

(call-with-database #f
  (lambda (conn)
    (def rows (duckdb-query conn "SELECT * FROM read_parquet('input.parquet')"))
    ;; rows is a list of hash-tables - write-json handles them directly
    (call-with-output-file "output.json"
      (lambda (port)
        (write-json rows port)))))
```

Or print each row as a separate JSON line to stdout:

```scheme
(import :clan/db/duckdb :std/text/json)

(call-with-database #f
  (lambda (conn)
    (def rows (duckdb-query conn "SELECT * FROM read_parquet('data.parquet')"))
    (for-each
      (lambda (row)
        (displayln (call-with-output-string (lambda (p) (write-json row p)))))
      rows)))
```

### Converting Parquet to CSV

```scheme
(call-with-database #f
  (lambda (conn)
    (duckdb-exec conn
      "COPY (SELECT * FROM read_parquet('input.parquet'))
       TO 'output.csv' (HEADER, DELIMITER ',')")))
```

### Querying Multiple Parquet Files with Globs

```scheme
(call-with-database #f
  (lambda (conn)
    (def rows (duckdb-query conn
      "SELECT * FROM read_parquet('data/*.parquet')
       WHERE amount > 100
       ORDER BY date DESC
       LIMIT 50"))
    rows))
```

### Parquet to Parquet (Filtered/Transformed)

```scheme
(call-with-database #f
  (lambda (conn)
    (duckdb-exec conn
      "COPY (
         SELECT customer_id, SUM(amount) as total
         FROM read_parquet('raw_orders/*.parquet')
         GROUP BY customer_id
         HAVING total > 1000
       ) TO 'high_value_customers.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)")))
```

### Reading CSV Files

```scheme
(call-with-database #f
  (lambda (conn)
    (def rows (duckdb-query conn
      "SELECT * FROM read_csv_auto('data/report.csv') LIMIT 10"))
    rows))
```

### Querying Remote Parquet Files (httpfs)

```scheme
(call-with-database #f
  (lambda (conn)
    ;; Install and load the httpfs extension
    (duckdb-exec conn "INSTALL httpfs")
    (duckdb-exec conn "LOAD httpfs")

    (def rows (duckdb-query conn
      "SELECT * FROM read_parquet('https://example.com/data.parquet') LIMIT 5"))
    rows))
```

### Column Introspection

```scheme
(call-with-database #f
  (lambda (conn)
    (def cols (duckdb-columns conn "SELECT * FROM read_parquet('data.parquet')"))
    (displayln "Columns: " cols)))
;; => Columns: (id name amount date ...)
```

### Error Handling

```scheme
(call-with-database #f
  (lambda (conn)
    (try
      (duckdb-query conn "SELECT * FROM nonexistent_table")
      (catch (duckdb-error? e)
        (displayln "Caught: " (error-message e))))))
```

### NULL Value Handling

```scheme
(call-with-database #f
  (lambda (conn)
    (duckdb-exec conn "CREATE TABLE sparse (id INTEGER, val VARCHAR)")
    (duckdb-exec conn "INSERT INTO sparse VALUES (1, NULL), (2, 'present')")
    (def rows (duckdb-query conn "SELECT * FROM sparse ORDER BY id"))
    (for-each
      (lambda (row)
        (let ((val (hash-ref row "val")))
          (if (void? val)
            (displayln "id=" (hash-ref row "id") " val=NULL")
            (displayln "id=" (hash-ref row "id") " val=" val))))
      rows)))
;; Output:
;;   id=1 val=NULL
;;   id=2 val=present
```

## Architecture

**Two-layer design:**

1. **`db/_duckdb.scm`** (FFI layer) - Gambit `c-lambda` bindings wrapping DuckDB's C API. Heap-allocates wrapper structs (`ffi_duckdb_db_t`, `ffi_duckdb_conn_t`, `ffi_duckdb_stmt_t`, `ffi_duckdb_result_t`) with GC finalizers for automatic cleanup. All symbols suffixed with `_` (e.g., `duckdb_open_`).

2. **`db/duckdb.ss`** (Gerbil layer) - Idiomatic Scheme API using `defstruct` wrappers (`duckdb`, `duckdb-conn`, `duckdb-stmt`). Adds `make-will` finalizers, error checking, type-dispatched parameter binding, and automatic result-to-hash-table conversion.

### Type Mapping (DuckDB Result to Scheme)

| DuckDB Type | Scheme Type |
|-------------|-------------|
| BOOLEAN | `#t` / `#f` |
| TINYINT, SMALLINT, INTEGER, BIGINT | integer |
| UTINYINT, USMALLINT, UINTEGER, UBIGINT | unsigned integer |
| FLOAT, DOUBLE | flonum |
| VARCHAR | string |
| NULL | `(void)` |
| DATE, TIME, TIMESTAMP, INTERVAL, HUGEINT, BLOB | string (varchar fallback) |
