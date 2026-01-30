;;; -*- Gerbil -*-
;;; DuckDB test suite

(import :std/test
        :std/sugar
        ./duckdb)

(export duckdb-test)

(def duckdb-test
  (test-suite "test :clan/db/duckdb"
    (test-case "test open/close in-memory database"
      (def db (duckdb-open))
      (check (duckdb? db) => #t)
      (duckdb-close db)
      ;; double close should be safe
      (duckdb-close db))

    (test-case "test connect/disconnect"
      (def db (duckdb-open))
      (def conn (duckdb-connect db))
      (check (duckdb-conn? conn) => #t)
      (duckdb-disconnect conn)
      (duckdb-close db))

    (test-case "test create table and insert"
      (call-with-database #f
        (lambda (conn)
          (duckdb-exec conn "CREATE TABLE test (id INTEGER, name VARCHAR, score DOUBLE)")
          (duckdb-exec conn "INSERT INTO test VALUES (1, 'Alice', 95.5)")
          (duckdb-exec conn "INSERT INTO test VALUES (2, 'Bob', 87.3)")
          (duckdb-exec conn "INSERT INTO test VALUES (3, 'Charlie', 92.1)")
          (def rows (duckdb-query conn "SELECT * FROM test ORDER BY id"))
          (check (length rows) => 3)
          ;; Check first row
          (check (hash-ref (car rows) "id") => 1)
          (check (hash-ref (car rows) "name") => "Alice")
          (check (hash-ref (car rows) "score") => 95.5)
          ;; Check last row
          (check (hash-ref (caddr rows) "name") => "Charlie"))))

    (test-case "test data types"
      (call-with-database #f
        (lambda (conn)
          (duckdb-exec conn "CREATE TABLE types_test (
             b BOOLEAN,
             ti TINYINT,
             si SMALLINT,
             i INTEGER,
             bi BIGINT,
             f FLOAT,
             d DOUBLE,
             v VARCHAR)")
          (duckdb-exec conn "INSERT INTO types_test VALUES (true, 42, 1000, 100000, 9999999999, 3.14, 2.718281828, 'hello')")
          (def rows (duckdb-query conn "SELECT * FROM types_test"))
          (check (length rows) => 1)
          (def row (car rows))
          (check (hash-ref row "b") => #t)
          (check (hash-ref row "ti") => 42)
          (check (hash-ref row "si") => 1000)
          (check (hash-ref row "i") => 100000)
          (check (hash-ref row "bi") => 9999999999)
          (check (hash-ref row "v") => "hello"))))

    (test-case "test NULL values"
      (call-with-database #f
        (lambda (conn)
          (duckdb-exec conn "CREATE TABLE null_test (id INTEGER, name VARCHAR)")
          (duckdb-exec conn "INSERT INTO null_test VALUES (1, NULL)")
          (duckdb-exec conn "INSERT INTO null_test VALUES (NULL, 'Bob')")
          (def rows (duckdb-query conn "SELECT * FROM null_test ORDER BY id NULLS LAST"))
          (check (length rows) => 2)
          (check (hash-ref (car rows) "id") => 1)
          (check (void? (hash-ref (car rows) "name")) => #t)
          (check (void? (hash-ref (cadr rows) "id")) => #t)
          (check (hash-ref (cadr rows) "name") => "Bob"))))

    (test-case "test prepared statements"
      (call-with-database #f
        (lambda (conn)
          (duckdb-exec conn "CREATE TABLE prep_test (id INTEGER, name VARCHAR, score DOUBLE)")
          ;; Insert with prepared statement
          (def stmt (duckdb-prepare conn "INSERT INTO prep_test VALUES ($1, $2, $3)"))
          (duckdb-bind stmt 1 1)
          (duckdb-bind stmt 2 "Alice")
          (duckdb-bind stmt 3 95.5)
          (duckdb-execute stmt)
          (duckdb-finalize stmt)
          ;; Query with prepared statement
          (def rows (duckdb-exec* conn "SELECT * FROM prep_test WHERE id = $1" 1))
          (check (length rows) => 1)
          (check (hash-ref (car rows) "name") => "Alice"))))

    (test-case "test duckdb-exec*"
      (call-with-database #f
        (lambda (conn)
          (duckdb-exec conn "CREATE TABLE exec_test (id INTEGER, val VARCHAR)")
          (duckdb-exec* conn "INSERT INTO exec_test VALUES ($1, $2)" 1 "one")
          (duckdb-exec* conn "INSERT INTO exec_test VALUES ($1, $2)" 2 "two")
          (duckdb-exec* conn "INSERT INTO exec_test VALUES ($1, $2)" 3 "three")
          (def rows (duckdb-query conn "SELECT * FROM exec_test ORDER BY id"))
          (check (length rows) => 3)
          (check (hash-ref (car rows) "val") => "one")
          (check (hash-ref (caddr rows) "val") => "three"))))

    (test-case "test error handling"
      (call-with-database #f
        (lambda (conn)
          (check-exception (duckdb-query conn "SELECT * FROM nonexistent_table")
                           duckdb-error?))))

    (test-case "test columns"
      (call-with-database #f
        (lambda (conn)
          (duckdb-exec conn "CREATE TABLE col_test (id INTEGER, name VARCHAR, score DOUBLE)")
          (def cols (duckdb-columns conn "SELECT * FROM col_test"))
          (check cols => '("id" "name" "score")))))

    (test-case "test call-with-database"
      (def result
        (call-with-database #f
          (lambda (conn)
            (duckdb-exec conn "CREATE TABLE cwd_test (x INTEGER)")
            (duckdb-exec conn "INSERT INTO cwd_test VALUES (42)")
            (duckdb-query conn "SELECT * FROM cwd_test"))))
      (check (length result) => 1)
      (check (hash-ref (car result) "x") => 42))

    (test-case "test aggregation"
      (call-with-database #f
        (lambda (conn)
          (duckdb-exec conn "CREATE TABLE agg_test (val INTEGER)")
          (duckdb-exec conn "INSERT INTO agg_test VALUES (10), (20), (30)")
          (def rows (duckdb-query conn "SELECT SUM(val)::BIGINT as total, AVG(val) as avg_val FROM agg_test"))
          (check (length rows) => 1)
          (def row (car rows))
          (check (hash-ref row "total") => 60)
          (check (hash-ref row "avg_val") => 20.0))))))

(def (main . args)
  (run-tests! duckdb-test)
  (test-report-summary!))
