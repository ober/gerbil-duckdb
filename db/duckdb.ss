;;; -*- Gerbil -*-
;;; DuckDB wrapper interface

(import :gerbil/gambit
        :std/error
        :std/sugar
        ./_duckdb)
(export duckdb-error?
        duckdb? duckdb-open duckdb-close
        duckdb-conn? duckdb-connect duckdb-disconnect
        duckdb-query duckdb-exec duckdb-columns
        duckdb-prepare duckdb-bind duckdb-execute duckdb-finalize
        duckdb-exec*
        call-with-database)

;; DuckDB column type constants (from duckdb_type enum)
(def DUCKDB_TYPE_INVALID    0)
(def DUCKDB_TYPE_BOOLEAN    1)
(def DUCKDB_TYPE_TINYINT    2)
(def DUCKDB_TYPE_SMALLINT   3)
(def DUCKDB_TYPE_INTEGER    4)
(def DUCKDB_TYPE_BIGINT     5)
(def DUCKDB_TYPE_UTINYINT   6)
(def DUCKDB_TYPE_USMALLINT  7)
(def DUCKDB_TYPE_UINTEGER   8)
(def DUCKDB_TYPE_UBIGINT    9)
(def DUCKDB_TYPE_FLOAT     10)
(def DUCKDB_TYPE_DOUBLE    11)
(def DUCKDB_TYPE_TIMESTAMP 12)
(def DUCKDB_TYPE_DATE      13)
(def DUCKDB_TYPE_TIME      14)
(def DUCKDB_TYPE_INTERVAL  15)
(def DUCKDB_TYPE_HUGEINT   16)
(def DUCKDB_TYPE_VARCHAR   17)
(def DUCKDB_TYPE_BLOB      18)

;; Error handling
(deferror-class DuckDBError () duckdb-error?)

(def (raise-duckdb-error where what)
  (raise (DuckDBError what where: where)))

(def (check-ptr ptr where)
  (or ptr (raise-duckdb-error where "operation returned NULL")))

;;; Gerbil structs wrapping DuckDB handles
(defstruct duckdb (db)
  final: #t)

(defstruct duckdb-conn (conn db)
  final: #t)

(defstruct duckdb-stmt (stmt conn)
  final: #t)

;;; Database lifecycle

(def (duckdb-open (path #f))
  (let* ((cpath (or path ":memory:"))
         (ptr (duckdb_open_ cpath)))
    (if ptr
      (let (db (make-duckdb ptr))
        (make-will db duckdb-close)
        db)
      (raise-duckdb-error 'duckdb-open "failed to open database"))))

(def (duckdb-close db)
  (with ((duckdb ptr) db)
    (when ptr
      (duckdb_close_ ptr)
      (set! (duckdb-db db) #f))))

(defmethod {destroy duckdb}
  duckdb-close)

;;; Connection lifecycle

(def (duckdb-connect db)
  (with ((duckdb ptr) db)
    (if ptr
      (let (conn-ptr (duckdb_connect_ ptr))
        (if conn-ptr
          (let (conn (make-duckdb-conn conn-ptr db))
            (make-will conn duckdb-disconnect)
            conn)
          (raise-duckdb-error 'duckdb-connect "failed to create connection")))
      (raise-duckdb-error 'duckdb-connect "database has been closed"))))

(def (duckdb-disconnect conn)
  (with ((duckdb-conn ptr) conn)
    (when ptr
      (duckdb_disconnect_ ptr)
      (set! (duckdb-conn-conn conn) #f))))

(defmethod {destroy duckdb-conn}
  duckdb-disconnect)

;;; Query execution

;; Execute SQL and return list of hash-tables (one per row)
(def (duckdb-query conn sql)
  (with ((duckdb-conn ptr) conn)
    (if ptr
      (let (res (duckdb_query_ ptr sql))
        (if res
          (let (err (duckdb_result_error_ res))
            (if err
              (let (msg (string-copy err))
                (raise-duckdb-error 'duckdb-query msg))
              (result->rows res)))
          (raise-duckdb-error 'duckdb-query "query execution failed")))
      (raise-duckdb-error 'duckdb-query "connection has been closed"))))

;; Execute SQL for side effects (DDL/DML), return rows-changed count
(def (duckdb-exec conn sql)
  (with ((duckdb-conn ptr) conn)
    (if ptr
      (let (res (duckdb_query_ ptr sql))
        (if res
          (let (err (duckdb_result_error_ res))
            (if err
              (let (msg (string-copy err))
                (raise-duckdb-error 'duckdb-exec msg))
              (duckdb_rows_changed_ res)))
          (raise-duckdb-error 'duckdb-exec "query execution failed")))
      (raise-duckdb-error 'duckdb-exec "connection has been closed"))))

;; Return column names as list of strings
(def (duckdb-columns conn sql)
  (with ((duckdb-conn ptr) conn)
    (if ptr
      (let (res (duckdb_query_ ptr sql))
        (if res
          (let (err (duckdb_result_error_ res))
            (if err
              (let (msg (string-copy err))
                (raise-duckdb-error 'duckdb-columns msg))
              (let (ncols (duckdb_column_count_ res))
                (let loop ((i 0) (acc []))
                  (if (< i ncols)
                    (loop (+ i 1)
                          (cons (duckdb_column_name_ res i) acc))
                    (reverse acc))))))
          (raise-duckdb-error 'duckdb-columns "query execution failed")))
      (raise-duckdb-error 'duckdb-columns "connection has been closed"))))

;;; Result conversion

(def (result->rows res)
  (let ((ncols (duckdb_column_count_ res))
        (nrows (duckdb_row_count_ res)))
    ;; Get column names and types
    (let ((col-names (let loop ((i 0) (acc []))
                       (if (< i ncols)
                         (loop (+ i 1) (cons (duckdb_column_name_ res i) acc))
                         (reverse! acc))))
          (col-types (let loop ((i 0) (acc []))
                       (if (< i ncols)
                         (loop (+ i 1) (cons (duckdb_column_type_ res i) acc))
                         (reverse! acc)))))
      ;; Build rows
      (let loop-rows ((row 0) (rows []))
        (if (< row nrows)
          (let (ht (make-hash-table size: ncols))
            (let loop-cols ((col 0)
                            (names col-names)
                            (types col-types))
              (when (< col ncols)
                (let ((name (car names))
                      (type (car types)))
                  (hash-put! ht name (extract-value res col row type))
                  (loop-cols (+ col 1) (cdr names) (cdr types)))))
            (loop-rows (+ row 1) (cons ht rows)))
          (reverse! rows))))))

(def (extract-value res col row type)
  (if (duckdb_value_is_null_ res col row)
    (void)  ; SQL NULL → Gerbil void
    (cond
     ((eqv? type DUCKDB_TYPE_BOOLEAN)
      (duckdb_value_boolean_ res col row))
     ((eqv? type DUCKDB_TYPE_TINYINT)
      (duckdb_value_int8_ res col row))
     ((eqv? type DUCKDB_TYPE_SMALLINT)
      (duckdb_value_int16_ res col row))
     ((eqv? type DUCKDB_TYPE_INTEGER)
      (duckdb_value_int32_ res col row))
     ((eqv? type DUCKDB_TYPE_BIGINT)
      (duckdb_value_int64_ res col row))
     ((eqv? type DUCKDB_TYPE_UTINYINT)
      (duckdb_value_uint8_ res col row))
     ((eqv? type DUCKDB_TYPE_USMALLINT)
      (duckdb_value_uint16_ res col row))
     ((eqv? type DUCKDB_TYPE_UINTEGER)
      (duckdb_value_uint32_ res col row))
     ((eqv? type DUCKDB_TYPE_UBIGINT)
      (duckdb_value_uint64_ res col row))
     ((eqv? type DUCKDB_TYPE_FLOAT)
      (duckdb_value_float_ res col row))
     ((eqv? type DUCKDB_TYPE_DOUBLE)
      (duckdb_value_double_ res col row))
     ((eqv? type DUCKDB_TYPE_HUGEINT)
      ;; Fall back to string representation for huge integers
      (duckdb_value_varchar_ res col row))
     (else
      ;; VARCHAR, DATE, TIME, TIMESTAMP, BLOB, and everything else:
      ;; fall back to varchar string representation
      (duckdb_value_varchar_ res col row)))))

;;; Prepared statements

(def (duckdb-prepare conn sql)
  (with ((duckdb-conn ptr) conn)
    (if ptr
      (let (stmt-ptr (duckdb_prepare_ ptr sql))
        (if stmt-ptr
          (let (err (duckdb_prepare_error_ stmt-ptr))
            (if err
              (let (msg (string-copy err))
                (duckdb_destroy_prepare_ stmt-ptr)
                (raise-duckdb-error 'duckdb-prepare msg))
              (let (stmt (make-duckdb-stmt stmt-ptr conn))
                (make-will stmt duckdb-finalize)
                stmt)))
          (raise-duckdb-error 'duckdb-prepare "failed to create prepared statement")))
      (raise-duckdb-error 'duckdb-prepare "connection has been closed"))))

(def (duckdb-finalize stmt)
  (with ((duckdb-stmt ptr) stmt)
    (when ptr
      (duckdb_destroy_prepare_ ptr)
      (set! (duckdb-stmt-stmt stmt) #f))))

(defmethod {destroy duckdb-stmt}
  duckdb-finalize)

;; Auto-dispatch bind based on Scheme type
(def (duckdb-bind stmt index value)
  (with ((duckdb-stmt ptr) stmt)
    (if ptr
      (let (rc (cond
                ((boolean? value)
                 (duckdb_bind_boolean_ ptr index value))
                ((fixnum? value)
                 (duckdb_bind_int64_ ptr index value))
                ((integer? value)
                 (duckdb_bind_int64_ ptr index value))
                ((flonum? value)
                 (duckdb_bind_double_ ptr index value))
                ((string? value)
                 (duckdb_bind_varchar_ ptr index value))
                ((u8vector? value)
                 (duckdb_bind_blob_ ptr index value))
                ((void? value)
                 (duckdb_bind_null_ ptr index))
                (else
                 (raise-duckdb-error 'duckdb-bind
                   (string-append "unsupported value type: "
                                  (object->string value))))))
        (when (eqv? rc 1)
          (raise-duckdb-error 'duckdb-bind "bind operation failed")))
      (raise-duckdb-error 'duckdb-bind "prepared statement has been finalized"))))

(def (duckdb-execute stmt)
  (with ((duckdb-stmt ptr) stmt)
    (if ptr
      (let (res (duckdb_execute_prepared_ ptr))
        (if res
          (let (err (duckdb_result_error_ res))
            (if err
              (let (msg (string-copy err))
                (raise-duckdb-error 'duckdb-execute msg))
              (result->rows res)))
          (raise-duckdb-error 'duckdb-execute "execution failed")))
      (raise-duckdb-error 'duckdb-execute "prepared statement has been finalized"))))

;;; Convenience functions

;; One-shot parameterized query
(def (duckdb-exec* conn sql . params)
  (let (stmt (duckdb-prepare conn sql))
    (try
     (let loop ((i 1) (rest params))
       (when (pair? rest)
         (duckdb-bind stmt i (car rest))
         (loop (+ i 1) (cdr rest))))
     (let (result (duckdb-execute stmt))
       (duckdb-finalize stmt)
       result)
     (catch (e)
       (duckdb-finalize stmt)
       (raise e)))))

;; Open database + connection, run proc, cleanup
(def (call-with-database path proc)
  (let* ((db (duckdb-open path))
         (conn (duckdb-connect db)))
    (try
     (proc conn)
     (finally
      (duckdb-disconnect conn)
      (duckdb-close db)))))
