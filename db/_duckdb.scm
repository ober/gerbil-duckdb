;;; -*- Scheme -*-
;;; DuckDB FFI bindings for Gerbil Scheme

;; compile: -ld-options "-lduckdb"

(declare
  (block)
  (standard-bindings)
  (extended-bindings)
  (not safe))

(namespace ("clan/db/_duckdb#"))
(##namespace ("" define-macro define let let* if or and when cond else
              quote quasiquote unquote unquote-splicing
              c-lambda c-define-type c-declare c-initialize
              ))

(c-declare #<<END-C
#include <duckdb.h>
#include <stdlib.h>
#include <string.h>

#ifndef ___HAVE_FFI_U8VECTOR
#define ___HAVE_FFI_U8VECTOR
#define U8_DATA(obj) ___CAST (___U8*, ___BODY_AS (obj, ___tSUBTYPED))
#define U8_LEN(obj) ___HD_BYTES (___HEADER (obj))
#endif

/* --- Wrapper structs for heap-allocating DuckDB handles --- */

typedef struct {
  duckdb_database db;
} ffi_duckdb_db_t;

typedef struct {
  duckdb_connection conn;
} ffi_duckdb_conn_t;

typedef struct {
  duckdb_prepared_statement stmt;
} ffi_duckdb_stmt_t;

typedef struct {
  duckdb_result result;
} ffi_duckdb_result_t;

/* --- Forward declarations --- */

static ___SCMOBJ ffi_free (void *ptr);
static ___SCMOBJ ffi_free_duckdb_db (void *ptr);
static ___SCMOBJ ffi_free_duckdb_conn (void *ptr);
static ___SCMOBJ ffi_free_duckdb_stmt (void *ptr);
static ___SCMOBJ ffi_free_duckdb_result (void *ptr);

static ffi_duckdb_db_t* ffi_duckdb_open (const char *path);
static void ffi_duckdb_close (ffi_duckdb_db_t *dbw);
static ffi_duckdb_conn_t* ffi_duckdb_connect (ffi_duckdb_db_t *dbw);
static void ffi_duckdb_disconnect (ffi_duckdb_conn_t *connw);
static ffi_duckdb_result_t* ffi_duckdb_query (ffi_duckdb_conn_t *connw, const char *sql);
static ffi_duckdb_stmt_t* ffi_duckdb_prepare (ffi_duckdb_conn_t *connw, const char *sql);
static void ffi_duckdb_destroy_prepare (ffi_duckdb_stmt_t *stmtw);
static ffi_duckdb_result_t* ffi_duckdb_execute_prepared (ffi_duckdb_stmt_t *stmtw);
static int ffi_duckdb_bind_blob (ffi_duckdb_stmt_t *stmtw, uint64_t idx, ___SCMOBJ data);
static char* ffi_duckdb_value_varchar (ffi_duckdb_result_t *resw, uint64_t col, uint64_t row);

END-C
)

;; -- helper macros --

(define-macro (define-c-lambda id args ret #!optional (name #f))
  (let ((name (or name (##symbol->string id))))
    `(define ,id
       (c-lambda ,args ,ret ,name))))

(define-macro (define-guard guard defn)
  (if (eval `(cond-expand (,guard #t) (else #f)))
    '(begin)
    (begin
      (eval `(define-cond-expand-feature ,guard))
      defn)))

;; -- type definitions --

(define-guard ffi-have-char*
  (c-define-type char*
    (pointer char (char*) "ffi_free")))

;; Database handle wrapper
(c-define-type ffi_duckdb_db_t "ffi_duckdb_db_t")
(c-define-type ffi_duckdb_db_t*
  (pointer ffi_duckdb_db_t (ffi_duckdb_db_t*) "ffi_free_duckdb_db"))

;; Connection handle wrapper
(c-define-type ffi_duckdb_conn_t "ffi_duckdb_conn_t")
(c-define-type ffi_duckdb_conn_t*
  (pointer ffi_duckdb_conn_t (ffi_duckdb_conn_t*) "ffi_free_duckdb_conn"))

;; Prepared statement handle wrapper
(c-define-type ffi_duckdb_stmt_t "ffi_duckdb_stmt_t")
(c-define-type ffi_duckdb_stmt_t*
  (pointer ffi_duckdb_stmt_t (ffi_duckdb_stmt_t*) "ffi_free_duckdb_stmt"))

;; Result wrapper
(c-define-type ffi_duckdb_result_t "ffi_duckdb_result_t")
(c-define-type ffi_duckdb_result_t*
  (pointer ffi_duckdb_result_t (ffi_duckdb_result_t*) "ffi_free_duckdb_result"))

;; -- c-lambda bindings --

;; Database lifecycle
(define-c-lambda duckdb_open_ (char-string) ffi_duckdb_db_t*
  "ffi_duckdb_open")
(define-c-lambda duckdb_close_ (ffi_duckdb_db_t*) void
  "ffi_duckdb_close")

;; Connection lifecycle
(define-c-lambda duckdb_connect_ (ffi_duckdb_db_t*) ffi_duckdb_conn_t*
  "ffi_duckdb_connect")
(define-c-lambda duckdb_disconnect_ (ffi_duckdb_conn_t*) void
  "ffi_duckdb_disconnect")

;; Query execution
(define-c-lambda duckdb_query_ (ffi_duckdb_conn_t* char-string) ffi_duckdb_result_t*
  "ffi_duckdb_query")

;; Result metadata
(define-c-lambda duckdb_result_error_ (ffi_duckdb_result_t*) char-string
  "___return ((char*)duckdb_result_error (&___arg1->result));")
(define-c-lambda duckdb_column_count_ (ffi_duckdb_result_t*) unsigned-int64
  "___return (duckdb_column_count (&___arg1->result));")
(define-c-lambda duckdb_row_count_ (ffi_duckdb_result_t*) unsigned-int64
  "___return (duckdb_row_count (&___arg1->result));")
(define-c-lambda duckdb_rows_changed_ (ffi_duckdb_result_t*) unsigned-int64
  "___return (duckdb_rows_changed (&___arg1->result));")
(define-c-lambda duckdb_column_name_ (ffi_duckdb_result_t* unsigned-int64) char-string
  "___return ((char*)duckdb_column_name (&___arg1->result, ___arg2));")
(define-c-lambda duckdb_column_type_ (ffi_duckdb_result_t* unsigned-int64) int
  "___return ((int)duckdb_column_type (&___arg1->result, ___arg2));")

;; Value extraction (row-based API)
(define-c-lambda duckdb_value_is_null_ (ffi_duckdb_result_t* unsigned-int64 unsigned-int64) bool
  "___return (duckdb_value_is_null (&___arg1->result, ___arg2, ___arg3));")
(define-c-lambda duckdb_value_boolean_ (ffi_duckdb_result_t* unsigned-int64 unsigned-int64) bool
  "___return (duckdb_value_boolean (&___arg1->result, ___arg2, ___arg3));")
(define-c-lambda duckdb_value_int8_ (ffi_duckdb_result_t* unsigned-int64 unsigned-int64) int8
  "___return (duckdb_value_int8 (&___arg1->result, ___arg2, ___arg3));")
(define-c-lambda duckdb_value_int16_ (ffi_duckdb_result_t* unsigned-int64 unsigned-int64) int16
  "___return (duckdb_value_int16 (&___arg1->result, ___arg2, ___arg3));")
(define-c-lambda duckdb_value_int32_ (ffi_duckdb_result_t* unsigned-int64 unsigned-int64) int32
  "___return (duckdb_value_int32 (&___arg1->result, ___arg2, ___arg3));")
(define-c-lambda duckdb_value_int64_ (ffi_duckdb_result_t* unsigned-int64 unsigned-int64) int64
  "___return (duckdb_value_int64 (&___arg1->result, ___arg2, ___arg3));")
(define-c-lambda duckdb_value_uint8_ (ffi_duckdb_result_t* unsigned-int64 unsigned-int64) unsigned-int8
  "___return (duckdb_value_uint8 (&___arg1->result, ___arg2, ___arg3));")
(define-c-lambda duckdb_value_uint16_ (ffi_duckdb_result_t* unsigned-int64 unsigned-int64) unsigned-int16
  "___return (duckdb_value_uint16 (&___arg1->result, ___arg2, ___arg3));")
(define-c-lambda duckdb_value_uint32_ (ffi_duckdb_result_t* unsigned-int64 unsigned-int64) unsigned-int32
  "___return (duckdb_value_uint32 (&___arg1->result, ___arg2, ___arg3));")
(define-c-lambda duckdb_value_uint64_ (ffi_duckdb_result_t* unsigned-int64 unsigned-int64) unsigned-int64
  "___return (duckdb_value_uint64 (&___arg1->result, ___arg2, ___arg3));")
(define-c-lambda duckdb_value_float_ (ffi_duckdb_result_t* unsigned-int64 unsigned-int64) float
  "___return (duckdb_value_float (&___arg1->result, ___arg2, ___arg3));")
(define-c-lambda duckdb_value_double_ (ffi_duckdb_result_t* unsigned-int64 unsigned-int64) double
  "___return (duckdb_value_double (&___arg1->result, ___arg2, ___arg3));")

;; VARCHAR: uses duckdb_value_varchar_internal which returns a pointer owned
;; by the result (no free needed). Safe as long as result is alive.
(define-c-lambda duckdb_value_varchar_ (ffi_duckdb_result_t* unsigned-int64 unsigned-int64) char-string
  "___return ((char*)duckdb_value_varchar_internal (&___arg1->result, ___arg2, ___arg3));")

;; Prepared statements
(define-c-lambda duckdb_prepare_ (ffi_duckdb_conn_t* char-string) ffi_duckdb_stmt_t*
  "ffi_duckdb_prepare")
(define-c-lambda duckdb_destroy_prepare_ (ffi_duckdb_stmt_t*) void
  "ffi_duckdb_destroy_prepare")
(define-c-lambda duckdb_prepare_error_ (ffi_duckdb_stmt_t*) char-string
  "___return ((char*)duckdb_prepare_error (___arg1->stmt));")
(define-c-lambda duckdb_nparams_ (ffi_duckdb_stmt_t*) unsigned-int64
  "___return (duckdb_nparams (___arg1->stmt));")
(define-c-lambda duckdb_execute_prepared_ (ffi_duckdb_stmt_t*) ffi_duckdb_result_t*
  "ffi_duckdb_execute_prepared")

;; Parameter binding (all return 0 = success, 1 = error)
(define-c-lambda duckdb_bind_boolean_ (ffi_duckdb_stmt_t* unsigned-int64 bool) int
  "___return ((int)duckdb_bind_boolean (___arg1->stmt, ___arg2, ___arg3));")
(define-c-lambda duckdb_bind_int32_ (ffi_duckdb_stmt_t* unsigned-int64 int32) int
  "___return ((int)duckdb_bind_int32 (___arg1->stmt, ___arg2, ___arg3));")
(define-c-lambda duckdb_bind_int64_ (ffi_duckdb_stmt_t* unsigned-int64 int64) int
  "___return ((int)duckdb_bind_int64 (___arg1->stmt, ___arg2, ___arg3));")
(define-c-lambda duckdb_bind_double_ (ffi_duckdb_stmt_t* unsigned-int64 double) int
  "___return ((int)duckdb_bind_double (___arg1->stmt, ___arg2, ___arg3));")
(define-c-lambda duckdb_bind_varchar_ (ffi_duckdb_stmt_t* unsigned-int64 char-string) int
  "___return ((int)duckdb_bind_varchar (___arg1->stmt, ___arg2, ___arg3));")
(define-c-lambda duckdb_bind_null_ (ffi_duckdb_stmt_t* unsigned-int64) int
  "___return ((int)duckdb_bind_null (___arg1->stmt, ___arg2));")
(define-c-lambda duckdb_bind_blob_ (ffi_duckdb_stmt_t* unsigned-int64 scheme-object) int
  "ffi_duckdb_bind_blob")

;; Clear bindings
(define-c-lambda duckdb_clear_bindings_ (ffi_duckdb_stmt_t*) int
  "___return ((int)duckdb_clear_bindings (___arg1->stmt));")

;; Destroy result explicitly
(define-c-lambda duckdb_destroy_result_ (ffi_duckdb_result_t*) void
  "duckdb_destroy_result (&___arg1->result);")

;; -- C implementations --

(c-declare #<<END-C

#ifndef ___HAVE_FFI_FREE
#define ___HAVE_FFI_FREE
___SCMOBJ ffi_free (void *ptr)
{
  free (ptr);
  return ___FIX (___NO_ERR);
}
#endif

/* --- GC finalizer cleanup functions --- */

___SCMOBJ ffi_free_duckdb_db (void *ptr)
{
  ffi_duckdb_db_t *dbw = (ffi_duckdb_db_t *)ptr;
  if (dbw->db) {
    duckdb_close (&dbw->db);
    dbw->db = NULL;
  }
  free (dbw);
  return ___FIX (___NO_ERR);
}

___SCMOBJ ffi_free_duckdb_conn (void *ptr)
{
  ffi_duckdb_conn_t *connw = (ffi_duckdb_conn_t *)ptr;
  if (connw->conn) {
    duckdb_disconnect (&connw->conn);
    connw->conn = NULL;
  }
  free (connw);
  return ___FIX (___NO_ERR);
}

___SCMOBJ ffi_free_duckdb_stmt (void *ptr)
{
  ffi_duckdb_stmt_t *stmtw = (ffi_duckdb_stmt_t *)ptr;
  if (stmtw->stmt) {
    duckdb_destroy_prepare (&stmtw->stmt);
    stmtw->stmt = NULL;
  }
  free (stmtw);
  return ___FIX (___NO_ERR);
}

___SCMOBJ ffi_free_duckdb_result (void *ptr)
{
  ffi_duckdb_result_t *resw = (ffi_duckdb_result_t *)ptr;
  duckdb_destroy_result (&resw->result);
  free (resw);
  return ___FIX (___NO_ERR);
}

/* --- Wrapper functions --- */

ffi_duckdb_db_t* ffi_duckdb_open (const char *path)
{
  ffi_duckdb_db_t *dbw = (ffi_duckdb_db_t *)malloc (sizeof (ffi_duckdb_db_t));
  if (!dbw) return NULL;
  dbw->db = NULL;
  if (duckdb_open (path, &dbw->db) == DuckDBError) {
    free (dbw);
    return NULL;
  }
  return dbw;
}

void ffi_duckdb_close (ffi_duckdb_db_t *dbw)
{
  if (dbw && dbw->db) {
    duckdb_close (&dbw->db);
    dbw->db = NULL;
  }
}

ffi_duckdb_conn_t* ffi_duckdb_connect (ffi_duckdb_db_t *dbw)
{
  if (!dbw || !dbw->db) return NULL;
  ffi_duckdb_conn_t *connw = (ffi_duckdb_conn_t *)malloc (sizeof (ffi_duckdb_conn_t));
  if (!connw) return NULL;
  connw->conn = NULL;
  if (duckdb_connect (dbw->db, &connw->conn) == DuckDBError) {
    free (connw);
    return NULL;
  }
  return connw;
}

void ffi_duckdb_disconnect (ffi_duckdb_conn_t *connw)
{
  if (connw && connw->conn) {
    duckdb_disconnect (&connw->conn);
    connw->conn = NULL;
  }
}

ffi_duckdb_result_t* ffi_duckdb_query (ffi_duckdb_conn_t *connw, const char *sql)
{
  if (!connw || !connw->conn) return NULL;
  ffi_duckdb_result_t *resw = (ffi_duckdb_result_t *)malloc (sizeof (ffi_duckdb_result_t));
  if (!resw) return NULL;
  memset (&resw->result, 0, sizeof (duckdb_result));
  duckdb_query (connw->conn, sql, &resw->result);
  return resw;
}

ffi_duckdb_stmt_t* ffi_duckdb_prepare (ffi_duckdb_conn_t *connw, const char *sql)
{
  if (!connw || !connw->conn) return NULL;
  ffi_duckdb_stmt_t *stmtw = (ffi_duckdb_stmt_t *)malloc (sizeof (ffi_duckdb_stmt_t));
  if (!stmtw) return NULL;
  stmtw->stmt = NULL;
  duckdb_prepare (connw->conn, sql, &stmtw->stmt);
  /* Always return - caller checks duckdb_prepare_error */
  return stmtw;
}

void ffi_duckdb_destroy_prepare (ffi_duckdb_stmt_t *stmtw)
{
  if (stmtw && stmtw->stmt) {
    duckdb_destroy_prepare (&stmtw->stmt);
    stmtw->stmt = NULL;
  }
}

ffi_duckdb_result_t* ffi_duckdb_execute_prepared (ffi_duckdb_stmt_t *stmtw)
{
  if (!stmtw || !stmtw->stmt) return NULL;
  ffi_duckdb_result_t *resw = (ffi_duckdb_result_t *)malloc (sizeof (ffi_duckdb_result_t));
  if (!resw) return NULL;
  memset (&resw->result, 0, sizeof (duckdb_result));
  duckdb_execute_prepared (stmtw->stmt, &resw->result);
  return resw;
}

int ffi_duckdb_bind_blob (ffi_duckdb_stmt_t *stmtw, uint64_t idx, ___SCMOBJ data)
{
  return (int)duckdb_bind_blob (stmtw->stmt, idx,
    (const uint8_t*)U8_DATA (data), U8_LEN (data));
}

END-C
)
