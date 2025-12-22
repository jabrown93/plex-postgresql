#ifndef PG_STATEMENT_H
#define PG_STATEMENT_H

#include "pg_types.h"
#include <sqlite3.h>

// Initialize statement module
void pg_statement_init(void);

// Statement Lifecycle
pg_stmt_t* pg_prepare(pg_connection_t *conn, const char *sql, sqlite3_stmt *sqlite_stmt_handle);
int pg_step(pg_stmt_t *stmt);
int pg_reset(pg_stmt_t *stmt);
int pg_finalize(pg_stmt_t *stmt);

// Parameter Binding
int pg_bind_int(pg_stmt_t *stmt, int idx, int val);
int pg_bind_int64(pg_stmt_t *stmt, int idx, sqlite3_int64 val);
int pg_bind_double(pg_stmt_t *stmt, int idx, double val);
int pg_bind_text(pg_stmt_t *stmt, int idx, const char *val, int len, void(*destructor)(void*));
int pg_bind_blob(pg_stmt_t *stmt, int idx, const void *val, int len, void(*destructor)(void*));
int pg_bind_null(pg_stmt_t *stmt, int idx);

// Column Access
int pg_column_count(pg_stmt_t *stmt);
int pg_column_type(pg_stmt_t *stmt, int idx);
const char* pg_column_name(pg_stmt_t *stmt, int idx);
int pg_column_int(pg_stmt_t *stmt, int idx);
sqlite3_int64 pg_column_int64(pg_stmt_t *stmt, int idx);
double pg_column_double(pg_stmt_t *stmt, int idx);
const unsigned char* pg_column_text(pg_stmt_t *stmt, int idx);
const void* pg_column_blob(pg_stmt_t *stmt, int idx);
int pg_column_bytes(pg_stmt_t *stmt, int idx);

// Helper to look up statement by sqlite handle
pg_stmt_t* find_pg_stmt(sqlite3_stmt *stmt);
void register_stmt(sqlite3_stmt *sqlite_stmt, pg_stmt_t *pg_stmt);
void unregister_stmt(sqlite3_stmt *sqlite_stmt);

// Helper for caching logic from TLS (exposed for column functions if needed?)
pg_stmt_t* find_any_pg_stmt(sqlite3_stmt *sqlite_stmt);

// Registry
void register_pg_stmt(pg_stmt_t *stmt);
void unregister_pg_stmt(pg_stmt_t *stmt);
int is_pg_stmt(pg_stmt_t *stmt);

// Helper for column value to never return NULL
sqlite3_value* pg_get_column_value(pg_stmt_t *stmt, int idx);

#endif // PG_STATEMENT_H
