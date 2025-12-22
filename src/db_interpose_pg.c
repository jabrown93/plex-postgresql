#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sqlite3.h>
#include <dlfcn.h>
#include <stdarg.h>
#include <mach-o/dyld.h>

#include "pg_types.h"
#include "pg_config.h"
#include "pg_logging.h"
#include "pg_client.h"
#include "pg_statement.h"
#include "sql_translator.h"

static _Thread_local int in_shim = 0;

#define SHIM_ENTER() \
    if (in_shim) return 0; \
    in_shim = 1;

#define SHIM_LEAVE() \
    in_shim = 0;

// Specialized macro for functions returning int
#define SHIM_GUARD_VOID(_orig_call) \
    if (in_shim) { _orig_call; return; } \
    in_shim = 1; \
    // ... logic ...
    in_shim = 0;

// ============================================================================
// DYLD Interpose Definitions
// ============================================================================

#define DYLD_INTERPOSE(_replacement, _original) \
    __attribute__((used)) static struct { \
        const void* replacement; \
        const void* original; \
    } _interpose_##_original __attribute__((section("__DATA,__interpose"))) = { \
        (const void*)(unsigned long)&_replacement, \
        (const void*)(unsigned long)&_original \
    };

// ============================================================================
// Original Function Pointers (Pre-resolved)
// ============================================================================

static int (*orig_sqlite3_open)(const char*, sqlite3**) = NULL;
static int (*orig_sqlite3_open_v2)(const char*, sqlite3**, int, const char*) = NULL;
static int (*orig_sqlite3_open16)(const void*, sqlite3**) = NULL;
static int (*orig_sqlite3_close)(sqlite3*) = NULL;
static int (*orig_sqlite3_close_v2)(sqlite3*) = NULL;
static int (*orig_sqlite3_prepare)(sqlite3*, const char*, int, sqlite3_stmt**, const char**) = NULL;
static int (*orig_sqlite3_prepare_v2)(sqlite3*, const char*, int, sqlite3_stmt**, const char**) = NULL;
static int (*orig_sqlite3_prepare_v3)(sqlite3*, const char*, int, unsigned int, sqlite3_stmt**, const char**) = NULL;
static int (*orig_sqlite3_step)(sqlite3_stmt*) = NULL;
static int (*orig_sqlite3_reset)(sqlite3_stmt*) = NULL;
static int (*orig_sqlite3_finalize)(sqlite3_stmt*) = NULL;
static int (*orig_sqlite3_exec)(sqlite3*, const char*, int(*)(void*,int,char**,char**), void*, char**) = NULL;
static sqlite3_int64 (*orig_sqlite3_last_insert_rowid)(sqlite3*) = NULL;
static int (*orig_sqlite3_changes)(sqlite3*) = NULL;
static const char* (*orig_sqlite3_errmsg)(sqlite3*) = NULL;
static int (*orig_sqlite3_errcode)(sqlite3*) = NULL;
static int (*orig_sqlite3_column_count)(sqlite3_stmt*) = NULL;
static const char* (*orig_sqlite3_column_name)(sqlite3_stmt*, int) = NULL;
static int (*orig_sqlite3_column_type)(sqlite3_stmt*, int) = NULL;
static int (*orig_sqlite3_column_int)(sqlite3_stmt*, int) = NULL;
static sqlite3_int64 (*orig_sqlite3_column_int64)(sqlite3_stmt*, int) = NULL;
static const unsigned char* (*orig_sqlite3_column_text)(sqlite3_stmt*, int) = NULL;
static const void* (*orig_sqlite3_column_blob)(sqlite3_stmt*, int) = NULL;
static double (*orig_sqlite3_column_double)(sqlite3_stmt*, int) = NULL;
static int (*orig_sqlite3_column_bytes)(sqlite3_stmt*, int) = NULL;
static int (*orig_sqlite3_bind_int)(sqlite3_stmt*, int, int) = NULL;
static int (*orig_sqlite3_bind_int64)(sqlite3_stmt*, int, sqlite3_int64) = NULL;
static int (*orig_sqlite3_bind_text)(sqlite3_stmt*, int, const char*, int, void(*)(void*)) = NULL;
static int (*orig_sqlite3_bind_double)(sqlite3_stmt*, int, double) = NULL;
static int (*orig_sqlite3_bind_blob)(sqlite3_stmt*, int, const void*, int, void(*)(void*)) = NULL;
static int (*orig_sqlite3_bind_null)(sqlite3_stmt*, int) = NULL;
static int (*orig_sqlite3_bind_parameter_count)(sqlite3_stmt*) = NULL;
static int (*orig_sqlite3_stmt_readonly)(sqlite3_stmt*) = NULL;
static const char* (*orig_sqlite3_column_decltype)(sqlite3_stmt*, int) = NULL;
static int (*orig_sqlite3_clear_bindings)(sqlite3_stmt*) = NULL;
static int (*orig_sqlite3_wal_checkpoint)(sqlite3*, const char*) = NULL;
static int (*orig_sqlite3_wal_checkpoint_v2)(sqlite3*, const char*, int, int*, int*) = NULL;
static int (*orig_sqlite3_table_column_metadata)(sqlite3*, const char*, const char*, const char*, const char**, const char**, int*, int*, int*) = NULL;

static void* resolve_one(void *handle, const char *symbol) {
    if (!handle) return NULL;
    void *s = dlsym(handle, symbol);
    if (!s) LOG_ERROR("Failed to resolve %s", symbol);
    return s;
}

static void resolve_all_symbols(void) {
    const char *bundled_path = "/Applications/Plex Media Server.app/Contents/Frameworks/libsqlite3.dylib";
    void *handle = dlopen(bundled_path, RTLD_LAZY | RTLD_LOCAL | RTLD_NOLOAD);
    if (!handle) handle = dlopen(bundled_path, RTLD_LAZY | RTLD_LOCAL);
    
    if (!handle) {
        LOG_ERROR("CRITICAL: libsqlite3.dylib NOT FOUND AT %s", bundled_path);
        // Fallback to system one just so we don't crash immediately
        handle = dlopen("/usr/lib/libsqlite3.dylib", RTLD_LAZY | RTLD_LOCAL);
    }
    
    if (handle) LOG_INFO("Resolving symbols using handle %p", handle);

    orig_sqlite3_open = resolve_one(handle, "sqlite3_open");
    orig_sqlite3_open_v2 = resolve_one(handle, "sqlite3_open_v2");
    orig_sqlite3_open16 = resolve_one(handle, "sqlite3_open16");
    orig_sqlite3_close = resolve_one(handle, "sqlite3_close");
    orig_sqlite3_close_v2 = resolve_one(handle, "sqlite3_close_v2");
    orig_sqlite3_prepare = resolve_one(handle, "sqlite3_prepare");
    orig_sqlite3_prepare_v2 = resolve_one(handle, "sqlite3_prepare_v2");
    orig_sqlite3_prepare_v3 = resolve_one(handle, "sqlite3_prepare_v3");
    orig_sqlite3_step = resolve_one(handle, "sqlite3_step");
    orig_sqlite3_reset = resolve_one(handle, "sqlite3_reset");
    orig_sqlite3_finalize = resolve_one(handle, "sqlite3_finalize");
    orig_sqlite3_exec = resolve_one(handle, "sqlite3_exec");
    orig_sqlite3_last_insert_rowid = resolve_one(handle, "sqlite3_last_insert_rowid");
    orig_sqlite3_changes = resolve_one(handle, "sqlite3_changes");
    orig_sqlite3_errmsg = resolve_one(handle, "sqlite3_errmsg");
    orig_sqlite3_errcode = resolve_one(handle, "sqlite3_errcode");
    orig_sqlite3_column_count = resolve_one(handle, "sqlite3_column_count");
    orig_sqlite3_column_name = resolve_one(handle, "sqlite3_column_name");
    orig_sqlite3_column_type = resolve_one(handle, "sqlite3_column_type");
    orig_sqlite3_column_int = resolve_one(handle, "sqlite3_column_int");
    orig_sqlite3_column_int64 = resolve_one(handle, "sqlite3_column_int64");
    orig_sqlite3_column_text = resolve_one(handle, "sqlite3_column_text");
    orig_sqlite3_column_blob = resolve_one(handle, "sqlite3_column_blob");
    orig_sqlite3_column_double = resolve_one(handle, "sqlite3_column_double");
    orig_sqlite3_column_bytes = resolve_one(handle, "sqlite3_column_bytes");
    orig_sqlite3_bind_int = resolve_one(handle, "sqlite3_bind_int");
    orig_sqlite3_bind_int64 = resolve_one(handle, "sqlite3_bind_int64");
    orig_sqlite3_bind_text = resolve_one(handle, "sqlite3_bind_text");
    orig_sqlite3_bind_double = resolve_one(handle, "sqlite3_bind_double");
    orig_sqlite3_bind_blob = resolve_one(handle, "sqlite3_bind_blob");
    orig_sqlite3_bind_null = resolve_one(handle, "sqlite3_bind_null");
    orig_sqlite3_bind_parameter_count = resolve_one(handle, "sqlite3_bind_parameter_count");
    orig_sqlite3_stmt_readonly = resolve_one(handle, "sqlite3_stmt_readonly");
    orig_sqlite3_column_decltype = resolve_one(handle, "sqlite3_column_decltype");
    orig_sqlite3_clear_bindings = resolve_one(handle, "sqlite3_clear_bindings");
    orig_sqlite3_wal_checkpoint = resolve_one(handle, "sqlite3_wal_checkpoint");
    orig_sqlite3_wal_checkpoint_v2 = resolve_one(handle, "sqlite3_wal_checkpoint_v2");
    orig_sqlite3_table_column_metadata = resolve_one(handle, "sqlite3_table_column_metadata");
    
    LOG_INFO("All symbols pre-resolved.");
}

// ============================================================================
// Core Interposers
// ============================================================================

static pthread_mutex_t open_serialization_mutex = PTHREAD_MUTEX_INITIALIZER;

static int my_sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags, const char *zVfs) {
    if (in_shim) return orig_sqlite3_open_v2 ? orig_sqlite3_open_v2(filename, ppDb, flags, zVfs) : SQLITE_ERROR;
    in_shim = 1;
    
    if (!orig_sqlite3_open_v2) { in_shim = 0; return SQLITE_ERROR; }
    
    pthread_mutex_lock(&open_serialization_mutex);
    LOG_INFO("OPEN_V2: %s", filename ? filename : "null");
    
    int rc = orig_sqlite3_open_v2(filename, ppDb, flags, zVfs);
    
    if (rc == SQLITE_OK && should_redirect(filename)) {
        sqlite3 *db = *ppDb;
        pg_connection_t *conn = calloc(1, sizeof(pg_connection_t));
        if (conn) {
            strncpy(conn->db_path, filename, sizeof(conn->db_path) - 1);
            conn->is_pg_active = 1;
            pthread_mutex_init(&conn->mutex, NULL);
            if (ensure_pg_connection(conn)) {
                pg_register_connection(db, conn);
                LOG_INFO("OPEN_V2 REDIRECTED: %p", (void*)db);
            } else {
                LOG_ERROR("OPEN_V2 REDIRECT FAIL: %s", filename);
                free(conn);
            }
        }
    }
    pthread_mutex_unlock(&open_serialization_mutex);
    in_shim = 0;
    return rc;
}

static int my_sqlite3_open(const char *filename, sqlite3 **ppDb) {
    if (in_shim) return orig_sqlite3_open ? orig_sqlite3_open(filename, ppDb) : SQLITE_ERROR;
    return my_sqlite3_open_v2(filename, ppDb, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
}

static int my_sqlite3_open16(const void *filename, sqlite3 **ppDb) {
    if (in_shim) return orig_sqlite3_open16 ? orig_sqlite3_open16(filename, ppDb) : SQLITE_ERROR;
    if (!orig_sqlite3_open16) return SQLITE_ERROR;
    LOG_INFO("OPEN16: UTF-16 open requested (not redirected)");
    return orig_sqlite3_open16(filename, ppDb);
}

static int my_sqlite3_close(sqlite3 *db) {
    if (in_shim) return orig_sqlite3_close ? orig_sqlite3_close(db) : SQLITE_OK;
    in_shim = 1;
    pg_connection_t *conn = find_pg_connection(db);
    if (conn) {
        pg_unregister_connection(db);
        pg_close(conn);
    }
    int rc = orig_sqlite3_close ? orig_sqlite3_close(db) : SQLITE_OK;
    in_shim = 0;
    return rc;
}

static int my_sqlite3_close_v2(sqlite3 *db) {
    return my_sqlite3_close(db);
}

static int my_sqlite3_prepare_v2(sqlite3 *db, const char *zSql, int nByte, sqlite3_stmt **ppStmt, const char **pzTail) {
    if (in_shim) return orig_sqlite3_prepare_v2 ? orig_sqlite3_prepare_v2(db, zSql, nByte, ppStmt, pzTail) : SQLITE_ERROR;
    in_shim = 1;

    pg_connection_t *conn = find_pg_connection(db);
    if (conn && conn->is_pg_active) {
        if (!should_skip_sql(zSql)) {
            char *sql_str = NULL;
            if (nByte < 0) sql_str = strdup(zSql);
            else { sql_str = malloc(nByte + 1); memcpy(sql_str, zSql, nByte); sql_str[nByte] = '\0'; }
            
            pg_stmt_t *stmt = pg_prepare(conn, sql_str, *ppStmt);
            free(sql_str);
            if (stmt) {
                *ppStmt = (sqlite3_stmt*)stmt;
                if (pzTail) *pzTail = zSql + strlen(zSql);
                in_shim = 0;
                return SQLITE_OK;
            }
            in_shim = 0;
            return SQLITE_ERROR;
        }
    }
    int rc = orig_sqlite3_prepare_v2 ? orig_sqlite3_prepare_v2(db, zSql, nByte, ppStmt, pzTail) : SQLITE_ERROR;
    in_shim = 0;
    return rc;
}

static int my_sqlite3_prepare(sqlite3 *db, const char *zSql, int nByte, sqlite3_stmt **ppStmt, const char **pzTail) {
    return my_sqlite3_prepare_v2(db, zSql, nByte, ppStmt, pzTail);
}

static int my_sqlite3_prepare_v3(sqlite3 *db, const char *zSql, int nByte, unsigned int prepFlags, sqlite3_stmt **ppStmt, const char **pzTail) {
    return my_sqlite3_prepare_v2(db, zSql, nByte, ppStmt, pzTail);
}

static int my_sqlite3_prepare16_v2(sqlite3 *db, const void *zSql, int nByte, sqlite3_stmt **ppStmt, const void **pzTail) {
    return SQLITE_ERROR;
}

static int my_sqlite3_step(sqlite3_stmt *pStmt) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_step((pg_stmt_t*)pStmt);
    if (in_shim) return orig_sqlite3_step ? orig_sqlite3_step(pStmt) : SQLITE_ERROR;
    in_shim = 1;
    int rc = orig_sqlite3_step ? orig_sqlite3_step(pStmt) : SQLITE_ERROR;
    in_shim = 0;
    return rc;
}

static int my_sqlite3_reset(sqlite3_stmt *pStmt) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_reset((pg_stmt_t*)pStmt);
    if (in_shim) return orig_sqlite3_reset ? orig_sqlite3_reset(pStmt) : SQLITE_OK;
    in_shim = 1;
    int rc = orig_sqlite3_reset ? orig_sqlite3_reset(pStmt) : SQLITE_OK;
    in_shim = 0;
    return rc;
}

static int my_sqlite3_finalize(sqlite3_stmt *pStmt) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_finalize((pg_stmt_t*)pStmt);
    if (in_shim) return orig_sqlite3_finalize ? orig_sqlite3_finalize(pStmt) : SQLITE_OK;
    in_shim = 1;
    int rc = orig_sqlite3_finalize ? orig_sqlite3_finalize(pStmt) : SQLITE_OK;
    in_shim = 0;
    return rc;
}

static int my_sqlite3_exec(sqlite3 *db, const char *sql, int (*callback)(void*,int,char**,char**), void *arg, char **errmsg) {
    if (in_shim) return orig_sqlite3_exec ? orig_sqlite3_exec(db, sql, callback, arg, errmsg) : SQLITE_ERROR;
    in_shim = 1;

    pg_connection_t *conn = find_pg_connection(db);
    if (conn && conn->is_pg_active && !should_skip_sql(sql)) {
         pg_stmt_t *stmt = pg_prepare(conn, sql, NULL);
         if (!stmt) { if(errmsg) *errmsg=strdup("PG Prepare Fail"); in_shim = 0; return SQLITE_ERROR; }
         int rc;
         while((rc = pg_step(stmt)) == SQLITE_ROW);
         pg_finalize(stmt);
         in_shim = 0;
         return SQLITE_OK;
    }
    int rc = orig_sqlite3_exec ? orig_sqlite3_exec(db, sql, callback, arg, errmsg) : SQLITE_ERROR;
    in_shim = 0;
    return rc;
}

static sqlite3_int64 my_sqlite3_last_insert_rowid(sqlite3 *db) {
    if (in_shim) return orig_sqlite3_last_insert_rowid ? orig_sqlite3_last_insert_rowid(db) : 0;
    in_shim = 1;
    pg_connection_t *conn = find_pg_connection(db);
    sqlite3_int64 res = (conn && conn->is_pg_active) ? conn->last_insert_rowid : (orig_sqlite3_last_insert_rowid ? orig_sqlite3_last_insert_rowid(db) : 0);
    in_shim = 0;
    return res;
}

static int my_sqlite3_changes(sqlite3 *db) {
    if (in_shim) return orig_sqlite3_changes ? orig_sqlite3_changes(db) : 0;
    in_shim = 1;
    pg_connection_t *conn = find_pg_connection(db);
    int res = (conn && conn->is_pg_active) ? conn->last_changes : (orig_sqlite3_changes ? orig_sqlite3_changes(db) : 0);
    in_shim = 0;
    return res;
}

static const char *my_sqlite3_errmsg(sqlite3 *db) {
    if (in_shim) return orig_sqlite3_errmsg ? orig_sqlite3_errmsg(db) : "Unknown Error";
    in_shim = 1;
    pg_connection_t *conn = find_pg_connection(db);
    const char *res = (conn && conn->is_pg_active) ? conn->last_error : (orig_sqlite3_errmsg ? orig_sqlite3_errmsg(db) : "Unknown Error");
    in_shim = 0;
    return res;
}

static int my_sqlite3_errcode(sqlite3 *db) {
    if (in_shim) return orig_sqlite3_errcode ? orig_sqlite3_errcode(db) : SQLITE_ERROR;
    in_shim = 1;
    pg_connection_t *conn = find_pg_connection(db);
    int res = (conn && conn->is_pg_active) ? conn->last_error_code : (orig_sqlite3_errcode ? orig_sqlite3_errcode(db) : SQLITE_ERROR);
    in_shim = 0;
    return res;
}

static int my_sqlite3_bind_parameter_count(sqlite3_stmt *pStmt) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return ((pg_stmt_t*)pStmt)->param_count;
    if (in_shim) return orig_sqlite3_bind_parameter_count ? orig_sqlite3_bind_parameter_count(pStmt) : 0;
    in_shim = 1;
    int res = orig_sqlite3_bind_parameter_count ? orig_sqlite3_bind_parameter_count(pStmt) : 0;
    in_shim = 0;
    return res;
}

static int my_sqlite3_stmt_readonly(sqlite3_stmt *pStmt) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return 0;
    if (in_shim) return orig_sqlite3_stmt_readonly ? orig_sqlite3_stmt_readonly(pStmt) : 0;
    in_shim = 1;
    int res = orig_sqlite3_stmt_readonly ? orig_sqlite3_stmt_readonly(pStmt) : 0;
    in_shim = 0;
    return res;
}

static const char* my_sqlite3_column_decltype(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return "text";
    if (in_shim) return orig_sqlite3_column_decltype ? orig_sqlite3_column_decltype(pStmt, idx) : NULL;
    in_shim = 1;
    const char *res = orig_sqlite3_column_decltype ? orig_sqlite3_column_decltype(pStmt, idx) : NULL;
    in_shim = 0;
    return res;
}

static int my_sqlite3_column_count(sqlite3_stmt *pStmt) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_count((pg_stmt_t*)pStmt);
    if (in_shim) return orig_sqlite3_column_count ? orig_sqlite3_column_count(pStmt) : 0;
    in_shim = 1;
    int res = orig_sqlite3_column_count ? orig_sqlite3_column_count(pStmt) : 0;
    in_shim = 0;
    return res;
}

static const char* my_sqlite3_column_name(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_name((pg_stmt_t*)pStmt, idx);
    if (in_shim) return orig_sqlite3_column_name ? orig_sqlite3_column_name(pStmt, idx) : NULL;
    in_shim = 1;
    const char *res = orig_sqlite3_column_name ? orig_sqlite3_column_name(pStmt, idx) : NULL;
    in_shim = 0;
    return res;
}

static int my_sqlite3_column_type(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_type((pg_stmt_t*)pStmt, idx);
    if (in_shim) return orig_sqlite3_column_type ? orig_sqlite3_column_type(pStmt, idx) : SQLITE_NULL;
    in_shim = 1;
    int res = orig_sqlite3_column_type ? orig_sqlite3_column_type(pStmt, idx) : SQLITE_NULL;
    in_shim = 0;
    return res;
}

static int my_sqlite3_column_int(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_int((pg_stmt_t*)pStmt, idx);
    if (in_shim) return orig_sqlite3_column_int ? orig_sqlite3_column_int(pStmt, idx) : 0;
    in_shim = 1;
    int res = orig_sqlite3_column_int ? orig_sqlite3_column_int(pStmt, idx) : 0;
    in_shim = 0;
    return res;
}

static sqlite3_int64 my_sqlite3_column_int64(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_int64((pg_stmt_t*)pStmt, idx);
    if (in_shim) return orig_sqlite3_column_int64 ? orig_sqlite3_column_int64(pStmt, idx) : 0;
    in_shim = 1;
    sqlite3_int64 res = orig_sqlite3_column_int64 ? orig_sqlite3_column_int64(pStmt, idx) : 0;
    in_shim = 0;
    return res;
}

static const unsigned char* my_sqlite3_column_text(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_text((pg_stmt_t*)pStmt, idx);
    if (in_shim) return orig_sqlite3_column_text ? orig_sqlite3_column_text(pStmt, idx) : NULL;
    in_shim = 1;
    const unsigned char *res = orig_sqlite3_column_text ? orig_sqlite3_column_text(pStmt, idx) : NULL;
    in_shim = 0;
    return res;
}

static const void* my_sqlite3_column_blob(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_blob((pg_stmt_t*)pStmt, idx);
    if (in_shim) return orig_sqlite3_column_blob ? orig_sqlite3_column_blob(pStmt, idx) : NULL;
    in_shim = 1;
    const void *res = orig_sqlite3_column_blob ? orig_sqlite3_column_blob(pStmt, idx) : NULL;
    in_shim = 0;
    return res;
}

static double my_sqlite3_column_double(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_double((pg_stmt_t*)pStmt, idx);
    if (in_shim) return orig_sqlite3_column_double ? orig_sqlite3_column_double(pStmt, idx) : 0.0;
    in_shim = 1;
    double res = orig_sqlite3_column_double ? orig_sqlite3_column_double(pStmt, idx) : 0.0;
    in_shim = 0;
    return res;
}

static int my_sqlite3_column_bytes(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_bytes((pg_stmt_t*)pStmt, idx);
    if (in_shim) return orig_sqlite3_column_bytes ? orig_sqlite3_column_bytes(pStmt, idx) : 0;
    in_shim = 1;
    int res = orig_sqlite3_column_bytes ? orig_sqlite3_column_bytes(pStmt, idx) : 0;
    in_shim = 0;
    return res;
}

static int my_sqlite3_bind_int(sqlite3_stmt *pStmt, int idx, int val) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_bind_int((pg_stmt_t*)pStmt, idx, val);
    if (in_shim) return orig_sqlite3_bind_int ? orig_sqlite3_bind_int(pStmt, idx, val) : SQLITE_ERROR;
    in_shim = 1;
    int rc = orig_sqlite3_bind_int ? orig_sqlite3_bind_int(pStmt, idx, val) : SQLITE_ERROR;
    in_shim = 0;
    return rc;
}

static int my_sqlite3_bind_int64(sqlite3_stmt *pStmt, int idx, sqlite3_int64 val) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_bind_int64((pg_stmt_t*)pStmt, idx, val);
    if (in_shim) return orig_sqlite3_bind_int64 ? orig_sqlite3_bind_int64(pStmt, idx, val) : SQLITE_ERROR;
    in_shim = 1;
    int rc = orig_sqlite3_bind_int64 ? orig_sqlite3_bind_int64(pStmt, idx, val) : SQLITE_ERROR;
    in_shim = 0;
    return rc;
}

static int my_sqlite3_bind_text(sqlite3_stmt *pStmt, int idx, const char *val, int len, void(*destructor)(void*)) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_bind_text((pg_stmt_t*)pStmt, idx, val, len, destructor);
    if (in_shim) return orig_sqlite3_bind_text ? orig_sqlite3_bind_text(pStmt, idx, val, len, destructor) : SQLITE_ERROR;
    in_shim = 1;
    int rc = orig_sqlite3_bind_text ? orig_sqlite3_bind_text(pStmt, idx, val, len, destructor) : SQLITE_ERROR;
    in_shim = 0;
    return rc;
}

static int my_sqlite3_bind_double(sqlite3_stmt *pStmt, int idx, double val) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_bind_double((pg_stmt_t*)pStmt, idx, val);
    if (in_shim) return orig_sqlite3_bind_double ? orig_sqlite3_bind_double(pStmt, idx, val) : SQLITE_ERROR;
    in_shim = 1;
    int rc = orig_sqlite3_bind_double ? orig_sqlite3_bind_double(pStmt, idx, val) : SQLITE_ERROR;
    in_shim = 0;
    return rc;
}

static int my_sqlite3_bind_blob(sqlite3_stmt *pStmt, int idx, const void *val, int len, void(*destructor)(void*)) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_bind_blob((pg_stmt_t*)pStmt, idx, val, len, destructor);
    if (in_shim) return orig_sqlite3_bind_blob ? orig_sqlite3_bind_blob(pStmt, idx, val, len, destructor) : SQLITE_ERROR;
    in_shim = 1;
    int rc = orig_sqlite3_bind_blob ? orig_sqlite3_bind_blob(pStmt, idx, val, len, destructor) : SQLITE_ERROR;
    in_shim = 0;
    return rc;
}

static int my_sqlite3_bind_null(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_bind_null((pg_stmt_t*)pStmt, idx);
    if (in_shim) return orig_sqlite3_bind_null ? orig_sqlite3_bind_null(pStmt, idx) : SQLITE_ERROR;
    in_shim = 1;
    int rc = orig_sqlite3_bind_null ? orig_sqlite3_bind_null(pStmt, idx) : SQLITE_ERROR;
    in_shim = 0;
    return rc;
}

static int my_sqlite3_clear_bindings(sqlite3_stmt *pStmt) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return SQLITE_OK;
    if (in_shim) return orig_sqlite3_clear_bindings ? orig_sqlite3_clear_bindings(pStmt) : SQLITE_OK;
    in_shim = 1;
    int rc = orig_sqlite3_clear_bindings ? orig_sqlite3_clear_bindings(pStmt) : SQLITE_OK;
    in_shim = 0;
    return rc;
}

static int my_sqlite3_wal_checkpoint(sqlite3 *db, const char *zDb) {
    if (in_shim) return orig_sqlite3_wal_checkpoint ? orig_sqlite3_wal_checkpoint(db, zDb) : SQLITE_OK;
    in_shim = 1;
    int rc = orig_sqlite3_wal_checkpoint ? orig_sqlite3_wal_checkpoint(db, zDb) : SQLITE_OK;
    in_shim = 0;
    return rc;
}

static int my_sqlite3_wal_checkpoint_v2(sqlite3 *db, const char *zDb, int eMode, int *pnLog, int *pnCkpt) {
    if (in_shim) return orig_sqlite3_wal_checkpoint_v2 ? orig_sqlite3_wal_checkpoint_v2(db, zDb, eMode, pnLog, pnCkpt) : SQLITE_OK;
    in_shim = 1;
    int rc = orig_sqlite3_wal_checkpoint_v2 ? orig_sqlite3_wal_checkpoint_v2(db, zDb, eMode, pnLog, pnCkpt) : SQLITE_OK;
    in_shim = 0;
    return rc;
}

static int my_sqlite3_table_column_metadata(sqlite3 *db, const char *zDbName, const char *zTableName, const char *zColumnName, const char **pzDataType, const char **pzCollSeq, int *pNotNull, int *pPrimaryKey, int *pAutoinc) {
    if (in_shim) return orig_sqlite3_table_column_metadata ? orig_sqlite3_table_column_metadata(db, zDbName, zTableName, zColumnName, pzDataType, pzCollSeq, pNotNull, pPrimaryKey, pAutoinc) : SQLITE_ERROR;
    in_shim = 1;
    int rc = orig_sqlite3_table_column_metadata ? orig_sqlite3_table_column_metadata(db, zDbName, zTableName, zColumnName, pzDataType, pzCollSeq, pNotNull, pPrimaryKey, pAutoinc) : SQLITE_ERROR;
    in_shim = 0;
    return rc;
}

// ============================================================================
// Interpose Map
// ============================================================================

DYLD_INTERPOSE(my_sqlite3_open, sqlite3_open)
DYLD_INTERPOSE(my_sqlite3_open_v2, sqlite3_open_v2)
DYLD_INTERPOSE(my_sqlite3_open16, sqlite3_open16)
DYLD_INTERPOSE(my_sqlite3_close, sqlite3_close)
DYLD_INTERPOSE(my_sqlite3_close_v2, sqlite3_close_v2)
DYLD_INTERPOSE(my_sqlite3_prepare, sqlite3_prepare)
DYLD_INTERPOSE(my_sqlite3_prepare_v2, sqlite3_prepare_v2)
DYLD_INTERPOSE(my_sqlite3_prepare_v3, sqlite3_prepare_v3)
DYLD_INTERPOSE(my_sqlite3_prepare16_v2, sqlite3_prepare16_v2)
DYLD_INTERPOSE(my_sqlite3_step, sqlite3_step)
DYLD_INTERPOSE(my_sqlite3_reset, sqlite3_reset)
DYLD_INTERPOSE(my_sqlite3_finalize, sqlite3_finalize)
DYLD_INTERPOSE(my_sqlite3_exec, sqlite3_exec)
DYLD_INTERPOSE(my_sqlite3_last_insert_rowid, sqlite3_last_insert_rowid)
DYLD_INTERPOSE(my_sqlite3_changes, sqlite3_changes)
DYLD_INTERPOSE(my_sqlite3_errmsg, sqlite3_errmsg)
DYLD_INTERPOSE(my_sqlite3_errcode, sqlite3_errcode)
DYLD_INTERPOSE(my_sqlite3_column_count, sqlite3_column_count)
DYLD_INTERPOSE(my_sqlite3_column_name, sqlite3_column_name)
DYLD_INTERPOSE(my_sqlite3_column_type, sqlite3_column_type)
DYLD_INTERPOSE(my_sqlite3_column_int, sqlite3_column_int)
DYLD_INTERPOSE(my_sqlite3_column_int64, sqlite3_column_int64)
DYLD_INTERPOSE(my_sqlite3_column_text, sqlite3_column_text)
DYLD_INTERPOSE(my_sqlite3_column_blob, sqlite3_column_blob)
DYLD_INTERPOSE(my_sqlite3_column_double, sqlite3_column_double)
DYLD_INTERPOSE(my_sqlite3_column_bytes, sqlite3_column_bytes)
DYLD_INTERPOSE(my_sqlite3_bind_int, sqlite3_bind_int)
DYLD_INTERPOSE(my_sqlite3_bind_int64, sqlite3_bind_int64)
DYLD_INTERPOSE(my_sqlite3_bind_text, sqlite3_bind_text)
DYLD_INTERPOSE(my_sqlite3_bind_double, sqlite3_bind_double)
DYLD_INTERPOSE(my_sqlite3_bind_blob, sqlite3_bind_blob)
DYLD_INTERPOSE(my_sqlite3_bind_null, sqlite3_bind_null)
DYLD_INTERPOSE(my_sqlite3_bind_parameter_count, sqlite3_bind_parameter_count)
DYLD_INTERPOSE(my_sqlite3_stmt_readonly, sqlite3_stmt_readonly)
DYLD_INTERPOSE(my_sqlite3_column_decltype, sqlite3_column_decltype)
DYLD_INTERPOSE(my_sqlite3_clear_bindings, sqlite3_clear_bindings)
DYLD_INTERPOSE(my_sqlite3_wal_checkpoint, sqlite3_wal_checkpoint)
DYLD_INTERPOSE(my_sqlite3_wal_checkpoint_v2, sqlite3_wal_checkpoint_v2)
DYLD_INTERPOSE(my_sqlite3_table_column_metadata, sqlite3_table_column_metadata)

__attribute__((constructor))
static void shim_init(void) {
    fprintf(stderr, "!!!!! SHIM INIT STARTING !!!!!\n");
    init_logging();
    load_config();
    pg_client_init();
    resolve_all_symbols();
    LOG_INFO("Shim initialized.");
}
