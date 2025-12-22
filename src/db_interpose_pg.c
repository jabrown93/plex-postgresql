#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sqlite3.h>
#include <dlfcn.h>
#include <stdarg.h>

#include "pg_types.h"
#include "pg_config.h"
#include "pg_logging.h"
#include "pg_client.h"
#include "pg_statement.h"
#include "fishhook.h"
#include "sql_translator.h"

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
// Helpers
// ============================================================================



// ... Connection functions ...

// Hooks for fishhook
static const char* (*orig_sqlite3_column_name)(sqlite3_stmt*, int) = NULL;

// ...


// ============================================================================
// Connection Functions
// ============================================================================

static void* get_bundled_sqlite_handle() {
    static void *handle = NULL;
    if (handle) return handle;
    
    // Hardcoded path to Plex bundled SQLite
    const char *bundled_path = "/Applications/Plex Media Server.app/Contents/Frameworks/libsqlite3.dylib";
    
    // Check if we are already using it? No, dlsym might find system.
    // Explicitly load it.
    handle = dlopen(bundled_path, RTLD_LAZY | RTLD_LOCAL);
    if (handle) {
        LOG_INFO("Loaded bundled SQLite: %s", bundled_path);
    } else {
        LOG_ERROR("Failed to load bundled SQLite: %s", dlerror());
    }
    return handle;
}

static void* resolve_sqlite_symbol(const char *symbol) {
    // 1. Try standard RTLD_NEXT
    void *sym = dlsym(RTLD_NEXT, symbol);
    
    // 2. Identify if we need to switch (System -> Bundled) or if we Failed
    int use_bundled = 0;
    
    if (!sym) {
        LOG_ERROR("dlsym(RTLD_NEXT, %s) FAILED", symbol);
        use_bundled = 1;
    } else {
        Dl_info info;
        if (dladdr(sym, &info) && strstr(info.dli_fname, "/usr/lib/libsqlite3.dylib")) {
            LOG_INFO("Symbol %s resolved to SYSTEM libs. Switching to bundled.", symbol);
            use_bundled = 1;
        }
    }
    
    if (use_bundled) {
        void *bundled = get_bundled_sqlite_handle();
        if (bundled) {
            void *bsym = dlsym(bundled, symbol);
            if (bsym) {
                if (!sym) LOG_INFO("Resolved %s via EXPLICIT BUNDLED lookup: %p", symbol, bsym);
                else LOG_INFO("Re-resolved %s to BUNDLED: %p", symbol, bsym);
                return bsym;
            } else {
                LOG_ERROR("Symbol %s NOT FOUND in bundled lib", symbol);
            }
        } else {
            LOG_ERROR("Could not load bundled library to resolve %s", symbol);
        }
    }
    
    return sym;
}

static int my_sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags, const char *zVfs) {
    if (should_redirect(filename)) {
        LOG_INFO("Redirecting DB: %s", filename);
        // We still need to open a REAL sqlite handle for fallback operations or valid pointer return
        // IF we return NULL, other code might crash.
        // But if we return a pointer to our pg_conn struct cast to sqlite3*, it will crash if used with real sqlite functions.
        // Currently our strategy is: Open Real SQLite, but attach pg_conn metadata.
    }
    
    static int (*orig_open_v2)(const char*, sqlite3**, int, const char*) = NULL;
    if (!orig_open_v2) {
        orig_open_v2 = (int(*)(const char*, sqlite3**, int, const char*)) resolve_sqlite_symbol("sqlite3_open_v2");
    }
    
    if (!orig_open_v2) return SQLITE_ERROR;

    int rc = orig_open_v2(filename, ppDb, flags, zVfs);
    
    if (rc == SQLITE_OK && should_redirect(filename)) {
        sqlite3 *db = *ppDb;
        pg_connection_t *conn = calloc(1, sizeof(pg_connection_t));
        if (conn) {
            strncpy(conn->db_path, filename ? filename : "", sizeof(conn->db_path) - 1);
            conn->is_pg_active = 1;
            pthread_mutex_init(&conn->mutex, NULL);
            if (ensure_pg_connection(conn)) {
                pg_register_connection(db, conn);
            } else {
                LOG_ERROR("Failed to connect PG for %s", filename);
                free(conn);
            }
        }
    }
    return rc;
}

static int my_sqlite3_open(const char *filename, sqlite3 **ppDb) {
    return my_sqlite3_open_v2(filename, ppDb, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
}

static int my_sqlite3_close(sqlite3 *db) {
    LOG_INFO("CLOSE db=%p", db);
    pg_connection_t *conn = find_pg_connection(db);
    if (conn) {
        pg_unregister_connection(db);
        pg_close(conn);
    }
    
    static int (*orig)(sqlite3*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*)) resolve_sqlite_symbol("sqlite3_close");
    return orig ? orig(db) : SQLITE_OK;
}

static int my_sqlite3_close_v2(sqlite3 *db) {
    LOG_INFO("CLOSE_V2 db=%p", db);
    pg_connection_t *conn = find_pg_connection(db);
    if (conn) {
        pg_unregister_connection(db);
        pg_close(conn);
    }
    
    static int (*orig)(sqlite3*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*)) resolve_sqlite_symbol("sqlite3_close_v2");
    return orig ? orig(db) : SQLITE_OK;
}

static int my_sqlite3_prepare_v2(sqlite3 *db, const char *zSql, int nByte, sqlite3_stmt **ppStmt, const char **pzTail) {
    pg_connection_t *conn = find_pg_connection(db);
    
    // ... PG Logic (kept same) ...
     if (conn && conn->is_pg_active) {
        if (should_skip_sql(zSql)) {
            LOG_INFO("SKIPPING (SQLite fallback): %.50s", zSql);
        } else {
            // ... PG Prepare ...
             char *sql_str = NULL;
            if (nByte < 0) sql_str = strdup(zSql);
            else { sql_str = malloc(nByte + 1); memcpy(sql_str, zSql, nByte); sql_str[nByte] = '\0'; }
            
            pg_stmt_t *stmt = pg_prepare(conn, sql_str, NULL);
            free(sql_str);
            if (stmt) {
                *ppStmt = (sqlite3_stmt*)stmt;
                if (pzTail) *pzTail = zSql + strlen(zSql);
                LOG_INFO("PREPARE success: pg_stmt=%p", stmt);
                return SQLITE_OK;
            }
            return SQLITE_ERROR;
        }
    }
    
    // Call original
    static int (*orig_prepare_v2)(sqlite3*, const char*, int, sqlite3_stmt**, const char**) = NULL;
    if (!orig_prepare_v2) {
         orig_prepare_v2 = (int(*)(sqlite3*, const char*, int, sqlite3_stmt**, const char**)) resolve_sqlite_symbol("sqlite3_prepare_v2");
    }
    int rc = orig_prepare_v2 ? orig_prepare_v2(db, zSql, nByte, ppStmt, pzTail) : SQLITE_ERROR;
    return rc;
}

static int my_sqlite3_prepare(sqlite3 *db, const char *zSql, int nByte, sqlite3_stmt **ppStmt, const char **pzTail) {
    return my_sqlite3_prepare_v2(db, zSql, nByte, ppStmt, pzTail);
}

// Stubs
static int my_sqlite3_prepare16_v2(sqlite3 *db, const void *zSql, int nByte, sqlite3_stmt **ppStmt, const void **pzTail) {
    LOG_INFO("PREPARE16 call");
    (void)db; (void)zSql; (void)nByte; (void)ppStmt; (void)pzTail;
    return SQLITE_ERROR;
}

static int my_sqlite3_step(sqlite3_stmt *pStmt) {
    // Check if it's OUR statement
    if (is_pg_stmt((pg_stmt_t*)pStmt)) {
        LOG_INFO("STEP pg_stmt=%p", pStmt);
        return pg_step((pg_stmt_t*)pStmt);
    }
    
    LOG_INFO("STEP fallback stmt=%p", pStmt);
    static int (*orig_step)(sqlite3_stmt*) = NULL;
    if (!orig_step) orig_step = (int(*)(sqlite3_stmt*)) resolve_sqlite_symbol("sqlite3_step");
    return orig_step ? orig_step(pStmt) : SQLITE_ERROR;
}

static int my_sqlite3_finalize(sqlite3_stmt *pStmt) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) {
        LOG_INFO("FINALIZE pg_stmt=%p", pStmt);
        return pg_finalize((pg_stmt_t*)pStmt);
    }
    LOG_INFO("FINALIZE fallback stmt=%p", pStmt);
    static int (*orig_finalize)(sqlite3_stmt*) = NULL;
    if (!orig_finalize) orig_finalize = (int(*)(sqlite3_stmt*)) resolve_sqlite_symbol("sqlite3_finalize");
    return orig_finalize ? orig_finalize(pStmt) : SQLITE_OK;
}

static int my_sqlite3_reset(sqlite3_stmt *pStmt) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) {
        return pg_reset((pg_stmt_t*)pStmt);
    }
    static int (*orig_reset)(sqlite3_stmt*) = NULL;
    if (!orig_reset) orig_reset = (int(*)(sqlite3_stmt*)) resolve_sqlite_symbol("sqlite3_reset");
    return orig_reset ? orig_reset(pStmt) : SQLITE_OK;
}

static int my_sqlite3_clear_bindings(sqlite3_stmt *pStmt) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return SQLITE_OK;
    static int (*orig_cb)(sqlite3_stmt*) = NULL;
    if (!orig_cb) orig_cb = dlsym(RTLD_NEXT, "sqlite3_clear_bindings");
    return orig_cb ? orig_cb(pStmt) : SQLITE_OK;
}

// Bindings
// ... (I will implement a macro to generate these to save space/risk)

static int my_sqlite3_bind_int(sqlite3_stmt *pStmt, int idx, int val) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_bind_int((pg_stmt_t*)pStmt, idx, val);
    static int (*orig)(sqlite3_stmt*, int, int) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_bind_int");
    return orig ? orig(pStmt, idx, val) : SQLITE_ERROR;
}

static int my_sqlite3_bind_int64(sqlite3_stmt *pStmt, int idx, sqlite3_int64 val) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_bind_int64((pg_stmt_t*)pStmt, idx, val);
    static int (*orig)(sqlite3_stmt*, int, sqlite3_int64) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_bind_int64");
    return orig ? orig(pStmt, idx, val) : SQLITE_ERROR;
}

static int my_sqlite3_bind_double(sqlite3_stmt *pStmt, int idx, double val) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_bind_double((pg_stmt_t*)pStmt, idx, val);
    static int (*orig)(sqlite3_stmt*, int, double) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_bind_double");
    return orig ? orig(pStmt, idx, val) : SQLITE_ERROR;
}

static int my_sqlite3_bind_text(sqlite3_stmt *pStmt, int idx, const char *val, int len, void(*destructor)(void*)) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_bind_text((pg_stmt_t*)pStmt, idx, val, len, destructor);
    static int (*orig)(sqlite3_stmt*, int, const char*, int, void(*)(void*)) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_bind_text");
    return orig ? orig(pStmt, idx, val, len, destructor) : SQLITE_ERROR;
}

static int my_sqlite3_bind_blob(sqlite3_stmt *pStmt, int idx, const void *val, int len, void(*destructor)(void*)) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_bind_blob((pg_stmt_t*)pStmt, idx, val, len, destructor);
    static int (*orig)(sqlite3_stmt*, int, const void*, int, void(*)(void*)) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_bind_blob");
    return orig ? orig(pStmt, idx, val, len, destructor) : SQLITE_ERROR;
}
// bind_null is slightly diff arg
static int my_sqlite3_bind_null(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_bind_null((pg_stmt_t*)pStmt, idx);
    static int (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_bind_null");
    return orig ? orig(pStmt, idx) : SQLITE_ERROR;
}

// Columns
static int my_sqlite3_column_count(sqlite3_stmt *pStmt) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_count((pg_stmt_t*)pStmt);
    static int (*orig)(sqlite3_stmt*) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_column_count");
    return orig ? orig(pStmt) : 0;
}

static int my_sqlite3_data_count(sqlite3_stmt *pStmt) {
    // data_count usually same as column_count for us if we have results
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_count((pg_stmt_t*)pStmt); 
    static int (*orig)(sqlite3_stmt*) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_data_count");
    return orig ? orig(pStmt) : 0;
}

static int my_sqlite3_column_type(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_type((pg_stmt_t*)pStmt, idx);
    static int (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_column_type");
    return orig ? orig(pStmt, idx) : 0;
}

static int my_sqlite3_column_int(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_int((pg_stmt_t*)pStmt, idx);
    static int (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_column_int");
    return orig ? orig(pStmt, idx) : 0;
}

static sqlite3_int64 my_sqlite3_column_int64(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_int64((pg_stmt_t*)pStmt, idx);
    static sqlite3_int64 (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_column_int64");
    return orig ? orig(pStmt, idx) : 0;
}

static double my_sqlite3_column_double(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_double((pg_stmt_t*)pStmt, idx);
    static double (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_column_double");
    return orig ? orig(pStmt, idx) : 0.0;
}

static const unsigned char* my_sqlite3_column_text(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_text((pg_stmt_t*)pStmt, idx);
    static const unsigned char* (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_column_text");
    return orig ? orig(pStmt, idx) : NULL;
}

static const void* my_sqlite3_column_blob(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_blob((pg_stmt_t*)pStmt, idx);
    static const void* (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_column_blob");
    return orig ? orig(pStmt, idx) : NULL;
}

static int my_sqlite3_column_bytes(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_bytes((pg_stmt_t*)pStmt, idx);
    static int (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_column_bytes");
    return orig ? orig(pStmt, idx) : 0;
}

static sqlite3_value* my_sqlite3_column_value(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_get_column_value((pg_stmt_t*)pStmt, idx);
    static sqlite3_value* (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_column_value");
    return orig ? orig(pStmt, idx) : NULL;
}

// Changes
static int my_sqlite3_changes(sqlite3 *db) {
    pg_connection_t *conn = find_pg_connection(db);
    if (conn) return conn->last_changes;
    static int (*orig)(sqlite3*) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_changes");
    return orig ? orig(db) : 0;
}

static sqlite3_int64 my_sqlite3_last_insert_rowid(sqlite3 *db) {
    pg_connection_t *conn = find_pg_connection(db);
    if (conn) return conn->last_insert_rowid;
    static sqlite3_int64 (*orig)(sqlite3*) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_last_insert_rowid");
    return orig ? orig(db) : 0;
}

static int my_sqlite3_exec(sqlite3 *db, const char *sql, int (*callback)(void*,int,char**,char**), void *arg, char **errmsg) {
    pg_connection_t *conn = find_pg_connection(db);
    if (conn && conn->is_pg_active) {
        if (should_skip_sql(sql)) {
             LOG_INFO("SKIPPING (SQLite fallback exec): %.50s", sql);
             // Fallthrough to orig
        } else {
             pg_stmt_t *stmt = pg_prepare(conn, sql, NULL);
             if (!stmt) { if(errmsg) *errmsg=strdup("Fail"); return SQLITE_ERROR; }
             while(pg_step(stmt)==SQLITE_ROW);
             pg_finalize(stmt);
             return SQLITE_OK;
        }
    }
    static int (*orig)(sqlite3*, const char*, int(*)(void*,int,char**,char**), void*, char**) = NULL;
    if (!orig) orig = dlsym(RTLD_NEXT, "sqlite3_exec");
    return orig ? orig(db, sql, callback, arg, errmsg) : SQLITE_ERROR;
}

// ... other missing functions ...
// For now, I'll rely on the default behavior for un-intercepted functions being "safe" because we use a REAL sqlite handle.
// EXCEPT for functions that take sqlite3_stmt*.
// If I pass pg_stmt_t* to a REAL function, it crashes.
// So I MUST intercept ALL functions taking sqlite3_stmt* that Plex uses.
// I listed most.

// DYLD Interpose Definitions
DYLD_INTERPOSE(my_sqlite3_open, sqlite3_open)
DYLD_INTERPOSE(my_sqlite3_open_v2, sqlite3_open_v2)
DYLD_INTERPOSE(my_sqlite3_close, sqlite3_close)
DYLD_INTERPOSE(my_sqlite3_close_v2, sqlite3_close_v2)
DYLD_INTERPOSE(my_sqlite3_prepare_v2, sqlite3_prepare_v2)
DYLD_INTERPOSE(my_sqlite3_prepare16_v2, sqlite3_prepare16_v2)
DYLD_INTERPOSE(my_sqlite3_prepare, sqlite3_prepare)
DYLD_INTERPOSE(my_sqlite3_bind_int, sqlite3_bind_int)
DYLD_INTERPOSE(my_sqlite3_bind_int64, sqlite3_bind_int64)
DYLD_INTERPOSE(my_sqlite3_bind_double, sqlite3_bind_double)
DYLD_INTERPOSE(my_sqlite3_bind_text, sqlite3_bind_text)
DYLD_INTERPOSE(my_sqlite3_bind_blob, sqlite3_bind_blob)
DYLD_INTERPOSE(my_sqlite3_bind_null, sqlite3_bind_null)
DYLD_INTERPOSE(my_sqlite3_step, sqlite3_step)
DYLD_INTERPOSE(my_sqlite3_reset, sqlite3_reset)
DYLD_INTERPOSE(my_sqlite3_finalize, sqlite3_finalize)

DYLD_INTERPOSE(my_sqlite3_clear_bindings, sqlite3_clear_bindings)
DYLD_INTERPOSE(my_sqlite3_column_count, sqlite3_column_count)
DYLD_INTERPOSE(my_sqlite3_column_type, sqlite3_column_type)
DYLD_INTERPOSE(my_sqlite3_column_int, sqlite3_column_int)
DYLD_INTERPOSE(my_sqlite3_column_int64, sqlite3_column_int64)
DYLD_INTERPOSE(my_sqlite3_column_double, sqlite3_column_double)
DYLD_INTERPOSE(my_sqlite3_column_text, sqlite3_column_text)
DYLD_INTERPOSE(my_sqlite3_column_blob, sqlite3_column_blob)
DYLD_INTERPOSE(my_sqlite3_column_bytes, sqlite3_column_bytes)
DYLD_INTERPOSE(my_sqlite3_column_value, sqlite3_column_value)
DYLD_INTERPOSE(my_sqlite3_data_count, sqlite3_data_count)
DYLD_INTERPOSE(my_sqlite3_changes, sqlite3_changes)
DYLD_INTERPOSE(my_sqlite3_last_insert_rowid, sqlite3_last_insert_rowid)
DYLD_INTERPOSE(my_sqlite3_exec, sqlite3_exec)

// Standard Interpose for column_name
static const char* my_sqlite3_column_name(sqlite3_stmt *pStmt, int idx) {
    if (is_pg_stmt((pg_stmt_t*)pStmt)) return pg_column_name((pg_stmt_t*)pStmt, idx);
    static const char* (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = (const char*(*)(sqlite3_stmt*, int)) resolve_sqlite_symbol("sqlite3_column_name");
    return orig ? orig(pStmt, idx) : NULL;
}

static const void* my_sqlite3_column_name16(sqlite3_stmt *pStmt, int idx) {
    // We don't support 16-bit names in PG shim yet, just return NULL or fallback
    static const void* (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = (const void*(*)(sqlite3_stmt*, int)) resolve_sqlite_symbol("sqlite3_column_name16");
    return orig ? orig(pStmt, idx) : NULL;
}

DYLD_INTERPOSE(my_sqlite3_column_name, sqlite3_column_name)
DYLD_INTERPOSE(my_sqlite3_column_name16, sqlite3_column_name16)

// Probes
static int my_sqlite3_table_column_metadata(sqlite3 *db, const char *zDbName, const char *zTableName, const char *zColumnName, const char **pzDataType, const char **pzCollSeq, int *pNotNull, int *pPrimaryKey, int *pAutoinc) {
    LOG_INFO("PROBE: sqlite3_table_column_metadata table=%s col=%s", zTableName, zColumnName);
    static int (*orig)(sqlite3*, const char*, const char*, const char*, const char**, const char**, int*, int*, int*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*, const char*, const char*, const char*, const char**, const char**, int*, int*, int*)) resolve_sqlite_symbol("sqlite3_table_column_metadata");
    return orig ? orig(db, zDbName, zTableName, zColumnName, pzDataType, pzCollSeq, pNotNull, pPrimaryKey, pAutoinc) : SQLITE_ERROR;
}

static int my_sqlite3_prepare_v3(sqlite3 *db, const char *zSql, int nByte, unsigned int prepFlags, sqlite3_stmt **ppStmt, const char **pzTail) {
    LOG_INFO("PROBE: sqlite3_prepare_v3 sql=%s", zSql);
    static int (*orig)(sqlite3*, const char*, int, unsigned int, sqlite3_stmt**, const char**) = NULL;
    if (!orig) orig = (int(*)(sqlite3*, const char*, int, unsigned int, sqlite3_stmt**, const char**)) resolve_sqlite_symbol("sqlite3_prepare_v3");
    return orig ? orig(db, zSql, nByte, prepFlags, ppStmt, pzTail) : SQLITE_ERROR;
}

static int my_sqlite3_wal_checkpoint(sqlite3 *db, const char *zDb) {
    LOG_INFO("PROBE: sqlite3_wal_checkpoint db=%p zDb=%s", db, zDb);
    static int (*orig)(sqlite3*, const char*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*, const char*)) resolve_sqlite_symbol("sqlite3_wal_checkpoint");
    return orig ? orig(db, zDb) : SQLITE_ERROR;
}

static int my_sqlite3_wal_checkpoint_v2(sqlite3 *db, const char *zDb, int eMode, int *pnLog, int *pnCkpt) {
    LOG_INFO("PROBE: sqlite3_wal_checkpoint_v2 db=%p zDb=%s mode=%d", db, zDb, eMode);
    static int (*orig)(sqlite3*, const char*, int, int*, int*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*, const char*, int, int*, int*)) resolve_sqlite_symbol("sqlite3_wal_checkpoint_v2");
    return orig ? orig(db, zDb, eMode, pnLog, pnCkpt) : SQLITE_ERROR;
}

DYLD_INTERPOSE(my_sqlite3_wal_checkpoint, sqlite3_wal_checkpoint)
DYLD_INTERPOSE(my_sqlite3_wal_checkpoint_v2, sqlite3_wal_checkpoint_v2)

DYLD_INTERPOSE(my_sqlite3_table_column_metadata, sqlite3_table_column_metadata)
DYLD_INTERPOSE(my_sqlite3_prepare_v3, sqlite3_prepare_v3)

__attribute__((constructor))
static void shim_init(void) {
    fprintf(stderr, "!!!!! SHIM INIT RUNNING !!!!!\n");
    init_logging();
    load_config();
    // Pre-load bundled sqlite handle to be safe?
    get_bundled_sqlite_handle();
    LOG_INFO("Shim initialized.");
}

__attribute__((destructor))
static void shim_cleanup(void) {
    LOG_INFO("Shim cleanup.");
}

// ============================================================================
// Missing Interpositions for libpython compatibility
// ============================================================================

static int my_sqlite3_enable_shared_cache(int enable) {
    static int (*orig)(int) = NULL;
    if (!orig) orig = (int(*)(int)) resolve_sqlite_symbol("sqlite3_enable_shared_cache");
    return orig ? orig(enable) : SQLITE_ERROR;
}

static void my_sqlite3_interrupt(sqlite3 *db) {
    static void (*orig)(sqlite3*) = NULL;
    if (!orig) orig = (void(*)(sqlite3*)) resolve_sqlite_symbol("sqlite3_interrupt");
    if (orig) orig(db);
}

static int my_sqlite3_busy_timeout(sqlite3 *db, int ms) {
    static int (*orig)(sqlite3*, int) = NULL;
    if (!orig) orig = (int(*)(sqlite3*, int)) resolve_sqlite_symbol("sqlite3_busy_timeout");
    return orig ? orig(db, ms) : SQLITE_ERROR;
}

static int my_sqlite3_complete(const char *sql) {
    static int (*orig)(const char*) = NULL;
    if (!orig) orig = (int(*)(const char*)) resolve_sqlite_symbol("sqlite3_complete");
    return orig ? orig(sql) : SQLITE_NOMEM;
}

static int my_sqlite3_create_collation(sqlite3* db, const char* zName, int eTextRep, void* pArg, int(*xCompare)(void*,int,const void*,int,const void*)) {
    static int (*orig)(sqlite3*, const char*, int, void*, int(*)(void*,int,const void*,int,const void*)) = NULL;
    if (!orig) orig = (int(*)(sqlite3*, const char*, int, void*, int(*)(void*,int,const void*,int,const void*))) resolve_sqlite_symbol("sqlite3_create_collation");
    return orig ? orig(db, zName, eTextRep, pArg, xCompare) : SQLITE_ERROR;
}

static int my_sqlite3_create_function(sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep, void *pApp, void (*xFunc)(sqlite3_context*,int,sqlite3_value**), void (*xStep)(sqlite3_context*,int,sqlite3_value**), void (*xFinal)(sqlite3_context*)) {
    static int (*orig)(sqlite3*, const char*, int, int, void*, void*, void*, void*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*, const char*, int, int, void*, void*, void*, void*)) resolve_sqlite_symbol("sqlite3_create_function");
    return orig ? orig(db, zFunctionName, nArg, eTextRep, pApp, xFunc, xStep, xFinal) : SQLITE_ERROR;
}

static int my_sqlite3_errcode(sqlite3 *db) {
    static int (*orig)(sqlite3*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*)) resolve_sqlite_symbol("sqlite3_errcode");
    return orig ? orig(db) : SQLITE_OK;
}

static const char *my_sqlite3_errmsg(sqlite3 *db) {
    static const char* (*orig)(sqlite3*) = NULL;
    if (!orig) orig = (const char*(*)(sqlite3*)) resolve_sqlite_symbol("sqlite3_errmsg");
    return orig ? orig(db) : "Shim Error";
}

static int my_sqlite3_get_autocommit(sqlite3 *db) {
    static int (*orig)(sqlite3*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*)) resolve_sqlite_symbol("sqlite3_get_autocommit");
    return orig ? orig(db) : 0;
}

static const char *my_sqlite3_libversion(void) {
    static const char* (*orig)(void) = NULL;
    if (!orig) orig = (const char*(*)(void)) resolve_sqlite_symbol("sqlite3_libversion");
    return orig ? orig() : "Unknown";
}

static void my_sqlite3_progress_handler(sqlite3 *db, int nOps, int (*xProgress)(void*), void *pArg) {
    static void (*orig)(sqlite3*, int, int (*)(void*), void*) = NULL;
    if (!orig) orig = (void(*)(sqlite3*, int, int (*)(void*), void*)) resolve_sqlite_symbol("sqlite3_progress_handler");
    if (orig) orig(db, nOps, xProgress, pArg);
}

static int my_sqlite3_set_authorizer(sqlite3 *db, int (*xAuth)(void*,int,const char*,const char*,const char*,const char*), void *pUserData) {
    static int (*orig)(sqlite3*, int (*)(void*,int,const char*,const char*,const char*,const char*), void*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*, int (*)(void*,int,const char*,const char*,const char*,const char*), void*)) resolve_sqlite_symbol("sqlite3_set_authorizer");
    return orig ? orig(db, xAuth, pUserData) : SQLITE_ERROR;
}

static int my_sqlite3_total_changes(sqlite3 *db) {
    static int (*orig)(sqlite3*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*)) resolve_sqlite_symbol("sqlite3_total_changes");
    return orig ? orig(db) : 0;
}

static void *my_sqlite3_user_data(sqlite3_context *ctx) {
    static void* (*orig)(sqlite3_context*) = NULL;
    if (!orig) orig = (void*(*)(sqlite3_context*)) resolve_sqlite_symbol("sqlite3_user_data");
    return orig ? orig(ctx) : NULL;
}

static void *my_sqlite3_aggregate_context(sqlite3_context *ctx, int nBytes) {
    static void* (*orig)(sqlite3_context*, int) = NULL;
    if (!orig) orig = (void*(*)(sqlite3_context*, int)) resolve_sqlite_symbol("sqlite3_aggregate_context");
    return orig ? orig(ctx, nBytes) : NULL;
}

static void my_sqlite3_result_blob(sqlite3_context *ctx, const void *data, int len, void(*destructor)(void*)) {
    static void (*orig)(sqlite3_context*, const void*, int, void(*)(void*)) = NULL;
    if (!orig) orig = (void(*)(sqlite3_context*, const void*, int, void(*)(void*))) resolve_sqlite_symbol("sqlite3_result_blob");
    if (orig) orig(ctx, data, len, destructor);
}

static void my_sqlite3_result_double(sqlite3_context *ctx, double val) {
    static void (*orig)(sqlite3_context*, double) = NULL;
    if (!orig) orig = (void(*)(sqlite3_context*, double)) resolve_sqlite_symbol("sqlite3_result_double");
    if (orig) orig(ctx, val);
}

static void my_sqlite3_result_error(sqlite3_context *ctx, const char *msg, int len) {
    static void (*orig)(sqlite3_context*, const char*, int) = NULL;
    if (!orig) orig = (void(*)(sqlite3_context*, const char*, int)) resolve_sqlite_symbol("sqlite3_result_error");
    if (orig) orig(ctx, msg, len);
}

static void my_sqlite3_result_int64(sqlite3_context *ctx, sqlite3_int64 val) {
    static void (*orig)(sqlite3_context*, sqlite3_int64) = NULL;
    if (!orig) orig = (void(*)(sqlite3_context*, sqlite3_int64)) resolve_sqlite_symbol("sqlite3_result_int64");
    if (orig) orig(ctx, val);
}

static void my_sqlite3_result_null(sqlite3_context *ctx) {
    static void (*orig)(sqlite3_context*) = NULL;
    if (!orig) orig = (void(*)(sqlite3_context*)) resolve_sqlite_symbol("sqlite3_result_null");
    if (orig) orig(ctx);
}

static void my_sqlite3_result_text(sqlite3_context *ctx, const char *str, int len, void(*destructor)(void*)) {
    static void (*orig)(sqlite3_context*, const char*, int, void(*)(void*)) = NULL;
    if (!orig) orig = (void(*)(sqlite3_context*, const char*, int, void(*)(void*))) resolve_sqlite_symbol("sqlite3_result_text");
    if (orig) orig(ctx, str, len, destructor);
}

static const void *my_sqlite3_value_blob(sqlite3_value *val) {
    static const void* (*orig)(sqlite3_value*) = NULL;
    if (!orig) orig = (const void*(*)(sqlite3_value*)) resolve_sqlite_symbol("sqlite3_value_blob");
    return orig ? orig(val) : NULL;
}

static int my_sqlite3_value_bytes(sqlite3_value *val) {
    static int (*orig)(sqlite3_value*) = NULL;
    if (!orig) orig = (int(*)(sqlite3_value*)) resolve_sqlite_symbol("sqlite3_value_bytes");
    return orig ? orig(val) : 0;
}

static double my_sqlite3_value_double(sqlite3_value *val) {
    static double (*orig)(sqlite3_value*) = NULL;
    if (!orig) orig = (double(*)(sqlite3_value*)) resolve_sqlite_symbol("sqlite3_value_double");
    return orig ? orig(val) : 0.0;
}

static sqlite3_int64 my_sqlite3_value_int64(sqlite3_value *val) {
    static sqlite3_int64 (*orig)(sqlite3_value*) = NULL;
    if (!orig) orig = (sqlite3_int64(*)(sqlite3_value*)) resolve_sqlite_symbol("sqlite3_value_int64");
    return orig ? orig(val) : 0;
}

static const unsigned char *my_sqlite3_value_text(sqlite3_value *val) {
    static const unsigned char* (*orig)(sqlite3_value*) = NULL;
    if (!orig) orig = (const unsigned char*(*)(sqlite3_value*)) resolve_sqlite_symbol("sqlite3_value_text");
    return orig ? orig(val) : NULL;
}

static int my_sqlite3_value_type(sqlite3_value *val) {
    static int (*orig)(sqlite3_value*) = NULL;
    if (!orig) orig = (int(*)(sqlite3_value*)) resolve_sqlite_symbol("sqlite3_value_type");
    return orig ? orig(val) : SQLITE_NULL;
}

static int my_sqlite3_bind_parameter_count(sqlite3_stmt *pStmt) {
    static int (*orig)(sqlite3_stmt*) = NULL;
    if (!orig) orig = (int(*)(sqlite3_stmt*)) resolve_sqlite_symbol("sqlite3_bind_parameter_count");
    return orig ? orig(pStmt) : 0;
}

static const char *my_sqlite3_bind_parameter_name(sqlite3_stmt *pStmt, int idx) {
    static const char* (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = (const char*(*)(sqlite3_stmt*, int)) resolve_sqlite_symbol("sqlite3_bind_parameter_name");
    return orig ? orig(pStmt, idx) : NULL;
}

static const char *my_sqlite3_column_decltype(sqlite3_stmt *pStmt, int idx) {
    static const char* (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = (const char*(*)(sqlite3_stmt*, int)) resolve_sqlite_symbol("sqlite3_column_decltype");
    return orig ? orig(pStmt, idx) : NULL;
}

// Interpose Definitions
DYLD_INTERPOSE(my_sqlite3_enable_shared_cache, sqlite3_enable_shared_cache)
DYLD_INTERPOSE(my_sqlite3_interrupt, sqlite3_interrupt)
DYLD_INTERPOSE(my_sqlite3_busy_timeout, sqlite3_busy_timeout)
DYLD_INTERPOSE(my_sqlite3_complete, sqlite3_complete)
DYLD_INTERPOSE(my_sqlite3_create_collation, sqlite3_create_collation)
DYLD_INTERPOSE(my_sqlite3_create_function, sqlite3_create_function)
DYLD_INTERPOSE(my_sqlite3_errcode, sqlite3_errcode)
DYLD_INTERPOSE(my_sqlite3_errmsg, sqlite3_errmsg)
DYLD_INTERPOSE(my_sqlite3_get_autocommit, sqlite3_get_autocommit)
DYLD_INTERPOSE(my_sqlite3_libversion, sqlite3_libversion)
DYLD_INTERPOSE(my_sqlite3_progress_handler, sqlite3_progress_handler)
DYLD_INTERPOSE(my_sqlite3_set_authorizer, sqlite3_set_authorizer)
DYLD_INTERPOSE(my_sqlite3_total_changes, sqlite3_total_changes)
DYLD_INTERPOSE(my_sqlite3_user_data, sqlite3_user_data)
DYLD_INTERPOSE(my_sqlite3_aggregate_context, sqlite3_aggregate_context)
DYLD_INTERPOSE(my_sqlite3_result_blob, sqlite3_result_blob)
DYLD_INTERPOSE(my_sqlite3_result_double, sqlite3_result_double)
DYLD_INTERPOSE(my_sqlite3_result_error, sqlite3_result_error)
DYLD_INTERPOSE(my_sqlite3_result_int64, sqlite3_result_int64)
DYLD_INTERPOSE(my_sqlite3_result_null, sqlite3_result_null)
DYLD_INTERPOSE(my_sqlite3_result_text, sqlite3_result_text)
DYLD_INTERPOSE(my_sqlite3_value_blob, sqlite3_value_blob)
DYLD_INTERPOSE(my_sqlite3_value_bytes, sqlite3_value_bytes)
DYLD_INTERPOSE(my_sqlite3_value_double, sqlite3_value_double)
DYLD_INTERPOSE(my_sqlite3_value_int64, sqlite3_value_int64)
DYLD_INTERPOSE(my_sqlite3_value_text, sqlite3_value_text)
DYLD_INTERPOSE(my_sqlite3_value_type, sqlite3_value_type)
DYLD_INTERPOSE(my_sqlite3_bind_parameter_count, sqlite3_bind_parameter_count)
DYLD_INTERPOSE(my_sqlite3_bind_parameter_name, sqlite3_bind_parameter_name)
DYLD_INTERPOSE(my_sqlite3_column_decltype, sqlite3_column_decltype)
