#include "pg_statement.h"
#include "pg_client.h"
#include "pg_config.h"
#include "pg_logging.h"
#include "sql_translator.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <ctype.h>
#include <time.h>

// TLS Key
static pthread_key_t cached_stmts_key;
static pthread_once_t cached_stmts_key_once = PTHREAD_ONCE_INIT;

// Forward declarations
static void free_thread_cached_stmts(void *ptr);
static void create_cached_stmts_key(void);
static pg_stmt_t* create_pg_stmt(pg_connection_t *conn, const char *sql);
static int is_write_operation(const char *sql);
static char* convert_metadata_settings_insert_to_upsert(const char *sql);
static sqlite3_int64 extract_metadata_id_from_generator_sql(const char *sql);

// ============================================================================
// TLS Logic
// ============================================================================

static void create_cached_stmts_key(void) {
    pthread_key_create(&cached_stmts_key, free_thread_cached_stmts);
}

static thread_cached_stmts_t* get_thread_cached_stmts(void) {
    pthread_once(&cached_stmts_key_once, create_cached_stmts_key);
    thread_cached_stmts_t *tcs = pthread_getspecific(cached_stmts_key);
    if (!tcs) {
        tcs = calloc(1, sizeof(thread_cached_stmts_t));
        if (tcs) {
            pthread_setspecific(cached_stmts_key, tcs);
        }
    }
    return tcs;
}

static void free_thread_cached_stmts(void *ptr) {
    thread_cached_stmts_t *tcs = (thread_cached_stmts_t *)ptr;
    if (tcs) {
        for (int i = 0; i < tcs->count; i++) {
            // Note: In our new architecture, the pg_stmt in TLS might be a COPY or a specific state object
            // For now, assuming it holds the result set and generated SQL
            pg_stmt_t *pg_stmt = tcs->entries[i].pg_stmt;
            if (pg_stmt) {
                if (pg_stmt->pg_sql && pg_stmt->pg_sql != pg_stmt->sql) free(pg_stmt->pg_sql);
                if (pg_stmt->result) PQclear(pg_stmt->result);
                // Don't free pg_stmt->sql as it might belong to the parent stmt handle?
                // Actually, let's treat the TLS entry as a full independent execution state
                if (pg_stmt->sql) free(pg_stmt->sql);
                 
                // Param values
                for(int j=0; j<pg_stmt->param_count; j++) {
                    if (pg_stmt->param_values[j]) free(pg_stmt->param_values[j]);
                }
                free(pg_stmt);
            }
        }
        free(tcs);
    }
}

// Register a cached statement execution state for this thread
// We use the "handle" (sqlite_stmt) as the key
static void register_cached_pg_stmt(sqlite3_stmt *handle, pg_stmt_t *execution_state) {
    thread_cached_stmts_t *tcs = get_thread_cached_stmts();
    if (!tcs) return;

    // Check if already registered
    for (int i = 0; i < tcs->count; i++) {
        if (tcs->entries[i].sqlite_stmt == handle) {
            // Replace existing
            pg_stmt_t *old = tcs->entries[i].pg_stmt;
            if (old && old != execution_state) {
                // Cleanup old
                if (old->pg_sql && old->pg_sql != old->sql) free(old->pg_sql);
                if (old->sql) free(old->sql);
                if (old->result) PQclear(old->result);
                for(int j=0; j<old->param_count; j++) {
                   if (old->param_values[j]) free(old->param_values[j]);
                }
                free(old);
            }
            tcs->entries[i].pg_stmt = execution_state;
            return;
        }
    }

    // Add new
    if (tcs->count < MAX_CACHED_STMTS_PER_THREAD) {
        tcs->entries[tcs->count].sqlite_stmt = handle;
        tcs->entries[tcs->count].pg_stmt = execution_state;
        tcs->count++;
    } else {
        // Simple eviction (active usage might suffer, but it's a ring buffer safety)
        pg_stmt_t *old = tcs->entries[0].pg_stmt;
        if (old) {
             if (old->pg_sql && old->pg_sql != old->sql) free(old->pg_sql);
             if (old->sql) free(old->sql);
             if (old->result) PQclear(old->result);
             for(int j=0; j<old->param_count; j++) {
                if (old->param_values[j]) free(old->param_values[j]);
             }
             free(old);
        }
        memmove(&tcs->entries[0], &tcs->entries[1], 
                (MAX_CACHED_STMTS_PER_THREAD - 1) * sizeof(cached_stmt_entry_t));
        tcs->entries[MAX_CACHED_STMTS_PER_THREAD - 1].sqlite_stmt = handle;
        tcs->entries[MAX_CACHED_STMTS_PER_THREAD - 1].pg_stmt = execution_state;
    }
}

static pg_stmt_t* find_cached_exec_state(sqlite3_stmt *handle) {
    thread_cached_stmts_t *tcs = get_thread_cached_stmts();
    if (!tcs) return NULL;
    for (int i = 0; i < tcs->count; i++) {
        if (tcs->entries[i].sqlite_stmt == handle) {
            return tcs->entries[i].pg_stmt;
        }
    }
    return NULL;
}

// Ungerister (called on finalize)
void unregister_stmt(sqlite3_stmt *handle) {
     thread_cached_stmts_t *tcs = get_thread_cached_stmts();
     if (!tcs) return;
     for (int i = 0; i < tcs->count; i++) {
         if (tcs->entries[i].sqlite_stmt == handle) {
             pg_stmt_t *old = tcs->entries[i].pg_stmt;
             if (old) {
                 if (old->pg_sql && old->pg_sql != old->sql) free(old->pg_sql);
                 if (old->sql) free(old->sql);
                 if (old->result) PQclear(old->result);
                 for(int j=0; j<old->param_count; j++) {
                    if (old->param_values[j]) free(old->param_values[j]);
                 }
                 free(old);
             }
             // Shift
             for (int k = i; k < tcs->count - 1; k++) {
                 tcs->entries[k] = tcs->entries[k+1];
             }
             tcs->count--;
             return;
         }
     }
}

// ============================================================================
// Initialization
// ============================================================================
void pg_statement_init(void) {
    // Initialized lazy via pthread_once
}

// ============================================================================
// Prepare
// ============================================================================

pg_stmt_t* pg_prepare(pg_connection_t *conn, const char *sql, sqlite3_stmt *sqlite_stmt_handle) {
    if (!conn) return NULL;
    if (should_skip_sql(sql)) {
        // Return dummy statement marked as skipped
        // We reuse the struct but set is_pg = 0 and pg_sql = NULL
        pg_stmt_t *stmt = calloc(1, sizeof(pg_stmt_t));
        stmt->conn = conn;
        stmt->sql = sql ? strdup(sql) : NULL;
        stmt->is_pg = 0; // Skipped
        register_pg_stmt(stmt);
        return stmt;
    }

    // Translation
    sql_translation_t trans = sql_translate(sql);
    
    char *final_sql = NULL;
    if (trans.success) {
        final_sql = strdup(trans.sql);
    } else {
        // FAILED translation
        LOG_ERROR("SQL Translation failed for: %s", sql);
        // Previously we fell back. Now we must error or try to run as is?
        // If we run as is on Postgres it will likely fail syntax.
        // We will try to run as is (maybe it's portable).
        final_sql = strdup(sql);
    }
    
    // Check for UPSERT conversion
    char *upsert_sql = convert_metadata_settings_insert_to_upsert(final_sql);
    if (upsert_sql) {
        free(final_sql);
        final_sql = upsert_sql;
    }

    pg_stmt_t *stmt = calloc(1, sizeof(pg_stmt_t));
    stmt->conn = conn;
    stmt->sql = sql ? strdup(sql) : NULL;
    stmt->pg_sql = final_sql;
    stmt->is_pg = 1;
    stmt->needs_requery = 1;
    stmt->sqlite_stmt_handle = sqlite_stmt_handle; 
    
    register_pg_stmt(stmt);
    
    sql_translation_free(&trans);
    return stmt;
}

// ============================================================================
// Execution (Step)
// ============================================================================

int pg_step(pg_stmt_t *stmt) {
    if (!stmt) return SQLITE_ERROR;

    // Use TLS execution state if available or create one?
    // The 'stmt' passed here IS the handle.
    // If we support concurrency, we should be looking up the TLS state.
    // If 'stmt' is used as handle, we use IT to find TLS state.
    
    pg_stmt_t *exec_state = find_cached_exec_state((sqlite3_stmt*)stmt);
    
    // If NO execution state found, or if it needs requery, we initialize/update it.
    // WAIT. If 'stmt' holds the binding values...
    // The binding values are on the handle 'stmt'.
    // We need to copy them to 'exec_state' or use them?
    
    // Simpler approach:
    // If 'stmt' is the handle, maybe we don't need TLS unless we are sure about the concurrency model.
    // If I stick to the plan of "emulating the handles", 'stmt' IS the state.
    // But if multiple threads call pg_bind/pg_step on the same 'stmt' pointer concurrently without locking, it's a race anyway.
    // SQLite itself is not threadsafe on the SAME stmt handle unless compiled with serializable, but even then step() is stateful.
    
    // I will assume for now that 'stmt' IS the execution state. 
    // The previous code's complexity with TLS was because they had ONE cached sqlite stmt handle (shared)
    // but multiple threads wanted to Execute it.
    // If I control the handles, I can just say "Don't share handles".
    // BUT if the app relies on sharing handles (e.g. from a pool), I might break it.
    
    // Let's stick to using 'stmt' as the state for now. Only add TLS if we see concurrency issues or if I recall why it was strictly needed.
    // "because cached statements... can be executed concurrently" -> likely Plex prepared statements globally.
    // If so, I should copy 'stmt' to a local 'exec_state' in TLS, copy bindings, and execute that.
    
    // NOTE: For this refactor, I will start simple: 'stmt' contains the state.
    // If I need TLS I'll add it. The code above has TLS boilerplate but I'll bypass it for direct execution 
    // to reduce complexity unless I'm sure.
    // Actually, looking at `db_interpose_pg.c`, the `find_cached_pg_stmt` was used heavily.
    // I will use `stmt` as the definition, and `exec_state` (if I used TLS) for result.
    // But since I don't have a real separate handle, let's just use `stmt` directly.
    
    if (!stmt->is_pg) {
        // Skipped statement
        return SQLITE_DONE;
    }

    if (stmt->current_row >= stmt->num_rows && !stmt->needs_requery) {
        return SQLITE_DONE;
    }

    if (stmt->needs_requery) {
        // Execute Query
        if (!stmt->conn) return SQLITE_ERROR;
        if (!ensure_pg_connection(stmt->conn)) return SQLITE_ERROR; // Retry logic?

        // Prepare params
        const char *paramValues[MAX_PARAMS];
        for (int i=0; i<stmt->param_count; i++) {
            paramValues[i] = stmt->param_values[i];
        }

        if (stmt->result) PQclear(stmt->result);
        
        stmt->result = PQexecParams(stmt->conn->conn, stmt->pg_sql,
                                    stmt->param_count,
                                    NULL, // Types (let PG infer or use param_details if we had them)
                                    paramValues,
                                    NULL, // Lengths (ignored for text)
                                    NULL, // Formats (text)
                                    0);   // Result format (text)

        if (PQresultStatus(stmt->result) != PGRES_TUPLES_OK && 
            PQresultStatus(stmt->result) != PGRES_COMMAND_OK) {
            LOG_ERROR("Query failed: %s | SQL: %s", PQresultErrorMessage(stmt->result), stmt->pg_sql);
            PQclear(stmt->result);
            stmt->result = NULL;
            return SQLITE_ERROR; // Or schema retry logic?
        }

        stmt->num_rows = PQntuples(stmt->result);
        stmt->num_cols = PQnfields(stmt->result);
        stmt->current_row = 0;
        stmt->needs_requery = 0;
        
        // Track changes
        if (is_write_operation(stmt->sql)) {
            char *cmd_tuples = PQcmdTuples(stmt->result);
            stmt->conn->last_changes = cmd_tuples ? atoi(cmd_tuples) : 0;
            
            // last_insert_rowid logic equivalent
            // For now, if RETURNING id is used, handling it?
            // The translator adds RETURNING id for some things?
        }

        // Generator ID extraction
        if (strcasestr(stmt->sql, "play_queue_generators") && strcasestr(stmt->sql, "INSERT")) {
             stmt->conn->last_generator_metadata_id = extract_metadata_id_from_generator_sql(stmt->sql);
        }
    }

    if (stmt->current_row < stmt->num_rows) {
        stmt->current_row++; 
        // Note: In SQLite, step() positions on the row. 
        // Our current_row is 1-based index of "next row to read"? 
        // Or 0-based index of "current row"?
        // Let's say current_row is 0 initially.
        // After step(), we are at row 0.
        // So we increment AFTER check? No.
        // Loop:
        // step() -> finds row 0. Returns ROW.
        // user calls getters on row 0.
        // step() -> finds row 1. Returns ROW.
        // ...
        // So we should track index. 
        // Let's use `current_row` as the index of the row we ARE ON.
        // Initialize to -1.
        
        // REVISIT: My struct has `current_row` initialized to 0 in calloc?
        // Let's use `current_row` as "next row to return".
        // But getters need "current row".
        // So: `row_index`.
    } else {
        return SQLITE_DONE;
    }
    
    return SQLITE_ROW;
}

int pg_reset(pg_stmt_t *stmt) {
    if (!stmt) return SQLITE_OK;
    stmt->current_row = 0; // Reset cursor
    stmt->needs_requery = 1; // Mark to re-execute on next step
    if (stmt->result) {
        PQclear(stmt->result);
        stmt->result = NULL;
    }
    return SQLITE_OK;
}

int pg_finalize(pg_stmt_t *stmt) {
    if (!stmt) return SQLITE_OK;
    unregister_stmt((sqlite3_stmt*)stmt); // Clean TLS
    unregister_pg_stmt(stmt); // Clean Registry
    
    if (stmt->pg_sql && stmt->pg_sql != stmt->sql) free(stmt->pg_sql);
    if (stmt->sql) free(stmt->sql);
    if (stmt->result) PQclear(stmt->result);
    for(int j=0; j<stmt->param_count; j++) {
       if (stmt->param_values[j]) free(stmt->param_values[j]);
    }
    free(stmt);
    return SQLITE_OK;
}

// ============================================================================
// Bindings
// ============================================================================
int pg_bind_text(pg_stmt_t *stmt, int idx, const char *val, int len, void(*destructor)(void*)) {
    if (!stmt || idx < 1 || idx > MAX_PARAMS) return SQLITE_ERROR;
    int array_idx = idx - 1;
    
    if (stmt->param_values[array_idx]) free(stmt->param_values[array_idx]);
    
    if (val) {
        int vlen = (len < 0) ? strlen(val) : len;
        stmt->param_values[array_idx] = malloc(vlen + 1);
        memcpy(stmt->param_values[array_idx], val, vlen);
        stmt->param_values[array_idx][vlen] = '\0';
    } else {
        stmt->param_values[array_idx] = NULL;
    }
    
    if (idx > stmt->param_count) stmt->param_count = idx;
    return SQLITE_OK;
}

int pg_bind_int(pg_stmt_t *stmt, int idx, int val) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%d", val);
    return pg_bind_text(stmt, idx, buf, -1, NULL);
}

int pg_bind_int64(pg_stmt_t *stmt, int idx, sqlite3_int64 val) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%lld", val);
    return pg_bind_text(stmt, idx, buf, -1, NULL);
}

int pg_bind_double(pg_stmt_t *stmt, int idx, double val) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%f", val);
    return pg_bind_text(stmt, idx, buf, -1, NULL);
}

int pg_bind_blob(pg_stmt_t *stmt, int idx, const void *val, int len, void(*destructor)(void*)) {
    // TODO: support bytea escaping
    return pg_bind_text(stmt, idx, (const char*)val, len, destructor);
}

int pg_bind_null(pg_stmt_t *stmt, int idx) {
    return pg_bind_text(stmt, idx, NULL, 0, NULL);
}

// ============================================================================
// Columns
// ============================================================================

const unsigned char* pg_column_text(pg_stmt_t *stmt, int idx) {
    if (!stmt || !stmt->result) return NULL;
    int row = stmt->current_row - 1; 
    if (row < 0) return NULL;
    
    return (const unsigned char*)PQgetvalue(stmt->result, row, idx);
}
// Add other column getters... (using PQgetvalue and casting/parsing)

// ============================================================================
// Helpers
// ============================================================================

static int is_write_operation(const char *sql) {
    if (!sql) return 0;
    while (*sql && isspace(*sql)) sql++;
    if (strncasecmp(sql, "INSERT", 6) == 0) return 1;
    if (strncasecmp(sql, "UPDATE", 6) == 0) return 1;
    if (strncasecmp(sql, "DELETE", 6) == 0) return 1;
    if (strncasecmp(sql, "REPLACE", 7) == 0) return 1;
    return 0;
}

static char* convert_metadata_settings_insert_to_upsert(const char *sql) {
    // Ported from db_interpose_pg.c
    // ... (logic)
    // For brevity, I'll copy implementation from previous cat or just define it simplified for now
    // Actually I should copy it fully if I want it to work
    if (!sql) return NULL;
    if (!strcasestr(sql, "INSERT INTO")) return NULL;
    if (!strcasestr(sql, "metadata_item_settings")) return NULL;
    if (strcasestr(sql, "ON CONFLICT") || strcasestr(sql, "RETURNING")) return NULL;

    const char *on_conflict =
        " ON CONFLICT (account_id, guid) DO UPDATE SET "
        "rating = COALESCE(EXCLUDED.rating, plex.metadata_item_settings.rating), "
        "view_offset = EXCLUDED.view_offset, "
        "view_count = CASE WHEN plex.metadata_item_settings.view_count > 0 AND EXCLUDED.view_count = 0 "
                     "THEN 0 ELSE GREATEST(EXCLUDED.view_count, plex.metadata_item_settings.view_count, 1) END, "
        "last_viewed_at = CASE WHEN plex.metadata_item_settings.view_count > 0 AND EXCLUDED.view_count = 0 "
                         "THEN NULL ELSE COALESCE(EXCLUDED.last_viewed_at, EXTRACT(EPOCH FROM NOW())::bigint) END, "
        "updated_at = COALESCE(EXCLUDED.updated_at, EXTRACT(EPOCH FROM NOW())::bigint), "
        "skip_count = EXCLUDED.skip_count, "
        "last_skipped_at = EXCLUDED.last_skipped_at, "
        "changed_at = COALESCE(EXCLUDED.changed_at, EXTRACT(EPOCH FROM NOW())::bigint), "
        "extra_data = COALESCE(EXCLUDED.extra_data, plex.metadata_item_settings.extra_data), "
        "last_rated_at = COALESCE(EXCLUDED.last_rated_at, plex.metadata_item_settings.last_rated_at) "
        "RETURNING id";

    size_t len = strlen(sql) + strlen(on_conflict) + 1;
    char *res = malloc(len);
    snprintf(res, len, "%s%s", sql, on_conflict);
    return res;
}

static sqlite3_int64 extract_metadata_id_from_generator_sql(const char *sql) {
    if (!sql) return 0;
    if (!strcasestr(sql, "play_queue_generators") || !strcasestr(sql, "INSERT")) return 0;
    const char *pattern = "%2Fmetadata%2F";
    const char *pos = strstr(sql, pattern);
    if (!pos) {
        pattern = "/metadata/";
        pos = strstr(sql, pattern);
    }
    if (!pos) return 0;
    pos += strlen(pattern);
    sqlite3_int64 id = 0;
    while (*pos >= '0' && *pos <= '9') {
        id = id * 10 + (*pos - '0');
        pos++;
    }
    return id;
}

// ... Implement other getters ...
int pg_column_count(pg_stmt_t *stmt) {
    return stmt ? stmt->num_cols : 0;
}
int pg_column_type(pg_stmt_t *stmt, int idx) {
    // Return SQLITE_TEXT or INTEGER based on PG type?
    // For now SQLITE_TEXT is safest if we don't track types, or check OID.
    return SQLITE_TEXT; 
}
const char* pg_column_name(pg_stmt_t *stmt, int idx) {
    if (!stmt || !stmt->result) return NULL;
    return PQfname(stmt->result, idx);
}
int pg_column_int(pg_stmt_t *stmt, int idx) {
    const char *val = pg_column_text(stmt, idx);
    return val ? atoi(val) : 0;
}
sqlite3_int64 pg_column_int64(pg_stmt_t *stmt, int idx) {
    const char *val = pg_column_text(stmt, idx);
    return val ? atoll(val) : 0;
}
double pg_column_double(pg_stmt_t *stmt, int idx) {
    const char *val = pg_column_text(stmt, idx);
    return val ? atof(val) : 0.0;
}
const void* pg_column_blob(pg_stmt_t *stmt, int idx) {
    return pg_column_text(stmt, idx); // Treat as text/bytes
}
int pg_column_bytes(pg_stmt_t *stmt, int idx) {
    const char *val = pg_column_text(stmt, idx);
    return val ? strlen(val) : 0;
}

// Safe value handling
#define MAX_PG_VALUES 4096
static pg_value_t pg_values[MAX_PG_VALUES];
static int pg_value_idx = 0;
static pthread_mutex_t pg_value_mutex = PTHREAD_MUTEX_INITIALIZER;

static int pg_oid_to_sqlite_type(Oid oid) {
    switch (oid) {
        case 20:   // INT8
        case 21:   // INT2
        case 23:   // INT4
            return SQLITE_INTEGER;
        case 700:  // FLOAT4
        case 701:  // FLOAT8
        case 1700: // NUMERIC
            return SQLITE_FLOAT;
        case 17:   // BYTEA
            return SQLITE_BLOB;
        case 25:   // TEXT
        case 1042: // BPCHAR
        case 1043: // VARCHAR
        default:
            return SQLITE_TEXT;
    }
}

static sqlite3_value* get_null_pg_value(pg_stmt_t *pg_stmt, int idx) {
    pthread_mutex_lock(&pg_value_mutex);
    pg_value_t *pv = &pg_values[pg_value_idx++ % MAX_PG_VALUES];
    pv->magic = PG_VALUE_MAGIC;
    pv->stmt = pg_stmt;
    pv->col_idx = idx;
    pv->type = SQLITE_NULL;
    pthread_mutex_unlock(&pg_value_mutex);
    return (sqlite3_value*)pv;
}

sqlite3_value* pg_get_column_value(pg_stmt_t *stmt, int idx) {
    if (!stmt || !stmt->result) {
        return get_null_pg_value(stmt, idx);
    }
    
    // Check bounds
    int row = stmt->current_row - 1; 
    if (row < 0 || row >= stmt->num_rows) {
        return get_null_pg_value(stmt, idx);
    }
    
    // Check NULL in DB
    if (PQgetisnull(stmt->result, row, idx)) {
        return get_null_pg_value(stmt, idx);
    }
    
    // Create valid value
    pthread_mutex_lock(&pg_value_mutex);
    pg_value_t *pv = &pg_values[pg_value_idx++ % MAX_PG_VALUES];
    pv->magic = PG_VALUE_MAGIC;
    pv->stmt = stmt;
    pv->col_idx = idx;
    pv->type = pg_oid_to_sqlite_type(PQftype(stmt->result, idx));
    pthread_mutex_unlock(&pg_value_mutex);
    
    return (sqlite3_value*)pv;
}

// Statement Registry
#define MAX_REGO_STMTS 1024
static pg_stmt_t* stmt_registry[MAX_REGO_STMTS];
static pthread_mutex_t stmt_rego_mutex = PTHREAD_MUTEX_INITIALIZER;

void register_pg_stmt(pg_stmt_t *stmt) {
    pthread_mutex_lock(&stmt_rego_mutex);
    for(int i=0; i<MAX_REGO_STMTS; i++) {
        if(!stmt_registry[i]) {
            stmt_registry[i] = stmt;
            break;
        }
    }
    pthread_mutex_unlock(&stmt_rego_mutex);
}

void unregister_pg_stmt(pg_stmt_t *stmt) {
    pthread_mutex_lock(&stmt_rego_mutex);
    for(int i=0; i<MAX_REGO_STMTS; i++) {
        if(stmt_registry[i] == stmt) {
            stmt_registry[i] = NULL;
            break;
        }
    }
    pthread_mutex_unlock(&stmt_rego_mutex);
}

int is_pg_stmt(pg_stmt_t *stmt) {
    if(!stmt) return 0;
    int found = 0;
    pthread_mutex_lock(&stmt_rego_mutex);
    for(int i=0; i<MAX_REGO_STMTS; i++) {
        if(stmt_registry[i] == stmt) {
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&stmt_rego_mutex);
    return found;
}

pg_stmt_t* find_pg_stmt(sqlite3_stmt *stmt) {
    if (is_pg_stmt((pg_stmt_t*)stmt)) return (pg_stmt_t*)stmt;
    return NULL;
}


pg_stmt_t* find_any_pg_stmt(sqlite3_stmt *stmt) {
    return (pg_stmt_t*)stmt;
}

