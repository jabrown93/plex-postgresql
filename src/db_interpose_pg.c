/*
 * Plex PostgreSQL Interposing Shim
 *
 * Uses macOS DYLD_INTERPOSE to intercept SQLite calls and redirect to PostgreSQL.
 * This approach allows the original SQLite to handle all functions we don't override.
 *
 * Build:
 *   clang -dynamiclib -o db_interpose_pg.dylib db_interpose_pg.c sql_translator.o \
 *         -I/opt/homebrew/opt/postgresql@15/include -L/opt/homebrew/opt/postgresql@15/lib \
 *         -lpq -lsqlite3 -flat_namespace
 *
 * Usage:
 *   export DYLD_INSERT_LIBRARIES=/path/to/db_interpose_pg.dylib
 *   export PLEX_PG_HOST=localhost
 *   ... other env vars
 *   open /Applications/Plex\ Media\ Server.app
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>
#include <stdarg.h>
#include <sqlite3.h>
#include <libpq-fe.h>
#include <dlfcn.h>

#include "sql_translator.h"

// DYLD interposing structure
#define DYLD_INTERPOSE(_replacement, _original) \
    __attribute__((used)) static struct { \
        const void* replacement; \
        const void* original; \
    } _interpose_##_original __attribute__((section("__DATA,__interpose"))) = { \
        (const void*)(unsigned long)&_replacement, \
        (const void*)(unsigned long)&_original \
    };

// ============================================================================
// Configuration
// ============================================================================

#define LOG_FILE "/tmp/plex_redirect_pg.log"
#define MAX_CONNECTIONS 16
#define MAX_PARAMS 64

#define ENV_PG_HOST     "PLEX_PG_HOST"
#define ENV_PG_PORT     "PLEX_PG_PORT"
#define ENV_PG_DATABASE "PLEX_PG_DATABASE"
#define ENV_PG_USER     "PLEX_PG_USER"
#define ENV_PG_PASSWORD "PLEX_PG_PASSWORD"
#define ENV_PG_SCHEMA   "PLEX_PG_SCHEMA"

static const char *REDIRECT_PATTERNS[] = {
    "com.plexapp.plugins.library.db",
    "com.plexapp.plugins.library.blobs.db",
    NULL
};

// ============================================================================
// Types
// ============================================================================

typedef struct {
    char host[256];
    int port;
    char database[128];
    char user[128];
    char password[256];
    char schema[64];
} pg_conn_config_t;

typedef struct pg_connection {
    PGconn *conn;
    sqlite3 *shadow_db;  // Real SQLite handle for fallback
    char db_path[1024];
    int is_pg_active;    // Whether we're using PostgreSQL
    int in_transaction;
    pthread_mutex_t mutex;
} pg_connection_t;

typedef struct pg_stmt {
    pg_connection_t *conn;
    sqlite3_stmt *shadow_stmt;  // Real SQLite statement for fallback
    char *sql;
    char *pg_sql;
    PGresult *result;
    int current_row;
    int num_rows;
    int num_cols;
    int is_pg;

    char *param_values[MAX_PARAMS];
    int param_lengths[MAX_PARAMS];
    int param_formats[MAX_PARAMS];
    int param_count;
} pg_stmt_t;

// ============================================================================
// Globals
// ============================================================================

static FILE *log_file = NULL;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t conn_mutex = PTHREAD_MUTEX_INITIALIZER;

static pg_conn_config_t pg_config;
static int config_loaded = 0;
static int shim_initialized = 0;

static pg_connection_t *connections[MAX_CONNECTIONS];
static int connection_count = 0;

// Statement tracking - maps sqlite3_stmt* to pg_stmt_t*
#define MAX_STATEMENTS 1024
static struct {
    sqlite3_stmt *sqlite_stmt;
    pg_stmt_t *pg_stmt;
} stmt_map[MAX_STATEMENTS];
static int stmt_count = 0;
static pthread_mutex_t stmt_mutex = PTHREAD_MUTEX_INITIALIZER;

// ============================================================================
// Logging
// ============================================================================

static void log_message(const char *fmt, ...) {
    if (!log_file) {
        log_file = fopen(LOG_FILE, "a");
        if (!log_file) return;
        setbuf(log_file, NULL);
    }

    pthread_mutex_lock(&log_mutex);

    time_t now = time(NULL);
    struct tm *tm = localtime(&now);
    fprintf(log_file, "[%04d-%02d-%02d %02d:%02d:%02d] ",
            tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
            tm->tm_hour, tm->tm_min, tm->tm_sec);

    va_list args;
    va_start(args, fmt);
    vfprintf(log_file, fmt, args);
    va_end(args);

    fprintf(log_file, "\n");

    pthread_mutex_unlock(&log_mutex);
}

// ============================================================================
// Configuration
// ============================================================================

static void load_config(void) {
    if (config_loaded) return;

    const char *val;

    val = getenv(ENV_PG_HOST);
    strncpy(pg_config.host, val ? val : "localhost", sizeof(pg_config.host) - 1);

    val = getenv(ENV_PG_PORT);
    pg_config.port = val ? atoi(val) : 5432;

    val = getenv(ENV_PG_DATABASE);
    strncpy(pg_config.database, val ? val : "plex", sizeof(pg_config.database) - 1);

    val = getenv(ENV_PG_USER);
    strncpy(pg_config.user, val ? val : "plex", sizeof(pg_config.user) - 1);

    val = getenv(ENV_PG_PASSWORD);
    strncpy(pg_config.password, val ? val : "", sizeof(pg_config.password) - 1);

    val = getenv(ENV_PG_SCHEMA);
    strncpy(pg_config.schema, val ? val : "plex", sizeof(pg_config.schema) - 1);

    config_loaded = 1;

    log_message("PostgreSQL config: %s@%s:%d/%s (schema: %s)",
                pg_config.user, pg_config.host, pg_config.port,
                pg_config.database, pg_config.schema);
}

// ============================================================================
// Helper Functions
// ============================================================================

static int should_redirect(const char *filename) {
    if (!filename) return 0;

    for (int i = 0; REDIRECT_PATTERNS[i]; i++) {
        if (strstr(filename, REDIRECT_PATTERNS[i])) {
            return 1;
        }
    }
    return 0;
}

// SQLite-specific functions/commands to skip on PostgreSQL
static const char *SQLITE_ONLY_PATTERNS[] = {
    "icu_load_collation",
    "fts3_tokenizer",    // SQLite FTS3 full-text search
    "fts4",              // SQLite FTS4
    "fts5",              // SQLite FTS5
    "SELECT load_extension",
    "VACUUM",
    "PRAGMA",
    "REINDEX",
    "ANALYZE sqlite_",
    "ATTACH DATABASE",
    "DETACH DATABASE",
    "ROLLBACK",          // Avoid "no transaction in progress" warnings
    "SAVEPOINT",
    "RELEASE SAVEPOINT",
    // SQLite allows partial GROUP BY, PostgreSQL doesn't
    "metadata_item_clusterings GROUP BY",
    "metadata_item_views",  // Complex GROUP BY issues
    // Complex JOIN queries with aliases that PostgreSQL handles differently
    "grandparents.id",
    "parents.id=metadata_items.parent_id",
    NULL
};

static int is_sqlite_only(const char *sql) {
    if (!sql) return 0;

    // Skip whitespace at start
    while (*sql && (*sql == ' ' || *sql == '\t' || *sql == '\n')) sql++;

    for (int i = 0; SQLITE_ONLY_PATTERNS[i]; i++) {
        if (strncasecmp(sql, SQLITE_ONLY_PATTERNS[i], strlen(SQLITE_ONLY_PATTERNS[i])) == 0) {
            return 1;
        }
        // Also check if it appears anywhere in the SQL
        if (strcasestr(sql, SQLITE_ONLY_PATTERNS[i])) {
            return 1;
        }
    }
    return 0;
}

static pg_connection_t* find_pg_connection(sqlite3 *db) {
    pthread_mutex_lock(&conn_mutex);
    for (int i = 0; i < connection_count; i++) {
        if (connections[i] && connections[i]->shadow_db == db) {
            pthread_mutex_unlock(&conn_mutex);
            return connections[i];
        }
    }
    pthread_mutex_unlock(&conn_mutex);
    return NULL;
}

// Check if SQL is a write operation (INSERT, UPDATE, DELETE)
static int is_write_operation(const char *sql) {
    if (!sql) return 0;

    // Skip whitespace
    while (*sql && (*sql == ' ' || *sql == '\t' || *sql == '\n')) sql++;

    // Check for write keywords (case insensitive)
    if (strncasecmp(sql, "INSERT", 6) == 0) return 1;
    if (strncasecmp(sql, "UPDATE", 6) == 0) return 1;
    if (strncasecmp(sql, "DELETE", 6) == 0) return 1;
    if (strncasecmp(sql, "REPLACE", 7) == 0) return 1;

    return 0;
}

// Check if SQL is a read operation (SELECT)
static int is_read_operation(const char *sql) {
    if (!sql) return 0;

    // Skip whitespace
    while (*sql && (*sql == ' ' || *sql == '\t' || *sql == '\n')) sql++;

    if (strncasecmp(sql, "SELECT", 6) == 0) return 1;

    return 0;
}

// Statement tracking functions
static void register_stmt(sqlite3_stmt *sqlite_stmt, pg_stmt_t *pg_stmt) {
    pthread_mutex_lock(&stmt_mutex);
    if (stmt_count < MAX_STATEMENTS) {
        stmt_map[stmt_count].sqlite_stmt = sqlite_stmt;
        stmt_map[stmt_count].pg_stmt = pg_stmt;
        stmt_count++;
    }
    pthread_mutex_unlock(&stmt_mutex);
}

static pg_stmt_t* find_pg_stmt(sqlite3_stmt *sqlite_stmt) {
    pthread_mutex_lock(&stmt_mutex);
    for (int i = 0; i < stmt_count; i++) {
        if (stmt_map[i].sqlite_stmt == sqlite_stmt) {
            pg_stmt_t *result = stmt_map[i].pg_stmt;
            pthread_mutex_unlock(&stmt_mutex);
            return result;
        }
    }
    pthread_mutex_unlock(&stmt_mutex);
    return NULL;
}

static void unregister_stmt(sqlite3_stmt *sqlite_stmt) {
    pthread_mutex_lock(&stmt_mutex);
    for (int i = 0; i < stmt_count; i++) {
        if (stmt_map[i].sqlite_stmt == sqlite_stmt) {
            // Free pg_stmt resources
            pg_stmt_t *pg_stmt = stmt_map[i].pg_stmt;
            if (pg_stmt) {
                if (pg_stmt->sql) free(pg_stmt->sql);
                if (pg_stmt->pg_sql) free(pg_stmt->pg_sql);
                if (pg_stmt->result) PQclear(pg_stmt->result);
                for (int j = 0; j < pg_stmt->param_count; j++) {
                    if (pg_stmt->param_values[j]) free(pg_stmt->param_values[j]);
                }
                free(pg_stmt);
            }
            // Remove from map by moving last entry here
            stmt_map[i] = stmt_map[--stmt_count];
            break;
        }
    }
    pthread_mutex_unlock(&stmt_mutex);
}

// ============================================================================
// PostgreSQL Connection Management
// ============================================================================

static pg_connection_t* pg_connect(const char *db_path, sqlite3 *shadow_db) {
    load_config();

    char conninfo[1024];
    snprintf(conninfo, sizeof(conninfo),
             "host=%s port=%d dbname=%s user=%s password=%s "
             "connect_timeout=10 application_name=plex_shim",
             pg_config.host, pg_config.port, pg_config.database,
             pg_config.user, pg_config.password);

    PGconn *conn = PQconnectdb(conninfo);

    if (PQstatus(conn) != CONNECTION_OK) {
        log_message("PostgreSQL connection failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return NULL;
    }

    // Set search path
    char schema_sql[256];
    snprintf(schema_sql, sizeof(schema_sql),
             "SET search_path TO %s, public", pg_config.schema);
    PGresult *res = PQexec(conn, schema_sql);
    PQclear(res);

    pg_connection_t *pg_conn = calloc(1, sizeof(pg_connection_t));
    pg_conn->conn = conn;
    pg_conn->shadow_db = shadow_db;
    pg_conn->is_pg_active = 1;
    strncpy(pg_conn->db_path, db_path, sizeof(pg_conn->db_path) - 1);
    pthread_mutex_init(&pg_conn->mutex, NULL);

    pthread_mutex_lock(&conn_mutex);
    if (connection_count < MAX_CONNECTIONS) {
        connections[connection_count++] = pg_conn;
    }
    pthread_mutex_unlock(&conn_mutex);

    log_message("PostgreSQL connected for: %s", db_path);

    return pg_conn;
}

// ============================================================================
// Interposed SQLite Functions
// ============================================================================

// Renamed versions that call the originals
static int my_sqlite3_open(const char *filename, sqlite3 **ppDb) {
    log_message("OPEN: %s (redirect=%d)", filename ? filename : "(null)", should_redirect(filename));

    // Always call real SQLite first
    int rc = sqlite3_open(filename, ppDb);

    if (rc == SQLITE_OK && should_redirect(filename)) {
        // Also connect to PostgreSQL
        pg_connection_t *pg_conn = pg_connect(filename, *ppDb);
        if (pg_conn) {
            log_message("PostgreSQL shadow connection established for: %s", filename);
        } else {
            log_message("PostgreSQL connection failed, using SQLite only");
        }
    }

    return rc;
}

static int my_sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags, const char *zVfs) {
    log_message("OPEN_V2: %s flags=0x%x (redirect=%d)",
                filename ? filename : "(null)", flags, should_redirect(filename));

    // Always call real SQLite first
    int rc = sqlite3_open_v2(filename, ppDb, flags, zVfs);

    if (rc == SQLITE_OK && should_redirect(filename)) {
        pg_connection_t *pg_conn = pg_connect(filename, *ppDb);
        if (pg_conn) {
            log_message("PostgreSQL shadow connection established for: %s", filename);
        }
    }

    return rc;
}

static int my_sqlite3_close(sqlite3 *db) {
    pg_connection_t *pg_conn = find_pg_connection(db);
    if (pg_conn) {
        log_message("CLOSE: PostgreSQL connection for %s", pg_conn->db_path);

        // Close PostgreSQL connection
        if (pg_conn->conn) {
            PQfinish(pg_conn->conn);
            pg_conn->conn = NULL;
        }

        // Remove from list
        pthread_mutex_lock(&conn_mutex);
        for (int i = 0; i < connection_count; i++) {
            if (connections[i] == pg_conn) {
                connections[i] = connections[--connection_count];
                break;
            }
        }
        pthread_mutex_unlock(&conn_mutex);

        pthread_mutex_destroy(&pg_conn->mutex);
        free(pg_conn);
    }

    return sqlite3_close(db);
}

static int my_sqlite3_close_v2(sqlite3 *db) {
    pg_connection_t *pg_conn = find_pg_connection(db);
    if (pg_conn) {
        if (pg_conn->conn) {
            PQfinish(pg_conn->conn);
            pg_conn->conn = NULL;
        }

        pthread_mutex_lock(&conn_mutex);
        for (int i = 0; i < connection_count; i++) {
            if (connections[i] == pg_conn) {
                connections[i] = connections[--connection_count];
                break;
            }
        }
        pthread_mutex_unlock(&conn_mutex);

        pthread_mutex_destroy(&pg_conn->mutex);
        free(pg_conn);
    }

    return sqlite3_close_v2(db);
}

static int my_sqlite3_exec(sqlite3 *db, const char *sql,
                          int (*callback)(void*, int, char**, char**),
                          void *arg, char **errmsg) {
    pg_connection_t *pg_conn = find_pg_connection(db);

    if (pg_conn && pg_conn->conn && pg_conn->is_pg_active) {
        // Skip SQLite-only commands
        if (is_sqlite_only(sql)) {
            // Just log and skip PostgreSQL execution
            // log_message("SKIP SQLite-only: %.50s...", sql);
        } else {
            // Execute on both PostgreSQL and SQLite (dual-write)
            sql_translation_t trans = sql_translate(sql);
            if (trans.success && trans.sql) {
                log_message("EXEC PG: %s", trans.sql);
                PGresult *res = PQexec(pg_conn->conn, trans.sql);
                ExecStatusType status = PQresultStatus(res);
                if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
                    log_message("PostgreSQL exec error: %s", PQerrorMessage(pg_conn->conn));
                }
                PQclear(res);
            }
            sql_translation_free(&trans);
        }

#if SQLITE_DISABLED
        // PostgreSQL-only mode: skip SQLite for redirected databases
        return SQLITE_OK;
#endif
    }

    // Execute on SQLite (for non-redirected DBs or when SQLITE_DISABLED is off)
    return sqlite3_exec(db, sql, callback, arg, errmsg);
}

// ============================================================================
// Prepared Statement Interception
// ============================================================================

static int my_sqlite3_prepare_v2(sqlite3 *db, const char *zSql, int nByte,
                                  sqlite3_stmt **ppStmt, const char **pzTail) {
    // Always prepare on SQLite first
    int rc = sqlite3_prepare_v2(db, zSql, nByte, ppStmt, pzTail);

    if (rc != SQLITE_OK || !*ppStmt) {
        return rc;
    }

    // Check if this is a redirected database
    pg_connection_t *pg_conn = find_pg_connection(db);

    // Track both reads and writes for PostgreSQL
    int is_write = is_write_operation(zSql);
    int is_read = is_read_operation(zSql);

    if (pg_conn && pg_conn->conn && pg_conn->is_pg_active && (is_write || is_read)) {
        // Skip SQLite-only statements
        if (is_sqlite_only(zSql)) {
            return rc;
        }

        // Create pg_stmt_t to track this statement
        pg_stmt_t *pg_stmt = calloc(1, sizeof(pg_stmt_t));
        if (pg_stmt) {
            pg_stmt->conn = pg_conn;
            pg_stmt->sql = strdup(zSql);
            pg_stmt->is_pg = is_write ? 1 : 2;  // 1 = write, 2 = read

            // Translate SQL for PostgreSQL
            sql_translation_t trans = sql_translate(zSql);
            if (trans.success && trans.sql) {
                pg_stmt->pg_sql = strdup(trans.sql);
            }
            sql_translation_free(&trans);

            // Count parameters (number of ? in SQL)
            const char *p = zSql;
            pg_stmt->param_count = 0;
            while (*p) {
                if (*p == '?') pg_stmt->param_count++;
                p++;
            }

            // Register the statement mapping
            register_stmt(*ppStmt, pg_stmt);

            // log_message("PREPARE %s: %s (params=%d)", is_write ? "WRITE" : "READ", zSql, pg_stmt->param_count);
        }
    }

    return rc;
}

static int my_sqlite3_bind_int(sqlite3_stmt *pStmt, int idx, int val) {
    // Always bind on SQLite
    int rc = sqlite3_bind_int(pStmt, idx, val);

    // Also track for PostgreSQL
    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && idx > 0 && idx <= MAX_PARAMS) {
        char buf[32];
        snprintf(buf, sizeof(buf), "%d", val);
        if (pg_stmt->param_values[idx-1]) free(pg_stmt->param_values[idx-1]);
        pg_stmt->param_values[idx-1] = strdup(buf);
        pg_stmt->param_lengths[idx-1] = 0;  // text format
        pg_stmt->param_formats[idx-1] = 0;  // text format
        if (idx > pg_stmt->param_count) pg_stmt->param_count = idx;
    }

    return rc;
}

static int my_sqlite3_bind_int64(sqlite3_stmt *pStmt, int idx, sqlite3_int64 val) {
    int rc = sqlite3_bind_int64(pStmt, idx, val);

    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && idx > 0 && idx <= MAX_PARAMS) {
        char buf[32];
        snprintf(buf, sizeof(buf), "%lld", val);
        if (pg_stmt->param_values[idx-1]) free(pg_stmt->param_values[idx-1]);
        pg_stmt->param_values[idx-1] = strdup(buf);
        pg_stmt->param_lengths[idx-1] = 0;
        pg_stmt->param_formats[idx-1] = 0;
        if (idx > pg_stmt->param_count) pg_stmt->param_count = idx;
    }

    return rc;
}

static int my_sqlite3_bind_double(sqlite3_stmt *pStmt, int idx, double val) {
    int rc = sqlite3_bind_double(pStmt, idx, val);

    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && idx > 0 && idx <= MAX_PARAMS) {
        char buf[64];
        snprintf(buf, sizeof(buf), "%.17g", val);
        if (pg_stmt->param_values[idx-1]) free(pg_stmt->param_values[idx-1]);
        pg_stmt->param_values[idx-1] = strdup(buf);
        pg_stmt->param_lengths[idx-1] = 0;
        pg_stmt->param_formats[idx-1] = 0;
        if (idx > pg_stmt->param_count) pg_stmt->param_count = idx;
    }

    return rc;
}

static int my_sqlite3_bind_text(sqlite3_stmt *pStmt, int idx, const char *val,
                                 int nBytes, void (*destructor)(void*)) {
    int rc = sqlite3_bind_text(pStmt, idx, val, nBytes, destructor);

    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && idx > 0 && idx <= MAX_PARAMS && val) {
        if (pg_stmt->param_values[idx-1]) free(pg_stmt->param_values[idx-1]);
        if (nBytes < 0) {
            pg_stmt->param_values[idx-1] = strdup(val);
        } else {
            pg_stmt->param_values[idx-1] = malloc(nBytes + 1);
            if (pg_stmt->param_values[idx-1]) {
                memcpy(pg_stmt->param_values[idx-1], val, nBytes);
                pg_stmt->param_values[idx-1][nBytes] = '\0';
            }
        }
        pg_stmt->param_lengths[idx-1] = 0;
        pg_stmt->param_formats[idx-1] = 0;
        if (idx > pg_stmt->param_count) pg_stmt->param_count = idx;
    }

    return rc;
}

static int my_sqlite3_bind_blob(sqlite3_stmt *pStmt, int idx, const void *val,
                                 int nBytes, void (*destructor)(void*)) {
    int rc = sqlite3_bind_blob(pStmt, idx, val, nBytes, destructor);

    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && idx > 0 && idx <= MAX_PARAMS && val && nBytes > 0) {
        if (pg_stmt->param_values[idx-1]) free(pg_stmt->param_values[idx-1]);
        pg_stmt->param_values[idx-1] = malloc(nBytes);
        if (pg_stmt->param_values[idx-1]) {
            memcpy(pg_stmt->param_values[idx-1], val, nBytes);
        }
        pg_stmt->param_lengths[idx-1] = nBytes;
        pg_stmt->param_formats[idx-1] = 1;  // binary format
        if (idx > pg_stmt->param_count) pg_stmt->param_count = idx;
    }

    return rc;
}

static int my_sqlite3_bind_null(sqlite3_stmt *pStmt, int idx) {
    int rc = sqlite3_bind_null(pStmt, idx);

    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && idx > 0 && idx <= MAX_PARAMS) {
        if (pg_stmt->param_values[idx-1]) {
            free(pg_stmt->param_values[idx-1]);
            pg_stmt->param_values[idx-1] = NULL;
        }
        pg_stmt->param_lengths[idx-1] = 0;
        pg_stmt->param_formats[idx-1] = 0;
        if (idx > pg_stmt->param_count) pg_stmt->param_count = idx;
    }

    return rc;
}

static int my_sqlite3_step(sqlite3_stmt *pStmt) {
    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);

    // Execute on PostgreSQL
    if (pg_stmt && pg_stmt->pg_sql && pg_stmt->conn && pg_stmt->conn->conn) {
        // For reads (is_pg == 2), only execute once and store result
        if (pg_stmt->is_pg == 2) {
            // First call - execute query
            if (!pg_stmt->result) {
                const char *paramValues[MAX_PARAMS];
                int paramLengths[MAX_PARAMS];
                int paramFormats[MAX_PARAMS];

                for (int i = 0; i < pg_stmt->param_count && i < MAX_PARAMS; i++) {
                    paramValues[i] = pg_stmt->param_values[i];
                    paramLengths[i] = pg_stmt->param_lengths[i];
                    paramFormats[i] = pg_stmt->param_formats[i];
                }

                pg_stmt->result = PQexecParams(
                    pg_stmt->conn->conn,
                    pg_stmt->pg_sql,
                    pg_stmt->param_count,
                    NULL,
                    paramValues,
                    paramLengths,
                    paramFormats,
                    0  // text format
                );

                ExecStatusType status = PQresultStatus(pg_stmt->result);
                if (status == PGRES_TUPLES_OK) {
                    pg_stmt->num_rows = PQntuples(pg_stmt->result);
                    pg_stmt->num_cols = PQnfields(pg_stmt->result);
                    pg_stmt->current_row = 0;
                    log_message("STEP READ: %d rows, %d cols for: %.500s",
                                pg_stmt->num_rows, pg_stmt->num_cols, pg_stmt->pg_sql);
                } else if (status != PGRES_COMMAND_OK) {
                    log_message("STEP PG read error: %s\nSQL: %s",
                                PQerrorMessage(pg_stmt->conn->conn), pg_stmt->pg_sql);
                    PQclear(pg_stmt->result);
                    pg_stmt->result = NULL;
                }
            } else {
                // Subsequent calls - advance to next row
                pg_stmt->current_row++;
            }

#if PG_READ_ENABLED
            // When reads are enabled, return based on PostgreSQL state
            if (pg_stmt->result) {
                if (pg_stmt->current_row < pg_stmt->num_rows) {
                    return SQLITE_ROW;
                } else {
                    return SQLITE_DONE;
                }
            }
            // If result is NULL (error), fall through to SQLite
#endif
        }
        // For writes (is_pg == 1), execute every time
        else if (pg_stmt->is_pg == 1) {
            const char *paramValues[MAX_PARAMS];
            int paramLengths[MAX_PARAMS];
            int paramFormats[MAX_PARAMS];

            for (int i = 0; i < pg_stmt->param_count && i < MAX_PARAMS; i++) {
                paramValues[i] = pg_stmt->param_values[i];
                paramLengths[i] = pg_stmt->param_lengths[i];
                paramFormats[i] = pg_stmt->param_formats[i];
            }

            PGresult *res = PQexecParams(
                pg_stmt->conn->conn,
                pg_stmt->pg_sql,
                pg_stmt->param_count,
                NULL,
                paramValues,
                paramLengths,
                paramFormats,
                0
            );

            ExecStatusType status = PQresultStatus(res);
            if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
                log_message("STEP PG write error: %s\nSQL: %s",
                            PQerrorMessage(pg_stmt->conn->conn), pg_stmt->pg_sql);
            }

            PQclear(res);
        }
    }

#if SQLITE_DISABLED
    // PostgreSQL-only mode: don't execute on SQLite for redirected databases
    if (pg_stmt && pg_stmt->is_pg) {
        // For reads (is_pg == 2), we already returned above via PG_READ_ENABLED
        // For writes (is_pg == 1), return success
        if (pg_stmt->is_pg == 1) {
            return SQLITE_DONE;
        }
        // For reads that failed on PG, fall through to SQLite as backup
    }
#endif

    // Execute on SQLite (for non-redirected DBs or as fallback)
    return sqlite3_step(pStmt);
}

static int my_sqlite3_reset(sqlite3_stmt *pStmt) {
    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);

    if (pg_stmt) {
        // Clear parameter values for reuse
        for (int i = 0; i < MAX_PARAMS; i++) {
            if (pg_stmt->param_values[i]) {
                free(pg_stmt->param_values[i]);
                pg_stmt->param_values[i] = NULL;
            }
            pg_stmt->param_lengths[i] = 0;
            pg_stmt->param_formats[i] = 0;
        }

        if (pg_stmt->result) {
            PQclear(pg_stmt->result);
            pg_stmt->result = NULL;
        }
    }

    return sqlite3_reset(pStmt);
}

static int my_sqlite3_finalize(sqlite3_stmt *pStmt) {
    // Clean up our tracking before SQLite finalizes
    unregister_stmt(pStmt);

    return sqlite3_finalize(pStmt);
}

static int my_sqlite3_clear_bindings(sqlite3_stmt *pStmt) {
    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);

    if (pg_stmt) {
        for (int i = 0; i < MAX_PARAMS; i++) {
            if (pg_stmt->param_values[i]) {
                free(pg_stmt->param_values[i]);
                pg_stmt->param_values[i] = NULL;
            }
            pg_stmt->param_lengths[i] = 0;
            pg_stmt->param_formats[i] = 0;
        }
    }

    return sqlite3_clear_bindings(pStmt);
}

// ============================================================================
// Column Value Interception (for reads from PostgreSQL)
// ============================================================================

// Set to 1 to read from PostgreSQL, 0 to use SQLite (safer for testing)
#define PG_READ_ENABLED 1

// Set to 1 to completely disable SQLite operations (PostgreSQL only mode)
// WARNING: Make sure all data is synced to PostgreSQL before enabling!
#define SQLITE_DISABLED 1

static int my_sqlite3_column_count(sqlite3_stmt *pStmt) {
#if PG_READ_ENABLED
    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && pg_stmt->is_pg == 2 && pg_stmt->result) {
        return pg_stmt->num_cols;
    }
#endif
    return sqlite3_column_count(pStmt);
}

static int my_sqlite3_column_type(sqlite3_stmt *pStmt, int idx) {
#if PG_READ_ENABLED
    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && pg_stmt->is_pg == 2 && pg_stmt->result) {
        if (pg_stmt->current_row < pg_stmt->num_rows) {
            if (PQgetisnull(pg_stmt->result, pg_stmt->current_row, idx)) {
                return SQLITE_NULL;
            }
            // Map PostgreSQL OIDs to SQLite types
            Oid oid = PQftype(pg_stmt->result, idx);
            switch (oid) {
                case 16:   // BOOL - treat as integer (0/1)
                case 23:   // INT4
                case 20:   // INT8
                case 21:   // INT2
                    return SQLITE_INTEGER;
                case 700:  // FLOAT4
                case 701:  // FLOAT8
                case 1700: // NUMERIC
                    return SQLITE_FLOAT;
                case 17:   // BYTEA
                    return SQLITE_BLOB;
                default:
                    return SQLITE_TEXT;
            }
        }
    }
#endif
    return sqlite3_column_type(pStmt, idx);
}

// Helper to convert PostgreSQL value to int (handles boolean t/f)
static int pg_value_to_int(const char *val) {
    if (!val || !*val) return 0;
    // PostgreSQL boolean: 't' = true = 1, 'f' = false = 0
    if (val[0] == 't' && val[1] == '\0') return 1;
    if (val[0] == 'f' && val[1] == '\0') return 0;
    return atoi(val);
}

// Helper to convert PostgreSQL value to int64 (handles boolean t/f)
static sqlite3_int64 pg_value_to_int64(const char *val) {
    if (!val || !*val) return 0;
    // PostgreSQL boolean: 't' = true = 1, 'f' = false = 0
    if (val[0] == 't' && val[1] == '\0') return 1;
    if (val[0] == 'f' && val[1] == '\0') return 0;
    return atoll(val);
}

static int my_sqlite3_column_int(sqlite3_stmt *pStmt, int idx) {
#if PG_READ_ENABLED
    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && pg_stmt->is_pg == 2 && pg_stmt->result) {
        if (pg_stmt->current_row < pg_stmt->num_rows &&
            !PQgetisnull(pg_stmt->result, pg_stmt->current_row, idx)) {
            return pg_value_to_int(PQgetvalue(pg_stmt->result, pg_stmt->current_row, idx));
        }
        return 0;
    }
#endif
    return sqlite3_column_int(pStmt, idx);
}

static sqlite3_int64 my_sqlite3_column_int64(sqlite3_stmt *pStmt, int idx) {
#if PG_READ_ENABLED
    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && pg_stmt->is_pg == 2 && pg_stmt->result) {
        if (pg_stmt->current_row < pg_stmt->num_rows &&
            !PQgetisnull(pg_stmt->result, pg_stmt->current_row, idx)) {
            return pg_value_to_int64(PQgetvalue(pg_stmt->result, pg_stmt->current_row, idx));
        }
        return 0;
    }
#endif
    return sqlite3_column_int64(pStmt, idx);
}

// Helper to convert PostgreSQL value to double (handles boolean t/f)
static double pg_value_to_double(const char *val) {
    if (!val || !*val) return 0.0;
    // PostgreSQL boolean: 't' = true = 1.0, 'f' = false = 0.0
    if (val[0] == 't' && val[1] == '\0') return 1.0;
    if (val[0] == 'f' && val[1] == '\0') return 0.0;
    return atof(val);
}

static double my_sqlite3_column_double(sqlite3_stmt *pStmt, int idx) {
#if PG_READ_ENABLED
    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && pg_stmt->is_pg == 2 && pg_stmt->result) {
        if (pg_stmt->current_row < pg_stmt->num_rows &&
            !PQgetisnull(pg_stmt->result, pg_stmt->current_row, idx)) {
            return pg_value_to_double(PQgetvalue(pg_stmt->result, pg_stmt->current_row, idx));
        }
        return 0.0;
    }
#endif
    return sqlite3_column_double(pStmt, idx);
}

static const unsigned char* my_sqlite3_column_text(sqlite3_stmt *pStmt, int idx) {
#if PG_READ_ENABLED
    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && pg_stmt->is_pg == 2 && pg_stmt->result) {
        if (pg_stmt->current_row < pg_stmt->num_rows) {
            if (PQgetisnull(pg_stmt->result, pg_stmt->current_row, idx)) {
                return NULL;
            }
            const char *val = PQgetvalue(pg_stmt->result, pg_stmt->current_row, idx);
            // Empty string should be returned as empty string, not converted
            return (const unsigned char*)val;
        }
        return NULL;
    }
#endif
    return sqlite3_column_text(pStmt, idx);
}

static const void* my_sqlite3_column_blob(sqlite3_stmt *pStmt, int idx) {
#if PG_READ_ENABLED
    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && pg_stmt->is_pg == 2 && pg_stmt->result) {
        if (pg_stmt->current_row < pg_stmt->num_rows &&
            !PQgetisnull(pg_stmt->result, pg_stmt->current_row, idx)) {
            return PQgetvalue(pg_stmt->result, pg_stmt->current_row, idx);
        }
        return NULL;
    }
#endif
    return sqlite3_column_blob(pStmt, idx);
}

static int my_sqlite3_column_bytes(sqlite3_stmt *pStmt, int idx) {
#if PG_READ_ENABLED
    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && pg_stmt->is_pg == 2 && pg_stmt->result) {
        if (pg_stmt->current_row < pg_stmt->num_rows &&
            !PQgetisnull(pg_stmt->result, pg_stmt->current_row, idx)) {
            return PQgetlength(pg_stmt->result, pg_stmt->current_row, idx);
        }
        return 0;
    }
#endif
    return sqlite3_column_bytes(pStmt, idx);
}

static const char* my_sqlite3_column_name(sqlite3_stmt *pStmt, int idx) {
#if PG_READ_ENABLED
    pg_stmt_t *pg_stmt = find_pg_stmt(pStmt);
    if (pg_stmt && pg_stmt->is_pg == 2 && pg_stmt->result) {
        return PQfname(pg_stmt->result, idx);
    }
#endif
    return sqlite3_column_name(pStmt, idx);
}

// Register interpositions
DYLD_INTERPOSE(my_sqlite3_open, sqlite3_open)
DYLD_INTERPOSE(my_sqlite3_open_v2, sqlite3_open_v2)
DYLD_INTERPOSE(my_sqlite3_close, sqlite3_close)
DYLD_INTERPOSE(my_sqlite3_close_v2, sqlite3_close_v2)
DYLD_INTERPOSE(my_sqlite3_exec, sqlite3_exec)

// Prepared statement interpositions
DYLD_INTERPOSE(my_sqlite3_prepare_v2, sqlite3_prepare_v2)
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

// Column value interpositions (for reads)
DYLD_INTERPOSE(my_sqlite3_column_count, sqlite3_column_count)
DYLD_INTERPOSE(my_sqlite3_column_type, sqlite3_column_type)
DYLD_INTERPOSE(my_sqlite3_column_int, sqlite3_column_int)
DYLD_INTERPOSE(my_sqlite3_column_int64, sqlite3_column_int64)
DYLD_INTERPOSE(my_sqlite3_column_double, sqlite3_column_double)
DYLD_INTERPOSE(my_sqlite3_column_text, sqlite3_column_text)
DYLD_INTERPOSE(my_sqlite3_column_blob, sqlite3_column_blob)
DYLD_INTERPOSE(my_sqlite3_column_bytes, sqlite3_column_bytes)
DYLD_INTERPOSE(my_sqlite3_column_name, sqlite3_column_name)

// ============================================================================
// Constructor/Destructor
// ============================================================================

__attribute__((constructor))
static void shim_init(void) {
    log_message("=== Plex PostgreSQL Interpose Shim loaded ===");
    sql_translator_init();
    load_config();
    shim_initialized = 1;
}

__attribute__((destructor))
static void shim_cleanup(void) {
    log_message("=== Plex PostgreSQL Interpose Shim unloading ===");

    pthread_mutex_lock(&conn_mutex);
    for (int i = 0; i < connection_count; i++) {
        if (connections[i] && connections[i]->conn) {
            PQfinish(connections[i]->conn);
        }
    }
    connection_count = 0;
    pthread_mutex_unlock(&conn_mutex);

    sql_translator_cleanup();

    if (log_file) {
        fclose(log_file);
        log_file = NULL;
    }
}
