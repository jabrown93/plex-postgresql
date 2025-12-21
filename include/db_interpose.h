/*
 * Plex PostgreSQL Interposing Shim - Header
 */

#ifndef DB_INTERPOSE_H
#define DB_INTERPOSE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>
#include <stdarg.h>
#include <sqlite3.h>
#include <libpq-fe.h>

#include "sql_translator.h"

// Configuration
#define PG_READ_ENABLED 1
#define SQLITE_DISABLED 1

#define LOG_FILE "/tmp/plex_redirect_pg.log"
#define MAX_CONNECTIONS 16
#define MAX_PARAMS 64
#define MAX_STATEMENTS 1024

// Types
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
    sqlite3 *shadow_db;
    char db_path[1024];
    int is_pg_active;
    int in_transaction;
    pthread_mutex_t mutex;
    int last_changes;
    sqlite3_int64 last_insert_rowid;
    sqlite3_int64 last_generator_metadata_id;
} pg_connection_t;

typedef struct pg_stmt {
    pg_connection_t *conn;
    sqlite3_stmt *shadow_stmt;
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

// Globals (extern declarations)
extern FILE *log_file;
extern pthread_mutex_t log_mutex;
extern pthread_mutex_t conn_mutex;
extern pg_conn_config_t pg_config;
extern int config_loaded;
extern int shim_initialized;
extern pg_connection_t *connections[MAX_CONNECTIONS];
extern int connection_count;
extern sqlite3_int64 global_generator_metadata_id;

// Statement tracking
extern struct {
    sqlite3_stmt *sqlite_stmt;
    pg_stmt_t *pg_stmt;
} stmt_map[MAX_STATEMENTS];
extern int stmt_count;
extern pthread_mutex_t stmt_mutex;

// Function declarations
void log_message(const char *fmt, ...);
void load_config(void);
int should_redirect(const char *filename);
int should_skip_sql(const char *sql);
pg_connection_t* find_pg_connection(sqlite3 *db);
pg_connection_t* find_any_pg_connection_for_library(void);
int is_write_operation(const char *sql);
int is_read_operation(const char *sql);
char* simple_str_replace(const char *str, const char *old, const char *new_str);
sqlite3_int64 extract_metadata_id_from_generator_sql(const char *sql);
char* convert_metadata_settings_insert_to_upsert(const char *sql);
void register_stmt(sqlite3_stmt *sqlite_stmt, pg_stmt_t *pg_stmt);
pg_stmt_t* find_pg_stmt(sqlite3_stmt *sqlite_stmt);
void unregister_stmt(sqlite3_stmt *sqlite_stmt);
pg_connection_t* pg_connect(const char *db_path, sqlite3 *shadow_db);

// DYLD interposing macro
#define DYLD_INTERPOSE(_replacement, _original) \
    __attribute__((used)) static struct { \
        const void* replacement; \
        const void* original; \
    } _interpose_##_original __attribute__((section("__DATA,__interpose"))) = { \
        (const void*)(unsigned long)&_replacement, \
        (const void*)(unsigned long)&_original \
    };

#endif // DB_INTERPOSE_H
