#include "pg_config.h"
#include "pg_logging.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <strings.h> // for strncasecmp, strcasestr

static pg_conn_config_t pg_config;
static int config_loaded = 0;

static const char *REDIRECT_PATTERNS[] = {
    "com.plexapp.plugins.library.db",
    "com.plexapp.plugins.library.blobs.db",
    NULL
};

// SQLite-specific commands that should be SKIPPED entirely (no-op, return success)
static const char *SQLITE_SKIP_PATTERNS[] = {
    "icu_load_collation",
    "fts3_tokenizer",    // SQLite FTS3 tokenizer setup
    "SELECT load_extension",
    "VACUUM",
    "PRAGMA",
    "REINDEX",
    "ANALYZE sqlite_",
    "ATTACH DATABASE",
    "DETACH DATABASE",
    "ROLLBACK",          // Avoid "no transaction in progress" warnings if we manage trans differently
    "SAVEPOINT",
    "RELEASE SAVEPOINT",
    "sqlite_schema",     // SQLite internal schema table
    "sqlite_master",     // SQLite internal master table (alias for sqlite_schema)
    "typeof(",           // SQLite typeof() function (used in schema migrations) - handled by translator? 
                         // Check: The original code skipped it here, so we keep it. 
                         // But wait, typeof() is translated in sql_translator.c. 
                         // If it's a "SELECT typeof(...)" query it might be skipped?
                         // The original code had "typeof(" in SKIP_PATTERNS.
    "last_insert_rowid()", // SQLite-specific function
    NULL
};

void load_config(void) {
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

    LOG_INFO("PostgreSQL config: %s@%s:%d/%s (schema: %s)",
                pg_config.user, pg_config.host, pg_config.port,
                pg_config.database, pg_config.schema);
}

pg_conn_config_t* get_config(void) {
    if (!config_loaded) load_config();
    return &pg_config;
}

int should_redirect(const char *filename) {
    if (!filename) return 0;

    for (int i = 0; REDIRECT_PATTERNS[i]; i++) {
        if (strstr(filename, REDIRECT_PATTERNS[i])) {
            return 1;
        }
    }
    return 0;
}

int should_skip_sql(const char *sql) {
    if (!sql) return 0;

    // Skip whitespace at start
    while (*sql && (*sql == ' ' || *sql == '\t' || *sql == '\n')) sql++;

    // Check patterns that should only match at start of SQL
    for (int i = 0; SQLITE_SKIP_PATTERNS[i]; i++) {
        if (strncasecmp(sql, SQLITE_SKIP_PATTERNS[i], strlen(SQLITE_SKIP_PATTERNS[i])) == 0) {
            return 1;
        }
    }

    // Patterns that can appear anywhere in SQL
    static const char *ANYWHERE_PATTERNS[] = {
        "sqlite_schema",
        "sqlite_master",
        "fts3_tokenizer",
        "fts4",
        "fts5",
        "spellfix",
        "icu_load_collation",
        NULL
    };
    for (int i = 0; ANYWHERE_PATTERNS[i]; i++) {
        if (strcasestr(sql, ANYWHERE_PATTERNS[i])) {
            return 1;
        }
    }
    return 0;
}
