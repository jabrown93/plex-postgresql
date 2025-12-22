#include "pg_client.h"
#include "pg_config.h"
#include "pg_logging.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static pg_connection_t* connections[MAX_CONNECTIONS];
static pthread_mutex_t registry_mutex = PTHREAD_MUTEX_INITIALIZER;

void pg_client_init(void) {
    memset(connections, 0, sizeof(connections));
}

void pg_register_connection(sqlite3 *handle, pg_connection_t *conn) {
    pthread_mutex_lock(&registry_mutex);
    int registered = 0;
    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        if (connections[i] == NULL) {
            conn->sqlite_handle = handle;
            connections[i] = conn;
            LOG_DEBUG("Registered connection %p for handle %p", (void*)conn, (void*)handle);
            registered = 1;
            break;
        }
    }
    if (!registered) {
        LOG_ERROR("CRITICAL: Connection registry FULL (MAX_CONNECTIONS=%d)", MAX_CONNECTIONS);
    }
    pthread_mutex_unlock(&registry_mutex);
}

void pg_unregister_connection(sqlite3 *handle) {
    pthread_mutex_lock(&registry_mutex);
    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        if (connections[i] && connections[i]->sqlite_handle == handle) {
            connections[i] = NULL;
            LOG_DEBUG("Unregistered connection for handle %p", (void*)handle);
            break;
        }
    }
    pthread_mutex_unlock(&registry_mutex);
}

pg_connection_t* find_pg_connection(sqlite3 *handle) {
    if (!handle) return NULL;
    // Fast path?
    pthread_mutex_lock(&registry_mutex);
    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        if (connections[i] && connections[i]->sqlite_handle == handle) {
            pthread_mutex_unlock(&registry_mutex);
            return connections[i];
        }
    }
    pthread_mutex_unlock(&registry_mutex);
    return NULL;
}


int ensure_pg_connection(pg_connection_t *conn) {
    if (!conn) return 0;
    
    pthread_mutex_lock(&conn->mutex);

    // Check if connected
    if (conn->conn) {
        if (PQstatus(conn->conn) == CONNECTION_OK) {
            pthread_mutex_unlock(&conn->mutex);
            return 1;
        }
        // Dead?
        PQfinish(conn->conn);
        conn->conn = NULL;
    }

    pg_conn_config_t *cfg = get_config();
    char conninfo[1024];
    snprintf(conninfo, sizeof(conninfo), 
             "host=%s port=%d dbname=%s user=%s password=%s connect_timeout=5",
             cfg->host, cfg->port, cfg->database, cfg->user, cfg->password);

    conn->conn = PQconnectdb(conninfo);

    if (PQstatus(conn->conn) != CONNECTION_OK) {
        LOG_ERROR("Connection to PostgreSQL failed: %s", PQerrorMessage(conn->conn));
        PQfinish(conn->conn);
        conn->conn = NULL;
        pthread_mutex_unlock(&conn->mutex);
        return 0;
    }

    LOG_INFO("Connected to PostgreSQL: %s@%s:%d/%s", cfg->user, cfg->host, cfg->port, cfg->database);

    // Set search path
    char schema_cmd[128];
    snprintf(schema_cmd, sizeof(schema_cmd), "SET search_path TO %s, public", cfg->schema);
    PGresult *res = PQexec(conn->conn, schema_cmd);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        LOG_ERROR("Failed to set schema: %s", PQresultErrorMessage(res));
    }
    PQclear(res);

    pthread_mutex_unlock(&conn->mutex);
    return 1;
}

void pg_close(pg_connection_t *conn) {
    if (!conn) return;
    
    pthread_mutex_lock(&conn->mutex);
    if (conn->conn) {
        PQfinish(conn->conn);
        conn->conn = NULL;
    }
    pthread_mutex_unlock(&conn->mutex);
    
    pthread_mutex_destroy(&conn->mutex);
    free(conn);
}

// Stub
pg_connection_t* pg_connect(const char *db_path, sqlite3 *shadow_db) {
    // Unused now
    (void)db_path; (void)shadow_db;
    return NULL;
}
