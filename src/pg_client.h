#ifndef PG_CLIENT_H
#define PG_CLIENT_H

#include "pg_types.h"
#include <sqlite3.h>

void pg_client_init(void);

// Connection Management
pg_connection_t* pg_connect(const char *db_path, sqlite3 *shadow_db); // Legacy/Helper
int ensure_pg_connection(pg_connection_t *conn);
void pg_close(pg_connection_t *conn);

// Registry for mapping real sqlite handles to pg connections
void pg_register_connection(sqlite3 *handle, pg_connection_t *conn);
void pg_unregister_connection(sqlite3 *handle);
pg_connection_t* find_pg_connection(sqlite3 *handle);

#endif // PG_CLIENT_H
