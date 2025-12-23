/*
 * PostgreSQL Shim - Configuration Module
 * Configuration loading and SQL classification
 */

#ifndef PG_CONFIG_H
#define PG_CONFIG_H

#include "pg_types.h"

// Configuration loading
void pg_config_init(void);
pg_conn_config_t* pg_config_get(void);

// SQL classification
int should_redirect(const char *filename);
int should_skip_sql(const char *sql);
int is_write_operation(const char *sql);
int is_read_operation(const char *sql);

#endif // PG_CONFIG_H
