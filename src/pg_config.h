#ifndef PG_CONFIG_H
#define PG_CONFIG_H

#include "pg_types.h"

// Configuration constants
#define ENV_PG_HOST     "PLEX_PG_HOST"
#define ENV_PG_PORT     "PLEX_PG_PORT"
#define ENV_PG_DATABASE "PLEX_PG_DATABASE"
#define ENV_PG_USER     "PLEX_PG_USER"
#define ENV_PG_PASSWORD "PLEX_PG_PASSWORD"
#define ENV_PG_SCHEMA   "PLEX_PG_SCHEMA"

void load_config(void);
pg_conn_config_t* get_config(void);
int should_redirect(const char *filename);
int should_skip_sql(const char *sql);

#endif // PG_CONFIG_H
