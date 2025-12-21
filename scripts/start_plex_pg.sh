#!/bin/bash
# Start Plex with PostgreSQL shim

export DYLD_INSERT_LIBRARIES=/Users/sander/plex-scanner-shim/db_interpose_pg.dylib
export PLEX_PG_HOST=localhost
export PLEX_PG_PORT=5432
export PLEX_PG_DATABASE=plex_test
export PLEX_PG_USER=plex
export PLEX_PG_PASSWORD=""
export PLEX_PG_SCHEMA=plex

# Clear log
> /tmp/plex_redirect_pg.log

exec "/Applications/Plex Media Server.app/Contents/MacOS/Plex Media Server"
