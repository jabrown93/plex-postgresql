# Linux Shim Implementation Plan

Bring `db_interpose_pg_linux.c` to feature parity with macOS version.

## Current State

| Aspect | macOS | Linux |
|--------|-------|-------|
| File | `db_interpose_pg.c` | `db_interpose_pg_linux.c` |
| Lines | 2458 | 2437 |
| Last updated | Jan 3 | Dec 21 |
| Uses modules | Yes (pg_*.c/h) | No (inline) |
| Interpose method | DYLD_INTERPOSE | LD_PRELOAD + dlsym |

## Goals

- Update Linux to use shared modules (pg_client.c, pg_statement.c, etc.)
- Sync all features and bugfixes from macOS
- Keep macOS file unchanged

## Step 1: Refactor Linux to Use Modules

Replace inline code with module imports:

```c
#include "pg_types.h"
#include "pg_logging.h"
#include "pg_config.h"
#include "pg_client.h"
#include "pg_statement.h"
#include "sql_translator.h"
```

Remove from Linux file:
- Inline PGconn/PGresult handling (use pg_client.c)
- Inline statement management (use pg_statement.c)
- Inline logging (use pg_logging.c)
- Inline config loading (use pg_config.c)

## Step 2: Update Makefile

```makefile
linux: $(OBJECTS) src/db_interpose_pg_linux.c
    gcc -shared -fPIC -o db_interpose_pg.so \
        src/db_interpose_pg_linux.c $(OBJECTS) \
        -I/usr/include/postgresql \
        -Iinclude -Isrc \
        -lpq -lsqlite3 -ldl -lpthread
```

## Step 3: Linux-Specific Interposition

Keep LD_PRELOAD pattern but use modules:

```c
// Get original SQLite functions via dlsym
static int (*orig_sqlite3_open)(const char*, sqlite3**) = NULL;

__attribute__((constructor))
static void init_interpose(void) {
    orig_sqlite3_open = dlsym(RTLD_NEXT, "sqlite3_open");
    // ... other functions
    init_logging();
    load_config();
}

int sqlite3_open(const char *filename, sqlite3 **ppDb) {
    // Use pg_client.c for connection management
    // Use pg_statement.c for statement handling
    // Same logic as macOS, different interpose mechanism
}
```

## Step 4: Features to Sync

From macOS version:

- [ ] Connection pool (50 default, 100 max, configurable)
- [ ] Socket timeouts (60s SO_RCVTIMEO/SO_SNDTIMEO)
- [ ] Prepared statement cache per connection
- [ ] sqlite3_value interception (fake value mechanism)
- [ ] Thread-local statement cache
- [ ] Reference counting for statements
- [ ] All SQL translator fixes (GROUP BY, HAVING, JSON, etc.)

## Step 5: Linux Wrapper Scripts

Create `scripts/install_wrappers_linux.sh`:

```bash
#!/bin/bash
PLEX_DIR="/usr/lib/plexmediaserver"
SHIM_PATH="/usr/local/lib/plex-postgresql/db_interpose_pg.so"

# Create wrapper script
cat > "$PLEX_DIR/Plex Media Server.wrapper" << 'EOF'
#!/bin/bash
export LD_PRELOAD="$SHIM_PATH"
export PLEX_PG_HOST="${PLEX_PG_HOST:-localhost}"
export PLEX_PG_PORT="${PLEX_PG_PORT:-5432}"
export PLEX_PG_DATABASE="${PLEX_PG_DATABASE:-plex}"
export PLEX_PG_USER="${PLEX_PG_USER:-plex}"
export PLEX_PG_PASSWORD="${PLEX_PG_PASSWORD:-}"
export PLEX_PG_SCHEMA="${PLEX_PG_SCHEMA:-plex}"
exec "$PLEX_DIR/Plex Media Server.original" "$@"
EOF

mv "$PLEX_DIR/Plex Media Server" "$PLEX_DIR/Plex Media Server.original"
mv "$PLEX_DIR/Plex Media Server.wrapper" "$PLEX_DIR/Plex Media Server"
chmod +x "$PLEX_DIR/Plex Media Server"
```

## Step 6: Docker Testing

Use existing `docker-compose.yml`:

```bash
# Build Linux shim
make linux

# Start test environment
docker-compose up -d

# Run migration
./scripts/migrate_sqlite_to_pg.sh

# Test
docker exec plex curl -s http://localhost:32400/library/sections
```

## Step 7: Validation

Test queries that were fixed on macOS:
- Movies library (sqlite3_value interception)
- TV Shows with many episodes (connection pool)
- Library scans (socket timeouts)
- Complex queries (GROUP BY, HAVING fixes)

## File Changes Summary

| File | Action |
|------|--------|
| `src/db_interpose_pg_linux.c` | Refactor to use modules |
| `Makefile` | Update linux target |
| `scripts/install_wrappers_linux.sh` | Create new |
| `scripts/uninstall_wrappers_linux.sh` | Create new |
| `docker-compose.yml` | Verify/update |

## Timeline

1. Refactor Linux file to use modules
2. Update Makefile
3. Test compilation
4. Create wrapper scripts
5. Docker test
6. Document in README
