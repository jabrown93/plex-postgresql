#!/bin/bash
# Install Plex wrapper scripts for PostgreSQL shim (macOS)
# This replaces the Plex binaries with wrapper scripts that inject the shim
# For Linux, use install_wrappers_linux.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SHIM_DIR="$(dirname "$SCRIPT_DIR")"
PLEX_APP="/Applications/Plex Media Server.app/Contents/MacOS"
SHIM_PATH="$SHIM_DIR/db_interpose_pg.dylib"
SQLITE_DB="$HOME/Library/Application Support/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db"

# PostgreSQL defaults
PG_HOST="${PLEX_PG_HOST:-localhost}"
PG_PORT="${PLEX_PG_PORT:-5432}"
PG_DATABASE="${PLEX_PG_DATABASE:-plex}"
PG_USER="${PLEX_PG_USER:-plex}"
PG_SCHEMA="${PLEX_PG_SCHEMA:-plex}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "=== Plex PostgreSQL Wrapper Installer ==="
echo ""

# Check if shim exists
if [[ ! -f "$SHIM_PATH" ]]; then
    echo -e "${RED}ERROR: Shim not found at $SHIM_PATH${NC}"
    echo "Run 'make' first to build the shim."
    exit 1
fi

# Check if Plex is running
if pgrep -x "Plex Media Server" >/dev/null 2>&1 || pgrep -x "Plex Media Server.original" >/dev/null 2>&1; then
    echo -e "${YELLOW}WARNING: Plex is running. Stop it first:${NC}"
    echo "  pkill -x 'Plex Media Server' 'Plex Media Server.original'"
    exit 1
fi

# ============================================================================
# Migration check and prompt
# ============================================================================

check_and_migrate() {
    # Check if SQLite database exists and has data
    if [[ ! -f "$SQLITE_DB" ]]; then
        echo -e "${BLUE}No existing Plex database found. Fresh install.${NC}"
        return 0
    fi

    local sqlite_count=$(sqlite3 "$SQLITE_DB" "SELECT COUNT(*) FROM metadata_items;" 2>/dev/null || echo "0")

    if [[ "$sqlite_count" -eq 0 ]]; then
        echo -e "${BLUE}Existing Plex database is empty. No migration needed.${NC}"
        return 0
    fi

    echo -e "${YELLOW}========================================${NC}"
    echo -e "${YELLOW}  EXISTING PLEX DATA DETECTED${NC}"
    echo -e "${YELLOW}========================================${NC}"
    echo ""
    echo "Found SQLite database with $sqlite_count items:"
    echo "  $SQLITE_DB"
    echo ""

    # Show breakdown
    echo "Content breakdown:"
    sqlite3 "$SQLITE_DB" "
        SELECT
            CASE metadata_type
                WHEN 1 THEN '  Movies'
                WHEN 2 THEN '  TV Shows'
                WHEN 3 THEN '  Seasons'
                WHEN 4 THEN '  Episodes'
                ELSE '  Other'
            END as type,
            COUNT(*) as count
        FROM metadata_items
        GROUP BY metadata_type
        ORDER BY metadata_type;
    " 2>/dev/null || true
    echo ""

    # Check PostgreSQL connection
    export PGHOST="$PG_HOST"
    export PGPORT="$PG_PORT"
    export PGDATABASE="$PG_DATABASE"
    export PGUSER="$PG_USER"
    export PGPASSWORD="${PLEX_PG_PASSWORD:-plex}"

    if ! psql -c "SELECT 1" >/dev/null 2>&1; then
        echo -e "${RED}ERROR: Cannot connect to PostgreSQL at $PG_HOST:$PG_PORT${NC}"
        echo "Start PostgreSQL first, then run this script again."
        exit 1
    fi

    # Check if PostgreSQL already has data
    local pg_count=$(psql -t -c "SELECT COUNT(*) FROM $PG_SCHEMA.metadata_items;" 2>/dev/null | tr -d ' ' || echo "0")

    if [[ "$pg_count" -gt 0 ]]; then
        echo -e "${YELLOW}PostgreSQL already has $pg_count items.${NC}"
        echo ""
        echo "Options:"
        echo "  1) Skip migration (keep existing PostgreSQL data)"
        echo "  2) Replace PostgreSQL data with SQLite data"
        echo "  3) Cancel installation"
        echo ""
        read -p "Choose [1/2/3]: " choice

        case $choice in
            1)
                echo "Skipping migration."
                return 0
                ;;
            2)
                echo "Will replace PostgreSQL data..."
                ;;
            3)
                echo "Installation cancelled."
                exit 0
                ;;
            *)
                echo "Invalid choice. Cancelling."
                exit 1
                ;;
        esac
    else
        echo -e "${YELLOW}PostgreSQL database is empty.${NC}"
        echo ""
        echo "Do you want to migrate your existing Plex data to PostgreSQL?"
        echo ""
        echo "  ${GREEN}Yes${NC} = Copy all data from SQLite to PostgreSQL"
        echo "  ${RED}No${NC}  = Start fresh (lose existing library data!)"
        echo ""
        read -p "Migrate data? [Y/n]: " migrate_choice

        if [[ "$migrate_choice" =~ ^[Nn] ]]; then
            echo -e "${YELLOW}Skipping migration. Your existing data will NOT be available in Plex.${NC}"
            return 0
        fi
    fi

    # Run migration
    echo ""
    echo -e "${GREEN}=== Starting Migration ===${NC}"
    echo ""

    migrate_sqlite_to_pg

    echo ""
    echo -e "${GREEN}=== Migration Complete ===${NC}"
}

migrate_sqlite_to_pg() {
    local schema="$PG_SCHEMA"

    # Ensure schema exists
    psql -c "CREATE SCHEMA IF NOT EXISTS $schema;" 2>/dev/null || true

    # Load schema if needed
    local table_count=$(psql -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '$schema';" 2>/dev/null | tr -d ' ')
    if [[ "$table_count" -lt 10 ]]; then
        echo "Loading PostgreSQL schema..."
        psql -f "$SHIM_DIR/schema/plex_schema.sql" >/dev/null 2>&1 || true
    fi

    # Get tables from SQLite (exclude FTS and virtual tables)
    local tables=$(sqlite3 "$SQLITE_DB" ".tables" | tr -s ' ' '\n' | grep -v '^$' | grep -v 'fts' | grep -v 'spellfix' | sort)

    local migrated=0
    local failed=0
    local skipped=0

    for table in $tables; do
        local count=$(sqlite3 "$SQLITE_DB" "SELECT COUNT(*) FROM \"$table\";" 2>/dev/null || echo "0")

        if [[ "$count" -gt 0 ]]; then
            printf "  %-35s %8s rows... " "$table" "$count"

            # Get SQLite columns
            local sqlite_cols_raw=$(sqlite3 "$SQLITE_DB" "PRAGMA table_info(\"$table\");" | cut -d'|' -f2)

            if [[ -z "$sqlite_cols_raw" ]]; then
                echo -e "${RED}SKIP (no columns)${NC}"
                continue
            fi

            # Get PostgreSQL columns
            local pg_cols=$(psql -t -c "SELECT string_agg(column_name, ',') FROM information_schema.columns WHERE table_schema = '$schema' AND table_name = '$table';" 2>/dev/null | tr -d ' ')

            if [[ -z "$pg_cols" ]]; then
                echo -e "${YELLOW}SKIP (no PG table)${NC}"
                ((skipped++))
                continue
            fi

            # Get column types from SQLite to detect BLOBs
            local col_types=$(sqlite3 "$SQLITE_DB" "PRAGMA table_info(\"$table\");" | cut -d'|' -f2,3)

            # Find common columns and build quoted lists
            # For BLOB columns, use hex() to convert binary to hex string
            local sqlite_select=""
            local pg_cols_list=""
            for col in $sqlite_cols_raw; do
                if echo ",$pg_cols," | grep -q ",$col,"; then
                    # Check if this column is a BLOB
                    local col_type=$(echo "$col_types" | grep "^$col|" | cut -d'|' -f2)
                    local select_expr
                    if [[ "$col_type" == "BLOB" ]]; then
                        # Use hex() for BLOB columns, prefix with \x for PostgreSQL bytea
                        select_expr="CASE WHEN \"$col\" IS NOT NULL THEN '\\x' || hex(\"$col\") ELSE NULL END AS \"$col\""
                    else
                        select_expr="\"$col\""
                    fi

                    if [[ -z "$sqlite_select" ]]; then
                        sqlite_select="$select_expr"
                        pg_cols_list="\"$col\""
                    else
                        sqlite_select="$sqlite_select,$select_expr"
                        pg_cols_list="$pg_cols_list,\"$col\""
                    fi
                fi
            done

            if [[ -z "$sqlite_select" ]]; then
                echo -e "${RED}SKIP (no common cols)${NC}"
                continue
            fi

            # Export to CSV with quoted columns (handles reserved words like 'index')
            sqlite3 -header -csv "$SQLITE_DB" "SELECT $sqlite_select FROM \"$table\";" > "/tmp/plex_migrate_$table.csv" 2>/dev/null

            if [[ -s "/tmp/plex_migrate_$table.csv" ]]; then
                # Disable triggers for faster import
                psql -q -c "ALTER TABLE $schema.\"$table\" DISABLE TRIGGER ALL;" 2>/dev/null || true

                # Truncate existing data (faster than DELETE, handles FK)
                psql -q -c "TRUNCATE $schema.\"$table\" CASCADE;" 2>/dev/null || true

                # Import with quoted columns (handles reserved words like 'default')
                if psql -q -c "\\copy $schema.\"$table\"($pg_cols_list) FROM '/tmp/plex_migrate_$table.csv' WITH CSV HEADER" 2>/dev/null; then
                    echo -e "${GREEN}OK${NC}"
                    ((migrated++))
                else
                    echo -e "${RED}FAIL${NC}"
                    ((failed++))
                fi

                # Re-enable triggers
                psql -q -c "ALTER TABLE $schema.\"$table\" ENABLE TRIGGER ALL;" 2>/dev/null || true
            else
                echo -e "${YELLOW}EMPTY${NC}"
            fi

            rm -f "/tmp/plex_migrate_$table.csv"
        fi
    done

    echo ""
    echo "Updating sequences..."
    psql -q -c "
        SELECT setval(pg_get_serial_sequence('$schema.metadata_items', 'id'), COALESCE((SELECT MAX(id) FROM $schema.metadata_items), 1));
        SELECT setval(pg_get_serial_sequence('$schema.media_items', 'id'), COALESCE((SELECT MAX(id) FROM $schema.media_items), 1));
        SELECT setval(pg_get_serial_sequence('$schema.media_parts', 'id'), COALESCE((SELECT MAX(id) FROM $schema.media_parts), 1));
        SELECT setval(pg_get_serial_sequence('$schema.tags', 'id'), COALESCE((SELECT MAX(id) FROM $schema.tags), 1));
    " >/dev/null 2>&1 || true

    echo ""
    echo "Migration summary:"
    echo "  Tables migrated: $migrated"
    echo "  Tables skipped:  $skipped"
    echo "  Tables failed:   $failed"

    local pg_total=$(psql -t -c "SELECT COUNT(*) FROM $schema.metadata_items;" 2>/dev/null | tr -d ' ')
    echo "  Total items in PostgreSQL: $pg_total"
}

# ============================================================================
# Run migration check before installing wrappers
# ============================================================================
check_and_migrate

# Backup and install Server wrapper
echo "Installing Plex Media Server wrapper..."
if [[ -f "$PLEX_APP/Plex Media Server" && ! -f "$PLEX_APP/Plex Media Server.original" ]]; then
    # First time - backup original binary
    if file "$PLEX_APP/Plex Media Server" | grep -q "Mach-O"; then
        echo "  Backing up original binary..."
        mv "$PLEX_APP/Plex Media Server" "$PLEX_APP/Plex Media Server.original"
    else
        echo -e "${YELLOW}  Wrapper already installed (not a Mach-O binary)${NC}"
    fi
fi

if [[ -f "$PLEX_APP/Plex Media Server.original" ]]; then
    cat > "$PLEX_APP/Plex Media Server" << 'WRAPPER'
#!/bin/bash
# Plex Media Server wrapper for PostgreSQL shim

SCRIPT_DIR="$(dirname "$0")"
SERVER_BINARY="$SCRIPT_DIR/Plex Media Server.original"
SHIM_DIR="/Users/sander/plex-postgresql"

# PostgreSQL configuration
export PLEX_PG_HOST="${PLEX_PG_HOST:-/tmp}"
export PLEX_PG_PORT="${PLEX_PG_PORT:-5432}"
export PLEX_PG_DATABASE="${PLEX_PG_DATABASE:-plex}"
export PLEX_PG_USER="${PLEX_PG_USER:-plex}"
export PLEX_PG_PASSWORD="${PLEX_PG_PASSWORD:-plex}"
export PLEX_PG_SCHEMA="${PLEX_PG_SCHEMA:-plex}"
export PLEX_MEDIA_SERVER_APPLICATION_SUPPORT_DIR="${PLEX_MEDIA_SERVER_APPLICATION_SUPPORT_DIR:-/Users/sander/Library/Application Support}"

# PostgreSQL shim
export DYLD_INSERT_LIBRARIES="$SHIM_DIR/db_interpose_pg.dylib"

# === Initialization Functions ===

wait_for_postgres() {
    echo "[plex-pg] Waiting for PostgreSQL at $PLEX_PG_HOST:$PLEX_PG_PORT..."
    local max_attempts=30
    local attempt=1

    export PGHOST="$PLEX_PG_HOST"
    export PGPORT="$PLEX_PG_PORT"
    export PGDATABASE="$PLEX_PG_DATABASE"
    export PGUSER="$PLEX_PG_USER"
    export PGPASSWORD="$PLEX_PG_PASSWORD"

    while [ $attempt -le $max_attempts ]; do
        if psql -c "SELECT 1" >/dev/null 2>&1; then
            echo "[plex-pg] PostgreSQL is ready!"
            return 0
        fi
        echo "[plex-pg] Attempt $attempt/$max_attempts - PostgreSQL not ready, waiting..."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo "[plex-pg] WARNING: PostgreSQL did not become ready, continuing anyway..."
    return 1
}

init_pg_schema() {
    local schema="$PLEX_PG_SCHEMA"
    local schema_file="$SHIM_DIR/schema/plex_schema.sql"

    # Create schema if not exists
    psql -c "CREATE SCHEMA IF NOT EXISTS $schema;" 2>/dev/null || true

    # Check if tables exist
    local table_count=$(psql -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '$schema';" 2>/dev/null | tr -d ' ')

    if [ "$table_count" -gt "0" ] 2>/dev/null; then
        echo "[plex-pg] PostgreSQL schema '$schema' ready with $table_count tables"
    else
        echo "[plex-pg] PostgreSQL schema '$schema' is empty, loading schema..."
        if [ -f "$schema_file" ]; then
            if psql -f "$schema_file" >/dev/null 2>&1; then
                local new_count=$(psql -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '$schema';" 2>/dev/null | tr -d ' ')
                echo "[plex-pg] Schema loaded! $new_count tables created."
            else
                echo "[plex-pg] WARNING: Schema load had errors, continuing anyway..."
            fi
        else
            echo "[plex-pg] WARNING: Schema file not found: $schema_file"
        fi
    fi
}

init_sqlite_schema() {
    local db_dir="$PLEX_MEDIA_SERVER_APPLICATION_SUPPORT_DIR/Plex Media Server/Plug-in Support/Databases"
    local schema_file="$SHIM_DIR/schema/sqlite_schema.sql"

    local db_files=(
        "$db_dir/com.plexapp.plugins.library.db"
        "$db_dir/com.plexapp.plugins.library.blobs.db"
    )

    mkdir -p "$db_dir"

    for db_file in "${db_files[@]}"; do
        local db_name=$(basename "$db_file")

        if [ ! -f "$db_file" ]; then
            echo "[plex-pg] Pre-initializing SQLite database $db_name..."
            if [ -f "$schema_file" ]; then
                sqlite3 "$db_file" < "$schema_file" 2>/dev/null || true
                sqlite3 "$db_file" "INSERT OR IGNORE INTO schema_migrations (version) VALUES ('pg_adapter_1.0.0');" 2>/dev/null || true
                echo "[plex-pg] SQLite database $db_name initialized"
            fi
        else
            # Ensure min_version column exists
            if ! sqlite3 "$db_file" "SELECT min_version FROM schema_migrations LIMIT 1" >/dev/null 2>&1; then
                echo "[plex-pg] Adding min_version column to $db_name..."
                sqlite3 "$db_file" "ALTER TABLE schema_migrations ADD COLUMN min_version TEXT;" 2>/dev/null || true
            fi
        fi
    done
}

# === Run Initialization ===
echo "[plex-pg] === Plex PostgreSQL Shim ==="

if command -v psql >/dev/null 2>&1; then
    wait_for_postgres
    init_pg_schema
else
    echo "[plex-pg] WARNING: psql not found, skipping PostgreSQL initialization"
fi

if command -v sqlite3 >/dev/null 2>&1; then
    init_sqlite_schema
else
    echo "[plex-pg] WARNING: sqlite3 not found, skipping SQLite initialization"
fi

echo "[plex-pg] Starting Plex Media Server..."

# Execute the original server
exec "$SERVER_BINARY" "$@"
WRAPPER
    chmod +x "$PLEX_APP/Plex Media Server"
    echo -e "${GREEN}  Server wrapper installed${NC}"
else
    echo -e "${RED}  ERROR: Original binary not found${NC}"
    exit 1
fi

# Backup and install Scanner wrapper
echo "Installing Plex Media Scanner wrapper..."
if [[ -f "$PLEX_APP/Plex Media Scanner" && ! -f "$PLEX_APP/Plex Media Scanner.original" ]]; then
    if file "$PLEX_APP/Plex Media Scanner" | grep -q "Mach-O"; then
        echo "  Backing up original binary..."
        mv "$PLEX_APP/Plex Media Scanner" "$PLEX_APP/Plex Media Scanner.original"
    else
        echo -e "${YELLOW}  Wrapper already installed (not a Mach-O binary)${NC}"
    fi
fi

if [[ -f "$PLEX_APP/Plex Media Scanner.original" ]]; then
    cat > "$PLEX_APP/Plex Media Scanner" << 'WRAPPER'
#!/bin/bash
# Plex Media Scanner wrapper for PostgreSQL shim

SCRIPT_DIR="$(dirname "$0")"
SCANNER_ORIGINAL="$SCRIPT_DIR/Plex Media Scanner.original"

# Ensure PostgreSQL shim is loaded
export DYLD_INSERT_LIBRARIES="${DYLD_INSERT_LIBRARIES:-/Users/sander/plex-postgresql/db_interpose_pg.dylib}"

# Disable shadow database logic
export PLEX_NO_SHADOW_SCAN=1

# Execute the original scanner
exec "$SCANNER_ORIGINAL" "$@"
WRAPPER
    chmod +x "$PLEX_APP/Plex Media Scanner"
    echo -e "${GREEN}  Scanner wrapper installed${NC}"
else
    echo -e "${RED}  ERROR: Original scanner binary not found${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}=== Installation complete ===${NC}"
echo ""
echo "Wrappers installed. Start Plex normally - the shim will be auto-injected."
echo ""
echo "To uninstall:"
echo "  ./scripts/uninstall_wrappers.sh"
