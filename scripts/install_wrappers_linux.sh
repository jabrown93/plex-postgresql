#!/bin/bash
# Install Plex wrapper scripts for PostgreSQL shim (Linux)
# This replaces the Plex binaries with wrapper scripts that inject the shim

set -e

PLEX_DIR="${PLEX_DIR:-/usr/lib/plexmediaserver}"
SHIM_PATH="${SHIM_PATH:-/usr/local/lib/plex-postgresql/db_interpose_pg.so}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SHIM_DIR="$(dirname "$SCRIPT_DIR")"

# Plex data location
if [[ -d "/var/lib/plexmediaserver" ]]; then
    PLEX_SUPPORT_DIR="/var/lib/plexmediaserver/Library/Application Support/Plex Media Server"
else
    PLEX_SUPPORT_DIR="$HOME/Library/Application Support/Plex Media Server"
fi
SQLITE_DB="$PLEX_SUPPORT_DIR/Plug-in Support/Databases/com.plexapp.plugins.library.db"

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

echo "=== Plex PostgreSQL Wrapper Installer (Linux) ==="
echo ""

# Check if running as root
if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}ERROR: This script must be run as root${NC}"
    echo "  sudo $0"
    exit 1
fi

# Check if shim exists
if [[ ! -f "$SHIM_PATH" ]]; then
    echo -e "${RED}ERROR: Shim not found at $SHIM_PATH${NC}"
    echo "Build and install the shim first:"
    echo "  make linux"
    echo "  sudo make install"
    exit 1
fi

# Check if Plex is running
if pgrep -f "Plex Media Server" >/dev/null 2>&1; then
    echo -e "${YELLOW}WARNING: Plex is running. Stop it first:${NC}"
    echo "  sudo systemctl stop plexmediaserver"
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
        echo -e "  ${GREEN}Yes${NC} = Copy all data from SQLite to PostgreSQL"
        echo -e "  ${RED}No${NC}  = Start fresh (lose existing library data!)"
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
if [[ -f "$PLEX_DIR/Plex Media Server" && ! -f "$PLEX_DIR/Plex Media Server.original" ]]; then
    if file "$PLEX_DIR/Plex Media Server" | grep -q "ELF"; then
        echo "  Backing up original binary..."
        mv "$PLEX_DIR/Plex Media Server" "$PLEX_DIR/Plex Media Server.original"
    else
        echo -e "${YELLOW}  Wrapper already installed (not an ELF binary)${NC}"
    fi
fi

if [[ -f "$PLEX_DIR/Plex Media Server.original" ]]; then
    cat > "$PLEX_DIR/Plex Media Server" << 'WRAPPER'
#!/bin/bash
# Plex Media Server wrapper for PostgreSQL shim

SCRIPT_DIR="$(dirname "$0")"
SERVER_BINARY="$SCRIPT_DIR/Plex Media Server.original"

# PostgreSQL shim
export LD_PRELOAD="/usr/local/lib/plex-postgresql/db_interpose_pg.so"
export PLEX_PG_HOST="${PLEX_PG_HOST:-localhost}"
export PLEX_PG_PORT="${PLEX_PG_PORT:-5432}"
export PLEX_PG_DATABASE="${PLEX_PG_DATABASE:-plex}"
export PLEX_PG_USER="${PLEX_PG_USER:-plex}"
export PLEX_PG_PASSWORD="${PLEX_PG_PASSWORD:-}"
export PLEX_PG_SCHEMA="${PLEX_PG_SCHEMA:-plex}"
export PLEX_PG_POOL_SIZE="${PLEX_PG_POOL_SIZE:-50}"

# Execute the original server
exec "$SERVER_BINARY" "$@"
WRAPPER
    chmod +x "$PLEX_DIR/Plex Media Server"
    echo -e "${GREEN}  Server wrapper installed${NC}"
else
    echo -e "${RED}  ERROR: Original binary not found${NC}"
    exit 1
fi

# Backup and install Scanner wrapper
echo "Installing Plex Media Scanner wrapper..."
if [[ -f "$PLEX_DIR/Plex Media Scanner" && ! -f "$PLEX_DIR/Plex Media Scanner.original" ]]; then
    if file "$PLEX_DIR/Plex Media Scanner" | grep -q "ELF"; then
        echo "  Backing up original binary..."
        mv "$PLEX_DIR/Plex Media Scanner" "$PLEX_DIR/Plex Media Scanner.original"
    else
        echo -e "${YELLOW}  Wrapper already installed (not an ELF binary)${NC}"
    fi
fi

if [[ -f "$PLEX_DIR/Plex Media Scanner.original" ]]; then
    cat > "$PLEX_DIR/Plex Media Scanner" << 'WRAPPER'
#!/bin/bash
# Plex Media Scanner wrapper for PostgreSQL shim

SCRIPT_DIR="$(dirname "$0")"
SCANNER_ORIGINAL="$SCRIPT_DIR/Plex Media Scanner.original"

# Ensure PostgreSQL shim is loaded
export LD_PRELOAD="${LD_PRELOAD:-/usr/local/lib/plex-postgresql/db_interpose_pg.so}"

# Execute the original scanner
exec "$SCANNER_ORIGINAL" "$@"
WRAPPER
    chmod +x "$PLEX_DIR/Plex Media Scanner"
    echo -e "${GREEN}  Scanner wrapper installed${NC}"
else
    echo -e "${RED}  ERROR: Original scanner binary not found${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}=== Installation complete ===${NC}"
echo ""
echo "Configure PostgreSQL connection in /etc/default/plexmediaserver:"
echo "  PLEX_PG_HOST=localhost"
echo "  PLEX_PG_DATABASE=plex"
echo "  PLEX_PG_USER=plex"
echo "  PLEX_PG_PASSWORD=yourpassword"
echo ""
echo "Then start Plex:"
echo "  sudo systemctl start plexmediaserver"
echo ""
echo "To uninstall:"
echo "  sudo ./scripts/uninstall_wrappers_linux.sh"
