#!/bin/bash
# Migrate Plex SQLite database to PostgreSQL

set -e

# Configuration
SQLITE_DB="$HOME/Library/Application Support/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db"
PG_HOST="${PLEX_PG_HOST:-localhost}"
PG_PORT="${PLEX_PG_PORT:-5432}"
PG_DATABASE="${PLEX_PG_DATABASE:-plex}"
PG_USER="${PLEX_PG_USER:-plex}"
PG_SCHEMA="${PLEX_PG_SCHEMA:-plex}"

PSQL="psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DATABASE"

echo "=== Plex SQLite to PostgreSQL Migration ==="
echo "SQLite: $SQLITE_DB"
echo "PostgreSQL: $PG_USER@$PG_HOST:$PG_PORT/$PG_DATABASE (schema: $PG_SCHEMA)"
echo ""

# Check if SQLite database exists
if [[ ! -f "$SQLITE_DB" ]]; then
    echo "ERROR: SQLite database not found: $SQLITE_DB"
    exit 1
fi

# Check PostgreSQL connection
if ! $PSQL -c "SELECT 1" >/dev/null 2>&1; then
    echo "ERROR: Cannot connect to PostgreSQL"
    exit 1
fi

echo "Step 1: Creating schema..."
$PSQL -c "CREATE SCHEMA IF NOT EXISTS $PG_SCHEMA;"

echo "Step 2: Applying schema..."
$PSQL -f "$(dirname "$0")/../schema/plex_schema.sql"

echo "Step 3: Exporting tables from SQLite..."

# Get list of tables
TABLES=$(sqlite3 "$SQLITE_DB" ".tables" | tr -s ' ' '\n' | grep -v '^$' | sort)

for TABLE in $TABLES; do
    echo "  Migrating: $TABLE"

    # Get column names
    COLUMNS=$(sqlite3 "$SQLITE_DB" "PRAGMA table_info($TABLE);" | cut -d'|' -f2 | tr '\n' ',' | sed 's/,$//')

    # Export to CSV and import to PostgreSQL
    sqlite3 -header -csv "$SQLITE_DB" "SELECT * FROM $TABLE;" > "/tmp/plex_migrate_$TABLE.csv"

    if [[ -s "/tmp/plex_migrate_$TABLE.csv" ]]; then
        $PSQL -c "\\copy $PG_SCHEMA.$TABLE FROM '/tmp/plex_migrate_$TABLE.csv' WITH CSV HEADER" 2>/dev/null || true
    fi

    rm -f "/tmp/plex_migrate_$TABLE.csv"
done

echo ""
echo "Step 4: Updating sequences..."
$PSQL -c "
SELECT setval(pg_get_serial_sequence('$PG_SCHEMA.metadata_items', 'id'),
       COALESCE((SELECT MAX(id) FROM $PG_SCHEMA.metadata_items), 1));
" 2>/dev/null || true

echo ""
echo "=== Migration Complete ==="
echo ""
echo "Verify with:"
echo "  $PSQL -c 'SELECT COUNT(*) FROM $PG_SCHEMA.metadata_items;'"
