# plex-postgresql

Run Plex Media Server with PostgreSQL instead of SQLite.

This project provides a shim library that intercepts Plex's SQLite calls and redirects them to PostgreSQL, allowing you to use a more scalable and robust database backend.

> **⚠️ Platform support**: macOS (uses `DYLD_INTERPOSE`) and Linux (uses `LD_PRELOAD`). Docker support included for easy testing.

## Features

- **Transparent interception** - Uses `DYLD_INTERPOSE` (macOS) or `LD_PRELOAD` (Linux)
- **Full SQL translation** - Automatically converts SQLite syntax to PostgreSQL
- **Zero Plex modifications** - Works with stock Plex Media Server
- **PostgreSQL-only mode** - Can run entirely on PostgreSQL without SQLite

## Requirements

### macOS
- Apple Silicon or Intel
- PostgreSQL 15+
- Plex Media Server
- Xcode Command Line Tools

### Linux
- GCC and build tools
- libpq-dev (PostgreSQL client library)
- libsqlite3-dev
- Plex Media Server

### Docker
- Docker and Docker Compose

## Quick Start (Docker)

> **Note**: Docker support with linuxserver/plex is experimental due to library conflicts.
> For production use, we recommend native Linux installation.

```bash
# Clone the repository
git clone https://github.com/yourusername/plex-postgresql.git
cd plex-postgresql

# Edit docker-compose.yml to set your media path
# Change /path/to/media to your actual media directory

# Start PostgreSQL and Plex
docker-compose up -d

# View logs
docker-compose logs -f plex
```

Access Plex at http://localhost:32400/web

## Quick Start (Native macOS)

### 1. Install PostgreSQL

```bash
brew install postgresql@15
brew services start postgresql@15
```

### 2. Create Database

```bash
createdb -U postgres plex
psql -U postgres -d plex -c "CREATE SCHEMA plex;"
psql -U postgres -d plex -f schema/plex_schema.sql
```

### 3. Migrate Data from SQLite

```bash
./scripts/migrate_sqlite_to_pg.sh
```

### 4. Build the Shim

```bash
make
```

### 5. Start Plex with PostgreSQL

```bash
./start_plex_pg.sh
```

Or use the LaunchAgent for automatic startup:

```bash
cp launchd/com.plex.postgresql.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.plex.postgresql.plist
```

## Quick Start (Native Linux)

### 1. Install Dependencies

```bash
# Debian/Ubuntu
sudo apt-get install build-essential libpq-dev libsqlite3-dev postgresql-15

# Start PostgreSQL
sudo systemctl start postgresql
```

### 2. Create Database

```bash
sudo -u postgres createdb plex
sudo -u postgres psql -d plex -c "CREATE SCHEMA plex;"
sudo -u postgres psql -d plex -f schema/plex_schema.sql
```

### 3. Build the Shim

```bash
make linux
```

### 4. Start Plex with PostgreSQL

```bash
export LD_PRELOAD=$(pwd)/db_interpose_pg.so
export PLEX_PG_HOST=localhost
export PLEX_PG_DATABASE=plex
export PLEX_PG_USER=plex
/usr/lib/plexmediaserver/Plex\ Media\ Server
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PLEX_PG_HOST` | localhost | PostgreSQL host |
| `PLEX_PG_PORT` | 5432 | PostgreSQL port |
| `PLEX_PG_DATABASE` | plex | Database name |
| `PLEX_PG_USER` | plex | Database user |
| `PLEX_PG_PASSWORD` | (empty) | Database password |
| `PLEX_PG_SCHEMA` | plex | Schema name |

## How It Works

The shim library (`db_interpose_pg.dylib`) intercepts calls to SQLite functions:

1. `sqlite3_open` / `sqlite3_open_v2` - Opens PostgreSQL connection
2. `sqlite3_prepare_v2` - Translates SQL and prepares statement
3. `sqlite3_step` - Executes query on PostgreSQL
4. `sqlite3_column_*` - Returns results from PostgreSQL

SQL translation handles:
- `?` placeholders → `$1, $2, ...` parameters
- `IFNULL()` → `COALESCE()`
- `GROUP_CONCAT()` → `STRING_AGG()`
- `strftime()` → `TO_CHAR()`
- Boolean `1/0` ↔ `true/false`
- And many more SQLite-specific functions

## Project Structure

```
plex-postgresql/
├── src/
│   ├── db_interpose_pg.c        # macOS shim (DYLD_INTERPOSE)
│   ├── db_interpose_pg_linux.c  # Linux shim (LD_PRELOAD)
│   └── sql_translator.c         # SQL translation engine
├── include/
│   └── sql_translator.h         # Header files
├── schema/
│   └── plex_schema.sql          # PostgreSQL schema
├── scripts/
│   ├── migrate_sqlite_to_pg.sh
│   └── start_plex_pg.sh
├── launchd/
│   └── com.plex.postgresql.plist
├── Dockerfile                    # Docker build for Linux
├── docker-compose.yml            # Complete stack with PostgreSQL
├── Makefile
└── README.md
```

## Performance

PostgreSQL typically provides:
- Better concurrent access (no database locking)
- Improved query performance with proper indexing
- More robust crash recovery
- Better scalability for large libraries

Recommended PostgreSQL settings for Plex:

```sql
ALTER SYSTEM SET shared_buffers = '512MB';
ALTER SYSTEM SET work_mem = '16MB';
ALTER SYSTEM SET random_page_cost = 1.1;
```

## Limitations

- Some SQLite-specific features may not be fully supported
- Requires initial data migration from SQLite
- Testing needed for all Plex workflows

## License

MIT License - see [LICENSE](LICENSE)

## Disclaimer

This is an unofficial project and is not affiliated with Plex Inc. Use at your own risk. Always maintain backups of your Plex database.
