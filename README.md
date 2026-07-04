# plex-postgresql

[![en](https://img.shields.io/badge/lang-en-red.svg)](README.md)
[![es](https://img.shields.io/badge/lang-es-yellow.svg)](README.es.md)
[![Build](https://img.shields.io/github/actions/workflow/status/jabrown93/plex-postgresql/docker-release.yml?label=build)](https://github.com/jabrown93/plex-postgresql/actions/workflows/docker-release.yml)
[![Issues](https://img.shields.io/github/issues/jabrown93/plex-postgresql)](https://github.com/jabrown93/plex-postgresql/issues)
[![License](https://img.shields.io/github/license/jabrown93/plex-postgresql)](LICENSE)

**Run Plex Media Server with PostgreSQL instead of SQLite.**

A shim library that intercepts Plex's SQLite calls and redirects them to PostgreSQL. Zero Plex modifications required.

## Installation

This fork is packaged and maintained as a **Docker image only** — `docker-compose.yml` builds the shim from this repo's own source against a pinned `linuxserver/plex` base, so the container always matches the code here. There are no standalone `.dylib`/`.so` binaries or native-install scripts published from this fork.

> Looking for a native macOS or Linux install (no Docker)? That's only available from the upstream project this was forked from: [cgnl/plex-postgresql](https://github.com/cgnl/plex-postgresql/releases).

### Quick Install

```bash
git clone https://github.com/jabrown93/plex-postgresql.git
cd plex-postgresql
docker-compose up -d
```

See [Quick Start (Docker)](#quick-start-docker) below for fresh-install vs. migration-from-SQLite instructions.

## Platform Support

| Platform | Architecture | Status |
|----------|-------------|---------|
| Docker | x86_64 + ARM64 | ✅ Multi-arch, built from this repo's source |

## Why PostgreSQL?

SQLite is great for most Plex installations, but has one major limitation: **database locking**.

- **No more locking** - SQLite locks the entire database during writes. Library scans block playback. Concurrent scans queue up. With PostgreSQL, everything runs simultaneously - scan your libraries while streaming without interruption.
- **Remote storage** - Better I/O patterns for rclone, Real-Debrid, or cloud storage setups.
- **Large libraries** - PostgreSQL's query optimizer handles 10K+ movies and 50K+ episodes efficiently.
- **Standard tooling** - pg_dump for backups, replication, any PostgreSQL client for debugging.

## Benchmark Results

### Concurrent Access (The Real Problem)

Real-world test: **Plex + Kometa + PMM + 4 concurrent streams** (7 separate processes, 15 seconds):

| Metric | SQLite | PostgreSQL (TCP) | PostgreSQL (Socket) |
|--------|--------|------------------|---------------------|
| Total Writes | 727,330 | 25,851 | 27,543 |
| **Write Errors** | **592,664 (81%)** | **0** | **0** |
| Total Reads | 5,173 | 1,115 | 1,121 |
| Read Errors | 0 | 0 | 0 |

**What this means:**
- SQLite: 81% of writes fail due to database locking
- SQLite: ~2.4 million errors per minute under load
- PostgreSQL: Zero errors, everything works simultaneously
- Unix socket: ~6% faster than TCP (negligible for most setups)

### Query Latency Comparison

| Query Type | SQLite | PostgreSQL (Socket) | Overhead |
|------------|--------|---------------------|----------|
| SELECT (PK lookup) | 3.9 µs | 18.2 µs | 4.6x |
| INSERT (batched) | 0.7 µs | 15.5 µs | 22x |
| Range Query | 22.0 µs | 45.2 µs | 2.1x |

PostgreSQL is slower per-query, but **never locks**. For Plex + rclone/Real-Debrid, smooth playback matters more than raw speed.

### Shim Overhead

| Component | Latency | Throughput |
|-----------|---------|------------|
| SQL Translation (uncached) | 17.5 µs | 57K/sec |
| **SQL Translation (cached)** | **0.12 µs** | **8.5M/sec** |
| Cache Lookup | 22.6 ns | 354M/sec |

The thread-local translation cache provides **145x speedup** for repeated queries. Shim overhead is **<1% of total query time**.

### Run Benchmarks

```bash
# Multi-process stress test (the definitive proof)
PLEX_PG_SOCKET=/tmp python3 scripts/benchmark_multiprocess.py

# SQLite vs PostgreSQL latency comparison
python3 tests/bench_sqlite_vs_pg.py

# Shim component micro-benchmarks
make benchmark

# Cache implementation comparison (mutex vs thread-local)
./tests/bin/bench_cache
```

For rclone/Real-Debrid setups with Kometa/PMM, **SQLite becomes unusable** during library scans. PostgreSQL handles it without issues.

## What's New in v0.9.2

### Critical Bug Fix: Timeline 500 Error

**Problem:** `/:/timeline` endpoint returned HTTP 500 errors during playback statistics recording with `Got exception from request handler: std::exception`.

**Root Cause:** Plex calls `sqlite3_last_insert_rowid()` with a different database handle than the one used for INSERT operations. The shim's connection lookup returned NULL, causing `last_insert_rowid()` to return 0, which triggered exceptions in Plex.

**Solution:** Enhanced `last_insert_rowid()` with fallback connection lookup using `pg_find_any_library_connection()` when exact handle match fails. Additionally, sequence advancement now occurs BEFORE skipping empty statistics_media INSERTs to ensure valid rowid values.

**Impact:**
- ✅ Timeline requests return HTTP 200 (was 500)
- ✅ Playback statistics correctly recorded
- ✅ Empty statistics_media rows still prevented (no 310M row accumulation)
- ✅ Multiple consecutive timeline requests succeed
- ✅ No SQLITE_MISUSE errors

**Technical Details:** See [upstream's v0.9.2 release notes](https://github.com/cgnl/plex-postgresql/releases/tag/v0.9.2) for the original writeup of this fix (this fork's Docker image already includes it).

## Quick Start (Docker)

The easiest way to run Plex with PostgreSQL - works on **all platforms** (Linux, macOS, Windows).

### Fresh Installation (No Existing Plex Database)

```bash
git clone https://github.com/jabrown93/plex-postgresql.git
cd plex-postgresql

# Start Plex + PostgreSQL
docker-compose up -d

# Check logs
docker-compose logs -f plex
```

**Setup:**
1. Open http://localhost:8080/web
2. Claim your server with Plex account
3. Add libraries via web interface
4. Done! Your libraries are stored in PostgreSQL

**What happens:**
- ✅ PostgreSQL schema auto-created (empty)
- ✅ v0.9.2 fixes active (timeline errors fixed)
- ✅ Multi-arch support (x86_64 + ARM64)
- ✅ All directories pre-created (Plug-ins, Metadata, Cache)
- ✅ No crashes, stable operation

### Migration from Existing SQLite Database

To migrate your existing Plex library to PostgreSQL:

1. **Edit `docker-compose.yml`**, uncomment and update the source database path:
   ```yaml
   volumes:
     - plex_config:/config
     - postgres_socket:/var/run/postgresql
     # Uncomment and edit this line:
     - "/path/to/your/Plex Media Server/Plug-in Support/Databases:/source-db:ro"
   ```

2. **Platform-specific paths:**
   - **macOS**: `"${HOME}/Library/Application Support/Plex Media Server/Plug-in Support/Databases:/source-db:ro"`
   - **Linux**: `"/var/lib/plexmediaserver/Library/Application Support/Plex Media Server/Plug-in Support/Databases:/source-db:ro"`
   - **Windows**: `"C:/Users/YourName/AppData/Local/Plex Media Server/Plug-in Support/Databases:/source-db:ro"`

3. **Start containers:**
   ```bash
   docker-compose up -d
   ```

4. **Monitor migration:**
   ```bash
   docker-compose logs -f plex | grep -E "migration|Migration"
   ```

**Migration performs:**
- ✅ Automatic detection of SQLite database
- ✅ Full data migration (all tables, metadata, posters, etc.)
- ✅ Tested: 34 tables, 89K+ items migrated successfully
- ✅ Original SQLite database remains unchanged (read-only mount)
- ✅ Automatic sequence updates
- ✅ Progress reporting per table

### Configuration

Default PostgreSQL connection (via Unix socket for best performance):
```yaml
environment:
  - PLEX_PG_HOST=/var/run/postgresql  # Unix socket (7% faster)
  - PLEX_PG_DATABASE=plex
  - PLEX_PG_USER=plex
  - PLEX_PG_PASSWORD=plex
  - PLEX_PG_SCHEMA=plex
  - PLEX_PG_POOL_SIZE=50
  - PLEX_PG_LOG_LEVEL=DEBUG  # 0=ERROR, 1=INFO, 2=DEBUG
```

To use TCP instead of Unix socket:
```yaml
environment:
  - PLEX_PG_HOST=postgres  # TCP connection
  - PLEX_PG_PORT=5432
```

Mount your media libraries:
```yaml
volumes:
  - /path/to/movies:/movies:ro
  - /path/to/tv:/tv:ro
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PLEX_PG_HOST` | localhost | PostgreSQL host (or socket directory like `/tmp`) |
| `PLEX_PG_PORT` | 5432 | PostgreSQL port |
| `PLEX_PG_DATABASE` | plex | Database name |
| `PLEX_PG_USER` | plex | Database user |
| `PLEX_PG_PASSWORD` | (empty) | Database password |
| `PLEX_PG_SCHEMA` | plex | Schema name |
| `PLEX_PG_POOL_SIZE` | 50 | Connection pool size (max 100) |
| `PLEX_PG_LOG_LEVEL` | 1 | 0=ERROR, 1=INFO, 2=DEBUG |

### Unix Socket vs TCP

For local PostgreSQL, Unix sockets are ~5-6% faster than TCP:

```bash
# Use Unix socket (recommended for local PostgreSQL)
export PLEX_PG_HOST=/tmp  # or /var/run/postgresql on Linux

# Use TCP (required for remote PostgreSQL)
export PLEX_PG_HOST=localhost
```

The performance difference is minimal - the real benefit of PostgreSQL is zero locking, not connection speed.

## How It Works

```
macOS:  Plex → SQLite API → DYLD_INTERPOSE shim → SQL Translator → PostgreSQL
Linux:  Plex → SQLite API → LD_PRELOAD shim    → SQL Translator → PostgreSQL
Docker: Plex → SQLite API → LD_PRELOAD shim    → SQL Translator → PostgreSQL (container)
```

The shim intercepts all `sqlite3_*` calls, translates SQL syntax (placeholders, functions, types), and executes on PostgreSQL via libpq.

### Architecture

The codebase uses a modular architecture with platform-specific cores:

```
src/
├── db_interpose_core.c        # macOS: DYLD_INTERPOSE + fishhook
├── db_interpose_core_linux.c  # Linux: LD_PRELOAD + dlsym(RTLD_NEXT)
├── db_interpose_*.c           # Shared: open, exec, prepare, bind, step, column, metadata
├── sql_translator.c           # SQLite → PostgreSQL SQL translation
├── sql_tr_*.c                 # Translation modules: functions, types, quotes, etc.
└── pg_*.c                     # PostgreSQL client, connection pool, statement cache
```

### Key Features

- **Connection pooling** - Efficient reuse of PostgreSQL connections
- **SQL translation** - Automatic SQLite → PostgreSQL syntax conversion
- **Prepared statements** - Query caching for performance
- **Schema initialization** - Auto-creates PostgreSQL schema on first run
- **Circular reference protection** - Trigger prevents self-referential parent_id crashes
- **Stack overflow protection** - Multi-layer defense against crashes (see below)
- **Auto-build** - Wrapper automatically rebuilds shim if dylib is missing

### SQL Translation Features

The translator handles SQLite-specific syntax automatically:

| SQLite | PostgreSQL |
|--------|------------|
| `COLLATE NOCASE` | `LOWER()` comparisons |
| `WHERE column LIKE '%x%' COLLATE NOCASE` | `WHERE column ILIKE '%x%'` |
| `WHERE 0` / `WHERE 1` | `WHERE FALSE` / `WHERE TRUE` |
| `iif(cond, a, b)` | `CASE WHEN cond THEN a ELSE b END` |
| `strftime('%s', x)` | `EXTRACT(EPOCH FROM x)::bigint` |
| `IFNULL(a, b)` | `COALESCE(a, b)` |
| `title MATCH 'action -comedy'` | FTS with `!` negation |
| `title MATCH 'term1 AND term2'` | FTS with `&` operator |
| `title MATCH '"exact phrase"'` | FTS with `<->` adjacency |
| `?` placeholders | `$1, $2, ...` numbered params |

### Stack Protection

Plex uses small thread stacks (544KB) which can overflow during complex queries. The shim provides multi-layer protection:

| Layer | Threshold | Action |
|-------|-----------|--------|
| Worker delegation | < 400KB remaining | Delegate to 8MB worker thread |
| Hard protection (normal) | < 64KB remaining | Return SQLITE_NOMEM |
| Hard protection (worker) | < 32KB remaining | Return SQLITE_NOMEM |

This prevents stack overflow crashes that occurred with deep recursive queries (e.g., OnDeck with 218 recursive frames).

## Testing

Run unit tests to validate the shim:

```bash
# All unit tests (87 tests total)
make unit-test

# Individual test suites
make test-recursion      # Recursion guards, loop detection (11 tests)
make test-crash          # Production crash scenarios (21 tests)
make test-sql            # SQL translation (32 tests)
make test-cache          # Query cache logic (16 tests)
make test-tls            # Thread-local storage (7 tests)

# Benchmarks
make benchmark           # Shim component micro-benchmarks
```

### Benchmarks

Compare SQLite vs PostgreSQL (TCP and Unix socket) performance:

```bash
# Multi-process stress test (the definitive proof)
PLEX_PG_SOCKET=/tmp python3 scripts/benchmark_multiprocess.py

# Library scan + playback simulation  
PLEX_PG_SOCKET=/tmp python3 scripts/benchmark_plex_stress.py

# Concurrent writers test
PLEX_PG_SOCKET=/tmp python3 scripts/benchmark_locking.py

# Query performance comparison
python3 scripts/benchmark_compare.py

# Bash benchmark (use --socket for Unix socket mode)
./scripts/benchmark.sh           # TCP mode
./scripts/benchmark.sh --socket  # Unix socket mode
```

The stack protection test validates all protection layers by simulating low-stack conditions without running Plex.

## Known Issues

### ✅ FIXED in v0.9.2: Timeline 500 Error

**Status:** Fixed in v0.9.2  
**Issue:** `/:/timeline` endpoint returned HTTP 500 errors during playback with `std::exception`  
**Root Cause:** `sqlite3_last_insert_rowid()` called with different database handle, causing NULL connection lookup  
**Solution:** Fallback connection lookup + sequence advancement before skipping empty INSERTs  
**Action:** None — already included in this fork's Docker image

See [What's New](#whats-new-in-v092) for details.

### ✅ FIXED in v0.8.12: TV Shows HTTP 500 Error

**Status:** Fixed in v0.8.12  
**Issue:** TV shows endpoint returned HTTP 500 with `std::bad_cast` exceptions  
**Root Cause:** Plex's SOCI library bug with BIGINT aggregate functions (count, sum, etc.)  
**Solution:** Aggregate functions declare as TEXT type to bypass SOCI's strict integer type checking  
**Impact:** TV shows now load correctly, MetadataCounterCache rebuilds work

## Known Limitations

### PostgreSQL Type Mapping

The shim translates SQLite types to PostgreSQL equivalents:

- **INTEGER** → INT4 (32-bit) or INT8 (64-bit based on context)
- **BIGINT** → INT8 (64-bit) - ✅ Fixed in v0.8.12
- **Aggregate functions** (count, sum, max, min, avg) → Declared as TEXT with 64-bit values
  - **Why TEXT?** Workaround for SOCI Issue #1190 - forces SOCI to use text-to-integer conversion which works correctly
  - **Impact:** None - values are still 64-bit integers, just declared differently to SOCI

### SOCI Type System Workaround

**Background:** Plex uses SOCI ORM which has a bug (SOCI Issue #1190) parsing BIGINT values from aggregate functions.

**Our solution (v0.8.12+):**
- Aggregate functions declare as TEXT type to SOCI
- Data is still 64-bit integers from PostgreSQL
- SOCI's text-to-int conversion works correctly
- Bypasses SOCI's buggy native BIGINT handling

**Impact:** Transparent to Plex - all functionality works correctly.

## Troubleshooting

```bash
# Check PostgreSQL
pg_isready -h localhost -U plex

# Check logs (Docker)
docker-compose logs -f plex

# Analyze fallbacks
./scripts/analyze_fallbacks.sh
```

### Common Issues

**Plex won't start**: Check if PostgreSQL is running and accessible.

**Database errors**: Ensure the schema exists: `psql -U plex -d plex -c "CREATE SCHEMA IF NOT EXISTS plex;"`

**Docker port conflict**: Change port in `docker-compose.yml` if 8080 is in use.

## License

MIT - See [LICENSE](LICENSE)

---
*Unofficial project, not affiliated with Plex Inc. Use at your own risk.*
