# Changelog

All notable changes to plex-postgresql will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.9.5] - 2026-01-12

### Fixed
- **Row index -1 out of bounds error** - libpq "row number -1 is out of range" error
  - Root cause: WRITE statements with RETURNING set `current_row = -1`
  - Column functions using fake values could access libpq with invalid row index
  - Added `row_idx >= 0` check to all fake value access points
  - Column functions now handle all PostgreSQL statements properly (not just those with results)

- **INSERT...RETURNING result storage causing issues**
  - Don't store RETURNING result for WRITE statements
  - SOCI uses `lastval()` via SQL translation, not the RETURNING columns
  - Prevents confusion from mixing WRITE and READ result handling

### Changed
- Column functions now use simpler `pg_stmt->is_pg` check instead of `is_pg == 2 || (is_pg == 1 && result)`
- This ensures proper fallback behavior for all PostgreSQL-intercepted statements

## [0.8.9.1] - 2026-01-12

### Fixed
- **Memory corruption when clearing metadata results** - Race condition in PQclear()
  - Root cause: v0.8.9's `clear_metadata_result_if_needed()` called `PQclear()` during bind operations
  - This caused race conditions when multiple threads accessed the same prepared statement result
  - Crash in libpq's `resetPQExpBuffer` with corrupted address `0x4d55545a00000000` (ASCII "MUTZ")
  - Solution: Don't call `PQclear()` in bind functions - set `metadata_only_result = 2` instead
  - Actual cleanup now handled safely in `step()` where proper locking is in place

### Changed
- `clear_metadata_result_if_needed()` now sets flag to 2 instead of calling PQclear()
- `step()` checks for `metadata_only_result == 2` to safely cleanup and re-execute

## [0.8.9] - 2026-01-11

### Fixed
- **Metadata-only results blocking step() re-execution** - "Step didn't return row" errors
  - Root cause: `ensure_pg_result_for_metadata()` executed queries BEFORE parameters were bound
  - This cached 0-row results, and `step()` saw the cached result instead of re-executing
  - Solution: Added `metadata_only_result` flag to track pre-step execution
  - Bind functions now clear this cached result via `clear_metadata_result_if_needed()`
  - `step()` properly re-executes with bound parameters

### Changed
- All 9 bind functions now call `clear_metadata_result_if_needed()` before binding
- `ensure_pg_result_for_metadata()` sets `metadata_only_result = 1` flag
- `step()` clears the flag after successful execution

## [0.8.8] - 2026-01-11

### Fixed
- **Bind functions not checking cached statement registry** - Race condition for cached statements
  - Root cause: `pg_find_stmt()` only checked primary registry, returning NULL for cached statements
  - This caused bind operations on cached statements to have no mutex protection
  - Solution: Use `pg_find_any_stmt()` which checks BOTH primary and cached registries
  - Applied to all 9 bind functions for consistent thread-safety

- **Auto-reset busy statements before binding**
  - Added `ensure_stmt_not_busy()` helper to auto-reset statements that are still in-use
  - Prevents SQLITE_MISUSE (21) "bind on busy prepared statement" errors
  - Called before every bind operation

## [0.8.7] - 2026-01-11

### Fixed
- **Deadlock when bind/reset trigger column functions** - std::exception crashes
  - Root cause: Non-recursive mutex caused deadlock when bind/reset internally triggered column operations
  - Solution: Use `PTHREAD_MUTEX_RECURSIVE` for statement mutex
  - Allows same thread to re-lock mutex without deadlock

## [0.8.6] - 2026-01-11

### Fixed
- **Thread-safety race condition in reset/clear_bindings** - Additional "bind on busy prepared statement" fix
  - Root cause: `sqlite3_reset()` and `sqlite3_clear_bindings()` released mutex BEFORE calling original SQLite
  - Solution: Hold mutex during entire `orig_sqlite3_reset()` and `orig_sqlite3_clear_bindings()` calls
  - Completes thread-safety fix started in v0.8.5

## [0.8.5] - 2026-01-11

### Fixed
- **Thread-safety race condition in bind operations** - "bind on busy prepared statement" errors
  - Root cause: Mutex was acquired AFTER calling SQLite, not before
  - Solution: Lock mutex BEFORE calling `orig_sqlite3_bind_*()` in all 9 bind functions
  - Prevents concurrent access when Thread A is stepping while Thread B is binding

- **lastval() error causing 500 on playQueues** - PostgreSQL error when no INSERT done yet
  - Root cause: `sqlite3_last_insert_rowid()` called `SELECT lastval()` which fails if no INSERT
  - Solution: Gracefully return 0 (like SQLite does) instead of propagating error

### Changed
- `make macos` now auto-cleans before building to prevent corrupt object files

## [0.8.1] - 2026-01-10

### Fixed
- **std::bad_cast exceptions** - SOCI ORM type conversion failures caused 500 errors
  - Root cause: `column_decltype()` returned NULL, causing SOCI type mismatch
  - Solution: Map PostgreSQL OIDs to SQLite-compatible type strings (INTEGER, REAL, TEXT, BLOB)
  - Types now match what `column_type()` returns, ensuring SOCI consistency

### Added
- **Robust C++ exception handler** (Linux only):
  - Per-exception-type tracking with stack traces for first occurrence of each type
  - Automatic source detection: "SHIM-RELATED" vs "external C++ code"
  - Library identification via `dladdr()` runtime linker
  - C++ symbol demangling via `__cxa_demangle`
  - Manual `/proc/self/maps` parsing (musl-compatible, no sscanf)
  - Throttling after 50 exceptions with type summary
- **musl build script** (`build_shim_musl.sh`) for Alpine/musl-based containers

### Changed
- Exception context tracking uses volatile globals instead of TLS (musl compatibility)
- Stack frame collection works on both ARM64 and x86_64

## [0.8.0] - 2026-01-10

### Added
- `sqlite3_column_decltype` interception for SOCI ORM compatibility
- `sqlite3_bind_parameter_index` for named parameter support
- Thread-local SQL translation cache with 512 entries per thread
- `ensure_pg_result_for_metadata()` for pre-step metadata access
- Comprehensive benchmark suite:
  - `tests/bench_cache.c` - Cache implementation comparison
  - `tests/bench_sqlite_vs_pg.py` - SQLite vs PostgreSQL latency
  - `tests/bench_translation.c` - SQL translation throughput
- Stack protection tests for macOS and Linux
- VERSION file for release tracking

### Changed
- SQL translation now uses lock-free thread-local cache (145x speedup)
- Updated README with detailed benchmark results
- Rewrote `docs/modules.md` with cache architecture documentation
- Reorganized debug documentation into `docs/debug/`

### Performance
- Cached SQL translation: 0.12 µs (was 17.5 µs uncached)
- Thread-local cache is 22x faster than mutex-protected cache
- Shim overhead is <1% of total query time
- Cache lookup: 22.6 ns per operation

### Fixed
- `sqlite3_column_value` now properly handles pre-step calls
- Column metadata functions work before `sqlite3_step()` is called

## [0.7.0] - 2026-01-08

### Added
- SQL normalization for parameterized query caching
- Prepared statement cache with O(1) hash table lookup
- Unix socket support for PostgreSQL connections
- `sqlite3_expanded_sql` implementation
- Boolean value conversions for PostgreSQL 't'/'f' values

### Fixed
- Double-free crash in connection cleanup
- Fork safety with pthread_atfork handlers

## [0.6.0] - 2026-01-06

### Added
- Stack overflow protection (multi-layer defense)
- Recursion guards with depth limiting
- OnDeck query special handling for low-stack conditions
- Loop detection for rapid repeated queries

### Fixed
- Stack overflow crash with 218 recursive frames
- Integer overflow in counter variables

## [0.5.0] - 2026-01-04

### Added
- COLLATE NOCASE translation to ILIKE/LOWER()
- FTS4 boolean search operators (AND, OR, NOT, phrases)
- Window functions support (ROW_NUMBER, RANK, DENSE_RANK)
- WHERE 0/1 to WHERE FALSE/TRUE translation

### Changed
- Improved GROUP BY expression rewriting

## [0.4.0] - 2026-01-02

### Added
- Connection pooling (50 connections default, max 100)
- Query result caching with TTL-based eviction
- Thread-local connection caching

### Fixed
- Connection exhaustion under heavy load

## [0.3.0] - 2025-12-30

### Added
- Full SQL translation pipeline
- Placeholder translation (? to $1, :name to $N)
- Function translations (iif, strftime, IFNULL, etc.)
- UPSERT translation (INSERT OR REPLACE to ON CONFLICT)

## [0.2.0] - 2025-12-28

### Added
- Linux support via LD_PRELOAD
- Docker support with docker-compose
- Schema auto-initialization

## [0.1.0] - 2025-12-25

### Added
- Initial release
- macOS support via DYLD_INTERPOSE + fishhook
- Basic SQLite to PostgreSQL interception
- Shadow database for SQLite-only queries
