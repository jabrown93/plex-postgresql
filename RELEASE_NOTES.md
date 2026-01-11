# Release Notes - v0.8.5

**Release Date:** January 11, 2026

This release fixes critical thread-safety issues causing intermittent 500 errors.

## Highlights

### Thread-Safety Fix for Bind Operations (Critical)

Fixed "bind on busy prepared statement" race condition that caused sporadic 500 errors:

| Before | After |
|--------|-------|
| Mutex locked AFTER SQLite call | Mutex locked BEFORE SQLite call |
| Thread B could bind while Thread A stepping | Full mutual exclusion |
| Intermittent SQLITE_MISUSE (error 21) | No more race conditions |

**Root Cause:** In `db_interpose_bind.c`, all 9 bind functions acquired the mutex AFTER calling the original SQLite function. This allowed Thread B to call `sqlite3_bind_*()` while Thread A was inside `sqlite3_step()`, causing SQLite to return SQLITE_MISUSE.

**Solution:** Move mutex acquisition to BEFORE the SQLite call in all bind functions.

### lastval() Graceful Fallback (Critical)

Fixed 500 errors on playQueues when `last_insert_rowid()` called before any INSERT:

| Before | After |
|--------|-------|
| PostgreSQL error: "lastval not yet defined" | Returns 0 (like SQLite) |
| Exception in transaction | Graceful fallback |
| 500 Internal Server Error | playQueues works |

**Root Cause:** Plex calls `sqlite3_last_insert_rowid()` before doing an INSERT. The shim translated this to `SELECT lastval()` which fails in PostgreSQL if no sequence has been used yet.

**Solution:** Catch the PostgreSQL error and return 0, matching SQLite's behavior.

### Build Improvements

- `make macos` now automatically runs `make clean` first to prevent corrupt object files
- Fixes "unknown file type" linker errors when switching branches

## Files Changed

- `src/db_interpose_bind.c` - All 9 bind functions fixed
- `src/db_interpose_metadata.c` - lastval() graceful fallback
- `Makefile` - Auto-clean for macOS builds
- `README.md` - Added Known Limitations section

## Upgrade Instructions

```bash
git pull
make macos
# Restart Plex
```
