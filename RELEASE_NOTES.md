# Release Notes - v0.8.6

**Release Date:** January 11, 2026

This release completes the thread-safety fix for "bind on busy prepared statement" race conditions.

## Highlights

### Thread-Safety Fix for Reset/Clear Operations (Critical)

Completes the race condition fix from v0.8.5 by addressing `sqlite3_reset()` and `sqlite3_clear_bindings()`:

| Function | Before | After |
|----------|--------|-------|
| `sqlite3_reset()` | Mutex released BEFORE orig_sqlite3_reset() | Mutex held during entire call |
| `sqlite3_clear_bindings()` | No mutex at all | Mutex held during entire call |

**Root Cause:** While v0.8.5 fixed bind functions, `my_sqlite3_reset()` still released the mutex before calling `orig_sqlite3_reset()`. This allowed Thread B to call bind functions while Thread A was still inside reset, causing SQLITE_MISUSE (error 21).

**Solution:** Hold mutex for the entire duration of original SQLite function calls in reset and clear_bindings.

## Files Changed

- `src/db_interpose_step.c` - `my_sqlite3_reset()` and `my_sqlite3_clear_bindings()` fixed

## Upgrade Instructions

```bash
git pull
make macos
# Restart Plex
```
