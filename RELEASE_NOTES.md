# Release Notes - v0.8.7

**Release Date:** January 11, 2026

This release fixes a critical deadlock that caused std::exception crashes after v0.8.5/v0.8.6 thread-safety improvements.

## Highlights

### Recursive Mutex Fix (Critical)

Fixed deadlock when bind/reset operations internally trigger column functions:

| Before | After |
|--------|-------|
| Non-recursive mutex | Recursive mutex (`PTHREAD_MUTEX_RECURSIVE`) |
| Thread blocks on own mutex | Thread can re-lock same mutex |
| std::exception crashes | Stable operation |

**Root Cause:** The thread-safety fixes in v0.8.5/v0.8.6 held the statement mutex during `orig_sqlite3_bind_*()` and `orig_sqlite3_reset()` calls. When SQLite internally triggered our intercepted column functions (e.g., `sqlite3_column_type`), those functions tried to lock the same mutex, causing a deadlock.

**Solution:** Use `PTHREAD_MUTEX_RECURSIVE` for the statement mutex, allowing the same thread to acquire the lock multiple times without deadlock.

## Files Changed

- `src/pg_statement.c` - Changed mutex initialization to use recursive mutex

## Upgrade Instructions

```bash
git pull
make macos
# Restart Plex
```
