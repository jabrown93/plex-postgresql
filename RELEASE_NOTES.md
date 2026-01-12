# Release Notes - v0.8.9.5

**Release Date:** January 12, 2026

This release fixes row index bounds errors and improves INSERT...RETURNING handling.

## Highlights

### Row Index -1 Out of Bounds Fix

Fixed libpq error "row number -1 is out of range" that occurred when column functions were called on WRITE statements:

| Before | After |
|--------|-------|
| WRITE statements stored RETURNING result with current_row=-1 | Don't store RETURNING result for WRITE |
| Fake values created with row_idx=-1 | Added row_idx >= 0 check to all access points |
| libpq printed "row number -1 is out of range" error | No invalid row access |

**Root Cause:** When executing INSERT...RETURNING, the shim stored the result with `current_row = -1`. When SOCI called column functions, fake values were created with this invalid row index. Later, when these values were accessed, libpq was called with row=-1.

**Solution:**
1. Don't store RETURNING results for WRITE statements - SOCI uses `lastval()` via SQL translation
2. Added `row_idx >= 0` check to all fake value access points
3. Column functions now handle all PostgreSQL statements properly

### Simplified Column Function Handling

Changed column function conditions from:
```c
if (pg_stmt && (pg_stmt->is_pg == 2 || (pg_stmt->is_pg == 1 && pg_stmt->result)))
```
to:
```c
if (pg_stmt && pg_stmt->is_pg)
```

This ensures all PostgreSQL-intercepted statements are handled correctly, preventing fallthrough to original SQLite functions for statements that don't have valid SQLite handles.

## Files Changed

- `src/db_interpose_step.c` - Don't store RETURNING result for WRITE statements
- `src/db_interpose_column.c` - Added row_idx >= 0 checks, simplified is_pg conditions

## Test Results

- `/library/metadata/18618` - 200 OK
- `/library/sections/8/all` (Movies) - 200 OK
- `/playQueues` - 500 (known issue, pre-existing in baseline)

**Note:** The playQueues 500 error is a pre-existing issue that was present even in the baseline code before v0.8.9.x changes. All database operations for playQueues complete successfully, but an error occurs in Plex's application layer. This requires further investigation with access to Plex debug logs or exception tracing.

## Upgrade Path

Direct upgrade from any 0.8.x version. No configuration changes required.
