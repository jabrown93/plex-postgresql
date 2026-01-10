# Release Notes - v0.8.1

**Release Date:** January 10, 2026

This release fixes critical std::bad_cast exceptions and adds robust exception diagnostics for Linux.

## Highlights

### std::bad_cast Fix (Critical)
Fixed SOCI ORM type conversion failures that caused HTTP 500 errors in Plex:

| Before | After |
|--------|-------|
| `column_decltype()` returned NULL | Returns proper SQLite type strings |
| SOCI threw `std::bad_cast` | Type conversion works correctly |
| 500 errors on library requests | Requests succeed |

**Root Cause:** SOCI uses `column_decltype()` to determine how to convert column values. When it returned NULL, SOCI defaulted to "char" type internally, but `column_type()` returned SQLITE_INTEGER, causing a type mismatch.

**Solution:** Map PostgreSQL OIDs to SQLite-compatible type strings that match `column_type()`:

| PostgreSQL Type | OID | SQLite decltype | column_type() |
|-----------------|-----|-----------------|---------------|
| bool, int2, int4, int8 | 16, 21, 23, 20 | `INTEGER` | SQLITE_INTEGER |
| float4, float8, numeric | 700, 701, 1700 | `REAL` | SQLITE_FLOAT |
| bytea | 17 | `BLOB` | SQLITE_BLOB |
| text, varchar, etc. | * | `TEXT` | SQLITE_TEXT |

### Robust Exception Handler (Linux)
New C++ exception interceptor provides automatic diagnostics:

```
╔══════════════════════════════════════════════════════════════════════╗
║ EXCEPTION: std::domain_error                                    #1   ║
╠══════════════════════════════════════════════════════════════════════╣
║ Source: NOT SHIM-RELATED (external C++ code)                         ║
╠══════════════════════════════════════════════════════════════════════╣
┌─────────────────────────────────────────────────────────────────────┐
│ STACK TRACE: std::domain_error                                      │
├─────────────────────────────────────────────────────────────────────┤
│  0: db_interpose_pg.so            __cxa_throw                       │
│  1: Plex Media Server             [0xffff98980b9c]                  │
│  2: Plex Media Server             [0xffff98980ca4]                  │
└─────────────────────────────────────────────────────────────────────┘
```

**Features:**
- Per-exception-type tracking (stack trace for first occurrence of each type)
- Automatic source detection: "SHIM-RELATED" vs "external C++ code"
- Library identification via `dladdr()` runtime linker
- C++ symbol demangling
- Throttling after 50 exceptions with type summary

## Technical Details

### musl libc Compatibility
The Linux shim now works correctly with musl-based containers (Alpine, etc.):

- Replaced `sscanf` with manual parsing (musl lacks `__isoc23_sscanf`)
- Exception context uses volatile globals instead of TLS
- Stack frame collection supports ARM64 and x86_64
- Build script: `build_shim_musl.sh`

### Files Changed

| File | Changes |
|------|---------|
| `src/db_interpose_column.c` | PostgreSQL OID → SQLite type mapping in `column_decltype()` |
| `src/db_interpose_core_linux.c` | Robust exception handler, stack trace analyzer |
| `build_shim_musl.sh` | Build script for musl-based containers |

## Upgrade Notes

This release is backward compatible. No configuration changes required.

**For musl-based containers (Alpine, etc.):**
```bash
# Copy source to container and build
docker cp src/ container:/tmp/shim_build/
docker exec container bash /tmp/shim_build/build_shim_musl.sh
```

## Bug Fixes

- Fixed `std::bad_cast` exceptions causing 500 errors
- Fixed `__isoc23_sscanf` symbol not found on musl
- Fixed TLS variables not capturing exception context on musl

## Known Issues

- `std::domain_error` exceptions from Python (`_Py_HashDouble`) are unrelated to the shim
  - These are internal Plex/Python exceptions, not causing request failures
  - The exception handler correctly identifies them as "NOT SHIM-RELATED"

## Contributors

- Claude Opus 4.5 (Anthropic)
