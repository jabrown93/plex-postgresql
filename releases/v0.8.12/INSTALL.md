# Installation Instructions - v0.8.12

## Quick Start (macOS ARM64)

This release includes a pre-compiled binary for macOS ARM64 (Apple Silicon).

### Prerequisites

- macOS ARM64 (Apple Silicon M1/M2/M3)
- Plex Media Server 1.42.x
- PostgreSQL 15.x server accessible from localhost
- Plex database already migrated to PostgreSQL

### Installation Steps

1. **Stop Plex Media Server:**
   ```bash
   pkill "Plex Media Server"
   ```

2. **Backup current shim (if upgrading):**
   ```bash
   cp /path/to/old/db_interpose_pg.dylib /path/to/old/db_interpose_pg.dylib.backup
   ```

3. **Install the new shim:**
   ```bash
   # Copy the dylib to your installation directory
   cp db_interpose_pg.dylib /path/to/your/installation/
   
   # Make sure it's executable
   chmod +x /path/to/your/installation/db_interpose_pg.dylib
   ```

4. **Start Plex with the shim:**
   ```bash
   DYLD_INSERT_LIBRARIES="/path/to/your/installation/db_interpose_pg.dylib" \
     "/Applications/Plex Media Server.app/Contents/MacOS/Plex Media Server.original" &
   ```

5. **Verify the fix:**
   ```bash
   # Test TV shows endpoint (replace TOKEN with your X-Plex-Token)
   curl -w "\nHTTP %{http_code}\n" \
     "http://localhost:32400/library/sections/6/all?type=2&X-Plex-Token=YOUR_TOKEN"
   
   # Expected: HTTP 200 (not 500)
   ```

6. **Run integration test (optional):**
   ```bash
   # Set your Plex token
   export PLEX_TOKEN="YOUR_TOKEN"
   
   # Run the test
   ./test_aggregate_decltype.sh
   ```

## What's Fixed in v0.8.12

✅ **TV Shows HTTP 500 Error Fixed**
- TV shows endpoint now returns HTTP 200 instead of 500
- MetadataCounterCache rebuilds without crashing
- No more `std::bad_cast` exceptions

✅ **Root Cause:**
- SOCI's BIGINT aggregate parsing bug workaround
- Aggregate functions (count, sum, max, min, avg) now declare as TEXT
- Bypasses SOCI's strict integer type checking

## Upgrading from Previous Versions

If you're upgrading from v0.8.10 or earlier:

1. No database migration required
2. Simply replace the dylib file
3. Restart Plex
4. Test TV shows endpoint

## Troubleshooting

### TV shows still return 500 error

1. Check if shim is loaded:
   ```bash
   grep "SHIM_INIT" /tmp/plex_redirect_pg.log
   ```

2. Check for aggregate workaround:
   ```bash
   grep "DECLTYPE_AGGREGATE.*TEXT" /tmp/plex_redirect_pg.log
   ```

3. Check Plex logs for exceptions:
   ```bash
   tail -100 ~/Library/Logs/Plex\ Media\ Server/Plex\ Media\ Server.log | grep -i exception
   ```

### Movies work but TV shows don't

This is the exact bug this release fixes! Make sure:
- You're using the v0.8.12 dylib
- Plex was restarted after installing
- The shim is actually loaded (check step 1 above)

## Building from Source

If you prefer to build from source:

```bash
git clone https://github.com/yourusername/plex-postgresql.git
cd plex-postgresql
git checkout v0.8.12
make clean && make
```

## Support

- GitHub Issues: https://github.com/yourusername/plex-postgresql/issues
- See RELEASE_NOTES.md for detailed technical information
- See supernerdanalyse.md for the full debugging journey

## Files Included

- `db_interpose_pg.dylib` - Pre-compiled shim for macOS ARM64
- `INSTALL.md` - This file
- `RELEASE_NOTES.md` - Detailed release notes
- `CHANGELOG.md` - Full changelog
- `VERSION` - Version number
- `test_aggregate_decltype.sh` - Integration test script

## License

See LICENSE file in the repository.
