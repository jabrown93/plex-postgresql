# Installation Instructions - v0.8.12

## Quick Start (macOS ARM64)

### Prerequisites
- macOS ARM64 (Apple Silicon M1/M2/M3)
- Plex Media Server 1.42.x
- PostgreSQL 15.x server
- Plex database already migrated to PostgreSQL

### Installation

1. **Stop Plex:**
   ```bash
   pkill "Plex Media Server"
   ```

2. **Install dylib:**
   ```bash
   cp db_interpose_pg.dylib /path/to/installation/
   chmod +x /path/to/installation/db_interpose_pg.dylib
   ```

3. **Set your Plex token:**
   ```bash
   export PLEX_TOKEN="YOUR_TOKEN_HERE"
   ```
   Find your token: https://support.plex.tv/articles/204059436-finding-an-authentication-token-x-plex-token/

4. **Start Plex:**
   ```bash
   DYLD_INSERT_LIBRARIES="/path/to/db_interpose_pg.dylib" \
     "/Applications/Plex Media Server.app/Contents/MacOS/Plex Media Server.original" &
   ```

5. **Test:**
   ```bash
   ./test_aggregate_decltype.sh
   ```

## What's Fixed
✅ TV shows endpoint: HTTP 200 (was 500)
✅ No std::bad_cast exceptions
✅ MetadataCounterCache works

See RELEASE_NOTES.md for details.
