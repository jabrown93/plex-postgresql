# Code Organization Guide

Dit document beschrijft de structuur van de codebase voor makkelijke navigatie.

## Project Structuur

```
plex-postgresql/
├── src/
│   ├── db_interpose_pg.c          (1943 lines) - Main shim library
│   ├── sql_translator.c            (2001 lines) - SQL translation engine
│   └── sql_translator_core.c       (New module - helpers)
├── include/
│   ├── db_interpose.h              - Shim library interface
│   └── sql_translator.h            - SQL translator interface
├── scripts/
│   └── analyze_fallbacks.sh        - Analyze PostgreSQL fallbacks
└── docs/
    ├── FALLBACK_IMPROVEMENT.md     - Guide for iterative SQL improvements
    └── MODULES.md                  - This file
```

---

## src/db_interpose_pg.c (1943 lines)

**Hoofd shim library** - Intercepteert SQLite calls en redirect naar PostgreSQL

### Sectie Overzicht:

| Lines      | Sectie                              | Beschrijving                                          |
|------------|-------------------------------------|-------------------------------------------------------|
| 1-100      | **Headers & Configuration**         | Includes, defines, global state                       |
| 101-250    | **PostgreSQL Connection**           | Connection pooling, initialization                    |
| 251-400    | **Skip Patterns & Fallback Logic**  | Bepaalt welke queries SQLite vs PostgreSQL gebruiken |
| 401-600    | **Statement Caching**               | TLS-based result caching voor performance            |
| 601-800    | **Parameter Binding**               | Translates SQLite params → PostgreSQL params         |
| 801-1000   | **Column Access Functions**         | column_int, column_text, etc - data retrieval        |
| 1001-1200  | **Prepared Statements**             | sqlite3_prepare_v2, prepare_v3 interception          |
| 1201-1400  | **Step & Execute**                  | Query execution, hybrid cached/live approach         |
| 1401-1600  | **Reset & Finalize**                | Cleanup, statement finalization                      |
| 1601-1800  | **Transaction Management**          | BEGIN, COMMIT, ROLLBACK handling                     |
| 1801-1943  | **Utility Functions**               | Helper functions, logging                            |

### Belangrijke Functies:

```c
// Connection Management
static PGconn* get_pg_connection()                  // Line ~150
static void ensure_pg_connection()                  // Line ~200

// Statement Execution
int sqlite3_step(sqlite3_stmt *stmt)                // Line ~1242 (MAIN!)
int sqlite3_prepare_v2(...)                         // Line ~1050

// Column Access (Hybrid Mode)
int sqlite3_column_int(sqlite3_stmt *stmt, int col) // Line ~820
const unsigned char* sqlite3_column_text(...)       // Line ~860

// Fallback Logging
static void log_sql_fallback(...)                   // Line ~400
static int is_known_translation_limitation(...)     // Line ~430
```

---

## src/sql_translator.c (2001 lines)

**SQL Translation Engine** - Converteer SQLite SQL → PostgreSQL SQL

### Sectie Overzicht:

| Lines       | Sectie                               | Beschrijving                                         |
|-------------|--------------------------------------|------------------------------------------------------|
| 1-100       | **Headers & Helpers**                | Includes, str_replace, extract_arg                   |
| 101-232     | **Placeholder Translation**          | ? en :name → $1, $2, ...                            |
| 233-450     | **Function Translations (Core)**     | iif, typeof, strftime, unixepoch, datetime          |
| 451-540     | **JSON & Advanced Functions**        | json_each → json_array_elements                     |
| 541-580     | **GROUP BY Fixes**                   | Fix strict PostgreSQL GROUP BY requirements         |
| 581-960     | **FTS & Query Optimizations**        | FTS4 → ILIKE, DISTINCT fixes, subquery aliases      |
| 961-1114    | **Main Translation Pipeline**        | sql_translate_functions() - orchestrator            |
| 1115-1181   | **Type Translations**                | AUTOINCREMENT, BLOB→BYTEA, dt_integer(8)            |
| 1182-1331   | **Quote Translations**               | Backticks→"", table.'column'→table."column"         |
| 1332-1491   | **DDL Translations**                 | CREATE TABLE IF NOT EXISTS, DDL quote fixes         |
| 1492-1662   | **Keyword Translations**             | BEGIN IMMEDIATE, REPLACE INTO, sqlite_master        |
| 1663-1730   | **Operator Spacing**                 | !=-1 → != -1 (PostgreSQL compatibility)             |
| 1731-1838   | **Main Orchestrator**                | sql_translate() - coordinates all steps             |

### Belangrijke Functies:

```c
// Main Entry Point
sql_translation_t sql_translate(const char *sql)    // Line ~1744 (MAIN!)

// Placeholders
char* sql_translate_placeholders(...)               // Line ~129

// Functions
static char* translate_iif(...)                     // Line ~239
static char* translate_json_each(...)               // Line ~450
static char* translate_last_insert_rowid(...)       // Line ~447

// Keywords & DDL
char* sql_translate_keywords(...)                   // Line ~1497
char* sql_translate_types(...)                      // Line ~1120

// Fixes
static char* fix_group_by_strict(...)               // Line ~493
static char* fix_operator_spacing(...)              // Line ~1670
```

---

## Translation Pipeline

De SQL translator volgt deze pipeline:

```
SQLite SQL
    ↓
1. Placeholders (? → $1, :name → $2)
    ↓
2. Functions (iif→CASE, json_each→json_array_elements)
    ↓
3. Types (AUTOINCREMENT→SERIAL, BLOB→BYTEA)
    ↓
4. Keywords (REPLACE INTO→INSERT, sqlite_master→pg_*)
    ↓
5. DDL Quotes ('table'→"table")
    ↓
6. IF NOT EXISTS (CREATE TABLE→CREATE TABLE IF NOT EXISTS)
    ↓
7. Operator Spacing (!=-1→!= -1)
    ↓
PostgreSQL SQL
```

---

## Execution Flow

### Normal Query Flow:

```
User App (Plex)
    ↓
sqlite3_prepare_v2() [db_interpose_pg.c:1050]
    ↓
sql_translate() [sql_translator.c:1744]
    ↓
PQprepare() → PostgreSQL
    ↓
sqlite3_step() [db_interpose_pg.c:1242]
    ↓
PQexecPrepared() → PostgreSQL
    ↓
Result Data → User App
```

### Cached Query Flow (Optimized):

```
sqlite3_step() [db_interpose_pg.c:1242]
    ↓
Check TLS cache
    ↓
Cache HIT → Return cached PGresult
    ↓
Cache MISS → Execute on PostgreSQL → Cache result
```

---

## Debugging & Development

### Add New Function Translation:

1. Open `src/sql_translator.c`
2. Find "Function Translation" section (~line 233)
3. Add your `translate_xxx()` function:
   ```c
   static char* translate_xxx(const char *sql) {
       // Your translation logic
       return str_replace_nocase(sql, "OLD_FUNC", "NEW_FUNC");
   }
   ```
4. Add to pipeline in `sql_translate_functions()` (~line 962):
   ```c
   temp = translate_xxx(current);
   free(current);
   if (!temp) return NULL;
   current = temp;
   ```
5. Rebuild: `make clean && make`

### Debug SQL Translation:

```bash
# Enable detailed logging
export PLEX_PG_DEBUG=1

# Check fallback log
./scripts/analyze_fallbacks.sh

# View full log
tail -f /tmp/plex_pg_fallbacks.log
```

### Analyze Fallbacks:

```bash
# See which queries fail
./scripts/analyze_fallbacks.sh

# Most common error types
grep "ERROR:" /tmp/plex_pg_fallbacks.log | sort | uniq -c | sort -rn

# Full query details
cat /tmp/plex_pg_fallbacks.log
```

---

## Performance Considerations

### Caching Strategy:

- **Prepared statements**: Cached in PostgreSQL (auto)
- **Result sets**: Cached in TLS for repeat calls (custom)
- **Connections**: Connection pooling (1 connection reused)

### Bottlenecks:

1. **SQL Translation**: ~0.1-1ms per query (negligible)
2. **Network RTT**: ~1-5ms to PostgreSQL (dominant factor)
3. **Result marshalling**: ~0.1ms (minimal)

### Optimization Tips:

- Use prepared statements (automatically cached)
- Batch operations when possible
- Let PostgreSQL handle complex queries (it's faster than SQLite!)

---

## Testing

### Unit Test a Translation:

```bash
# Test specific function
make test

# Check if translation works
psql -h localhost -U plex -d plex -c "YOUR_TRANSLATED_SQL_HERE"
```

### Integration Test:

```bash
# Start Plex
make run

# Use Plex normally, check logs
tail -f /tmp/plex_redirect_pg.log

# Check for fallbacks
./scripts/analyze_fallbacks.sh
```

---

## Common Issues

### Issue: "operator does not exist"

**Oorzaak**: Type mismatch (bijv. `integer = text`)
**Fix**: Add type casting in translator

```c
// Before: SELECT * WHERE id IN (SELECT value FROM ...)
// After:  SELECT * WHERE id IN (SELECT value::integer FROM ...)
```

### Issue: "must appear in GROUP BY clause"

**Oorzaak**: PostgreSQL strict mode
**Fix**: Add missing columns to GROUP BY or use aggregate function

```c
fix_group_by_strict() // Already handles most cases
```

### Issue: "no unique or exclusion constraint"

**Oorzaak**: ON CONFLICT without matching constraint
**Fix**: Ensure table has appropriate UNIQUE constraint

```sql
-- Add constraint if missing:
ALTER TABLE preferences ADD CONSTRAINT preferences_name_key UNIQUE (name);
```

---

## Future Improvements

### Potential Optimizations:

1. **Connection Pooling**: Implement connection pool (currently 1 connection)
2. **Async Queries**: Use PQsendQuery for non-blocking execution
3. **Batch Execution**: Combine multiple INSERTs into single query
4. **Smart Caching**: Cache more than just last result per statement

### SQL Translator Enhancements:

1. **More Functions**: Add translations as needed (use analyze_fallbacks.sh)
2. **Better GROUP BY**: Automatic detection of missing columns
3. **Window Functions**: Translate SQLite window functions if needed
4. **CTEs**: Ensure Common Table Expressions work correctly

---

## Architecture Decisions

### Why DYLD_INTERPOSE?

**Pro**: No Plex code modification needed
**Con**: macOS only (Linux uses LD_PRELOAD)
**Alternative**: Fork Plex and modify directly (not practical)

### Why PostgreSQL?

**Pro**: Better performance, scalability, reliability
**Con**: More complex setup than SQLite
**Result**: Worth it for large libraries (10k+ items)

### Why SQL Translation?

**Pro**: Minimal changes to Plex's SQL queries
**Con**: Some queries impossible to translate perfectly
**Result**: 99%+ success rate with fallback to SQLite for edge cases

---

## File Size Reduction Strategy

Instead of splitting into many small files, we use:

1. **Clear section markers** (// ====...)
2. **Table of Contents** (this document)
3. **Function index** (line numbers for quick jump)
4. **Logical grouping** (related functions together)

This keeps compilation simple while improving navigation.

---

## Quick Reference

```bash
# Build
make clean && make

# Run
make run

# Stop
make stop

# Analyze
./scripts/analyze_fallbacks.sh

# Debug
tail -f /tmp/plex_redirect_pg.log

# Check PostgreSQL
psql -h localhost -U plex -d plex -c "SELECT COUNT(*) FROM metadata_items;"
```

**Happy coding! 🚀**
