/*
 * Unit tests for column_decltype / SOCI ORM Type Compatibility
 *
 * Tests the fix for std::bad_cast exceptions in SOCI. The problem was that
 * column_decltype() returned types that didn't match what column_type() returned,
 * causing SOCI to use the wrong extraction method (e.g., column_text for INTEGER).
 *
 * SOCI uses column_decltype() to determine how to read column values:
 * - "INTEGER" -> column_int/int64
 * - "REAL"    -> column_double
 * - "TEXT"    -> column_text
 * - "BLOB"    -> column_blob
 *
 * If decltype returns NULL or an unrecognized type, SOCI defaults to "char" (string),
 * but column_type() might return SQLITE_INTEGER, causing std::bad_cast.
 *
 * See: https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=984534
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>  // for strcasecmp

// Test counters
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) printf("  Testing: %s... ", name)
#define PASS() do { printf("\033[32mPASS\033[0m\n"); tests_passed++; } while(0)
#define FAIL(msg) do { printf("\033[31mFAIL: %s\033[0m\n", msg); tests_failed++; } while(0)

// ============================================================================
// Simulated SQLite type constants
// ============================================================================
#define SQLITE_INTEGER 1
#define SQLITE_FLOAT   2
#define SQLITE_TEXT    3
#define SQLITE_BLOB    4
#define SQLITE_NULL    5

// ============================================================================
// Duplicate of pg_oid_to_sqlite_type() for testing
// This must match the implementation in pg_statement.c
// ============================================================================
static int pg_oid_to_sqlite_type(unsigned int oid) {
    switch (oid) {
        case 16:   // BOOL
        case 20:   // INT8
        case 21:   // INT2
        case 23:   // INT4
            return SQLITE_INTEGER;
        case 700:  // FLOAT4
        case 701:  // FLOAT8
        case 1700: // NUMERIC
            return SQLITE_FLOAT;
        case 17:   // BYTEA
            return SQLITE_BLOB;
        case 25:   // TEXT
        case 1042: // BPCHAR
        case 1043: // VARCHAR
        default:
            return SQLITE_TEXT;
    }
}

// ============================================================================
// Duplicate of pg_oid_to_sqlite_decltype() for testing
// This must match the implementation in pg_statement.c
// ============================================================================
static const char* pg_oid_to_sqlite_decltype(unsigned int oid) {
    switch (oid) {
        case 16:   // BOOL
            return "INTEGER";  // SQLite has no BOOL, use INTEGER
        case 20:   // INT8 (bigint)
            return "INTEGER";
        case 21:   // INT2 (smallint)
            return "INTEGER";
        case 23:   // INT4 (integer)
            return "INTEGER";
        case 700:  // FLOAT4
            return "REAL";
        case 701:  // FLOAT8
            return "REAL";
        case 1700: // NUMERIC
            return "REAL";
        case 17:   // BYTEA
            return "BLOB";
        case 25:   // TEXT
            return "TEXT";
        case 1042: // BPCHAR (char)
            return "TEXT";
        case 1043: // VARCHAR
            return "TEXT";
        default:
            return "TEXT";
    }
}

// ============================================================================
// Duplicate of normalize_sqlite_decltype() for testing
// This must match the implementation in db_interpose_column.c
// ============================================================================
static const char* normalize_sqlite_decltype(const char *plex_type) {
    if (!plex_type) return NULL;

    // Check for Plex's DT_INTEGER(n) format
    if (strncmp(plex_type, "DT_INTEGER", 10) == 0) {
        return "INTEGER";
    }

    // Normalize boolean to INTEGER (SQLite doesn't have native boolean)
    if (strcasecmp(plex_type, "boolean") == 0) {
        return "INTEGER";
    }

    // Already standard SQLite type - return as-is
    // Common types: INTEGER, REAL, TEXT, BLOB, NUMERIC
    if (strcasecmp(plex_type, "INTEGER") == 0) return "INTEGER";
    if (strcasecmp(plex_type, "REAL") == 0) return "REAL";
    if (strcasecmp(plex_type, "TEXT") == 0) return "TEXT";
    if (strcasecmp(plex_type, "BLOB") == 0) return "BLOB";
    if (strcasecmp(plex_type, "NUMERIC") == 0) return "NUMERIC";

    // Unknown type (including VARCHAR(n)) - default to TEXT for safety
    // This matches how the actual implementation handles non-standard types
    return "TEXT";
}

// ============================================================================
// Helper: Map SQLite decltype string to expected SQLite type constant
// This simulates what SOCI does when it maps decltype to extraction method
// ============================================================================
static int decltype_to_sqlite_type(const char *decltype) {
    if (!decltype) return SQLITE_TEXT;  // SOCI default for NULL decltype
    if (strcasecmp(decltype, "INTEGER") == 0) return SQLITE_INTEGER;
    if (strcasecmp(decltype, "REAL") == 0) return SQLITE_FLOAT;
    if (strcasecmp(decltype, "TEXT") == 0) return SQLITE_TEXT;
    if (strcasecmp(decltype, "BLOB") == 0) return SQLITE_BLOB;
    if (strcasecmp(decltype, "NUMERIC") == 0) return SQLITE_FLOAT;  // SOCI treats NUMERIC as float
    return SQLITE_TEXT;  // Default for unknown types
}

// ============================================================================
// Test 1: PostgreSQL OID to SQLite type mapping
// Verifies that column_type returns correct SQLite types for PostgreSQL OIDs
// ============================================================================

static void test_column_decltype_oid_mapping(void) {
    printf("\n\033[1mTest 1: PostgreSQL OID to SQLite type mapping\033[0m\n");

    // OID 16 (bool) -> INTEGER
    TEST("OID 16 (bool) -> INTEGER decltype");
    const char *decltype_bool = pg_oid_to_sqlite_decltype(16);
    if (decltype_bool && strcmp(decltype_bool, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("bool should map to INTEGER decltype");
        if (decltype_bool) printf("    Got: %s\n", decltype_bool);
    }

    TEST("OID 16 (bool) -> SQLITE_INTEGER column_type");
    int type_bool = pg_oid_to_sqlite_type(16);
    if (type_bool == SQLITE_INTEGER) {
        PASS();
    } else {
        FAIL("bool should return SQLITE_INTEGER");
        printf("    Got: %d\n", type_bool);
    }

    // OID 20 (int8/bigint) -> INTEGER
    TEST("OID 20 (int8) -> INTEGER decltype");
    const char *decltype_int8 = pg_oid_to_sqlite_decltype(20);
    if (decltype_int8 && strcmp(decltype_int8, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("int8 should map to INTEGER decltype");
        if (decltype_int8) printf("    Got: %s\n", decltype_int8);
    }

    TEST("OID 20 (int8) -> SQLITE_INTEGER column_type");
    int type_int8 = pg_oid_to_sqlite_type(20);
    if (type_int8 == SQLITE_INTEGER) {
        PASS();
    } else {
        FAIL("int8 should return SQLITE_INTEGER");
        printf("    Got: %d\n", type_int8);
    }

    // OID 23 (int4/integer) -> INTEGER
    TEST("OID 23 (int4) -> INTEGER decltype");
    const char *decltype_int4 = pg_oid_to_sqlite_decltype(23);
    if (decltype_int4 && strcmp(decltype_int4, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("int4 should map to INTEGER decltype");
        if (decltype_int4) printf("    Got: %s\n", decltype_int4);
    }

    TEST("OID 23 (int4) -> SQLITE_INTEGER column_type");
    int type_int4 = pg_oid_to_sqlite_type(23);
    if (type_int4 == SQLITE_INTEGER) {
        PASS();
    } else {
        FAIL("int4 should return SQLITE_INTEGER");
        printf("    Got: %d\n", type_int4);
    }

    // OID 700 (float4) -> REAL
    TEST("OID 700 (float4) -> REAL decltype");
    const char *decltype_float4 = pg_oid_to_sqlite_decltype(700);
    if (decltype_float4 && strcmp(decltype_float4, "REAL") == 0) {
        PASS();
    } else {
        FAIL("float4 should map to REAL decltype");
        if (decltype_float4) printf("    Got: %s\n", decltype_float4);
    }

    TEST("OID 700 (float4) -> SQLITE_FLOAT column_type");
    int type_float4 = pg_oid_to_sqlite_type(700);
    if (type_float4 == SQLITE_FLOAT) {
        PASS();
    } else {
        FAIL("float4 should return SQLITE_FLOAT");
        printf("    Got: %d\n", type_float4);
    }

    // OID 701 (float8/double) -> REAL
    TEST("OID 701 (float8) -> REAL decltype");
    const char *decltype_float8 = pg_oid_to_sqlite_decltype(701);
    if (decltype_float8 && strcmp(decltype_float8, "REAL") == 0) {
        PASS();
    } else {
        FAIL("float8 should map to REAL decltype");
        if (decltype_float8) printf("    Got: %s\n", decltype_float8);
    }

    TEST("OID 701 (float8) -> SQLITE_FLOAT column_type");
    int type_float8 = pg_oid_to_sqlite_type(701);
    if (type_float8 == SQLITE_FLOAT) {
        PASS();
    } else {
        FAIL("float8 should return SQLITE_FLOAT");
        printf("    Got: %d\n", type_float8);
    }

    // OID 17 (bytea) -> BLOB
    TEST("OID 17 (bytea) -> BLOB decltype");
    const char *decltype_bytea = pg_oid_to_sqlite_decltype(17);
    if (decltype_bytea && strcmp(decltype_bytea, "BLOB") == 0) {
        PASS();
    } else {
        FAIL("bytea should map to BLOB decltype");
        if (decltype_bytea) printf("    Got: %s\n", decltype_bytea);
    }

    TEST("OID 17 (bytea) -> SQLITE_BLOB column_type");
    int type_bytea = pg_oid_to_sqlite_type(17);
    if (type_bytea == SQLITE_BLOB) {
        PASS();
    } else {
        FAIL("bytea should return SQLITE_BLOB");
        printf("    Got: %d\n", type_bytea);
    }

    // OID 25 (text) -> TEXT
    TEST("OID 25 (text) -> TEXT decltype");
    const char *decltype_text = pg_oid_to_sqlite_decltype(25);
    if (decltype_text && strcmp(decltype_text, "TEXT") == 0) {
        PASS();
    } else {
        FAIL("text should map to TEXT decltype");
        if (decltype_text) printf("    Got: %s\n", decltype_text);
    }

    TEST("OID 25 (text) -> SQLITE_TEXT column_type");
    int type_text = pg_oid_to_sqlite_type(25);
    if (type_text == SQLITE_TEXT) {
        PASS();
    } else {
        FAIL("text should return SQLITE_TEXT");
        printf("    Got: %d\n", type_text);
    }

    // OID 1043 (varchar) -> TEXT
    TEST("OID 1043 (varchar) -> TEXT decltype");
    const char *decltype_varchar = pg_oid_to_sqlite_decltype(1043);
    if (decltype_varchar && strcmp(decltype_varchar, "TEXT") == 0) {
        PASS();
    } else {
        FAIL("varchar should map to TEXT decltype");
        if (decltype_varchar) printf("    Got: %s\n", decltype_varchar);
    }

    TEST("OID 1043 (varchar) -> SQLITE_TEXT column_type");
    int type_varchar = pg_oid_to_sqlite_type(1043);
    if (type_varchar == SQLITE_TEXT) {
        PASS();
    } else {
        FAIL("varchar should return SQLITE_TEXT");
        printf("    Got: %d\n", type_varchar);
    }
}

// ============================================================================
// Test 2: NULL/fallback decltype handling
// When decltype lookup fails, should return sensible default
// ============================================================================

static void test_column_decltype_null_result_handling(void) {
    printf("\n\033[1mTest 2: NULL/fallback decltype handling\033[0m\n");

    TEST("NULL plex_type -> NULL (no crash)");
    const char *result = normalize_sqlite_decltype(NULL);
    if (result == NULL) {
        PASS();
    } else {
        FAIL("NULL input should return NULL");
        printf("    Got: %s\n", result);
    }

    TEST("Unknown type defaults to TEXT");
    const char *unknown = normalize_sqlite_decltype("UNKNOWN_CUSTOM_TYPE");
    if (unknown && strcmp(unknown, "TEXT") == 0) {
        PASS();
    } else {
        FAIL("Unknown type should default to TEXT");
        if (unknown) printf("    Got: %s\n", unknown);
    }

    TEST("Empty string defaults to TEXT");
    const char *empty = normalize_sqlite_decltype("");
    if (empty && strcmp(empty, "TEXT") == 0) {
        PASS();
    } else {
        FAIL("Empty string should default to TEXT");
        if (empty) printf("    Got: %s\n", empty);
    }
}

// ============================================================================
// Test 3: Plex custom types normalized
// Plex uses custom type annotations that need normalization for SOCI
// ============================================================================

static void test_column_decltype_plex_custom_types(void) {
    printf("\n\033[1mTest 3: Plex custom type normalization\033[0m\n");

    // DT_INTEGER(8) -> INTEGER
    TEST("DT_INTEGER(8) -> INTEGER");
    const char *dt_int8 = normalize_sqlite_decltype("DT_INTEGER(8)");
    if (dt_int8 && strcmp(dt_int8, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("DT_INTEGER(8) should normalize to INTEGER");
        if (dt_int8) printf("    Got: %s\n", dt_int8);
    }

    // DT_INTEGER(4) -> INTEGER
    TEST("DT_INTEGER(4) -> INTEGER");
    const char *dt_int4 = normalize_sqlite_decltype("DT_INTEGER(4)");
    if (dt_int4 && strcmp(dt_int4, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("DT_INTEGER(4) should normalize to INTEGER");
        if (dt_int4) printf("    Got: %s\n", dt_int4);
    }

    // DT_INTEGER(2) -> INTEGER
    TEST("DT_INTEGER(2) -> INTEGER");
    const char *dt_int2 = normalize_sqlite_decltype("DT_INTEGER(2)");
    if (dt_int2 && strcmp(dt_int2, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("DT_INTEGER(2) should normalize to INTEGER");
        if (dt_int2) printf("    Got: %s\n", dt_int2);
    }

    // VARCHAR(255) -> TEXT
    TEST("VARCHAR(255) -> TEXT");
    const char *varchar = normalize_sqlite_decltype("VARCHAR(255)");
    if (varchar && strcmp(varchar, "TEXT") == 0) {
        PASS();
    } else {
        FAIL("VARCHAR(255) should normalize to TEXT");
        if (varchar) printf("    Got: %s\n", varchar);
    }

    // VARCHAR(50) -> TEXT
    TEST("VARCHAR(50) -> TEXT");
    const char *varchar50 = normalize_sqlite_decltype("VARCHAR(50)");
    if (varchar50 && strcmp(varchar50, "TEXT") == 0) {
        PASS();
    } else {
        FAIL("VARCHAR(50) should normalize to TEXT");
        if (varchar50) printf("    Got: %s\n", varchar50);
    }

    // boolean -> INTEGER
    TEST("boolean -> INTEGER");
    const char *bool_lower = normalize_sqlite_decltype("boolean");
    if (bool_lower && strcmp(bool_lower, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("boolean should normalize to INTEGER");
        if (bool_lower) printf("    Got: %s\n", bool_lower);
    }

    // BOOLEAN -> INTEGER
    TEST("BOOLEAN -> INTEGER");
    const char *bool_upper = normalize_sqlite_decltype("BOOLEAN");
    if (bool_upper && strcmp(bool_upper, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("BOOLEAN should normalize to INTEGER");
        if (bool_upper) printf("    Got: %s\n", bool_upper);
    }
}

// ============================================================================
// Test 4: INTEGER columns must not be treated as TEXT
// Critical test: Ensures decltype and column_type are consistent
// This is the root cause of std::bad_cast exceptions in SOCI
// ============================================================================

static void test_integer_column_not_treated_as_text(void) {
    printf("\n\033[1mTest 4: INTEGER column type consistency (std::bad_cast prevention)\033[0m\n");

    // Test all integer-like OIDs for type consistency
    unsigned int integer_oids[] = {16, 20, 21, 23};  // bool, int8, int2, int4
    const char *oid_names[] = {"bool", "int8", "int2", "int4"};

    for (int i = 0; i < 4; i++) {
        unsigned int oid = integer_oids[i];
        const char *name = oid_names[i];

        char test_name[128];
        snprintf(test_name, sizeof(test_name), "OID %u (%s) - decltype matches column_type", oid, name);
        TEST(test_name);

        int column_type = pg_oid_to_sqlite_type(oid);
        const char *decltype = pg_oid_to_sqlite_decltype(oid);
        int decltype_implies = decltype_to_sqlite_type(decltype);

        // The critical check: what SOCI expects (from decltype) must match what column_type returns
        if (column_type == decltype_implies) {
            PASS();
        } else {
            FAIL("decltype/column_type mismatch - would cause std::bad_cast in SOCI!");
            printf("    column_type returns: %d (SQLITE_INTEGER=%d, SQLITE_TEXT=%d)\n",
                   column_type, SQLITE_INTEGER, SQLITE_TEXT);
            printf("    decltype '%s' implies: %d\n", decltype ? decltype : "(null)", decltype_implies);
        }
    }

    // Test float-like OIDs for type consistency
    printf("\n");
    unsigned int float_oids[] = {700, 701, 1700};  // float4, float8, numeric
    const char *float_names[] = {"float4", "float8", "numeric"};

    for (int i = 0; i < 3; i++) {
        unsigned int oid = float_oids[i];
        const char *name = float_names[i];

        char test_name[128];
        snprintf(test_name, sizeof(test_name), "OID %u (%s) - decltype matches column_type", oid, name);
        TEST(test_name);

        int column_type = pg_oid_to_sqlite_type(oid);
        const char *decltype = pg_oid_to_sqlite_decltype(oid);
        int decltype_implies = decltype_to_sqlite_type(decltype);

        if (column_type == decltype_implies) {
            PASS();
        } else {
            FAIL("decltype/column_type mismatch - would cause std::bad_cast in SOCI!");
            printf("    column_type returns: %d\n", column_type);
            printf("    decltype '%s' implies: %d\n", decltype ? decltype : "(null)", decltype_implies);
        }
    }

    // Test blob type
    printf("\n");
    TEST("OID 17 (bytea) - decltype matches column_type");
    {
        int column_type = pg_oid_to_sqlite_type(17);
        const char *decltype = pg_oid_to_sqlite_decltype(17);
        int decltype_implies = decltype_to_sqlite_type(decltype);

        if (column_type == decltype_implies) {
            PASS();
        } else {
            FAIL("decltype/column_type mismatch - would cause std::bad_cast in SOCI!");
            printf("    column_type returns: %d\n", column_type);
            printf("    decltype '%s' implies: %d\n", decltype ? decltype : "(null)", decltype_implies);
        }
    }

    // Test text types
    unsigned int text_oids[] = {25, 1042, 1043};  // text, bpchar, varchar
    const char *text_names[] = {"text", "bpchar", "varchar"};

    for (int i = 0; i < 3; i++) {
        unsigned int oid = text_oids[i];
        const char *name = text_names[i];

        char test_name[128];
        snprintf(test_name, sizeof(test_name), "OID %u (%s) - decltype matches column_type", oid, name);
        TEST(test_name);

        int column_type = pg_oid_to_sqlite_type(oid);
        const char *decltype = pg_oid_to_sqlite_decltype(oid);
        int decltype_implies = decltype_to_sqlite_type(decltype);

        if (column_type == decltype_implies) {
            PASS();
        } else {
            FAIL("decltype/column_type mismatch - would cause std::bad_cast in SOCI!");
            printf("    column_type returns: %d\n", column_type);
            printf("    decltype '%s' implies: %d\n", decltype ? decltype : "(null)", decltype_implies);
        }
    }
}

// ============================================================================
// Test 5: Plex custom types maintain type consistency
// Ensures DT_INTEGER types don't accidentally become TEXT
// ============================================================================

static void test_plex_types_maintain_consistency(void) {
    printf("\n\033[1mTest 5: Plex custom types maintain SOCI type consistency\033[0m\n");

    // DT_INTEGER(8) should result in INTEGER handling
    TEST("DT_INTEGER(8) -> SQLITE_INTEGER handling");
    {
        const char *normalized = normalize_sqlite_decltype("DT_INTEGER(8)");
        int soci_type = decltype_to_sqlite_type(normalized);
        if (soci_type == SQLITE_INTEGER) {
            PASS();
        } else {
            FAIL("DT_INTEGER(8) should cause SOCI to use INTEGER handling");
            printf("    normalized to: %s\n", normalized ? normalized : "(null)");
            printf("    SOCI type: %d (expected %d)\n", soci_type, SQLITE_INTEGER);
        }
    }

    // DT_INTEGER(4) should result in INTEGER handling
    TEST("DT_INTEGER(4) -> SQLITE_INTEGER handling");
    {
        const char *normalized = normalize_sqlite_decltype("DT_INTEGER(4)");
        int soci_type = decltype_to_sqlite_type(normalized);
        if (soci_type == SQLITE_INTEGER) {
            PASS();
        } else {
            FAIL("DT_INTEGER(4) should cause SOCI to use INTEGER handling");
            printf("    normalized to: %s\n", normalized ? normalized : "(null)");
            printf("    SOCI type: %d (expected %d)\n", soci_type, SQLITE_INTEGER);
        }
    }

    // boolean should result in INTEGER handling
    TEST("boolean -> SQLITE_INTEGER handling");
    {
        const char *normalized = normalize_sqlite_decltype("boolean");
        int soci_type = decltype_to_sqlite_type(normalized);
        if (soci_type == SQLITE_INTEGER) {
            PASS();
        } else {
            FAIL("boolean should cause SOCI to use INTEGER handling");
            printf("    normalized to: %s\n", normalized ? normalized : "(null)");
            printf("    SOCI type: %d (expected %d)\n", soci_type, SQLITE_INTEGER);
        }
    }

    // VARCHAR should result in TEXT handling (not cause bad_cast)
    TEST("VARCHAR(255) -> SQLITE_TEXT handling");
    {
        const char *normalized = normalize_sqlite_decltype("VARCHAR(255)");
        int soci_type = decltype_to_sqlite_type(normalized);
        if (soci_type == SQLITE_TEXT) {
            PASS();
        } else {
            FAIL("VARCHAR should cause SOCI to use TEXT handling");
            printf("    normalized to: %s\n", normalized ? normalized : "(null)");
            printf("    SOCI type: %d (expected %d)\n", soci_type, SQLITE_TEXT);
        }
    }
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("\n\033[1m=== column_decltype / SOCI Type Compatibility Tests ===\033[0m\n");
    printf("Testing std::bad_cast exception prevention for SOCI ORM\n");

    test_column_decltype_oid_mapping();
    test_column_decltype_null_result_handling();
    test_column_decltype_plex_custom_types();
    test_integer_column_not_treated_as_text();
    test_plex_types_maintain_consistency();

    printf("\n\033[1m=== Results ===\033[0m\n");
    printf("Passed: \033[32m%d\033[0m\n", tests_passed);
    printf("Failed: \033[31m%d\033[0m\n", tests_failed);
    printf("\n");

    if (tests_failed > 0) {
        printf("\033[31mWARNING: Failed tests indicate potential std::bad_cast exceptions in SOCI!\033[0m\n");
        printf("When column_decltype returns a type that doesn't match column_type,\n");
        printf("SOCI will use the wrong extraction method and throw std::bad_cast.\n\n");
    }

    return tests_failed > 0 ? 1 : 0;
}
