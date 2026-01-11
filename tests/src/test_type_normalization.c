/*
 * Unit tests for Type Normalization (normalize_sqlite_decltype)
 *
 * Tests the fix for db_interpose_column.c that normalizes Plex's custom
 * SQLite type annotations (like DT_INTEGER(8), BOOLEAN) to standard SQLite
 * types (INTEGER, REAL, TEXT, BLOB) for SOCI compatibility.
 *
 * The normalize_sqlite_decltype() function is static in db_interpose_column.c,
 * so we duplicate its logic here for testing purposes.
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
// Duplicate of normalize_sqlite_decltype() for testing
// This must match the implementation in db_interpose_column.c
// ============================================================================

// Normalize Plex custom type annotations to standard SQLite types
// Plex uses "DT_INTEGER(8)" for BIGINT, "BOOLEAN" for booleans, etc.
// SOCI only understands: INTEGER, REAL, TEXT, BLOB
// Returns pointer to static string (do not free)
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

    // Unknown type - default to TEXT for safety
    return "TEXT";
}

// ============================================================================
// Type Normalization Tests (Bug Fix Tests)
// Tests that Plex's custom type annotations are normalized to standard SQLite types
// ============================================================================

static void test_dt_integer_8(void) {
    TEST("Type Norm - DT_INTEGER(8) -> INTEGER");
    const char *result = normalize_sqlite_decltype("DT_INTEGER(8)");

    if (result && strcmp(result, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("DT_INTEGER(8) should normalize to INTEGER");
        if (result) printf("    Got: %s\n", result);
    }
}

static void test_dt_integer_4(void) {
    TEST("Type Norm - DT_INTEGER(4) -> INTEGER");
    const char *result = normalize_sqlite_decltype("DT_INTEGER(4)");

    if (result && strcmp(result, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("DT_INTEGER(4) should normalize to INTEGER");
        if (result) printf("    Got: %s\n", result);
    }
}

static void test_dt_integer_2(void) {
    TEST("Type Norm - DT_INTEGER(2) -> INTEGER");
    const char *result = normalize_sqlite_decltype("DT_INTEGER(2)");

    if (result && strcmp(result, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("DT_INTEGER(2) should normalize to INTEGER");
        if (result) printf("    Got: %s\n", result);
    }
}

static void test_boolean_lowercase(void) {
    TEST("Type Norm - boolean -> INTEGER");
    const char *result = normalize_sqlite_decltype("boolean");

    if (result && strcmp(result, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("boolean should normalize to INTEGER");
        if (result) printf("    Got: %s\n", result);
    }
}

static void test_boolean_uppercase(void) {
    TEST("Type Norm - BOOLEAN -> INTEGER");
    const char *result = normalize_sqlite_decltype("BOOLEAN");

    if (result && strcmp(result, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("BOOLEAN should normalize to INTEGER");
        if (result) printf("    Got: %s\n", result);
    }
}

static void test_integer_passthrough(void) {
    TEST("Type Norm - INTEGER -> INTEGER (passthrough)");
    const char *result = normalize_sqlite_decltype("INTEGER");

    if (result && strcmp(result, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("INTEGER should pass through unchanged");
        if (result) printf("    Got: %s\n", result);
    }
}

static void test_text_passthrough(void) {
    TEST("Type Norm - TEXT -> TEXT (passthrough)");
    const char *result = normalize_sqlite_decltype("TEXT");

    if (result && strcmp(result, "TEXT") == 0) {
        PASS();
    } else {
        FAIL("TEXT should pass through unchanged");
        if (result) printf("    Got: %s\n", result);
    }
}

static void test_real_passthrough(void) {
    TEST("Type Norm - REAL -> REAL (passthrough)");
    const char *result = normalize_sqlite_decltype("REAL");

    if (result && strcmp(result, "REAL") == 0) {
        PASS();
    } else {
        FAIL("REAL should pass through unchanged");
        if (result) printf("    Got: %s\n", result);
    }
}

static void test_blob_passthrough(void) {
    TEST("Type Norm - BLOB -> BLOB (passthrough)");
    const char *result = normalize_sqlite_decltype("BLOB");

    if (result && strcmp(result, "BLOB") == 0) {
        PASS();
    } else {
        FAIL("BLOB should pass through unchanged");
        if (result) printf("    Got: %s\n", result);
    }
}

static void test_numeric_passthrough(void) {
    TEST("Type Norm - NUMERIC -> NUMERIC (passthrough)");
    const char *result = normalize_sqlite_decltype("NUMERIC");

    if (result && strcmp(result, "NUMERIC") == 0) {
        PASS();
    } else {
        FAIL("NUMERIC should pass through unchanged");
        if (result) printf("    Got: %s\n", result);
    }
}

static void test_null_input(void) {
    TEST("Type Norm - NULL input -> NULL");
    const char *result = normalize_sqlite_decltype(NULL);

    if (result == NULL) {
        PASS();
    } else {
        FAIL("NULL input should return NULL");
        printf("    Got: %s\n", result);
    }
}

static void test_unknown_type(void) {
    TEST("Type Norm - unknown type -> TEXT (default)");
    const char *result = normalize_sqlite_decltype("CUSTOM_TYPE");

    if (result && strcmp(result, "TEXT") == 0) {
        PASS();
    } else {
        FAIL("Unknown type should default to TEXT");
        if (result) printf("    Got: %s\n", result);
    }
}

static void test_case_insensitive_integer(void) {
    TEST("Type Norm - integer (lowercase) -> INTEGER");
    const char *result = normalize_sqlite_decltype("integer");

    if (result && strcmp(result, "INTEGER") == 0) {
        PASS();
    } else {
        FAIL("integer (lowercase) should normalize to INTEGER");
        if (result) printf("    Got: %s\n", result);
    }
}

static void test_case_insensitive_text(void) {
    TEST("Type Norm - text (lowercase) -> TEXT");
    const char *result = normalize_sqlite_decltype("text");

    if (result && strcmp(result, "TEXT") == 0) {
        PASS();
    } else {
        FAIL("text (lowercase) should normalize to TEXT");
        if (result) printf("    Got: %s\n", result);
    }
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("\n\033[1m=== Type Normalization Tests ===\033[0m\n\n");

    printf("\033[1mDT_INTEGER Variants:\033[0m\n");
    test_dt_integer_8();
    test_dt_integer_4();
    test_dt_integer_2();

    printf("\n\033[1mBoolean Normalization:\033[0m\n");
    test_boolean_lowercase();
    test_boolean_uppercase();

    printf("\n\033[1mStandard Type Passthrough:\033[0m\n");
    test_integer_passthrough();
    test_text_passthrough();
    test_real_passthrough();
    test_blob_passthrough();
    test_numeric_passthrough();

    printf("\n\033[1mEdge Cases:\033[0m\n");
    test_null_input();
    test_unknown_type();
    test_case_insensitive_integer();
    test_case_insensitive_text();

    printf("\n\033[1m=== Results ===\033[0m\n");
    printf("Passed: \033[32m%d\033[0m\n", tests_passed);
    printf("Failed: \033[31m%d\033[0m\n", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
