/*
 * Unit tests for SQLite API functions
 * Tests sqlite3_free, sqlite3_db_handle, sqlite3_sql,
 * sqlite3_bind_parameter_count, sqlite3_stmt_readonly
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>
#include <sqlite3.h>

// Test counters
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) printf("  Testing: %s... ", name)
#define PASS() do { printf("\033[32mPASS\033[0m\n"); tests_passed++; } while(0)
#define FAIL(msg) do { printf("\033[31mFAIL: %s\033[0m\n", msg); tests_failed++; } while(0)

// ============================================================================
// sqlite3_free Tests
// ============================================================================

static void test_sqlite3_free_null(void) {
    TEST("sqlite3_free - NULL pointer (should not crash)");
    sqlite3_free(NULL);
    PASS();  // If we get here, it didn't crash
}

static void test_sqlite3_free_allocated(void) {
    TEST("sqlite3_free - allocated memory");
    char *ptr = sqlite3_malloc(100);
    if (!ptr) {
        FAIL("sqlite3_malloc returned NULL");
        return;
    }
    strcpy(ptr, "test data");
    sqlite3_free(ptr);
    PASS();  // If we get here, it didn't crash
}

// ============================================================================
// sqlite3_db_handle Tests
// ============================================================================

static void test_sqlite3_db_handle_basic(void) {
    TEST("sqlite3_db_handle - get db from statement");
    sqlite3 *db = NULL;
    int rc = sqlite3_open(":memory:", &db);
    if (rc != SQLITE_OK) {
        FAIL("Failed to open database");
        return;
    }

    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(db, "SELECT 1", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        FAIL("Failed to prepare statement");
        sqlite3_close(db);
        return;
    }

    sqlite3 *handle = sqlite3_db_handle(stmt);
    if (handle == db) {
        PASS();
    } else {
        FAIL("db_handle returned wrong pointer");
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

static void test_sqlite3_db_handle_null(void) {
    TEST("sqlite3_db_handle - NULL statement");
    sqlite3 *handle = sqlite3_db_handle(NULL);
    if (handle == NULL) {
        PASS();
    } else {
        FAIL("Expected NULL for NULL statement");
    }
}

// ============================================================================
// sqlite3_sql Tests
// ============================================================================

static void test_sqlite3_sql_basic(void) {
    TEST("sqlite3_sql - get SQL from statement");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt = NULL;
    const char *sql = "SELECT * FROM sqlite_master";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);

    const char *returned_sql = sqlite3_sql(stmt);
    if (returned_sql && strstr(returned_sql, "SELECT")) {
        PASS();
    } else {
        FAIL("sql() did not return expected SQL");
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

static void test_sqlite3_sql_null(void) {
    TEST("sqlite3_sql - NULL statement");
    const char *sql = sqlite3_sql(NULL);
    if (sql == NULL) {
        PASS();
    } else {
        FAIL("Expected NULL for NULL statement");
    }
}

// ============================================================================
// sqlite3_bind_parameter_count Tests
// ============================================================================

static void test_bind_parameter_count_none(void) {
    TEST("bind_parameter_count - no parameters");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "SELECT 1", -1, &stmt, NULL);

    int count = sqlite3_bind_parameter_count(stmt);
    if (count == 0) {
        PASS();
    } else {
        FAIL("Expected 0 parameters");
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

static void test_bind_parameter_count_multiple(void) {
    TEST("bind_parameter_count - multiple parameters");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt = NULL;
    // Use a query that doesn't require a real table
    sqlite3_prepare_v2(db, "SELECT ? + ? + ?", -1, &stmt, NULL);

    int count = sqlite3_bind_parameter_count(stmt);
    if (count == 3) {
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected 3 parameters, got %d", count);
        FAIL(msg);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

static void test_bind_parameter_count_null(void) {
    TEST("bind_parameter_count - NULL statement");
    int count = sqlite3_bind_parameter_count(NULL);
    if (count == 0) {
        PASS();
    } else {
        FAIL("Expected 0 for NULL statement");
    }
}

// ============================================================================
// sqlite3_stmt_readonly Tests
// ============================================================================

static void test_stmt_readonly_select(void) {
    TEST("stmt_readonly - SELECT is readonly");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "SELECT 1", -1, &stmt, NULL);

    int readonly = sqlite3_stmt_readonly(stmt);
    if (readonly != 0) {
        PASS();
    } else {
        FAIL("SELECT should be readonly");
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

static void test_stmt_readonly_insert(void) {
    TEST("stmt_readonly - INSERT is not readonly");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    sqlite3_exec(db, "CREATE TABLE t(x)", NULL, NULL, NULL);

    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "INSERT INTO t VALUES(1)", -1, &stmt, NULL);

    int readonly = sqlite3_stmt_readonly(stmt);
    if (readonly == 0) {
        PASS();
    } else {
        FAIL("INSERT should not be readonly");
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

static void test_stmt_readonly_null(void) {
    TEST("stmt_readonly - NULL statement");
    int readonly = sqlite3_stmt_readonly(NULL);
    if (readonly != 0) {
        PASS();  // NULL treated as readonly (safe default)
    } else {
        FAIL("NULL should be treated as readonly");
    }
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("\n=== SQLite API Function Tests ===\n\n");

    printf("sqlite3_free:\n");
    test_sqlite3_free_null();
    test_sqlite3_free_allocated();

    printf("\nsqlite3_db_handle:\n");
    test_sqlite3_db_handle_basic();
    test_sqlite3_db_handle_null();

    printf("\nsqlite3_sql:\n");
    test_sqlite3_sql_basic();
    test_sqlite3_sql_null();

    printf("\nsqlite3_bind_parameter_count:\n");
    test_bind_parameter_count_none();
    test_bind_parameter_count_multiple();
    test_bind_parameter_count_null();

    printf("\nsqlite3_stmt_readonly:\n");
    test_stmt_readonly_select();
    test_stmt_readonly_insert();
    test_stmt_readonly_null();

    printf("\n=== Results ===\n");
    printf("Passed: %d\n", tests_passed);
    printf("Failed: %d\n", tests_failed);
    printf("Total:  %d\n", tests_passed + tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
