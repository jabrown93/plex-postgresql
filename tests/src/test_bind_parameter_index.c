/*
 * Unit tests for sqlite3_bind_parameter_index
 *
 * Tests the mapping of named parameters (:name, @name, $name) to positional
 * indices as required by PostgreSQL ($N notation).
 *
 * These tests verify SQLite's standard behavior for named parameters, which
 * the shim must correctly emulate for PostgreSQL statements.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>

// Test counters
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) printf("  Testing: %s... ", name)
#define PASS() do { printf("\033[32mPASS\033[0m\n"); tests_passed++; } while(0)
#define FAIL(msg) do { printf("\033[31mFAIL: %s\033[0m\n", msg); tests_failed++; } while(0)

// ============================================================================
// Basic Named Parameter Tests
// ============================================================================

static void test_bind_parameter_index_basic(void) {
    TEST(":id in SELECT returns index 1");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, "SELECT * FROM sqlite_master WHERE name = :id", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        FAIL("Failed to prepare statement");
        sqlite3_close(db);
        return;
    }

    int idx = sqlite3_bind_parameter_index(stmt, ":id");
    if (idx == 1) {
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected index 1, got %d", idx);
        FAIL(msg);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

// ============================================================================
// Multiple Parameters Tests
// ============================================================================

static void test_bind_parameter_index_multiple_params(void) {
    TEST(":name and :age return correct indices 1, 2");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, "SELECT :name, :age", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        FAIL("Failed to prepare statement");
        sqlite3_close(db);
        return;
    }

    int idx_name = sqlite3_bind_parameter_index(stmt, ":name");
    int idx_age = sqlite3_bind_parameter_index(stmt, ":age");

    if (idx_name == 1 && idx_age == 2) {
        PASS();
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg), "Expected indices 1,2, got %d,%d", idx_name, idx_age);
        FAIL(msg);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

// ============================================================================
// Same Name Used Twice Tests
// ============================================================================

static void test_bind_parameter_index_same_name_twice(void) {
    TEST(":id used twice returns same index both times");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt = NULL;
    // When the same named parameter appears twice, SQLite only counts it once
    int rc = sqlite3_prepare_v2(db, "SELECT :id, :other, :id", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        FAIL("Failed to prepare statement");
        sqlite3_close(db);
        return;
    }

    // First lookup of :id
    int idx1 = sqlite3_bind_parameter_index(stmt, ":id");
    // Second lookup of :id - should return the same index
    int idx2 = sqlite3_bind_parameter_index(stmt, ":id");
    // Verify parameter count
    int count = sqlite3_bind_parameter_count(stmt);

    if (idx1 == idx2 && idx1 == 1) {
        // Also verify :other gets index 2
        int idx_other = sqlite3_bind_parameter_index(stmt, ":other");
        if (idx_other == 2 && count == 2) {
            PASS();
        } else {
            char msg[128];
            snprintf(msg, sizeof(msg), "Param count/other index wrong: count=%d, other=%d", count, idx_other);
            FAIL(msg);
        }
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected same index for both :id lookups, got %d and %d", idx1, idx2);
        FAIL(msg);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

// ============================================================================
// Parameter Not Found Tests
// ============================================================================

static void test_bind_parameter_index_not_found(void) {
    TEST("Non-existent parameter returns 0");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, "SELECT :existing", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        FAIL("Failed to prepare statement");
        sqlite3_close(db);
        return;
    }

    int idx = sqlite3_bind_parameter_index(stmt, ":nonexistent");
    if (idx == 0) {
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected 0 for non-existent param, got %d", idx);
        FAIL(msg);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

// ============================================================================
// Mixed Positional and Named Parameters Tests
// ============================================================================

static void test_bind_parameter_index_positional_mixed(void) {
    TEST("Mix of ? and :name handled correctly");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt = NULL;
    // SQLite assigns: ?1 = 1, :name = 2, ?3 = 3 (note: ? without number gets next available)
    int rc = sqlite3_prepare_v2(db, "SELECT ?, :name, ?", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        FAIL("Failed to prepare statement");
        sqlite3_close(db);
        return;
    }

    int count = sqlite3_bind_parameter_count(stmt);
    int idx_name = sqlite3_bind_parameter_index(stmt, ":name");

    // With mixed ? and :name, SQLite should have 3 params
    // :name should be at index 2
    if (count == 3 && idx_name == 2) {
        PASS();
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg), "Expected count=3, :name=2; got count=%d, :name=%d", count, idx_name);
        FAIL(msg);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

// ============================================================================
// @ Syntax Tests (Alternative to :)
// ============================================================================

static void test_bind_parameter_index_at_syntax(void) {
    TEST("@param syntax (alternative to :param)");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, "SELECT @param1, @param2", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        FAIL("Failed to prepare statement");
        sqlite3_close(db);
        return;
    }

    int idx1 = sqlite3_bind_parameter_index(stmt, "@param1");
    int idx2 = sqlite3_bind_parameter_index(stmt, "@param2");

    if (idx1 == 1 && idx2 == 2) {
        PASS();
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg), "Expected indices 1,2, got %d,%d", idx1, idx2);
        FAIL(msg);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

// ============================================================================
// $ Syntax Tests (TCL-style parameters)
// ============================================================================

static void test_bind_parameter_index_dollar_syntax(void) {
    TEST("$param syntax (TCL-style)");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt = NULL;
    // Note: SQLite treats $name as a named parameter (TCL compatibility)
    int rc = sqlite3_prepare_v2(db, "SELECT $user, $pass", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        FAIL("Failed to prepare statement");
        sqlite3_close(db);
        return;
    }

    int idx_user = sqlite3_bind_parameter_index(stmt, "$user");
    int idx_pass = sqlite3_bind_parameter_index(stmt, "$pass");

    if (idx_user == 1 && idx_pass == 2) {
        PASS();
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg), "Expected indices 1,2, got %d,%d", idx_user, idx_pass);
        FAIL(msg);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

// ============================================================================
// Edge Cases
// ============================================================================

static void test_bind_parameter_index_null_stmt(void) {
    TEST("NULL statement returns 0");
    int idx = sqlite3_bind_parameter_index(NULL, ":param");
    if (idx == 0) {
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected 0 for NULL stmt, got %d", idx);
        FAIL(msg);
    }
}

static void test_bind_parameter_index_null_name(void) {
    TEST("NULL name returns 0");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "SELECT :param", -1, &stmt, NULL);

    int idx = sqlite3_bind_parameter_index(stmt, NULL);
    if (idx == 0) {
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected 0 for NULL name, got %d", idx);
        FAIL(msg);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

static void test_bind_parameter_index_empty_name(void) {
    TEST("Empty name returns 0");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "SELECT :param", -1, &stmt, NULL);

    int idx = sqlite3_bind_parameter_index(stmt, "");
    if (idx == 0) {
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected 0 for empty name, got %d", idx);
        FAIL(msg);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

static void test_bind_parameter_index_case_sensitive(void) {
    TEST("Parameter names are case-sensitive");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "SELECT :MyParam", -1, &stmt, NULL);

    int idx_exact = sqlite3_bind_parameter_index(stmt, ":MyParam");
    int idx_lower = sqlite3_bind_parameter_index(stmt, ":myparam");
    int idx_upper = sqlite3_bind_parameter_index(stmt, ":MYPARAM");

    if (idx_exact == 1 && idx_lower == 0 && idx_upper == 0) {
        PASS();
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg), "Case sensitivity failed: exact=%d, lower=%d, upper=%d",
                 idx_exact, idx_lower, idx_upper);
        FAIL(msg);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

// ============================================================================
// Integration with bind functions
// ============================================================================

static void test_bind_parameter_index_with_bind(void) {
    TEST("bind_parameter_index works with actual binding");
    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);

    // Create a table and prepare an insert
    sqlite3_exec(db, "CREATE TABLE test(id INTEGER, name TEXT)", NULL, NULL, NULL);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, "INSERT INTO test VALUES(:id, :name)", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        FAIL("Failed to prepare statement");
        sqlite3_close(db);
        return;
    }

    // Get indices and bind values
    int idx_id = sqlite3_bind_parameter_index(stmt, ":id");
    int idx_name = sqlite3_bind_parameter_index(stmt, ":name");

    sqlite3_bind_int(stmt, idx_id, 42);
    sqlite3_bind_text(stmt, idx_name, "test_user", -1, SQLITE_STATIC);

    // Execute and verify
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        FAIL("Insert failed");
        sqlite3_close(db);
        return;
    }

    // Verify the data was inserted correctly
    sqlite3_stmt *select_stmt = NULL;
    sqlite3_prepare_v2(db, "SELECT id, name FROM test", -1, &select_stmt, NULL);
    if (sqlite3_step(select_stmt) == SQLITE_ROW) {
        int id = sqlite3_column_int(select_stmt, 0);
        const char *name = (const char *)sqlite3_column_text(select_stmt, 1);
        if (id == 42 && name && strcmp(name, "test_user") == 0) {
            PASS();
        } else {
            char msg[128];
            snprintf(msg, sizeof(msg), "Wrong data: id=%d, name=%s", id, name ? name : "NULL");
            FAIL(msg);
        }
    } else {
        FAIL("No data found after insert");
    }

    sqlite3_finalize(select_stmt);
    sqlite3_close(db);
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("\n=== sqlite3_bind_parameter_index Tests ===\n\n");

    printf("Basic named parameter:\n");
    test_bind_parameter_index_basic();

    printf("\nMultiple parameters:\n");
    test_bind_parameter_index_multiple_params();

    printf("\nSame name used twice:\n");
    test_bind_parameter_index_same_name_twice();

    printf("\nParameter not found:\n");
    test_bind_parameter_index_not_found();

    printf("\nMixed positional and named:\n");
    test_bind_parameter_index_positional_mixed();

    printf("\n@ syntax (alternative):\n");
    test_bind_parameter_index_at_syntax();

    printf("\n$ syntax (TCL-style):\n");
    test_bind_parameter_index_dollar_syntax();

    printf("\nEdge cases:\n");
    test_bind_parameter_index_null_stmt();
    test_bind_parameter_index_null_name();
    test_bind_parameter_index_empty_name();
    test_bind_parameter_index_case_sensitive();

    printf("\nIntegration test:\n");
    test_bind_parameter_index_with_bind();

    printf("\n=== Results ===\n");
    printf("Passed: %d\n", tests_passed);
    printf("Failed: %d\n", tests_failed);
    printf("Total:  %d\n", tests_passed + tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
