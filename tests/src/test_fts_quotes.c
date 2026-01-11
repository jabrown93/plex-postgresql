/*
 * Unit tests for FTS (Full Text Search) escaped quote handling
 *
 * Tests the fix for simplify_fts_for_sqlite() in db_interpose_prepare.c
 *
 * The bug: FTS queries with apostrophes like "it's" or "O'Brien" failed
 * because SQL escaped quotes ('') were not handled correctly when finding
 * the closing quote of MATCH clauses.
 *
 * The fix: The function now loops through chars and skips '' pairs,
 * and returns "1=0" instead of "1=1" so SQLite returns no results
 * (since the real work is done by PostgreSQL with proper tsquery).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>  // for strcasestr

// ============================================================================
// Copy of simplify_fts_for_sqlite from db_interpose_prepare.c for testing
// This allows unit testing without pulling in all libpq dependencies
// ============================================================================

// Helper to create a simplified SQL for SQLite when query uses FTS
// Removes FTS joins and MATCH clauses since SQLite shadow DB doesn't have FTS tables
static char* simplify_fts_for_sqlite(const char *sql) {
    if (!sql || !strcasestr(sql, "fts4_")) return NULL;

    char *result = malloc(strlen(sql) * 2 + 100);
    if (!result) return NULL;
    strcpy(result, sql);

    // Remove JOINs with fts4_* tables
    const char *fts_patterns[] = {
        "join fts4_metadata_titles_icu",
        "join fts4_metadata_titles",
        "join fts4_tag_titles_icu",
        "join fts4_tag_titles"
    };

    for (int p = 0; p < 4; p++) {
        char *join_start;
        while ((join_start = strcasestr(result, fts_patterns[p])) != NULL) {
            char *join_end = join_start;
            while (*join_end) {
                if (strncasecmp(join_end, " where ", 7) == 0 ||
                    strncasecmp(join_end, " join ", 6) == 0 ||
                    strncasecmp(join_end, " left ", 6) == 0 ||
                    strncasecmp(join_end, " group ", 7) == 0 ||
                    strncasecmp(join_end, " order ", 7) == 0) {
                    break;
                }
                join_end++;
            }
            memmove(join_start, join_end, strlen(join_end) + 1);
        }
    }

    // Remove MATCH clauses: "fts4_*.title match 'term'" -> "1=0"
    // Also handle title_sort match
    const char *match_patterns[] = {
        "fts4_metadata_titles_icu.title match ",
        "fts4_metadata_titles_icu.title_sort match ",
        "fts4_metadata_titles.title match ",
        "fts4_metadata_titles.title_sort match ",
        "fts4_tag_titles_icu.title match ",
        "fts4_tag_titles_icu.tag match ",
        "fts4_tag_titles.title match ",
        "fts4_tag_titles.tag match "
    };
    int num_patterns = 8;

    for (int p = 0; p < num_patterns; p++) {
        char *match_pos;
        while ((match_pos = strcasestr(result, match_patterns[p])) != NULL) {
            char *quote_start = strchr(match_pos, '\'');
            if (!quote_start) break;
            
            // Find closing quote, handling SQL escaped quotes ('')
            char *quote_end = quote_start + 1;
            while (*quote_end) {
                if (*quote_end == '\'') {
                    // Check if next char is also quote (escaped quote '')
                    if (*(quote_end + 1) == '\'') {
                        quote_end += 2;  // Skip both quotes
                        continue;
                    }
                    break;  // Found real closing quote
                }
                quote_end++;
            }
            if (*quote_end != '\'') break;  // No closing quote found

            // Replace entire "fts_table.col match 'term'" with "1=0"
            // Using 1=0 (FALSE) so SQLite shadow query returns no results
            // The real work is done by PostgreSQL with proper tsquery
            const char *replacement = "1=0";
            size_t new_len = strlen(replacement);

            memmove(match_pos + new_len, quote_end + 1, strlen(quote_end + 1) + 1);
            memcpy(match_pos, replacement, new_len);
        }
    }

    return result;
}

// Test counters
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) printf("  Testing: %s... ", name)
#define PASS() do { printf("\033[32mPASS\033[0m\n"); tests_passed++; } while(0)
#define FAIL(msg) do { printf("\033[31mFAIL: %s\033[0m\n", msg); tests_failed++; } while(0)

// ============================================================================
// Helper function to check if result contains expected patterns
// ============================================================================

static int contains_pattern(const char *str, const char *pattern) {
    return str && pattern && strstr(str, pattern) != NULL;
}

static int contains_pattern_nocase(const char *str, const char *pattern) {
    return str && pattern && strcasestr(str, pattern) != NULL;
}

// ============================================================================
// FTS Quote Handling Tests
// ============================================================================

static void test_fts_simple_query(void) {
    TEST("FTS simple query - no quotes");
    
    const char *sql = "SELECT * FROM metadata_items "
                      "JOIN fts4_metadata_titles ON metadata_items.id = fts4_metadata_titles.id "
                      "WHERE fts4_metadata_titles.title match 'test'";
    
    char *result = simplify_fts_for_sqlite(sql);
    
    if (result) {
        // Should have removed the FTS join
        int no_fts_join = !contains_pattern_nocase(result, "fts4_metadata_titles");
        // Should have replaced MATCH with 1=0
        int has_false_condition = contains_pattern(result, "1=0");
        
        if (no_fts_join && has_false_condition) {
            PASS();
        } else {
            FAIL("Expected FTS join removed and 1=0 condition");
            printf("    no_fts_join=%d has_false_condition=%d\n", no_fts_join, has_false_condition);
            printf("    Result: %.200s\n", result);
        }
        free(result);
    } else {
        FAIL("simplify_fts_for_sqlite returned NULL");
    }
}

static void test_fts_single_quote(void) {
    TEST("FTS single quote - apostrophe in 'it's'");
    
    // SQL escaped quote: it's becomes it''s in SQL string literal
    const char *sql = "SELECT * FROM metadata_items "
                      "JOIN fts4_metadata_titles ON metadata_items.id = fts4_metadata_titles.id "
                      "WHERE fts4_metadata_titles.title match 'it''s a test'";
    
    char *result = simplify_fts_for_sqlite(sql);
    
    if (result) {
        // Should have successfully parsed past the escaped quote
        int has_false_condition = contains_pattern(result, "1=0");
        // Should not have dangling quote or broken SQL
        int no_fts_match = !contains_pattern_nocase(result, " match ");
        
        if (has_false_condition && no_fts_match) {
            PASS();
        } else {
            FAIL("Expected escaped quote to be handled correctly");
            printf("    has_false_condition=%d no_fts_match=%d\n", has_false_condition, no_fts_match);
            printf("    Result: %.200s\n", result);
        }
        free(result);
    } else {
        FAIL("simplify_fts_for_sqlite returned NULL");
    }
}

static void test_fts_escaped_quote(void) {
    TEST("FTS escaped quote - SQL escaped ''");
    
    // Test explicit SQL escaping: two single quotes represent one quote
    const char *sql = "SELECT * FROM items "
                      "JOIN fts4_metadata_titles ON items.id = fts4_metadata_titles.id "
                      "WHERE fts4_metadata_titles.title match 'can''t stop'";
    
    char *result = simplify_fts_for_sqlite(sql);
    
    if (result) {
        // Should have replaced MATCH clause with 1=0
        int has_false_condition = contains_pattern(result, "1=0");
        // The MATCH clause should be fully replaced
        int match_replaced = !contains_pattern_nocase(result, "match 'can");
        
        if (has_false_condition && match_replaced) {
            PASS();
        } else {
            FAIL("Escaped quote '' should be skipped correctly");
            printf("    has_false_condition=%d match_replaced=%d\n", has_false_condition, match_replaced);
            printf("    Result: %.200s\n", result);
        }
        free(result);
    } else {
        FAIL("simplify_fts_for_sqlite returned NULL");
    }
}

static void test_fts_multiple_quotes(void) {
    TEST("FTS multiple quotes - 'don't won't'");
    
    // Multiple apostrophes in one query
    const char *sql = "SELECT * FROM media "
                      "JOIN fts4_metadata_titles_icu ON media.id = fts4_metadata_titles_icu.id "
                      "WHERE fts4_metadata_titles_icu.title match 'don''t won''t'";
    
    char *result = simplify_fts_for_sqlite(sql);
    
    if (result) {
        // Should handle multiple escaped quotes
        int has_false_condition = contains_pattern(result, "1=0");
        int match_replaced = !contains_pattern_nocase(result, " match ");
        
        if (has_false_condition && match_replaced) {
            PASS();
        } else {
            FAIL("Multiple escaped quotes should all be handled");
            printf("    has_false_condition=%d match_replaced=%d\n", has_false_condition, match_replaced);
            printf("    Result: %.200s\n", result);
        }
        free(result);
    } else {
        FAIL("simplify_fts_for_sqlite returned NULL");
    }
}

static void test_fts_name_with_apostrophe(void) {
    TEST("FTS name with apostrophe - O'Brien, McDonald's");
    
    // Names with apostrophes are common in media titles
    const char *sql = "SELECT * FROM metadata_items "
                      "JOIN fts4_tag_titles ON metadata_items.id = fts4_tag_titles.id "
                      "WHERE fts4_tag_titles.tag match 'O''Brien' "
                      "OR fts4_tag_titles.tag match 'McDonald''s'";
    
    char *result = simplify_fts_for_sqlite(sql);
    
    if (result) {
        // Both MATCH clauses should be replaced
        int has_false_condition = contains_pattern(result, "1=0");
        // Neither MATCH should remain
        int no_match_obrien = !contains_pattern(result, "match 'O");
        int no_match_mcdonalds = !contains_pattern(result, "match 'McDonald");
        
        if (has_false_condition && no_match_obrien && no_match_mcdonalds) {
            PASS();
        } else {
            FAIL("Names with apostrophes should be handled");
            printf("    has_false=%d no_obrien=%d no_mcd=%d\n", 
                   has_false_condition, no_match_obrien, no_match_mcdonalds);
            printf("    Result: %.300s\n", result);
        }
        free(result);
    } else {
        FAIL("simplify_fts_for_sqlite returned NULL");
    }
}

static void test_fts_returns_false_condition(void) {
    TEST("FTS returns false condition - output contains '1=0' not '1=1'");
    
    const char *sql = "SELECT * FROM shows "
                      "JOIN fts4_metadata_titles ON shows.id = fts4_metadata_titles.id "
                      "WHERE fts4_metadata_titles.title match 'test'";
    
    char *result = simplify_fts_for_sqlite(sql);
    
    if (result) {
        // CRITICAL: Must return 1=0 (FALSE) so SQLite returns no results
        // The real FTS work is done by PostgreSQL with proper tsquery
        int has_1_eq_0 = contains_pattern(result, "1=0");
        int no_1_eq_1 = !contains_pattern(result, "1=1");
        
        if (has_1_eq_0 && no_1_eq_1) {
            PASS();
        } else {
            FAIL("Must use '1=0' (FALSE) not '1=1' (TRUE)");
            printf("    has_1_eq_0=%d no_1_eq_1=%d\n", has_1_eq_0, no_1_eq_1);
            printf("    Result: %.200s\n", result);
        }
        free(result);
    } else {
        FAIL("simplify_fts_for_sqlite returned NULL");
    }
}

// ============================================================================
// Edge Case Tests
// ============================================================================

static void test_fts_no_fts_table(void) {
    TEST("Edge case - no fts4_ table (returns NULL)");
    
    const char *sql = "SELECT * FROM metadata_items WHERE id = 1";
    
    char *result = simplify_fts_for_sqlite(sql);
    
    // Function should return NULL when no FTS table is found
    if (result == NULL) {
        PASS();
    } else {
        FAIL("Should return NULL when no fts4_ table");
        printf("    Result: %.100s\n", result);
        free(result);
    }
}

static void test_fts_null_input(void) {
    TEST("Edge case - NULL input");
    
    char *result = simplify_fts_for_sqlite(NULL);
    
    // Should handle NULL gracefully
    if (result == NULL) {
        PASS();
    } else {
        FAIL("Should return NULL for NULL input");
        free(result);
    }
}

static void test_fts_consecutive_escaped_quotes(void) {
    TEST("Edge case - consecutive escaped quotes ''''");
    
    // Four single quotes = two actual quotes in the string
    const char *sql = "SELECT * FROM items "
                      "JOIN fts4_metadata_titles ON items.id = fts4_metadata_titles.id "
                      "WHERE fts4_metadata_titles.title match 'test''''value'";
    
    char *result = simplify_fts_for_sqlite(sql);
    
    if (result) {
        int has_false_condition = contains_pattern(result, "1=0");
        int match_replaced = !contains_pattern_nocase(result, " match 'test");
        
        if (has_false_condition && match_replaced) {
            PASS();
        } else {
            FAIL("Consecutive escaped quotes should be handled");
            printf("    Result: %.200s\n", result);
        }
        free(result);
    } else {
        FAIL("simplify_fts_for_sqlite returned NULL");
    }
}

static void test_fts_quote_at_end(void) {
    TEST("Edge case - escaped quote at end of string");
    
    // Quote at the very end: ends with '
    const char *sql = "SELECT * FROM items "
                      "JOIN fts4_metadata_titles ON items.id = fts4_metadata_titles.id "
                      "WHERE fts4_metadata_titles.title match 'test'''";
    
    char *result = simplify_fts_for_sqlite(sql);
    
    if (result) {
        int has_false_condition = contains_pattern(result, "1=0");
        
        if (has_false_condition) {
            PASS();
        } else {
            FAIL("Escaped quote at end should be handled");
            printf("    Result: %.200s\n", result);
        }
        free(result);
    } else {
        FAIL("simplify_fts_for_sqlite returned NULL");
    }
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("\n\033[1m=== FTS Escaped Quote Tests ===\033[0m\n\n");
    
    printf("\033[1mBasic FTS Quote Handling:\033[0m\n");
    test_fts_simple_query();
    test_fts_single_quote();
    test_fts_escaped_quote();
    test_fts_multiple_quotes();
    test_fts_name_with_apostrophe();
    test_fts_returns_false_condition();
    
    printf("\n\033[1mEdge Cases:\033[0m\n");
    test_fts_no_fts_table();
    test_fts_null_input();
    test_fts_consecutive_escaped_quotes();
    test_fts_quote_at_end();
    
    printf("\n\033[1m=== Results ===\033[0m\n");
    printf("Passed: \033[32m%d\033[0m\n", tests_passed);
    printf("Failed: \033[31m%d\033[0m\n", tests_failed);
    printf("\n");
    
    return tests_failed > 0 ? 1 : 0;
}
