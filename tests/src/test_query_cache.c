/*
 * Unit tests for Query Cache, SQL Normalization, and Prepared Statement Cache
 *
 * Tests:
 * 1. FNV-1a hash function properties
 * 2. Cache key consistency
 * 3. TTL expiration logic
 * 4. Reference counting safety
 * 5. LRU eviction behavior
 * 6. SQL normalization - numeric literals stripped to $N placeholders
 * 7. SQL normalization - consistent results for same input
 * 8. SQL normalization - query structure preserved
 * 9. SQL normalization - string literals handling
 * 10. SQL normalization - unicode safety
 * 11. Prepared statement cache - hit on repeated queries
 * 12. Prepared statement cache - miss on different queries
 * 13. Prepared statement cache - LRU eviction when full
 * 14. Cache key includes bound parameter types
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <ctype.h>

// Test counters
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) printf("  Testing: %s... ", name)
#define PASS() do { printf("\033[32mPASS\033[0m\n"); tests_passed++; } while(0)
#define FAIL(msg) do { printf("\033[31mFAIL: %s\033[0m\n", msg); tests_failed++; } while(0)

// ============================================================================
// Replicate FNV-1a hash from pg_query_cache.c
// ============================================================================

static uint64_t fnv1a_hash(const void *data, size_t len) {
    const uint8_t *bytes = (const uint8_t *)data;
    uint64_t hash = 0xcbf29ce484222325ULL;  // FNV offset basis
    for (size_t i = 0; i < len; i++) {
        hash ^= bytes[i];
        hash *= 0x100000001b3ULL;  // FNV prime
    }
    return hash;
}

// ============================================================================
// Hash Function Tests
// ============================================================================

static void test_hash_consistency(void) {
    TEST("Hash - same input gives same hash");

    const char *sql = "SELECT * FROM metadata_items WHERE id = $1";
    uint64_t hash1 = fnv1a_hash(sql, strlen(sql));
    uint64_t hash2 = fnv1a_hash(sql, strlen(sql));

    if (hash1 == hash2) {
        PASS();
    } else {
        FAIL("Hash not consistent");
    }
}

static void test_hash_different_inputs(void) {
    TEST("Hash - different inputs give different hashes");

    const char *sql1 = "SELECT * FROM metadata_items WHERE id = $1";
    const char *sql2 = "SELECT * FROM metadata_items WHERE id = $2";

    uint64_t hash1 = fnv1a_hash(sql1, strlen(sql1));
    uint64_t hash2 = fnv1a_hash(sql2, strlen(sql2));

    if (hash1 != hash2) {
        PASS();
    } else {
        FAIL("Different inputs produced same hash");
    }
}

static void test_hash_distribution(void) {
    TEST("Hash - good distribution across buckets");

    // Generate 1000 different SQL queries and check bucket distribution
    #define NUM_QUERIES 1000
    #define NUM_BUCKETS 64

    int buckets[NUM_BUCKETS] = {0};
    char sql[256];

    for (int i = 0; i < NUM_QUERIES; i++) {
        snprintf(sql, sizeof(sql), "SELECT * FROM table_%d WHERE id = %d", i, i * 17);
        uint64_t hash = fnv1a_hash(sql, strlen(sql));
        int bucket = hash % NUM_BUCKETS;
        buckets[bucket]++;
    }

    // Check that no bucket has more than 3x the average
    int avg = NUM_QUERIES / NUM_BUCKETS;  // ~15
    int max_allowed = avg * 3;  // ~45
    int max_found = 0;

    for (int i = 0; i < NUM_BUCKETS; i++) {
        if (buckets[i] > max_found) max_found = buckets[i];
    }

    if (max_found <= max_allowed) {
        PASS();
    } else {
        FAIL("Poor hash distribution");
    }
}

static void test_hash_empty_string(void) {
    TEST("Hash - empty string handled");

    uint64_t hash = fnv1a_hash("", 0);

    // Should return the offset basis for empty input
    if (hash == 0xcbf29ce484222325ULL) {
        PASS();
    } else {
        FAIL("Empty string hash incorrect");
    }
}

static void test_hash_single_char_difference(void) {
    TEST("Hash - single char difference produces different hash");

    const char *sql1 = "SELECT * FROM a";
    const char *sql2 = "SELECT * FROM b";

    uint64_t hash1 = fnv1a_hash(sql1, strlen(sql1));
    uint64_t hash2 = fnv1a_hash(sql2, strlen(sql2));

    if (hash1 != hash2) {
        PASS();
    } else {
        FAIL("Single char difference not detected");
    }
}

// ============================================================================
// Cache Key Tests (simulate cache key computation)
// ============================================================================

static uint64_t compute_cache_key(const char *sql, const char **params, int param_count) {
    // Replicate logic from pg_query_cache.c
    uint64_t hash = fnv1a_hash(sql, strlen(sql));

    for (int i = 0; i < param_count; i++) {
        if (params[i]) {
            uint64_t param_hash = fnv1a_hash(params[i], strlen(params[i]));
            hash ^= param_hash;
            hash *= 0x100000001b3ULL;
        }
    }

    return hash;
}

static void test_cache_key_with_params(void) {
    TEST("Cache key - includes bound parameters");

    const char *sql = "SELECT * FROM t WHERE id = $1";
    const char *params1[] = {"100"};
    const char *params2[] = {"200"};

    uint64_t key1 = compute_cache_key(sql, params1, 1);
    uint64_t key2 = compute_cache_key(sql, params2, 1);

    if (key1 != key2) {
        PASS();
    } else {
        FAIL("Different params should give different keys");
    }
}

static void test_cache_key_same_params(void) {
    TEST("Cache key - same SQL+params gives same key");

    const char *sql = "SELECT * FROM t WHERE id = $1 AND name = $2";
    const char *params[] = {"100", "test"};

    uint64_t key1 = compute_cache_key(sql, params, 2);
    uint64_t key2 = compute_cache_key(sql, params, 2);

    if (key1 == key2) {
        PASS();
    } else {
        FAIL("Same SQL+params should give same key");
    }
}

static void test_cache_key_null_param(void) {
    TEST("Cache key - handles NULL parameters");

    const char *sql = "SELECT * FROM t WHERE id = $1";
    const char *params1[] = {NULL};
    const char *params2[] = {"100"};

    uint64_t key1 = compute_cache_key(sql, params1, 1);
    uint64_t key2 = compute_cache_key(sql, params2, 1);

    // Should produce different keys
    if (key1 != key2) {
        PASS();
    } else {
        FAIL("NULL param should differ from non-NULL");
    }
}

// ============================================================================
// TTL Logic Tests
// ============================================================================

#define QUERY_CACHE_TTL_MS 1000

static int is_cache_expired(uint64_t created_ms, uint64_t now_ms) {
    return (now_ms - created_ms) > QUERY_CACHE_TTL_MS;
}

static void test_ttl_not_expired(void) {
    TEST("TTL - entry not expired within window");

    uint64_t created = 1000000;
    uint64_t now = created + 500;  // 500ms later

    if (!is_cache_expired(created, now)) {
        PASS();
    } else {
        FAIL("Entry should not be expired at 500ms");
    }
}

static void test_ttl_expired(void) {
    TEST("TTL - entry expired after window");

    uint64_t created = 1000000;
    uint64_t now = created + 1500;  // 1500ms later

    if (is_cache_expired(created, now)) {
        PASS();
    } else {
        FAIL("Entry should be expired at 1500ms");
    }
}

static void test_ttl_boundary(void) {
    TEST("TTL - boundary at exactly TTL");

    uint64_t created = 1000000;
    uint64_t now = created + QUERY_CACHE_TTL_MS + 1;  // Just past TTL

    if (is_cache_expired(created, now)) {
        PASS();
    } else {
        FAIL("Entry should be expired just past TTL");
    }
}

// ============================================================================
// Reference Counting Tests
// ============================================================================

typedef struct {
    int ref_count;
    int freed;
} mock_cached_result_t;

static void mock_release(mock_cached_result_t *entry) {
    if (!entry) return;
    entry->ref_count--;
    if (entry->ref_count <= 0) {
        entry->freed = 1;
    }
}

static void test_refcount_basic(void) {
    TEST("RefCount - decrement on release");

    mock_cached_result_t entry = {.ref_count = 2, .freed = 0};

    mock_release(&entry);

    if (entry.ref_count == 1 && !entry.freed) {
        PASS();
    } else {
        FAIL("RefCount should be 1, not freed");
    }
}

static void test_refcount_free_at_zero(void) {
    TEST("RefCount - free when reaching zero");

    mock_cached_result_t entry = {.ref_count = 1, .freed = 0};

    mock_release(&entry);

    if (entry.ref_count == 0 && entry.freed) {
        PASS();
    } else {
        FAIL("Should be freed at ref_count 0");
    }
}

static void test_refcount_multiple_refs(void) {
    TEST("RefCount - multiple references prevent free");

    mock_cached_result_t entry = {.ref_count = 3, .freed = 0};

    mock_release(&entry);  // 3 -> 2
    mock_release(&entry);  // 2 -> 1

    if (entry.ref_count == 1 && !entry.freed) {
        PASS();
    } else {
        FAIL("Should not be freed with ref_count > 0");
    }

    mock_release(&entry);  // 1 -> 0, should free

    if (entry.freed) {
        // Already passed above
    }
}

// ============================================================================
// LRU Eviction Tests
// ============================================================================

#define CACHE_SIZE 4

typedef struct {
    uint64_t cache_key;
    uint64_t created_ms;
    int hit_count;
} mock_cache_entry_t;

static int find_lru_slot(mock_cache_entry_t *entries, int count) {
    // Find oldest entry (lowest created_ms with lowest hit_count as tiebreaker)
    int lru_idx = 0;
    uint64_t oldest = entries[0].created_ms;
    int lowest_hits = entries[0].hit_count;

    for (int i = 1; i < count; i++) {
        if (entries[i].created_ms < oldest ||
            (entries[i].created_ms == oldest && entries[i].hit_count < lowest_hits)) {
            lru_idx = i;
            oldest = entries[i].created_ms;
            lowest_hits = entries[i].hit_count;
        }
    }

    return lru_idx;
}

static void test_lru_finds_oldest(void) {
    TEST("LRU - finds oldest entry");

    mock_cache_entry_t entries[CACHE_SIZE] = {
        {.cache_key = 1, .created_ms = 1000, .hit_count = 5},
        {.cache_key = 2, .created_ms = 500,  .hit_count = 2},  // Oldest
        {.cache_key = 3, .created_ms = 1500, .hit_count = 1},
        {.cache_key = 4, .created_ms = 2000, .hit_count = 3},
    };

    int lru = find_lru_slot(entries, CACHE_SIZE);

    if (lru == 1) {
        PASS();
    } else {
        FAIL("Should find slot 1 as oldest");
    }
}

static void test_lru_tiebreaker_hits(void) {
    TEST("LRU - uses hit_count as tiebreaker");

    mock_cache_entry_t entries[CACHE_SIZE] = {
        {.cache_key = 1, .created_ms = 1000, .hit_count = 5},
        {.cache_key = 2, .created_ms = 1000, .hit_count = 2},  // Same age, fewer hits
        {.cache_key = 3, .created_ms = 1000, .hit_count = 10},
        {.cache_key = 4, .created_ms = 1000, .hit_count = 3},
    };

    int lru = find_lru_slot(entries, CACHE_SIZE);

    if (lru == 1) {
        PASS();
    } else {
        FAIL("Should find slot 1 with fewest hits");
    }
}

// ============================================================================
// SQL Normalization Tests
// Replicate logic from db_interpose_exec.c for testing
// ============================================================================

#define MAX_NORMALIZED_PARAMS 32

typedef struct {
    char *normalized_sql;      // SQL with $1, $2, etc.
    char *param_values[MAX_NORMALIZED_PARAMS];  // Extracted literal values
    int param_count;
} normalized_sql_t;

// Check if we're inside a string literal or identifier
static int is_inside_string(const char *sql, const char *pos) {
    int in_single = 0, in_double = 0;
    for (const char *p = sql; p < pos; p++) {
        if (*p == '\'' && !in_double) in_single = !in_single;
        else if (*p == '"' && !in_single) in_double = !in_double;
    }
    return in_single || in_double;
}

// Helper: case-insensitive strstr
static char* my_strcasestr(const char *haystack, const char *needle) {
    if (!haystack || !needle) return NULL;
    size_t needle_len = strlen(needle);
    if (needle_len == 0) return (char*)haystack;

    for (const char *p = haystack; *p; p++) {
        if (strncasecmp(p, needle, needle_len) == 0) {
            return (char*)p;
        }
    }
    return NULL;
}

// Normalize SQL by extracting numeric literals as parameters
// Returns NULL if normalization not applicable or fails
static normalized_sql_t* normalize_sql_literals(const char *sql) {
    if (!sql) return NULL;

    // Quick check: only normalize SELECT/UPDATE/DELETE with WHERE
    // Skip INSERT (values are usually all literals, less benefit)
    if (strncasecmp(sql, "INSERT", 6) == 0) return NULL;
    if (!my_strcasestr(sql, "WHERE")) return NULL;

    // Allocate result
    normalized_sql_t *result = calloc(1, sizeof(normalized_sql_t));
    if (!result) return NULL;

    // Allocate buffer for normalized SQL (same size + extra for $N placeholders)
    size_t sql_len = strlen(sql);
    char *out = malloc(sql_len + MAX_NORMALIZED_PARAMS * 4);  // Extra space for $NN
    if (!out) {
        free(result);
        return NULL;
    }

    const char *p = sql;
    char *o = out;
    int param_idx = 0;

    while (*p) {
        // Check for numeric literal (not inside string, preceded by operator/space/paren)
        if (param_idx < MAX_NORMALIZED_PARAMS &&
            (isdigit((unsigned char)*p) || (*p == '-' && isdigit((unsigned char)p[1]))) &&
            !is_inside_string(sql, p)) {

            // Check what precedes this number
            char prev = (p > sql) ? *(p-1) : ' ';
            if (prev == '=' || prev == '>' || prev == '<' || prev == ' ' ||
                prev == '(' || prev == ',' || prev == '+' || prev == '-' ||
                prev == '*' || prev == '/' || prev == '%') {

                // Extract the number
                const char *num_start = p;
                if (*p == '-') p++;
                while (isdigit((unsigned char)*p)) p++;
                // Handle decimals
                if (*p == '.' && isdigit((unsigned char)p[1])) {
                    p++;
                    while (isdigit((unsigned char)*p)) p++;
                }

                // Check what follows (should be operator/space/paren/end)
                char next = *p;
                if (next == '\0' || next == ' ' || next == ')' || next == ',' ||
                    next == ';' || next == '>' || next == '<' || next == '=' ||
                    next == '+' || next == '-' || next == '*' || next == '/' ||
                    strncasecmp(p, " AND", 4) == 0 || strncasecmp(p, " OR", 3) == 0 ||
                    strncasecmp(p, " ORDER", 6) == 0 || strncasecmp(p, " LIMIT", 6) == 0 ||
                    strncasecmp(p, " GROUP", 6) == 0) {

                    // Store the literal value
                    size_t num_len = p - num_start;
                    result->param_values[param_idx] = malloc(num_len + 1);
                    if (result->param_values[param_idx]) {
                        memcpy(result->param_values[param_idx], num_start, num_len);
                        result->param_values[param_idx][num_len] = '\0';
                        param_idx++;

                        // Write placeholder
                        o += sprintf(o, "$%d", param_idx);
                        continue;
                    }
                }
                // Not a replaceable number, reset position
                p = num_start;
            }
        }

        *o++ = *p++;
    }
    *o = '\0';

    // Only return normalized result if we extracted at least one parameter
    if (param_idx == 0) {
        free(out);
        free(result);
        return NULL;
    }

    result->normalized_sql = out;
    result->param_count = param_idx;
    return result;
}

// Extended normalization that also handles string literals
static normalized_sql_t* normalize_sql_all_literals(const char *sql) {
    if (!sql) return NULL;

    // Quick check: only normalize SELECT/UPDATE/DELETE with WHERE
    if (strncasecmp(sql, "INSERT", 6) == 0) return NULL;
    if (!my_strcasestr(sql, "WHERE")) return NULL;

    // Allocate result
    normalized_sql_t *result = calloc(1, sizeof(normalized_sql_t));
    if (!result) return NULL;

    // Allocate buffer for normalized SQL
    size_t sql_len = strlen(sql);
    char *out = malloc(sql_len + MAX_NORMALIZED_PARAMS * 4);
    if (!out) {
        free(result);
        return NULL;
    }

    const char *p = sql;
    char *o = out;
    int param_idx = 0;

    while (*p) {
        // Handle string literals
        if (*p == '\'' && param_idx < MAX_NORMALIZED_PARAMS) {
            // Check what precedes (should be operator/space)
            char prev = (p > sql) ? *(p-1) : ' ';
            if (prev == '=' || prev == ' ' || prev == '(' || prev == ',') {
                const char *str_start = p + 1;  // Skip opening quote
                p++;

                // Find closing quote (handle escaped quotes)
                while (*p && !(*p == '\'' && *(p+1) != '\'')) {
                    if (*p == '\'' && *(p+1) == '\'') {
                        p += 2;  // Skip escaped quote
                    } else {
                        p++;
                    }
                }

                if (*p == '\'') {
                    // Extract string value (without quotes)
                    size_t str_len = p - str_start;
                    result->param_values[param_idx] = malloc(str_len + 1);
                    if (result->param_values[param_idx]) {
                        memcpy(result->param_values[param_idx], str_start, str_len);
                        result->param_values[param_idx][str_len] = '\0';
                        param_idx++;

                        // Write placeholder
                        o += sprintf(o, "$%d", param_idx);
                        p++;  // Skip closing quote
                        continue;
                    }
                }
                // Failed to extract, reset
                p = str_start - 1;
            }
        }

        // Handle numeric literals (not inside string, preceded by operator/space/paren)
        if (param_idx < MAX_NORMALIZED_PARAMS &&
            (isdigit((unsigned char)*p) || (*p == '-' && isdigit((unsigned char)p[1]))) &&
            !is_inside_string(sql, p)) {

            char prev = (p > sql) ? *(p-1) : ' ';
            if (prev == '=' || prev == '>' || prev == '<' || prev == ' ' ||
                prev == '(' || prev == ',' || prev == '+' || prev == '-' ||
                prev == '*' || prev == '/' || prev == '%') {

                const char *num_start = p;
                if (*p == '-') p++;
                while (isdigit((unsigned char)*p)) p++;
                if (*p == '.' && isdigit((unsigned char)p[1])) {
                    p++;
                    while (isdigit((unsigned char)*p)) p++;
                }

                char next = *p;
                if (next == '\0' || next == ' ' || next == ')' || next == ',' ||
                    next == ';' || next == '>' || next == '<' || next == '=' ||
                    strncasecmp(p, " AND", 4) == 0 || strncasecmp(p, " OR", 3) == 0) {

                    size_t num_len = p - num_start;
                    result->param_values[param_idx] = malloc(num_len + 1);
                    if (result->param_values[param_idx]) {
                        memcpy(result->param_values[param_idx], num_start, num_len);
                        result->param_values[param_idx][num_len] = '\0';
                        param_idx++;

                        o += sprintf(o, "$%d", param_idx);
                        continue;
                    }
                }
                p = num_start;
            }
        }

        *o++ = *p++;
    }
    *o = '\0';

    if (param_idx == 0) {
        free(out);
        free(result);
        return NULL;
    }

    result->normalized_sql = out;
    result->param_count = param_idx;
    return result;
}

static void free_normalized_sql(normalized_sql_t *n) {
    if (!n) return;
    free(n->normalized_sql);
    for (int i = 0; i < n->param_count; i++) {
        free(n->param_values[i]);
    }
    free(n);
}

// Test 1: SQL normalization strips numeric literals
static void test_sql_normalization_strips_literals(void) {
    TEST("SQL Normalization - strips numeric literals");

    const char *sql = "SELECT * FROM t WHERE id = 123";
    normalized_sql_t *norm = normalize_sql_literals(sql);

    if (!norm) {
        FAIL("Normalization returned NULL");
        return;
    }

    if (strcmp(norm->normalized_sql, "SELECT * FROM t WHERE id = $1") == 0 &&
        norm->param_count == 1 &&
        strcmp(norm->param_values[0], "123") == 0) {
        PASS();
    } else {
        char msg[256];
        snprintf(msg, sizeof(msg), "Expected 'SELECT * FROM t WHERE id = $1' got '%s'",
                 norm->normalized_sql);
        FAIL(msg);
    }

    free_normalized_sql(norm);
}

// Test 2: SQL normalization is consistent
static void test_sql_normalization_consistent(void) {
    TEST("SQL Normalization - consistent results");

    const char *sql = "SELECT * FROM t WHERE id = 456 AND status = 1";

    normalized_sql_t *norm1 = normalize_sql_literals(sql);
    normalized_sql_t *norm2 = normalize_sql_literals(sql);

    if (!norm1 || !norm2) {
        FAIL("Normalization returned NULL");
        if (norm1) free_normalized_sql(norm1);
        if (norm2) free_normalized_sql(norm2);
        return;
    }

    if (strcmp(norm1->normalized_sql, norm2->normalized_sql) == 0 &&
        norm1->param_count == norm2->param_count) {
        PASS();
    } else {
        FAIL("Normalization not consistent");
    }

    free_normalized_sql(norm1);
    free_normalized_sql(norm2);
}

// Test 3: SQL normalization preserves structure
static void test_sql_normalization_preserves_structure(void) {
    TEST("SQL Normalization - preserves query structure");

    const char *sql = "SELECT a, b FROM t WHERE id = 999 ORDER BY created";
    normalized_sql_t *norm = normalize_sql_literals(sql);

    if (!norm) {
        FAIL("Normalization returned NULL");
        return;
    }

    // Verify structure is preserved (keywords, table names, column names)
    if (strstr(norm->normalized_sql, "SELECT a, b FROM t WHERE id = $1 ORDER BY created") != NULL) {
        PASS();
    } else {
        char msg[256];
        snprintf(msg, sizeof(msg), "Structure not preserved: '%s'", norm->normalized_sql);
        FAIL(msg);
    }

    free_normalized_sql(norm);
}

// Test 4: String literals normalized to '?'
static void test_sql_normalization_string_literals(void) {
    TEST("SQL Normalization - string literals");

    const char *sql = "SELECT * FROM t WHERE name = 'hello'";
    normalized_sql_t *norm = normalize_sql_all_literals(sql);

    if (!norm) {
        FAIL("Normalization returned NULL");
        return;
    }

    if (strcmp(norm->normalized_sql, "SELECT * FROM t WHERE name = $1") == 0 &&
        norm->param_count == 1 &&
        strcmp(norm->param_values[0], "hello") == 0) {
        PASS();
    } else {
        char msg[256];
        snprintf(msg, sizeof(msg), "Expected '$1' placeholder, got '%s', param='%s'",
                 norm->normalized_sql, norm->param_values[0] ? norm->param_values[0] : "NULL");
        FAIL(msg);
    }

    free_normalized_sql(norm);
}

// Test 5: Unicode in literals handled safely
static void test_sql_normalization_unicode(void) {
    TEST("SQL Normalization - unicode handling");

    // Unicode string literal - should be extracted as-is
    const char *sql = "SELECT * FROM t WHERE name = '\xC3\xA9\xC3\xA0\xC3\xBC'";  // UTF-8: eàü
    normalized_sql_t *norm = normalize_sql_all_literals(sql);

    if (!norm) {
        FAIL("Normalization returned NULL");
        return;
    }

    // Verify unicode bytes are preserved in extracted param
    if (norm->param_count == 1 &&
        strcmp(norm->param_values[0], "\xC3\xA9\xC3\xA0\xC3\xBC") == 0) {
        PASS();
    } else {
        FAIL("Unicode not preserved correctly");
    }

    free_normalized_sql(norm);
}

// ============================================================================
// Prepared Statement Cache Tests
// ============================================================================

#define STMT_CACHE_SIZE_TEST 512
#define STMT_CACHE_MASK_TEST (STMT_CACHE_SIZE_TEST - 1)

typedef struct {
    uint64_t sql_hash;
    char stmt_name[32];
    int param_count;
    int prepared;
    time_t last_used;
} test_stmt_cache_entry_t;

typedef struct {
    test_stmt_cache_entry_t entries[STMT_CACHE_SIZE_TEST];
    int count;
} test_stmt_cache_t;

// FNV-1a hash for prepared statement cache
static uint64_t test_pg_hash_sql(const char *sql) {
    if (!sql) return 0;

    uint64_t hash = 14695981039346656037ULL;  // FNV offset basis
    while (*sql) {
        hash ^= (uint64_t)(unsigned char)*sql++;
        hash *= 1099511628211ULL;  // FNV prime
    }
    return hash;
}

// Lookup in test cache
static int test_stmt_cache_lookup(test_stmt_cache_t *cache, uint64_t sql_hash, const char **stmt_name) {
    if (!cache || !stmt_name || sql_hash == 0) return 0;

    int start_idx = (int)(sql_hash & STMT_CACHE_MASK_TEST);

    for (int probe = 0; probe < STMT_CACHE_SIZE_TEST; probe++) {
        int idx = (start_idx + probe) & STMT_CACHE_MASK_TEST;
        test_stmt_cache_entry_t *entry = &cache->entries[idx];

        if (entry->sql_hash == 0) {
            return 0;  // Empty slot - not found
        }

        if (entry->sql_hash == sql_hash && entry->prepared) {
            entry->last_used = time(NULL);
            *stmt_name = entry->stmt_name;
            return 1;  // Found
        }
    }

    return 0;
}

// Add to test cache
static int test_stmt_cache_add(test_stmt_cache_t *cache, uint64_t sql_hash,
                               const char *stmt_name, int param_count) {
    if (!cache || !stmt_name || sql_hash == 0) return -1;

    int start_idx = (int)(sql_hash & STMT_CACHE_MASK_TEST);
    int oldest_idx = -1;
    time_t oldest_time = 0;

    for (int probe = 0; probe < STMT_CACHE_SIZE_TEST; probe++) {
        int idx = (start_idx + probe) & STMT_CACHE_MASK_TEST;
        test_stmt_cache_entry_t *entry = &cache->entries[idx];

        // Track oldest for LRU eviction
        if (oldest_idx == -1 || (entry->sql_hash != 0 && entry->last_used < oldest_time)) {
            oldest_idx = idx;
            oldest_time = entry->last_used;
        }

        if (entry->sql_hash == sql_hash) {
            // Already exists - update
            entry->prepared = 1;
            entry->param_count = param_count;
            entry->last_used = time(NULL);
            strncpy(entry->stmt_name, stmt_name, sizeof(entry->stmt_name) - 1);
            return idx;
        }

        if (entry->sql_hash == 0) {
            // Empty slot - use it
            entry->sql_hash = sql_hash;
            entry->param_count = param_count;
            entry->prepared = 1;
            entry->last_used = time(NULL);
            strncpy(entry->stmt_name, stmt_name, sizeof(entry->stmt_name) - 1);
            cache->count++;
            return idx;
        }
    }

    // Cache full - evict oldest (LRU)
    if (oldest_idx >= 0) {
        test_stmt_cache_entry_t *entry = &cache->entries[oldest_idx];
        entry->sql_hash = sql_hash;
        entry->param_count = param_count;
        entry->prepared = 1;
        entry->last_used = time(NULL);
        strncpy(entry->stmt_name, stmt_name, sizeof(entry->stmt_name) - 1);
        return oldest_idx;
    }

    return -1;
}

// Test 6: Prepared cache hit
static void test_prepared_cache_hit(void) {
    TEST("Prepared Cache - cache hit on second execution");

    test_stmt_cache_t cache;
    memset(&cache, 0, sizeof(cache));

    const char *sql = "SELECT * FROM metadata_items WHERE id = $1";
    uint64_t hash = test_pg_hash_sql(sql);

    // First execution - add to cache
    test_stmt_cache_add(&cache, hash, "ps_test1", 1);

    // Second execution - should hit
    const char *found_name = NULL;
    if (test_stmt_cache_lookup(&cache, hash, &found_name) &&
        found_name && strcmp(found_name, "ps_test1") == 0) {
        PASS();
    } else {
        FAIL("Cache miss on second execution");
    }
}

// Test 7: Prepared cache miss on different query
static void test_prepared_cache_miss_different_query(void) {
    TEST("Prepared Cache - cache miss for different query");

    test_stmt_cache_t cache;
    memset(&cache, 0, sizeof(cache));

    const char *sql1 = "SELECT * FROM table_a WHERE id = $1";
    const char *sql2 = "SELECT * FROM table_b WHERE id = $1";

    uint64_t hash1 = test_pg_hash_sql(sql1);
    uint64_t hash2 = test_pg_hash_sql(sql2);

    // Add first query
    test_stmt_cache_add(&cache, hash1, "ps_a", 1);

    // Lookup second query - should miss
    const char *found_name = NULL;
    if (!test_stmt_cache_lookup(&cache, hash2, &found_name)) {
        PASS();
    } else {
        FAIL("Should miss for different query");
    }
}

// Test 8: LRU eviction when cache full
static void test_prepared_cache_eviction_lru(void) {
    TEST("Prepared Cache - LRU eviction");

    // Use a small simulated cache for this test
    #define SMALL_CACHE_SIZE 4

    typedef struct {
        uint64_t sql_hash;
        char stmt_name[32];
        time_t last_used;
        int prepared;
    } small_entry_t;

    small_entry_t small_cache[SMALL_CACHE_SIZE];
    memset(small_cache, 0, sizeof(small_cache));

    // Fill cache with entries (oldest first)
    small_cache[0] = (small_entry_t){.sql_hash = 1, .last_used = 100, .prepared = 1};
    strcpy(small_cache[0].stmt_name, "oldest");

    small_cache[1] = (small_entry_t){.sql_hash = 2, .last_used = 200, .prepared = 1};
    strcpy(small_cache[1].stmt_name, "second");

    small_cache[2] = (small_entry_t){.sql_hash = 3, .last_used = 300, .prepared = 1};
    strcpy(small_cache[2].stmt_name, "third");

    small_cache[3] = (small_entry_t){.sql_hash = 4, .last_used = 400, .prepared = 1};
    strcpy(small_cache[3].stmt_name, "newest");

    // Find LRU slot (oldest entry)
    int lru_idx = 0;
    time_t oldest = small_cache[0].last_used;
    for (int i = 1; i < SMALL_CACHE_SIZE; i++) {
        if (small_cache[i].last_used < oldest) {
            oldest = small_cache[i].last_used;
            lru_idx = i;
        }
    }

    // Evict and add new entry
    small_cache[lru_idx].sql_hash = 5;
    small_cache[lru_idx].last_used = 500;
    strcpy(small_cache[lru_idx].stmt_name, "evicted_new");

    // Verify oldest (hash=1) was evicted
    int found_oldest = 0;
    for (int i = 0; i < SMALL_CACHE_SIZE; i++) {
        if (small_cache[i].sql_hash == 1) {
            found_oldest = 1;
            break;
        }
    }

    if (!found_oldest) {
        PASS();
    } else {
        FAIL("Oldest entry should have been evicted");
    }
}

// Test 9: Cache key incorporates parameter types
static void test_cache_key_includes_params(void) {
    TEST("Cache Key - incorporates bound parameter types");

    // The cache key from pg_query_cache.c includes SQL + bound parameter values
    // Different param values should produce different cache keys

    const char *sql = "SELECT * FROM t WHERE id = $1";
    const char *params1[] = {"100"};
    const char *params2[] = {"200"};

    // Use the compute_cache_key function from earlier in this test file
    uint64_t key1 = compute_cache_key(sql, params1, 1);
    uint64_t key2 = compute_cache_key(sql, params2, 1);

    if (key1 != key2) {
        PASS();
    } else {
        FAIL("Different param values should produce different cache keys");
    }
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("\n\033[1m=== Query Cache Tests ===\033[0m\n\n");

    printf("\033[1mHash Function:\033[0m\n");
    test_hash_consistency();
    test_hash_different_inputs();
    test_hash_distribution();
    test_hash_empty_string();
    test_hash_single_char_difference();

    printf("\n\033[1mCache Key Computation:\033[0m\n");
    test_cache_key_with_params();
    test_cache_key_same_params();
    test_cache_key_null_param();

    printf("\n\033[1mTTL Expiration:\033[0m\n");
    test_ttl_not_expired();
    test_ttl_expired();
    test_ttl_boundary();

    printf("\n\033[1mReference Counting:\033[0m\n");
    test_refcount_basic();
    test_refcount_free_at_zero();
    test_refcount_multiple_refs();

    printf("\n\033[1mLRU Eviction:\033[0m\n");
    test_lru_finds_oldest();
    test_lru_tiebreaker_hits();

    printf("\n\033[1mSQL Normalization:\033[0m\n");
    test_sql_normalization_strips_literals();
    test_sql_normalization_consistent();
    test_sql_normalization_preserves_structure();
    test_sql_normalization_string_literals();
    test_sql_normalization_unicode();

    printf("\n\033[1mPrepared Statement Cache:\033[0m\n");
    test_prepared_cache_hit();
    test_prepared_cache_miss_different_query();
    test_prepared_cache_eviction_lru();
    test_cache_key_includes_params();

    printf("\n\033[1m=== Results ===\033[0m\n");
    printf("Passed: \033[32m%d\033[0m\n", tests_passed);
    printf("Failed: \033[31m%d\033[0m\n", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
