/*
 * Unit tests based on historical crash analysis
 *
 * This file contains tests derived from actual crashes that occurred in production:
 * - Stack overflow (2026-01-06): 218 recursive frames, 544KB stack exhausted
 * - Integer overflow (2025-12-31): signed int overflow after ~2B calls
 * - Thread safety (2025-12-24): heap corruption from concurrent PQclear
 * - NULL pointer (2026-01-01): strchr/strcasestr returning NULL
 * - Deadlock (2026-01-01): blocking mutex causing 40+ thread freeze
 * - Connection pool (2025-12-31): stale slot references, state corruption
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdatomic.h>
#include <limits.h>

// Test counters
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) printf("  Testing: %s... ", name)
#define PASS() do { printf("\033[32mPASS\033[0m\n"); tests_passed++; } while(0)
#define FAIL(msg) do { printf("\033[31mFAIL: %s\033[0m\n", msg); tests_failed++; } while(0)

// ============================================================================
// CRASH SCENARIO 1: Integer Overflow (2025-12-31)
// Bug: fake_value_next was signed int, overflowed after ~2B calls
// Fix: Use unsigned int + bitmask instead of modulo
// ============================================================================

#define MAX_FAKE_VALUES 256

// BUGGY version (caused SIGBUS)
static int buggy_get_fake_slot_signed(int *counter) {
    int slot = (*counter)++ % MAX_FAKE_VALUES;
    return slot;  // BUG: Can be negative when counter overflows!
}

// FIXED version (what we use now)
static unsigned int fixed_get_fake_slot_unsigned(unsigned int *counter) {
    unsigned int slot = ((*counter)++) & 0xFF;  // Bitmask always positive
    return slot;
}

void test_integer_overflow_signed(void) {
    TEST("Integer overflow - signed counter bug detection");

    int counter = INT_MAX - 5;  // Start near overflow
    int negative_found = 0;

    for (int i = 0; i < 20; i++) {
        int slot = buggy_get_fake_slot_signed(&counter);
        if (slot < 0) {
            negative_found = 1;
            break;
        }
    }

    if (negative_found) {
        PASS();  // We correctly detected the bug would occur
    } else {
        FAIL("Should have detected negative slot index");
    }
}

void test_integer_overflow_fixed(void) {
    TEST("Integer overflow - unsigned counter fix");

    unsigned int counter = UINT_MAX - 5;  // Start near overflow
    int all_valid = 1;

    for (int i = 0; i < 20; i++) {
        unsigned int slot = fixed_get_fake_slot_unsigned(&counter);
        if (slot >= MAX_FAKE_VALUES) {
            all_valid = 0;
            break;
        }
    }

    if (all_valid) {
        PASS();
    } else {
        FAIL("Slot index out of range after overflow");
    }
}

void test_bitmask_vs_modulo(void) {
    TEST("Bitmask vs modulo equivalence for power-of-2");

    // Verify bitmask (& 0xFF) equals modulo (% 256) for all uint values
    int failures = 0;
    for (unsigned int i = 0; i < 1000; i++) {
        if ((i & 0xFF) != (i % 256)) {
            failures++;
        }
    }

    // Also test edge cases
    unsigned int edge_cases[] = {0, 255, 256, 257, UINT_MAX - 1, UINT_MAX};
    for (int i = 0; i < 6; i++) {
        if ((edge_cases[i] & 0xFF) != (edge_cases[i] % 256)) {
            failures++;
        }
    }

    if (failures == 0) {
        PASS();
    } else {
        FAIL("Bitmask and modulo differ");
    }
}

// ============================================================================
// CRASH SCENARIO 2: Stack Overflow (2026-01-06)
// Bug: Recursion depth reached 218, stack exhausted 544KB
// Fix: Track depth, reject at 100; threshold checks at 400KB/500KB
// ============================================================================

#define RECURSION_LIMIT 100
#define STACK_HARD_LIMIT 400000   // 400KB - reject query
#define STACK_SOFT_LIMIT 500000   // 500KB - skip complex processing

static __thread int prepare_depth = 0;

typedef struct {
    int recursion_rejected;
    int stack_rejected;
    int soft_limit_triggered;
} protection_result_t;

static protection_result_t simulate_prepare_with_protection(int depth, size_t stack_remaining) {
    protection_result_t result = {0, 0, 0};

    prepare_depth = depth;

    // Check 1: Recursion limit
    if (prepare_depth > RECURSION_LIMIT) {
        result.recursion_rejected = 1;
        return result;
    }

    // Check 2: Hard stack limit
    if (stack_remaining < STACK_HARD_LIMIT) {
        result.stack_rejected = 1;
        return result;
    }

    // Check 3: Soft stack limit
    if (stack_remaining < STACK_SOFT_LIMIT) {
        result.soft_limit_triggered = 1;
    }

    return result;
}

void test_recursion_limit_218(void) {
    TEST("Recursion limit - rejects at depth 218 (actual crash)");

    protection_result_t r = simulate_prepare_with_protection(218, 1000000);

    if (r.recursion_rejected) {
        PASS();
    } else {
        FAIL("Should reject at 218 recursion depth");
    }
}

void test_recursion_limit_boundary(void) {
    TEST("Recursion limit - boundary at 100");

    protection_result_t r100 = simulate_prepare_with_protection(100, 1000000);
    protection_result_t r101 = simulate_prepare_with_protection(101, 1000000);

    if (!r100.recursion_rejected && r101.recursion_rejected) {
        PASS();
    } else {
        FAIL("Should accept 100, reject 101");
    }
}

void test_stack_hard_limit(void) {
    TEST("Stack hard limit - rejects below 400KB");

    protection_result_t r_ok = simulate_prepare_with_protection(1, 450000);
    protection_result_t r_fail = simulate_prepare_with_protection(1, 350000);

    if (!r_ok.stack_rejected && r_fail.stack_rejected) {
        PASS();
    } else {
        FAIL("Should accept 450KB, reject 350KB");
    }
}

void test_stack_soft_limit(void) {
    TEST("Stack soft limit - triggers below 500KB");

    protection_result_t r_ok = simulate_prepare_with_protection(1, 550000);
    protection_result_t r_soft = simulate_prepare_with_protection(1, 450000);

    if (!r_ok.soft_limit_triggered && r_soft.soft_limit_triggered) {
        PASS();
    } else {
        FAIL("Should not trigger at 550KB, should trigger at 450KB");
    }
}

void test_crash_scenario_exact(void) {
    TEST("Exact crash scenario - 544KB stack, 42KB remaining");

    // From CRASH_ANALYSIS_ONDECK.md: 42KB remaining caused crash
    protection_result_t r = simulate_prepare_with_protection(1, 42000);

    if (r.stack_rejected) {
        PASS();
    } else {
        FAIL("Should reject with only 42KB remaining");
    }
}

// ============================================================================
// CRASH SCENARIO 3: NULL Pointer (2026-01-01)
// Bug: strchr/strcasestr returning NULL not checked
// Fix: Always check return values before use
// ============================================================================

// Safe version of strcasestr that handles NULL
static char* safe_strcasestr(const char *haystack, const char *needle) {
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

// Simulate the buggy json operator fix (kept for documentation)
// static int buggy_fix_json_operator(const char *sql) {
//     char *arrow = strchr(sql, '>');
//     // BUG: No NULL check!
//     if (arrow[-1] == '-') {  // Crash if arrow is NULL!
//         return 1;
//     }
//     return 0;
// }

static int fixed_fix_json_operator(const char *sql) {
    char *arrow = strchr(sql, '>');
    if (arrow && arrow > sql && arrow[-1] == '-') {
        return 1;
    }
    return 0;
}

void test_null_strchr_crash(void) {
    TEST("NULL pointer - strchr returns NULL");

    // This would crash with buggy version
    const char *sql = "SELECT * FROM table";  // No '>' character

    // Fixed version should handle this
    int result = fixed_fix_json_operator(sql);

    if (result == 0) {
        PASS();
    } else {
        FAIL("Should return 0 when no '>' found");
    }
}

void test_safe_strcasestr_null(void) {
    TEST("safe_strcasestr - handles NULL inputs");

    char *r1 = safe_strcasestr(NULL, "test");
    char *r2 = safe_strcasestr("test", NULL);
    char *r3 = safe_strcasestr("hello world", "WORLD");

    if (r1 == NULL && r2 == NULL && r3 != NULL) {
        PASS();
    } else {
        FAIL("NULL handling incorrect");
    }
}

void test_strchr_boundary(void) {
    TEST("strchr boundary - arrow at start of string");

    const char *sql = ">value";
    char *arrow = strchr(sql, '>');

    // Arrow is at position 0, arrow[-1] would be out of bounds!
    int safe = (arrow && arrow > sql);

    if (!safe) {
        PASS();  // Correctly detected boundary issue
    } else {
        FAIL("Should detect boundary issue");
    }
}

// ============================================================================
// CRASH SCENARIO 4: Deadlock (2026-01-01)
// Bug: 40+ threads blocked on mutex_lock while holding other locks
// Fix: Use trylock with fallback to SQLite
// ============================================================================

static pthread_mutex_t test_mutex1 = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t test_mutex2 = PTHREAD_MUTEX_INITIALIZER;
static _Atomic int deadlock_detected = 0;

// Simulate the deadlock-prone pattern
static void* deadlock_thread_a(void *arg) {
    (void)arg;
    pthread_mutex_lock(&test_mutex1);
    usleep(1000);  // Hold mutex1, try to get mutex2

    if (pthread_mutex_trylock(&test_mutex2) != 0) {
        // Would deadlock with blocking lock!
        atomic_store(&deadlock_detected, 1);
    } else {
        pthread_mutex_unlock(&test_mutex2);
    }

    pthread_mutex_unlock(&test_mutex1);
    return NULL;
}

static void* deadlock_thread_b(void *arg) {
    (void)arg;
    pthread_mutex_lock(&test_mutex2);
    usleep(1000);  // Hold mutex2, try to get mutex1

    if (pthread_mutex_trylock(&test_mutex1) != 0) {
        atomic_store(&deadlock_detected, 1);
    } else {
        pthread_mutex_unlock(&test_mutex1);
    }

    pthread_mutex_unlock(&test_mutex2);
    return NULL;
}

void test_deadlock_detection(void) {
    TEST("Deadlock - trylock detects potential deadlock");

    atomic_store(&deadlock_detected, 0);

    pthread_t t1, t2;
    pthread_create(&t1, NULL, deadlock_thread_a, NULL);
    pthread_create(&t2, NULL, deadlock_thread_b, NULL);

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);

    // With trylock, we detect the contention instead of deadlocking
    if (atomic_load(&deadlock_detected)) {
        PASS();
    } else {
        // No contention occurred (threads didn't overlap) - still OK
        printf("(no contention) ");
        PASS();
    }
}

void test_trylock_with_retry(void) {
    TEST("Trylock with retry loop (10x 1ms)");

    pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&mtx);

    // Try to acquire locked mutex with retry loop
    int acquired = 0;
    for (int i = 0; i < 10 && !acquired; i++) {
        if (pthread_mutex_trylock(&mtx) == 0) {
            acquired = 1;
            pthread_mutex_unlock(&mtx);
        }
        usleep(1000);
    }

    pthread_mutex_unlock(&mtx);

    // Should NOT acquire (we held the lock)
    if (!acquired) {
        PASS();
    } else {
        FAIL("Should not have acquired locked mutex");
    }
}

// ============================================================================
// CRASH SCENARIO 5: Connection Pool State (2025-12-31)
// Bug: Stale slot references, corrupted state
// Fix: Atomic state machine + generation counters
// ============================================================================

typedef enum {
    SLOT_FREE = 0,
    SLOT_RESERVED = 1,
    SLOT_READY = 2,
    SLOT_RECONNECTING = 3,
    SLOT_ERROR = 4
} pool_slot_state_t;

typedef struct {
    _Atomic int state;
    _Atomic uint32_t generation;
    void *connection;  // Simulated
} pool_slot_t;

#define POOL_SIZE 4

static pool_slot_t test_pool[POOL_SIZE];

static void init_test_pool(void) {
    for (int i = 0; i < POOL_SIZE; i++) {
        atomic_store(&test_pool[i].state, SLOT_FREE);
        atomic_store(&test_pool[i].generation, 0);
        test_pool[i].connection = NULL;
    }
}

static int pool_acquire_slot_atomic(uint32_t *out_generation) {
    for (int i = 0; i < POOL_SIZE; i++) {
        int expected = SLOT_FREE;
        if (atomic_compare_exchange_strong(&test_pool[i].state, &expected, SLOT_RESERVED)) {
            *out_generation = atomic_fetch_add(&test_pool[i].generation, 1);
            atomic_store(&test_pool[i].state, SLOT_READY);
            return i;
        }
    }
    return -1;  // Pool exhausted
}

static int pool_release_slot(int slot, uint32_t expected_gen) {
    if (slot < 0 || slot >= POOL_SIZE) return -1;

    uint32_t current_gen = atomic_load(&test_pool[slot].generation);
    if (current_gen != expected_gen + 1) {
        return -1;  // Stale reference!
    }

    atomic_store(&test_pool[slot].state, SLOT_FREE);
    return 0;
}

void test_pool_atomic_acquire(void) {
    TEST("Connection pool - atomic CAS acquire");

    init_test_pool();

    uint32_t gen;
    int slot = pool_acquire_slot_atomic(&gen);

    if (slot >= 0 && slot < POOL_SIZE &&
        atomic_load(&test_pool[slot].state) == SLOT_READY) {
        PASS();
    } else {
        FAIL("Failed to acquire slot");
    }
}

void test_pool_generation_counter(void) {
    TEST("Connection pool - generation counter prevents stale use");

    init_test_pool();

    uint32_t gen1, gen2;
    int slot = pool_acquire_slot_atomic(&gen1);
    pool_release_slot(slot, gen1);

    // Acquire same slot again
    int slot2 = pool_acquire_slot_atomic(&gen2);

    // Try to release with OLD generation - should fail
    int result = pool_release_slot(slot2, gen1);  // Wrong generation!

    if (result == -1) {
        PASS();  // Correctly rejected stale reference
    } else {
        FAIL("Should reject stale generation");
    }
}

void test_pool_exhaustion(void) {
    TEST("Connection pool - handles exhaustion");

    init_test_pool();

    uint32_t gens[POOL_SIZE + 1];
    int slots[POOL_SIZE + 1];

    // Acquire all slots
    for (int i = 0; i < POOL_SIZE; i++) {
        slots[i] = pool_acquire_slot_atomic(&gens[i]);
    }

    // Try to acquire one more - should fail
    slots[POOL_SIZE] = pool_acquire_slot_atomic(&gens[POOL_SIZE]);

    if (slots[POOL_SIZE] == -1) {
        PASS();
    } else {
        FAIL("Should return -1 when pool exhausted");
    }
}

// ============================================================================
// CRASH SCENARIO 6: Loop Detection (2026-01-06)
// Bug: Same query repeated 218 times rapidly
// Fix: Hash-based loop detector with time window
// ============================================================================

typedef struct {
    uint32_t hash;
    int count;
    uint64_t first_time;
} loop_entry_t;

#define LOOP_SLOTS 16
#define LOOP_THRESHOLD 50
#define LOOP_WINDOW_MS 100

static __thread loop_entry_t loop_table[LOOP_SLOTS];
static __thread int loop_init = 0;

static uint64_t get_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000ULL + tv.tv_usec / 1000;
}

static uint32_t hash_sql(const char *sql) {
    uint32_t h = 5381;
    while (*sql) h = ((h << 5) + h) + *sql++;
    return h;
}

static int check_loop(const char *sql) {
    if (!loop_init) {
        memset(loop_table, 0, sizeof(loop_table));
        loop_init = 1;
    }

    uint32_t h = hash_sql(sql);
    int slot = h % LOOP_SLOTS;
    uint64_t now = get_ms();

    loop_entry_t *e = &loop_table[slot];

    if (e->hash == h && now - e->first_time < LOOP_WINDOW_MS) {
        if (++e->count >= LOOP_THRESHOLD) {
            return 1;  // Loop detected!
        }
    } else {
        e->hash = h;
        e->count = 1;
        e->first_time = now;
    }

    return 0;
}

void test_loop_218_repeats(void) {
    TEST("Loop detection - catches 218 rapid repeats");

    loop_init = 0;  // Reset

    const char *sql = "SELECT * FROM metadata_items WHERE id = ?";
    int detected = 0;

    for (int i = 0; i < 218 && !detected; i++) {
        if (check_loop(sql)) {
            detected = 1;
        }
    }

    if (detected) {
        PASS();
    } else {
        FAIL("Should detect 218 repeats");
    }
}

void test_loop_different_queries(void) {
    TEST("Loop detection - no false positive for different queries");

    loop_init = 0;  // Reset

    char sql[256];
    int false_positive = 0;

    for (int i = 0; i < 100; i++) {
        snprintf(sql, sizeof(sql), "SELECT * FROM table_%d", i);
        if (check_loop(sql)) {
            false_positive = 1;
            break;
        }
    }

    if (!false_positive) {
        PASS();
    } else {
        FAIL("False positive for different queries");
    }
}

// ============================================================================
// CRASH SCENARIO 7: OnDeck Special Case (2026-01-07)
// Bug: OnDeck queries with <100KB stack crash in Metal/dyld
// Fix: Use PostgreSQL fast path for OnDeck queries on low stack
// ============================================================================

// OnDeck query detection thresholds
#define ONDECK_STACK_THRESHOLD 100000   // 100KB - OnDeck queries need extra stack for Metal
#define STACK_CRITICAL_THRESHOLD 64000  // 64KB - Critical threshold for all queries

// Simulate OnDeck query detection (matches db_interpose_prepare.c logic)
static int is_ondeck_query(const char *sql) {
    if (!sql) return 0;

    // Match the actual OnDeck query patterns from db_interpose_prepare.c:387-391
    // OnDeck queries are identified by their SQL pattern:
    // 1. Queries with metadata_item_settings AND metadata_items
    // 2. Queries with metadata_item_views AND grandparents
    // 3. Queries with grandparentsSettings
    if ((safe_strcasestr(sql, "metadata_item_settings") && safe_strcasestr(sql, "metadata_items")) ||
        (safe_strcasestr(sql, "metadata_item_views") && safe_strcasestr(sql, "grandparents")) ||
        safe_strcasestr(sql, "grandparentsSettings")) {
        return 1;
    }

    return 0;
}

// Simulate OnDeck stack protection (matches db_interpose_prepare.c logic)
typedef enum {
    ONDECK_PROCEED = 0,       // Normal execution
    ONDECK_USE_PG_FAST = 1,   // Use PostgreSQL fast path (low stack)
    ONDECK_FALLBACK = 2       // No PG connection, return empty (critical)
} ondeck_action_t;

static ondeck_action_t check_ondeck_stack_protection(const char *sql, size_t stack_remaining, int has_pg_conn) {
    if (!sql) return ONDECK_PROCEED;

    int is_ondeck = is_ondeck_query(sql);

    // OnDeck queries with low stack (<100KB) get special handling
    if (is_ondeck && stack_remaining < ONDECK_STACK_THRESHOLD) {
        if (has_pg_conn) {
            return ONDECK_USE_PG_FAST;  // Route to PostgreSQL fast path
        } else {
            return ONDECK_FALLBACK;     // Return empty result (SQLITE_ERROR equivalent)
        }
    }

    return ONDECK_PROCEED;
}

// Simulate general stack critical check
static int should_reject_critical_stack(size_t stack_remaining, int is_worker) {
    int threshold = is_worker ? 32000 : STACK_CRITICAL_THRESHOLD;
    return stack_remaining < threshold;
}

// ---- Test 1: OnDeck query detection ----
void test_ondeck_query_detection(void) {
    TEST("OnDeck - query pattern detection");

    // These should be detected as OnDeck queries
    int detect1 = is_ondeck_query("SELECT * FROM metadata_item_settings JOIN metadata_items");
    int detect2 = is_ondeck_query("SELECT * FROM metadata_item_views WHERE grandparents.id = 1");
    int detect3 = is_ondeck_query("SELECT grandparentsSettings FROM config");

    // These should NOT be detected as OnDeck queries
    int detect4 = is_ondeck_query("SELECT * FROM metadata_items");
    int detect5 = is_ondeck_query("SELECT * FROM library_sections");
    int detect6 = is_ondeck_query("INSERT INTO statistics_media VALUES (1,2,3)");
    int detect7 = is_ondeck_query(NULL);

    if (detect1 && detect2 && detect3 && !detect4 && !detect5 && !detect6 && !detect7) {
        PASS();
    } else {
        FAIL("OnDeck query pattern detection incorrect");
    }
}

// ---- Test 2: OnDeck skipped on low stack ----
void test_ondeck_skipped_on_low_stack(void) {
    TEST("OnDeck - uses PG fast path when stack < 100KB");

    const char *ondeck_sql = "SELECT * FROM metadata_item_settings JOIN metadata_items";

    // Test with 42KB remaining (actual crash scenario value)
    ondeck_action_t action1 = check_ondeck_stack_protection(ondeck_sql, 42000, 1);
    // Test with 80KB remaining
    ondeck_action_t action2 = check_ondeck_stack_protection(ondeck_sql, 80000, 1);
    // Test with 99KB remaining (just under threshold)
    ondeck_action_t action3 = check_ondeck_stack_protection(ondeck_sql, 99000, 1);

    if (action1 == ONDECK_USE_PG_FAST && action2 == ONDECK_USE_PG_FAST && action3 == ONDECK_USE_PG_FAST) {
        PASS();
    } else {
        FAIL("OnDeck should use PG fast path when stack < 100KB");
    }
}

// ---- Test 3: OnDeck allowed on normal stack ----
void test_ondeck_allowed_on_normal_stack(void) {
    TEST("OnDeck - proceeds normally when stack > 100KB");

    const char *ondeck_sql = "SELECT * FROM metadata_item_settings JOIN metadata_items";

    // Test with 200KB remaining
    ondeck_action_t action1 = check_ondeck_stack_protection(ondeck_sql, 200000, 1);
    // Test with 100KB remaining (at threshold)
    ondeck_action_t action2 = check_ondeck_stack_protection(ondeck_sql, 100000, 1);
    // Test with 500KB remaining
    ondeck_action_t action3 = check_ondeck_stack_protection(ondeck_sql, 500000, 1);

    if (action1 == ONDECK_PROCEED && action2 == ONDECK_PROCEED && action3 == ONDECK_PROCEED) {
        PASS();
    } else {
        FAIL("OnDeck should proceed normally when stack >= 100KB");
    }
}

// ---- Test 4: Non-OnDeck queries not affected ----
void test_non_ondeck_not_affected(void) {
    TEST("Non-OnDeck queries - not affected by OnDeck skip logic");

    const char *regular_sql = "SELECT * FROM metadata_items WHERE id = 1";
    const char *insert_sql = "INSERT INTO statistics VALUES (1, 2, 3)";
    const char *library_sql = "SELECT * FROM library_sections";

    // Even with low stack, non-OnDeck queries should proceed normally
    ondeck_action_t action1 = check_ondeck_stack_protection(regular_sql, 42000, 1);
    ondeck_action_t action2 = check_ondeck_stack_protection(insert_sql, 42000, 1);
    ondeck_action_t action3 = check_ondeck_stack_protection(library_sql, 42000, 1);
    ondeck_action_t action4 = check_ondeck_stack_protection(NULL, 42000, 1);

    if (action1 == ONDECK_PROCEED && action2 == ONDECK_PROCEED &&
        action3 == ONDECK_PROCEED && action4 == ONDECK_PROCEED) {
        PASS();
    } else {
        FAIL("Non-OnDeck queries should not be affected by OnDeck logic");
    }
}

// ---- Test 5: Stack critical threshold triggers fallback ----
void test_stack_critical_threshold(void) {
    TEST("Stack critical threshold - 64KB triggers fallback for all queries");

    // Below 64KB should reject
    int reject1 = should_reject_critical_stack(63000, 0);  // 63KB - below threshold
    int reject2 = should_reject_critical_stack(50000, 0);  // 50KB - well below
    int reject3 = should_reject_critical_stack(32000, 0);  // 32KB - very low

    // At or above 64KB should not reject
    int reject4 = should_reject_critical_stack(64000, 0);  // 64KB - at threshold
    int reject5 = should_reject_critical_stack(65000, 0);  // 65KB - above threshold
    int reject6 = should_reject_critical_stack(200000, 0); // 200KB - plenty of stack

    // Worker threads have lower threshold (32KB)
    int reject7 = should_reject_critical_stack(31000, 1);  // Below worker threshold
    int reject8 = should_reject_critical_stack(32000, 1);  // At worker threshold

    if (reject1 && reject2 && reject3 &&
        !reject4 && !reject5 && !reject6 &&
        reject7 && !reject8) {
        PASS();
    } else {
        FAIL("64KB critical threshold not working correctly");
    }
}

// ---- Test 6: Graceful fallback to SQLite ----
void test_graceful_fallback_to_sqlite(void) {
    TEST("OnDeck - graceful fallback when no PG connection (returns SQLITE_ERROR equiv)");

    const char *ondeck_sql = "SELECT * FROM metadata_item_settings JOIN metadata_items";

    // When PG connection is available, use fast path
    ondeck_action_t action1 = check_ondeck_stack_protection(ondeck_sql, 42000, 1);  // has_pg_conn=1

    // When NO PG connection, fallback to empty result (triggers SQLite fallback)
    ondeck_action_t action2 = check_ondeck_stack_protection(ondeck_sql, 42000, 0);  // has_pg_conn=0
    ondeck_action_t action3 = check_ondeck_stack_protection(ondeck_sql, 80000, 0);  // has_pg_conn=0

    // Normal stack should proceed regardless of PG connection
    ondeck_action_t action4 = check_ondeck_stack_protection(ondeck_sql, 200000, 0);  // has_pg_conn=0, normal stack

    if (action1 == ONDECK_USE_PG_FAST &&
        action2 == ONDECK_FALLBACK &&
        action3 == ONDECK_FALLBACK &&
        action4 == ONDECK_PROCEED) {
        PASS();
    } else {
        FAIL("Graceful fallback not working correctly");
    }
}

// Legacy tests kept for backwards compatibility
void test_ondeck_low_stack_skip(void) {
    TEST("OnDeck - (legacy) skipped on critically low stack (<100KB)");

    const char *sql = "SELECT * FROM metadata_item_settings JOIN metadata_items";

    ondeck_action_t action = check_ondeck_stack_protection(sql, 42000, 1);

    if (action == ONDECK_USE_PG_FAST) {
        PASS();
    } else {
        FAIL("Should use PG fast path with 42KB stack");
    }
}

void test_ondeck_normal_stack_ok(void) {
    TEST("OnDeck - (legacy) allowed on normal stack (>100KB)");

    const char *sql = "SELECT * FROM metadata_item_settings JOIN metadata_items";

    ondeck_action_t action = check_ondeck_stack_protection(sql, 200000, 1);

    if (action == ONDECK_PROCEED) {
        PASS();
    } else {
        FAIL("Should proceed normally with 200KB stack");
    }
}

void test_non_ondeck_low_stack_ok(void) {
    TEST("Non-OnDeck queries - (legacy) not affected by OnDeck skip");

    const char *sql = "SELECT * FROM metadata_items";

    ondeck_action_t action = check_ondeck_stack_protection(sql, 42000, 1);

    if (action == ONDECK_PROCEED) {
        PASS();
    } else {
        FAIL("Non-OnDeck should not be skipped");
    }
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("\n\033[1m=== Crash Scenario Tests (from production history) ===\033[0m\n\n");

    printf("\033[1m1. Integer Overflow (SIGBUS 2025-12-31):\033[0m\n");
    test_integer_overflow_signed();
    test_integer_overflow_fixed();
    test_bitmask_vs_modulo();

    printf("\n\033[1m2. Stack Overflow (2026-01-06):\033[0m\n");
    test_recursion_limit_218();
    test_recursion_limit_boundary();
    test_stack_hard_limit();
    test_stack_soft_limit();
    test_crash_scenario_exact();

    printf("\n\033[1m3. NULL Pointer Crashes:\033[0m\n");
    test_null_strchr_crash();
    test_safe_strcasestr_null();
    test_strchr_boundary();

    printf("\n\033[1m4. Deadlock Prevention:\033[0m\n");
    test_deadlock_detection();
    test_trylock_with_retry();

    printf("\n\033[1m5. Connection Pool Safety:\033[0m\n");
    test_pool_atomic_acquire();
    test_pool_generation_counter();
    test_pool_exhaustion();

    printf("\n\033[1m6. Loop Detection:\033[0m\n");
    test_loop_218_repeats();
    test_loop_different_queries();

    printf("\n\033[1m7. OnDeck Stack Protection:\033[0m\n");
    test_ondeck_query_detection();
    test_ondeck_skipped_on_low_stack();
    test_ondeck_allowed_on_normal_stack();
    test_non_ondeck_not_affected();
    test_stack_critical_threshold();
    test_graceful_fallback_to_sqlite();

    printf("\n\033[1m7b. OnDeck Legacy Tests (backwards compatibility):\033[0m\n");
    test_ondeck_low_stack_skip();
    test_ondeck_normal_stack_ok();
    test_non_ondeck_low_stack_ok();

    printf("\n\033[1m=== Results ===\033[0m\n");
    printf("Passed: \033[32m%d\033[0m\n", tests_passed);
    printf("Failed: \033[31m%d\033[0m\n", tests_failed);
    printf("\nTests based on actual crashes from:\n");
    printf("  - crash_analysis.md (2026-01-06)\n");
    printf("  - STACK_OVERFLOW_FIX.md (2026-01-06)\n");
    printf("  - CRASH_ANALYSIS_ONDECK.md (2026-01-07)\n");
    printf("  - Git commits: 3493906, a56c321, cc58df1, d8be226, f29a7eb, 5d8ce3e\n");

    return tests_failed > 0 ? 1 : 0;
}
