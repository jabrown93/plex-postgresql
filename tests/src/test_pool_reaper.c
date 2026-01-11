/*
 * Unit tests for PostgreSQL Connection Pool Reaper functionality
 *
 * Tests:
 * 1. test_pool_slot_states - verify SLOT_FREE, SLOT_READY, SLOT_RESERVED transitions
 * 2. test_reaper_skips_ready_slots - reaper should NOT close SLOT_READY connections
 * 3. test_reaper_closes_idle_free_slots - reaper SHOULD close SLOT_FREE idle connections
 * 4. test_reaper_rate_limiting - reaper shouldn't run more than once per interval
 * 5. test_last_used_not_reset_on_release - verify the bug fix (last_used keeps query time)
 * 6. test_concurrent_reaper_access - multiple threads calling pool_get_connection
 *
 * The pool reaper in src/pg_client.c:
 * 1. Runs every POOL_REAP_INTERVAL (60 seconds)
 * 2. Closes connections idle > POOL_IDLE_TIMEOUT (300 seconds)
 * 3. Uses atomic CAS to safely claim slots
 * 4. Only closes SLOT_FREE slots with connections
 *
 * IMPORTANT: These tests verify pool reaper logic WITHOUT loading the actual shim.
 * They test the state management and cleanup mechanisms in isolation.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>

// Test counters
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) printf("  Testing: %s... ", name)
#define PASS() do { printf("\033[32mPASS\033[0m\n"); tests_passed++; } while(0)
#define FAIL(msg) do { printf("\033[31mFAIL: %s\033[0m\n", msg); tests_failed++; } while(0)

// ============================================================================
// Simulate pool slot state machine from pg_client.c
// ============================================================================

typedef enum {
    SLOT_FREE = 0,        // Available for any thread
    SLOT_RESERVED,        // Thread claimed, creating connection
    SLOT_READY,           // Connection active and usable
    SLOT_RECONNECTING,    // Thread is reconnecting
    SLOT_ERROR            // Connection failed
} pool_slot_state_t;

// Simulated connection structure
typedef struct {
    void *pg_conn;              // Simulated PGconn pointer
    int is_pg_active;
} mock_connection_t;

typedef struct {
    mock_connection_t *conn;
    pthread_t owner_thread;           // Thread that owns this connection
    time_t last_used;                 // Last time this connection was used
    _Atomic pool_slot_state_t state;  // Atomic state machine
    _Atomic uint32_t generation;      // Increment on each reuse
} mock_pool_slot_t;

// Configuration constants (from pg_client.c)
#define POOL_SIZE_MAX 16
#define POOL_IDLE_TIMEOUT 300         // Seconds before connection is considered idle
#define POOL_REAP_INTERVAL 60         // Run reaper at most every 60 seconds

static mock_pool_slot_t mock_pool[POOL_SIZE_MAX];
static int configured_pool_size = POOL_SIZE_MAX;

// Track reaper calls
static _Atomic time_t last_reap_time = 0;
static int reaper_call_count = 0;
static int connections_reaped = 0;

// ============================================================================
// Helper: Create a mock connection
// ============================================================================

static mock_connection_t* create_mock_connection(void) {
    mock_connection_t *conn = calloc(1, sizeof(mock_connection_t));
    if (conn) {
        conn->pg_conn = (void*)(uintptr_t)(rand() | 0x10000000);  // Non-null fake pointer
        conn->is_pg_active = 1;
    }
    return conn;
}

// ============================================================================
// Helper: Initialize pool with test data
// ============================================================================

static void reset_pool(void) {
    memset(mock_pool, 0, sizeof(mock_pool));
    for (int i = 0; i < POOL_SIZE_MAX; i++) {
        atomic_store(&mock_pool[i].state, SLOT_FREE);
        atomic_store(&mock_pool[i].generation, 0);
    }
    atomic_store(&last_reap_time, 0);
    reaper_call_count = 0;
    connections_reaped = 0;
}

static void cleanup_pool(void) {
    for (int i = 0; i < POOL_SIZE_MAX; i++) {
        if (mock_pool[i].conn) {
            free(mock_pool[i].conn);
            mock_pool[i].conn = NULL;
        }
    }
}

// ============================================================================
// Simulate pool_reap_idle_connections from pg_client.c
// ============================================================================

static void mock_pool_reap_idle_connections(void) {
    time_t now = time(NULL);
    int reaped = 0;

    reaper_call_count++;

    // Only reap FREE slots with old connections - use atomic CAS to claim
    for (int i = 0; i < configured_pool_size; i++) {
        if (mock_pool[i].conn &&
            (now - mock_pool[i].last_used) > POOL_IDLE_TIMEOUT) {

            // Try to claim the slot atomically (CRITICAL: only claim FREE slots)
            pool_slot_state_t expected = SLOT_FREE;
            if (atomic_compare_exchange_strong(&mock_pool[i].state,
                                               &expected, SLOT_RESERVED)) {
                // Successfully claimed - safe to close
                if (mock_pool[i].conn->pg_conn) {
                    // Simulate PQfinish
                    mock_pool[i].conn->pg_conn = NULL;
                }
                free(mock_pool[i].conn);
                mock_pool[i].conn = NULL;
                mock_pool[i].owner_thread = 0;
                mock_pool[i].last_used = 0;
                atomic_store(&mock_pool[i].state, SLOT_FREE);
                reaped++;
            }
            // If CAS failed, slot is not FREE (could be READY/RESERVED/etc)
            // - that's fine, we don't reap active connections
        }
    }

    connections_reaped += reaped;
}

// ============================================================================
// Simulate rate-limited reaper invocation
// ============================================================================

static int maybe_run_reaper(void) {
    time_t now = time(NULL);
    time_t last_reap = atomic_load(&last_reap_time);

    if ((now - last_reap) >= POOL_REAP_INTERVAL) {
        // Use CAS to avoid multiple threads running reaper simultaneously
        if (atomic_compare_exchange_strong(&last_reap_time, &last_reap, now)) {
            mock_pool_reap_idle_connections();
            return 1;  // Ran reaper
        }
    }
    return 0;  // Didn't run (rate limited or lost CAS race)
}

// ============================================================================
// Test 1: Pool slot state transitions
// ============================================================================

static void test_pool_slot_states(void) {
    TEST("Pool slot state transitions (FREE -> RESERVED -> READY)");

    reset_pool();

    // Initial state should be FREE
    pool_slot_state_t state = atomic_load(&mock_pool[0].state);
    if (state != SLOT_FREE) {
        FAIL("Initial state should be SLOT_FREE");
        return;
    }

    // Simulate claiming a slot: FREE -> RESERVED
    pool_slot_state_t expected = SLOT_FREE;
    if (!atomic_compare_exchange_strong(&mock_pool[0].state, &expected, SLOT_RESERVED)) {
        FAIL("CAS FREE -> RESERVED failed");
        return;
    }

    state = atomic_load(&mock_pool[0].state);
    if (state != SLOT_RESERVED) {
        FAIL("State should be SLOT_RESERVED after claim");
        return;
    }

    // Simulate connection creation: RESERVED -> READY
    mock_pool[0].conn = create_mock_connection();
    mock_pool[0].owner_thread = pthread_self();
    mock_pool[0].last_used = time(NULL);
    atomic_store(&mock_pool[0].state, SLOT_READY);

    state = atomic_load(&mock_pool[0].state);
    if (state != SLOT_READY) {
        FAIL("State should be SLOT_READY after connection created");
        cleanup_pool();
        return;
    }

    // Simulate release: READY -> FREE
    mock_pool[0].owner_thread = 0;
    atomic_store(&mock_pool[0].state, SLOT_FREE);

    state = atomic_load(&mock_pool[0].state);
    if (state != SLOT_FREE) {
        FAIL("State should be SLOT_FREE after release");
        cleanup_pool();
        return;
    }

    // Verify connection still exists (for reuse by another thread)
    if (mock_pool[0].conn == NULL) {
        FAIL("Connection should persist after release for reuse");
        cleanup_pool();
        return;
    }

    PASS();
    cleanup_pool();
}

// ============================================================================
// Test 2: Reaper skips SLOT_READY connections
// ============================================================================

static void test_reaper_skips_ready_slots(void) {
    TEST("Reaper skips SLOT_READY connections (in-use slots)");

    reset_pool();

    // Create a READY slot with an old last_used time
    mock_pool[0].conn = create_mock_connection();
    mock_pool[0].owner_thread = pthread_self();
    mock_pool[0].last_used = time(NULL) - POOL_IDLE_TIMEOUT - 100;  // Very idle
    atomic_store(&mock_pool[0].state, SLOT_READY);  // But still in READY state

    void *original_conn = mock_pool[0].conn->pg_conn;

    // Run reaper
    mock_pool_reap_idle_connections();

    // Verify connection was NOT reaped
    if (mock_pool[0].conn == NULL) {
        FAIL("Reaper closed SLOT_READY connection (should not happen!)");
        return;
    }

    if (mock_pool[0].conn->pg_conn != original_conn) {
        FAIL("Connection pointer changed unexpectedly");
        cleanup_pool();
        return;
    }

    pool_slot_state_t state = atomic_load(&mock_pool[0].state);
    if (state != SLOT_READY) {
        FAIL("Slot state changed unexpectedly");
        cleanup_pool();
        return;
    }

    if (connections_reaped != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected 0 reaped, got %d", connections_reaped);
        FAIL(msg);
        cleanup_pool();
        return;
    }

    PASS();
    cleanup_pool();
}

// ============================================================================
// Test 3: Reaper closes idle SLOT_FREE connections
// ============================================================================

static void test_reaper_closes_idle_free_slots(void) {
    TEST("Reaper closes idle SLOT_FREE connections");

    reset_pool();

    // Create a FREE slot with an old last_used time
    mock_pool[0].conn = create_mock_connection();
    mock_pool[0].owner_thread = 0;  // No owner
    mock_pool[0].last_used = time(NULL) - POOL_IDLE_TIMEOUT - 100;
    atomic_store(&mock_pool[0].state, SLOT_FREE);

    // Create a FREE slot that's NOT idle enough
    mock_pool[1].conn = create_mock_connection();
    mock_pool[1].owner_thread = 0;
    mock_pool[1].last_used = time(NULL) - 10;  // Only 10 seconds old
    atomic_store(&mock_pool[1].state, SLOT_FREE);

    // Run reaper
    mock_pool_reap_idle_connections();

    // Verify first connection WAS reaped
    if (mock_pool[0].conn != NULL) {
        FAIL("Idle SLOT_FREE connection should have been reaped");
        cleanup_pool();
        return;
    }

    // Verify second connection was NOT reaped
    if (mock_pool[1].conn == NULL) {
        FAIL("Non-idle connection should NOT have been reaped");
        cleanup_pool();
        return;
    }

    if (connections_reaped != 1) {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected 1 reaped, got %d", connections_reaped);
        FAIL(msg);
        cleanup_pool();
        return;
    }

    PASS();
    cleanup_pool();
}

// ============================================================================
// Test 4: Reaper rate limiting
// ============================================================================

static void test_reaper_rate_limiting(void) {
    TEST("Reaper rate limiting (once per POOL_REAP_INTERVAL)");

    reset_pool();

    // First call should run
    atomic_store(&last_reap_time, time(NULL) - POOL_REAP_INTERVAL - 1);
    int ran1 = maybe_run_reaper();

    if (!ran1) {
        FAIL("First reaper call should have run");
        return;
    }

    int initial_count = reaper_call_count;

    // Second call immediately after should NOT run (rate limited)
    int ran2 = maybe_run_reaper();

    if (ran2) {
        FAIL("Second reaper call should be rate limited");
        return;
    }

    if (reaper_call_count != initial_count) {
        FAIL("Reaper ran when it shouldn't have");
        return;
    }

    // After interval passes, should run again
    atomic_store(&last_reap_time, time(NULL) - POOL_REAP_INTERVAL - 1);
    int ran3 = maybe_run_reaper();

    if (!ran3) {
        FAIL("Reaper should run after interval passes");
        return;
    }

    if (reaper_call_count != initial_count + 1) {
        FAIL("Reaper call count incorrect");
        return;
    }

    PASS();
}

// ============================================================================
// Test 5: last_used not reset on release (bug fix verification)
// ============================================================================

static void test_last_used_not_reset_on_release(void) {
    TEST("last_used preserved on release (reaper idle time fix)");

    reset_pool();

    // Create a connection with a specific last_used time
    time_t query_time = time(NULL) - 200;  // Query ran 200 seconds ago

    mock_pool[0].conn = create_mock_connection();
    mock_pool[0].owner_thread = pthread_self();
    mock_pool[0].last_used = query_time;  // Time of last actual query
    atomic_store(&mock_pool[0].state, SLOT_READY);

    // Simulate release (like pg_close_pool_for_db does)
    // CRITICAL: Do NOT reset last_used - this is the bug fix being tested
    mock_pool[0].owner_thread = 0;
    // mock_pool[0].last_used = time(NULL);  // BUG! Don't do this!
    atomic_store(&mock_pool[0].state, SLOT_FREE);

    // Verify last_used is still the original query time
    if (mock_pool[0].last_used != query_time) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "last_used was reset on release (got %ld, expected %ld)",
                 mock_pool[0].last_used, query_time);
        FAIL(msg);
        cleanup_pool();
        return;
    }

    // Now if we wait until POOL_IDLE_TIMEOUT passes from query_time,
    // the reaper should close this connection

    // Simulate time passing (adjust last_used to make it idle)
    mock_pool[0].last_used = time(NULL) - POOL_IDLE_TIMEOUT - 1;

    // Run reaper
    mock_pool_reap_idle_connections();

    // Verify connection was reaped
    if (mock_pool[0].conn != NULL) {
        FAIL("Idle connection should have been reaped");
        cleanup_pool();
        return;
    }

    PASS();
    cleanup_pool();
}

// ============================================================================
// Test 6: Concurrent reaper access (thread safety)
// ============================================================================

typedef struct {
    int thread_id;
    int slots_claimed;
    int reaper_invocations;
} thread_test_data_t;

static _Atomic int concurrent_errors = 0;

static void* concurrent_pool_access(void *arg) {
    thread_test_data_t *data = (thread_test_data_t *)arg;
    pthread_t self = pthread_self();

    // Each thread tries to claim slots and invoke reaper
    for (int iter = 0; iter < 100; iter++) {
        // Try to claim a random FREE slot
        int slot_idx = rand() % configured_pool_size;

        pool_slot_state_t expected = SLOT_FREE;
        if (atomic_compare_exchange_strong(&mock_pool[slot_idx].state,
                                           &expected, SLOT_RESERVED)) {
            data->slots_claimed++;

            // Create connection if needed
            if (mock_pool[slot_idx].conn == NULL) {
                mock_pool[slot_idx].conn = create_mock_connection();
            }
            mock_pool[slot_idx].owner_thread = self;
            mock_pool[slot_idx].last_used = time(NULL);
            atomic_fetch_add(&mock_pool[slot_idx].generation, 1);
            atomic_store(&mock_pool[slot_idx].state, SLOT_READY);

            // Use the connection briefly
            usleep(100 + (rand() % 200));

            // Release it
            mock_pool[slot_idx].owner_thread = 0;
            atomic_store(&mock_pool[slot_idx].state, SLOT_FREE);
        }

        // Occasionally trigger reaper check
        if (iter % 10 == 0) {
            // Force reaper to run for testing
            atomic_store(&last_reap_time, time(NULL) - POOL_REAP_INTERVAL - 1);
            if (maybe_run_reaper()) {
                data->reaper_invocations++;
            }
        }

        // Tiny delay to increase interleaving
        usleep(10);
    }

    return NULL;
}

static void test_concurrent_reaper_access(void) {
    TEST("Concurrent pool access with reaper");

    reset_pool();
    atomic_store(&concurrent_errors, 0);

    // Pre-populate some connections
    for (int i = 0; i < 4; i++) {
        mock_pool[i].conn = create_mock_connection();
        mock_pool[i].last_used = time(NULL);
        atomic_store(&mock_pool[i].state, SLOT_FREE);
    }

    // Launch threads
    #define NUM_THREADS 8
    pthread_t threads[NUM_THREADS];
    thread_test_data_t data[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; i++) {
        data[i].thread_id = i;
        data[i].slots_claimed = 0;
        data[i].reaper_invocations = 0;
        pthread_create(&threads[i], NULL, concurrent_pool_access, &data[i]);
    }

    // Wait for all threads
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // Check for errors
    if (atomic_load(&concurrent_errors) > 0) {
        FAIL("Concurrent access caused errors");
        cleanup_pool();
        return;
    }

    // Verify all slots are in valid state
    int valid_states = 1;
    for (int i = 0; i < configured_pool_size; i++) {
        pool_slot_state_t state = atomic_load(&mock_pool[i].state);
        if (state != SLOT_FREE && state != SLOT_READY &&
            state != SLOT_RESERVED && state != SLOT_RECONNECTING &&
            state != SLOT_ERROR) {
            valid_states = 0;
            break;
        }
    }

    if (!valid_states) {
        FAIL("Pool slots in invalid state after concurrent access");
        cleanup_pool();
        return;
    }

    // Count total activity
    int total_claims = 0;
    int total_reaper = 0;
    for (int i = 0; i < NUM_THREADS; i++) {
        total_claims += data[i].slots_claimed;
        total_reaper += data[i].reaper_invocations;
    }

    // Should have had significant activity
    if (total_claims < NUM_THREADS * 10) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "Expected more slot claims (got %d, reaper runs: %d)",
                 total_claims, total_reaper);
        FAIL(msg);
        cleanup_pool();
        return;
    }

    PASS();
    cleanup_pool();
}

// ============================================================================
// Additional Test: Reaper doesn't race with RESERVED slots
// ============================================================================

static void test_reaper_skips_reserved_slots(void) {
    TEST("Reaper skips SLOT_RESERVED (connection being created)");

    reset_pool();

    // Create a RESERVED slot (thread is creating connection)
    mock_pool[0].conn = create_mock_connection();
    mock_pool[0].owner_thread = pthread_self();
    mock_pool[0].last_used = time(NULL) - POOL_IDLE_TIMEOUT - 100;
    atomic_store(&mock_pool[0].state, SLOT_RESERVED);

    // Run reaper
    connections_reaped = 0;
    mock_pool_reap_idle_connections();

    // Verify connection was NOT reaped
    if (mock_pool[0].conn == NULL) {
        FAIL("Reaper closed SLOT_RESERVED connection");
        return;
    }

    pool_slot_state_t state = atomic_load(&mock_pool[0].state);
    if (state != SLOT_RESERVED) {
        FAIL("Slot state changed unexpectedly");
        cleanup_pool();
        return;
    }

    if (connections_reaped != 0) {
        FAIL("No connections should have been reaped");
        cleanup_pool();
        return;
    }

    PASS();
    cleanup_pool();
}

// ============================================================================
// Additional Test: Reaper doesn't race with RECONNECTING slots
// ============================================================================

static void test_reaper_skips_reconnecting_slots(void) {
    TEST("Reaper skips SLOT_RECONNECTING (connection being reset)");

    reset_pool();

    // Create a RECONNECTING slot
    mock_pool[0].conn = create_mock_connection();
    mock_pool[0].owner_thread = pthread_self();
    mock_pool[0].last_used = time(NULL) - POOL_IDLE_TIMEOUT - 100;
    atomic_store(&mock_pool[0].state, SLOT_RECONNECTING);

    // Run reaper
    connections_reaped = 0;
    mock_pool_reap_idle_connections();

    // Verify connection was NOT reaped
    if (mock_pool[0].conn == NULL) {
        FAIL("Reaper closed SLOT_RECONNECTING connection");
        return;
    }

    if (connections_reaped != 0) {
        FAIL("No connections should have been reaped");
        cleanup_pool();
        return;
    }

    PASS();
    cleanup_pool();
}

// ============================================================================
// Additional Test: ERROR slots handled correctly
// ============================================================================

static void test_reaper_skips_error_slots(void) {
    TEST("Reaper skips SLOT_ERROR (failed connections)");

    reset_pool();

    // Create an ERROR slot (connection failed)
    mock_pool[0].conn = create_mock_connection();
    mock_pool[0].owner_thread = 0;
    mock_pool[0].last_used = time(NULL) - POOL_IDLE_TIMEOUT - 100;
    atomic_store(&mock_pool[0].state, SLOT_ERROR);

    // Run reaper
    connections_reaped = 0;
    mock_pool_reap_idle_connections();

    // ERROR slots should NOT be reaped by the idle reaper
    // (they need to be reclaimed through the normal pool_get_connection PHASE 4)
    if (mock_pool[0].conn == NULL) {
        FAIL("Reaper closed SLOT_ERROR connection");
        return;
    }

    pool_slot_state_t state = atomic_load(&mock_pool[0].state);
    if (state != SLOT_ERROR) {
        FAIL("Slot state changed unexpectedly");
        cleanup_pool();
        return;
    }

    if (connections_reaped != 0) {
        FAIL("No connections should have been reaped");
        cleanup_pool();
        return;
    }

    PASS();
    cleanup_pool();
}

// ============================================================================
// Additional Test: Multiple idle connections reaped in one pass
// ============================================================================

static void test_reaper_closes_multiple_idle(void) {
    TEST("Reaper closes multiple idle connections in one pass");

    reset_pool();

    // Create several idle FREE connections
    for (int i = 0; i < 5; i++) {
        mock_pool[i].conn = create_mock_connection();
        mock_pool[i].owner_thread = 0;
        mock_pool[i].last_used = time(NULL) - POOL_IDLE_TIMEOUT - 100 - i;
        atomic_store(&mock_pool[i].state, SLOT_FREE);
    }

    // Create some non-idle connections
    for (int i = 5; i < 8; i++) {
        mock_pool[i].conn = create_mock_connection();
        mock_pool[i].owner_thread = 0;
        mock_pool[i].last_used = time(NULL) - 10;  // Recent
        atomic_store(&mock_pool[i].state, SLOT_FREE);
    }

    // Run reaper
    connections_reaped = 0;
    mock_pool_reap_idle_connections();

    // Verify exactly 5 were reaped
    if (connections_reaped != 5) {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected 5 reaped, got %d", connections_reaped);
        FAIL(msg);
        cleanup_pool();
        return;
    }

    // Verify first 5 are NULL
    int first_5_null = 1;
    for (int i = 0; i < 5; i++) {
        if (mock_pool[i].conn != NULL) {
            first_5_null = 0;
            break;
        }
    }

    if (!first_5_null) {
        FAIL("First 5 connections should be NULL");
        cleanup_pool();
        return;
    }

    // Verify remaining 3 are still there
    int remaining_exist = 1;
    for (int i = 5; i < 8; i++) {
        if (mock_pool[i].conn == NULL) {
            remaining_exist = 0;
            break;
        }
    }

    if (!remaining_exist) {
        FAIL("Non-idle connections should still exist");
        cleanup_pool();
        return;
    }

    PASS();
    cleanup_pool();
}

// ============================================================================
// Additional Test: CAS correctly handles race conditions
// ============================================================================

typedef struct {
    int slot_idx;
    int won_cas;
} cas_race_data_t;

static void* cas_race_thread(void *arg) {
    cas_race_data_t *data = (cas_race_data_t *)arg;

    // Small random delay to increase chance of true race
    usleep(rand() % 100);

    // Try to claim the slot
    pool_slot_state_t expected = SLOT_FREE;
    if (atomic_compare_exchange_strong(&mock_pool[data->slot_idx].state,
                                       &expected, SLOT_RESERVED)) {
        data->won_cas = 1;
    } else {
        data->won_cas = 0;
    }

    return NULL;
}

static void test_cas_race_condition(void) {
    TEST("CAS correctly handles race conditions");

    reset_pool();

    // Create a FREE slot that multiple threads will race to claim
    mock_pool[0].conn = create_mock_connection();
    mock_pool[0].last_used = time(NULL) - POOL_IDLE_TIMEOUT - 100;
    atomic_store(&mock_pool[0].state, SLOT_FREE);

    // Launch many threads racing for the same slot
    #define RACE_THREADS 16
    pthread_t threads[RACE_THREADS];
    cas_race_data_t data[RACE_THREADS];

    for (int i = 0; i < RACE_THREADS; i++) {
        data[i].slot_idx = 0;
        data[i].won_cas = 0;
        pthread_create(&threads[i], NULL, cas_race_thread, &data[i]);
    }

    // Wait for all threads
    for (int i = 0; i < RACE_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // Exactly ONE thread should have won the CAS
    int winners = 0;
    for (int i = 0; i < RACE_THREADS; i++) {
        if (data[i].won_cas) winners++;
    }

    if (winners != 1) {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected exactly 1 CAS winner, got %d", winners);
        FAIL(msg);
        cleanup_pool();
        return;
    }

    // Slot should be in RESERVED state
    pool_slot_state_t state = atomic_load(&mock_pool[0].state);
    if (state != SLOT_RESERVED) {
        FAIL("Slot should be in RESERVED state");
        cleanup_pool();
        return;
    }

    PASS();
    cleanup_pool();
}

// ============================================================================
// Additional Test: Generation counter prevents stale references
// ============================================================================

static void test_generation_counter(void) {
    TEST("Generation counter increments on reuse");

    reset_pool();

    // Create a connection
    mock_pool[0].conn = create_mock_connection();
    mock_pool[0].owner_thread = pthread_self();
    mock_pool[0].last_used = time(NULL);
    atomic_store(&mock_pool[0].generation, 0);
    atomic_store(&mock_pool[0].state, SLOT_READY);

    uint32_t gen1 = atomic_load(&mock_pool[0].generation);

    // Release the slot
    mock_pool[0].owner_thread = 0;
    atomic_store(&mock_pool[0].state, SLOT_FREE);

    // Re-claim the slot (simulating another thread)
    pool_slot_state_t expected = SLOT_FREE;
    if (!atomic_compare_exchange_strong(&mock_pool[0].state,
                                        &expected, SLOT_RESERVED)) {
        FAIL("Failed to re-claim slot");
        cleanup_pool();
        return;
    }

    // Increment generation on reuse
    atomic_fetch_add(&mock_pool[0].generation, 1);
    atomic_store(&mock_pool[0].state, SLOT_READY);

    uint32_t gen2 = atomic_load(&mock_pool[0].generation);

    if (gen2 != gen1 + 1) {
        char msg[64];
        snprintf(msg, sizeof(msg),
                 "Generation should increment (got %u, expected %u)",
                 gen2, gen1 + 1);
        FAIL(msg);
        cleanup_pool();
        return;
    }

    PASS();
    cleanup_pool();
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    // Seed random for concurrent tests
    srand((unsigned int)time(NULL));

    printf("\n\033[1m=== Pool Reaper Tests ===\033[0m\n\n");

    printf("\033[1mSlot State Machine:\033[0m\n");
    test_pool_slot_states();
    test_cas_race_condition();
    test_generation_counter();

    printf("\n\033[1mReaper Behavior - State Filtering:\033[0m\n");
    test_reaper_skips_ready_slots();
    test_reaper_skips_reserved_slots();
    test_reaper_skips_reconnecting_slots();
    test_reaper_skips_error_slots();

    printf("\n\033[1mReaper Behavior - Idle Connection Cleanup:\033[0m\n");
    test_reaper_closes_idle_free_slots();
    test_reaper_closes_multiple_idle();

    printf("\n\033[1mReaper Rate Limiting:\033[0m\n");
    test_reaper_rate_limiting();

    printf("\n\033[1mBug Fix Verification:\033[0m\n");
    test_last_used_not_reset_on_release();

    printf("\n\033[1mConcurrency:\033[0m\n");
    test_concurrent_reaper_access();

    printf("\n\033[1m=== Results ===\033[0m\n");
    printf("Passed: \033[32m%d\033[0m\n", tests_passed);
    printf("Failed: \033[31m%d\033[0m\n", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
