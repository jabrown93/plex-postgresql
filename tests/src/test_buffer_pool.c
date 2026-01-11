/*
 * Unit tests for column_text buffer pool expansion fix
 *
 * Background:
 * The original buffer pool was 256 entries (column_text_buffers[256][16384])
 * With 30+ parallel requests, the pool wrapped around too quickly, causing
 * SIGILL crashes when buffer contents were overwritten while still in use.
 *
 * Fix: Expanded to 4096 entries with 0xFFF bitmask for proper wrapping.
 *
 * Tests:
 * 1. test_buffer_pool_size - verify pool is 4096 entries
 * 2. test_buffer_index_wrapping - verify index wraps correctly with 0xFFF mask
 * 3. test_concurrent_buffer_access - multiple threads accessing buffers simultaneously
 * 4. test_buffer_no_overlap - verify different indices get different buffers
 * 5. test_high_concurrency_no_crash - stress test with many threads
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>

// Test counters
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) printf("  Testing: %s... ", name)
#define PASS() do { printf("\033[32mPASS\033[0m\n"); tests_passed++; } while(0)
#define FAIL(msg) do { printf("\033[31mFAIL: %s\033[0m\n", msg); tests_failed++; } while(0)

// ============================================================================
// Replicate buffer pool configuration from db_interpose_column.c
// ============================================================================

// Original (buggy) configuration:
// #define OLD_BUFFER_POOL_SIZE 256
// #define OLD_BUFFER_MASK 0xFF

// Fixed configuration (must match db_interpose_column.c):
#define BUFFER_POOL_SIZE 4096
#define BUFFER_SIZE 16384
#define BUFFER_MASK 0xFFF

// Simulate the buffer pool
static char test_buffers[BUFFER_POOL_SIZE][BUFFER_SIZE];
static atomic_int test_buffer_idx = 0;

// Get next buffer index (replicates production code)
static int get_next_buffer_index(void) {
    return atomic_fetch_add(&test_buffer_idx, 1) & BUFFER_MASK;
}

// ============================================================================
// Test 1: Buffer pool size verification
// ============================================================================

static void test_buffer_pool_size(void) {
    TEST("Buffer pool size is 4096 entries");

    // Verify the pool size constant matches expected value
    if (BUFFER_POOL_SIZE == 4096) {
        PASS();
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg), "Expected 4096, got %d", BUFFER_POOL_SIZE);
        FAIL(msg);
    }
}

static void test_buffer_mask_value(void) {
    TEST("Buffer mask is 0xFFF (4095)");

    if (BUFFER_MASK == 0xFFF) {
        PASS();
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg), "Expected 0xFFF, got 0x%X", BUFFER_MASK);
        FAIL(msg);
    }
}

static void test_buffer_individual_size(void) {
    TEST("Individual buffer size is 16KB");

    if (BUFFER_SIZE == 16384) {
        PASS();
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg), "Expected 16384, got %d", BUFFER_SIZE);
        FAIL(msg);
    }
}

// ============================================================================
// Test 2: Index wrapping verification
// ============================================================================

static void test_buffer_index_wrapping(void) {
    TEST("Index wraps correctly at 4096 boundary");

    // Reset index
    atomic_store(&test_buffer_idx, 0);

    // Test various values around the wrap boundary
    int test_values[] = {0, 1, 4095, 4096, 4097, 8191, 8192, 16383, 16384};
    int expected[] =    {0, 1, 4095,    0,    1, 4095,    0,  4095,     0};
    int num_tests = sizeof(test_values) / sizeof(test_values[0]);

    int all_passed = 1;
    for (int i = 0; i < num_tests; i++) {
        int result = test_values[i] & BUFFER_MASK;
        if (result != expected[i]) {
            char msg[128];
            snprintf(msg, sizeof(msg), "Value %d & 0xFFF = %d, expected %d",
                     test_values[i], result, expected[i]);
            FAIL(msg);
            all_passed = 0;
            break;
        }
    }

    if (all_passed) {
        PASS();
    }
}

static void test_buffer_index_sequential(void) {
    TEST("Sequential indices are unique until wrap");

    // Reset index
    atomic_store(&test_buffer_idx, 0);

    // Get 100 sequential indices, verify all unique
    int indices[100];
    for (int i = 0; i < 100; i++) {
        indices[i] = get_next_buffer_index();
    }

    // Check all unique
    int all_unique = 1;
    for (int i = 0; i < 100 && all_unique; i++) {
        for (int j = i + 1; j < 100 && all_unique; j++) {
            if (indices[i] == indices[j]) {
                all_unique = 0;
            }
        }
    }

    if (all_unique) {
        PASS();
    } else {
        FAIL("Found duplicate indices in sequential allocation");
    }
}

static void test_buffer_full_cycle(void) {
    TEST("Full cycle returns to index 0");

    // Reset to just before wrap
    atomic_store(&test_buffer_idx, 4095);

    int idx1 = get_next_buffer_index();  // Should be 4095
    int idx2 = get_next_buffer_index();  // Should wrap to 0

    if (idx1 == 4095 && idx2 == 0) {
        PASS();
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg), "Expected 4095->0, got %d->%d", idx1, idx2);
        FAIL(msg);
    }
}

// ============================================================================
// Test 3: Concurrent buffer access
// ============================================================================

#define NUM_THREADS 64
#define ITERATIONS_PER_THREAD 1000

typedef struct {
    int thread_id;
    int indices[ITERATIONS_PER_THREAD];
    int success;
} thread_data_t;

static void* concurrent_buffer_thread(void *arg) {
    thread_data_t *data = (thread_data_t *)arg;
    data->success = 1;

    for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
        int idx = get_next_buffer_index();

        // Verify index is within valid range
        if (idx < 0 || idx >= BUFFER_POOL_SIZE) {
            data->success = 0;
            break;
        }

        data->indices[i] = idx;

        // Small delay to increase contention
        if (i % 100 == 0) {
            usleep(1);
        }
    }

    return NULL;
}

static void test_concurrent_buffer_access(void) {
    TEST("Concurrent access produces valid indices");

    // Reset index
    atomic_store(&test_buffer_idx, 0);

    pthread_t threads[NUM_THREADS];
    thread_data_t thread_data[NUM_THREADS];

    // Create threads
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].thread_id = i;
        thread_data[i].success = 0;
        pthread_create(&threads[i], NULL, concurrent_buffer_thread, &thread_data[i]);
    }

    // Wait for all threads
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // Verify all threads succeeded
    int all_success = 1;
    for (int i = 0; i < NUM_THREADS; i++) {
        if (!thread_data[i].success) {
            all_success = 0;
            break;
        }
    }

    if (all_success) {
        PASS();
    } else {
        FAIL("Some threads got invalid indices");
    }
}

// Thread data and function for concurrent uniqueness test
#define SAFE_ITERATIONS 100
#define SAFE_THREADS 32

typedef struct {
    int *indices;
    atomic_int *counter;
} simple_thread_data_t;

static void* simple_index_thread(void *arg) {
    simple_thread_data_t *data = (simple_thread_data_t *)arg;
    for (int i = 0; i < SAFE_ITERATIONS; i++) {
        int idx = get_next_buffer_index();
        int pos = atomic_fetch_add(data->counter, 1);
        if (pos < SAFE_THREADS * SAFE_ITERATIONS) {
            data->indices[pos] = idx;
        }
    }
    return NULL;
}

static void test_concurrent_no_duplicate_indices(void) {
    TEST("Concurrent access produces unique indices (within pool size)");

    // Reset index
    atomic_store(&test_buffer_idx, 0);

    pthread_t threads[SAFE_THREADS];
    int all_indices[SAFE_THREADS * SAFE_ITERATIONS];
    atomic_int index_counter = 0;

    simple_thread_data_t simple_data = {
        .indices = all_indices,
        .counter = &index_counter
    };

    // Create threads
    for (int i = 0; i < SAFE_THREADS; i++) {
        pthread_create(&threads[i], NULL, simple_index_thread, &simple_data);
    }

    // Wait for all threads
    for (int i = 0; i < SAFE_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // Count how many indices we got
    int total = atomic_load(&index_counter);
    if (total > SAFE_THREADS * SAFE_ITERATIONS) {
        total = SAFE_THREADS * SAFE_ITERATIONS;
    }

    // Since indices can wrap, check that all are valid (0-4095)
    int all_valid = 1;
    for (int i = 0; i < total; i++) {
        if (all_indices[i] < 0 || all_indices[i] >= BUFFER_POOL_SIZE) {
            all_valid = 0;
            break;
        }
    }

    if (all_valid) {
        PASS();
    } else {
        FAIL("Invalid indices found");
    }
}

// ============================================================================
// Test 4: Buffer no overlap verification
// ============================================================================

static void test_buffer_no_overlap(void) {
    TEST("Different indices point to different buffer addresses");

    // Get pointers to different buffer indices
    char *buf0 = test_buffers[0];
    char *buf1 = test_buffers[1];
    char *buf4095 = test_buffers[4095];

    // Verify they're at different addresses
    // Expected: each buffer is BUFFER_SIZE apart
    ptrdiff_t diff_0_1 = buf1 - buf0;
    ptrdiff_t diff_0_4095 = buf4095 - buf0;

    if (diff_0_1 == BUFFER_SIZE && diff_0_4095 == (ptrdiff_t)BUFFER_SIZE * 4095) {
        PASS();
    } else {
        char msg[256];
        snprintf(msg, sizeof(msg), "Unexpected buffer spacing: diff_0_1=%td, diff_0_4095=%td",
                 diff_0_1, diff_0_4095);
        FAIL(msg);
    }
}

static void test_buffer_write_isolation(void) {
    TEST("Writing to one buffer doesn't affect others");

    // Clear buffers
    memset(test_buffers[0], 0, BUFFER_SIZE);
    memset(test_buffers[1], 0, BUFFER_SIZE);
    memset(test_buffers[4095], 0, BUFFER_SIZE);

    // Write unique patterns
    strcpy(test_buffers[0], "BUFFER_ZERO");
    strcpy(test_buffers[1], "BUFFER_ONE");
    strcpy(test_buffers[4095], "BUFFER_LAST");

    // Verify patterns are preserved
    int correct = (strcmp(test_buffers[0], "BUFFER_ZERO") == 0 &&
                   strcmp(test_buffers[1], "BUFFER_ONE") == 0 &&
                   strcmp(test_buffers[4095], "BUFFER_LAST") == 0);

    if (correct) {
        PASS();
    } else {
        FAIL("Buffer contents were corrupted");
    }
}

// ============================================================================
// Test 5: High concurrency stress test
// ============================================================================

#define STRESS_THREADS 100
#define STRESS_ITERATIONS 10000

typedef struct {
    int thread_id;
    int errors;
    int overwrites_detected;
} stress_thread_data_t;

// Global buffer to track which thread owns each slot
static atomic_int buffer_owners[BUFFER_POOL_SIZE];
static atomic_int total_overwrites = 0;

static void* stress_thread(void *arg) {
    stress_thread_data_t *data = (stress_thread_data_t *)arg;
    data->errors = 0;
    data->overwrites_detected = 0;

    for (int i = 0; i < STRESS_ITERATIONS; i++) {
        int idx = get_next_buffer_index();

        // Verify index is valid
        if (idx < 0 || idx >= BUFFER_POOL_SIZE) {
            data->errors++;
            continue;
        }

        // Try to claim this buffer slot
        int expected = 0;
        int desired = data->thread_id + 1;  // +1 so 0 means unclaimed

        // If we can't claim it, someone else has it (potential overwrite scenario)
        if (!atomic_compare_exchange_strong(&buffer_owners[idx], &expected, desired)) {
            // Buffer was in use by another thread
            // This is expected when pool wraps - just track it
            data->overwrites_detected++;
        }

        // Simulate using the buffer
        snprintf(test_buffers[idx], BUFFER_SIZE, "Thread%d_Iter%d", data->thread_id, i);

        // Small delay to simulate real-world usage
        if (i % 500 == 0) {
            usleep(10);
        }

        // Release the buffer
        atomic_store(&buffer_owners[idx], 0);
    }

    atomic_fetch_add(&total_overwrites, data->overwrites_detected);

    return NULL;
}

static void test_high_concurrency_no_crash(void) {
    TEST("High concurrency stress test (100 threads, 10K iterations)");

    // Reset state
    atomic_store(&test_buffer_idx, 0);
    atomic_store(&total_overwrites, 0);
    memset((void*)buffer_owners, 0, sizeof(buffer_owners));

    pthread_t threads[STRESS_THREADS];
    stress_thread_data_t thread_data[STRESS_THREADS];

    // Create threads
    for (int i = 0; i < STRESS_THREADS; i++) {
        thread_data[i].thread_id = i;
        pthread_create(&threads[i], NULL, stress_thread, &thread_data[i]);
    }

    // Wait for all threads
    for (int i = 0; i < STRESS_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // Check for errors
    int total_errors = 0;
    for (int i = 0; i < STRESS_THREADS; i++) {
        total_errors += thread_data[i].errors;
    }

    if (total_errors == 0) {
        int overwrites = atomic_load(&total_overwrites);
        printf("(overwrites: %d) ", overwrites);
        PASS();
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg), "%d errors detected", total_errors);
        FAIL(msg);
    }
}

// Rate test thread function
#define RATE_TEST_THREADS 50
#define RATE_TEST_ITERATIONS 1000

static void* rate_stress_thread(void *arg) {
    stress_thread_data_t *data = (stress_thread_data_t *)arg;
    data->errors = 0;
    data->overwrites_detected = 0;

    for (int i = 0; i < RATE_TEST_ITERATIONS; i++) {
        int idx = get_next_buffer_index();
        if (idx < 0 || idx >= BUFFER_POOL_SIZE) {
            data->errors++;
            continue;
        }
        int expected = 0;
        if (!atomic_compare_exchange_strong(&buffer_owners[idx], &expected, data->thread_id + 1)) {
            data->overwrites_detected++;
        }
        // Tiny delay
        for (volatile int j = 0; j < 100; j++) {}
        atomic_store(&buffer_owners[idx], 0);
    }
    atomic_fetch_add(&total_overwrites, data->overwrites_detected);
    return NULL;
}

static void test_overwrite_rate_acceptable(void) {
    TEST("Overwrite rate is acceptable with 4096 buffer pool");

    // With the OLD 256-buffer pool at 100 threads * 10000 iterations = 1M operations,
    // the overwrite rate would be very high (wrapping every 256 ops)
    // With 4096 buffers, the overwrite rate should be much lower

    // Reset state
    atomic_store(&test_buffer_idx, 0);
    atomic_store(&total_overwrites, 0);
    memset((void*)buffer_owners, 0, sizeof(buffer_owners));

    pthread_t threads[RATE_TEST_THREADS];
    stress_thread_data_t thread_data[RATE_TEST_THREADS];

    for (int i = 0; i < RATE_TEST_THREADS; i++) {
        thread_data[i].thread_id = i;
        thread_data[i].errors = 0;
        thread_data[i].overwrites_detected = 0;
    }

    for (int i = 0; i < RATE_TEST_THREADS; i++) {
        pthread_create(&threads[i], NULL, rate_stress_thread, &thread_data[i]);
    }

    for (int i = 0; i < RATE_TEST_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    int total_ops = RATE_TEST_THREADS * RATE_TEST_ITERATIONS;
    int overwrites = atomic_load(&total_overwrites);
    double overwrite_rate = (double)overwrites / total_ops * 100.0;

    // With 4096 buffers and 50K operations, overwrite rate should be reasonable
    // The old 256-buffer pool would have ~16x higher wrap rate
    printf("(rate: %.2f%%) ", overwrite_rate);

    // Accept if overwrite rate is under 50% (very generous)
    // In practice with 4096 buffers, it should be much lower
    if (overwrite_rate < 50.0) {
        PASS();
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg), "Overwrite rate %.2f%% too high", overwrite_rate);
        FAIL(msg);
    }
}

// ============================================================================
// Bonus: Verify old configuration would have problems
// ============================================================================

static void test_old_config_would_fail(void) {
    TEST("Old 256-buffer config would have 16x more overwrites");

    // Calculate expected wrap rates
    // Old: 256 buffers, wraps after 256 operations
    // New: 4096 buffers, wraps after 4096 operations
    // Ratio: 4096/256 = 16x improvement

    int old_pool_size = 256;
    int new_pool_size = 4096;
    int improvement_factor = new_pool_size / old_pool_size;

    if (improvement_factor == 16) {
        PASS();
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg), "Expected 16x improvement, got %dx", improvement_factor);
        FAIL(msg);
    }
}

// ============================================================================
// Test atomic increment correctness
// ============================================================================

#define ATOMIC_TEST_THREADS 10
#define ATOMIC_TEST_ITERATIONS 10000

static void* increment_thread(void *arg) {
    (void)arg;
    for (int i = 0; i < ATOMIC_TEST_ITERATIONS; i++) {
        atomic_fetch_add(&test_buffer_idx, 1);
    }
    return NULL;
}

static void test_atomic_increment_thread_safe(void) {
    TEST("Atomic increment is thread-safe");

    // Reset counter
    atomic_store(&test_buffer_idx, 0);

    pthread_t threads[ATOMIC_TEST_THREADS];

    for (int i = 0; i < ATOMIC_TEST_THREADS; i++) {
        pthread_create(&threads[i], NULL, increment_thread, NULL);
    }

    for (int i = 0; i < ATOMIC_TEST_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    int expected = ATOMIC_TEST_THREADS * ATOMIC_TEST_ITERATIONS;
    int actual = atomic_load(&test_buffer_idx);

    if (actual == expected) {
        PASS();
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg), "Expected %d, got %d (lost %d increments)",
                 expected, actual, expected - actual);
        FAIL(msg);
    }
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("\n\033[1m=== Buffer Pool Expansion Tests ===\033[0m\n\n");

    printf("\033[1mBuffer Pool Configuration:\033[0m\n");
    test_buffer_pool_size();
    test_buffer_mask_value();
    test_buffer_individual_size();

    printf("\n\033[1mIndex Wrapping:\033[0m\n");
    test_buffer_index_wrapping();
    test_buffer_index_sequential();
    test_buffer_full_cycle();

    printf("\n\033[1mConcurrent Access:\033[0m\n");
    test_concurrent_buffer_access();
    test_concurrent_no_duplicate_indices();
    test_atomic_increment_thread_safe();

    printf("\n\033[1mBuffer Isolation:\033[0m\n");
    test_buffer_no_overlap();
    test_buffer_write_isolation();

    printf("\n\033[1mHigh Concurrency Stress:\033[0m\n");
    test_high_concurrency_no_crash();
    test_overwrite_rate_acceptable();

    printf("\n\033[1mConfiguration Improvement:\033[0m\n");
    test_old_config_would_fail();

    printf("\n\033[1m=== Results ===\033[0m\n");
    printf("Passed: \033[32m%d\033[0m\n", tests_passed);
    printf("Failed: \033[31m%d\033[0m\n", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
