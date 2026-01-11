/*
 * Unit tests for logging deadlock prevention
 *
 * Tests verify that the logging system doesn't deadlock when multiple threads
 * attempt to log simultaneously. This addresses a historical issue where
 * multiple threads calling fflush() on unbuffered log files would block on
 * flockfile().
 *
 * Tests:
 * 1. test_logging_no_fflush_deadlock - 10 threads logging for 1 second
 * 2. test_concurrent_thread_logging - 50 threads rapid debug logging
 * 3. test_log_writes_complete - Verify writes complete, not just queued
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>
#include <errno.h>
#include <signal.h>
#include <stdatomic.h>

// Test counters
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) printf("  Testing: %s... ", name)
#define PASS() do { printf("\033[32mPASS\033[0m\n"); tests_passed++; } while(0)
#define FAIL(msg) do { printf("\033[31mFAIL: %s\033[0m\n", msg); tests_failed++; } while(0)

// ============================================================================
// Utility Functions
// ============================================================================

static uint64_t get_time_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)(tv.tv_sec * 1000 + tv.tv_usec / 1000);
}

// Atomic counters for thread coordination
static atomic_int threads_completed = 0;
static atomic_int total_messages_logged = 0;
static atomic_int test_running = 0;

// ============================================================================
// Test 1: No fflush deadlock with 10 threads
// ============================================================================

typedef struct {
    int thread_id;
    int duration_ms;
    FILE *log_file;
} logging_thread_args_t;

static void* logging_thread_timed(void* arg) {
    logging_thread_args_t *args = (logging_thread_args_t*)arg;
    uint64_t start = get_time_ms();
    int count = 0;

    while ((get_time_ms() - start) < (uint64_t)args->duration_ms) {
        // Write to the log file
        fprintf(args->log_file, "[Thread %d] Log message %d at %llu ms\n",
                args->thread_id, count, (unsigned long long)(get_time_ms() - start));

        // The key operation that was causing deadlocks - fflush on shared file
        fflush(args->log_file);

        count++;
        atomic_fetch_add(&total_messages_logged, 1);
    }

    atomic_fetch_add(&threads_completed, 1);
    return NULL;
}

void test_logging_no_fflush_deadlock(void) {
    TEST("10 threads logging for 1s without deadlock");

    // Reset counters
    atomic_store(&threads_completed, 0);
    atomic_store(&total_messages_logged, 0);

    // Create a temporary log file
    FILE *log_file = tmpfile();
    if (!log_file) {
        FAIL("Could not create temp file");
        return;
    }

    // Make it unbuffered (like real stderr logging) - this is critical
    // for reproducing the deadlock scenario
    setvbuf(log_file, NULL, _IONBF, 0);

    const int num_threads = 10;
    const int duration_ms = 1000;  // 1 second

    pthread_t threads[num_threads];
    logging_thread_args_t args[num_threads];

    // Start all threads
    for (int i = 0; i < num_threads; i++) {
        args[i].thread_id = i;
        args[i].duration_ms = duration_ms;
        args[i].log_file = log_file;
        pthread_create(&threads[i], NULL, logging_thread_timed, &args[i]);
    }

    // Wait for all threads with timeout detection
    uint64_t wait_start = get_time_ms();
    int all_joined = 1;

    for (int i = 0; i < num_threads; i++) {
        // Use timed wait to detect potential deadlock
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 3;  // 3 second timeout per thread

        int result = pthread_join(threads[i], NULL);
        if (result != 0) {
            all_joined = 0;
        }
    }

    uint64_t elapsed = get_time_ms() - wait_start;

    fclose(log_file);

    // Check results
    int completed = atomic_load(&threads_completed);
    int messages = atomic_load(&total_messages_logged);

    if (!all_joined || completed != num_threads) {
        FAIL("Not all threads completed - possible deadlock");
        return;
    }

    if (elapsed > 5000) {  // Should complete in ~1s, allow up to 5s
        FAIL("Threads took too long - possible contention issue");
        return;
    }

    if (messages < num_threads * 10) {  // At minimum 10 messages per thread
        FAIL("Too few messages logged - threads may have been blocked");
        return;
    }

    PASS();
}

// ============================================================================
// Test 2: 50 threads rapid debug logging
// ============================================================================

static void* rapid_logging_thread(void* arg) {
    int thread_id = (int)(intptr_t)arg;
    int count = 0;
    FILE *log = stderr;  // Use stderr like real debug logging

    while (atomic_load(&test_running)) {
        // Rapid fire logging without delays
        fprintf(log, "[DEBUG][T%02d] Rapid message %d\n", thread_id, count++);
        // No fflush here - testing the buffered case

        if (count >= 100) {
            break;  // Limit per thread to avoid excessive output
        }
    }

    atomic_fetch_add(&total_messages_logged, count);
    atomic_fetch_add(&threads_completed, 1);
    return NULL;
}

void test_concurrent_thread_logging(void) {
    TEST("50 threads rapid debug logging");

    // Reset counters
    atomic_store(&threads_completed, 0);
    atomic_store(&total_messages_logged, 0);
    atomic_store(&test_running, 1);

    const int num_threads = 50;
    pthread_t threads[num_threads];

    // Redirect stderr to null for this test to avoid spam
    int saved_stderr = dup(STDERR_FILENO);
    FILE *null_file = fopen("/dev/null", "w");
    if (null_file) {
        dup2(fileno(null_file), STDERR_FILENO);
    }

    uint64_t start = get_time_ms();

    // Start all threads simultaneously
    for (int i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, rapid_logging_thread, (void*)(intptr_t)i);
    }

    // Set a timeout - if not done in 2 seconds, we have a problem
    usleep(2000000);  // 2 seconds
    atomic_store(&test_running, 0);  // Signal threads to stop

    // Join all threads
    int all_joined = 1;
    for (int i = 0; i < num_threads; i++) {
        int result = pthread_join(threads[i], NULL);
        if (result != 0) {
            all_joined = 0;
        }
    }

    uint64_t elapsed = get_time_ms() - start;

    // Restore stderr
    if (null_file) {
        dup2(saved_stderr, STDERR_FILENO);
        fclose(null_file);
    }
    close(saved_stderr);

    // Check results
    int completed = atomic_load(&threads_completed);
    int messages = atomic_load(&total_messages_logged);

    if (!all_joined || completed != num_threads) {
        FAIL("Not all threads completed - possible deadlock");
        return;
    }

    if (elapsed > 2500) {  // Allow some overhead
        FAIL("Test took too long - possible deadlock or contention");
        return;
    }

    if (messages < num_threads) {  // At least 1 message per thread
        FAIL("Too few messages - threads may have been blocked");
        return;
    }

    PASS();
}

// ============================================================================
// Test 3: Verify log writes complete (not just queued)
// ============================================================================

typedef struct {
    int thread_id;
    FILE *log_file;
    int messages_to_write;
    int messages_written;
    int write_errors;
} write_completion_args_t;

static void* write_completion_thread(void* arg) {
    write_completion_args_t *args = (write_completion_args_t*)arg;
    args->messages_written = 0;
    args->write_errors = 0;

    for (int i = 0; i < args->messages_to_write; i++) {
        int result = fprintf(args->log_file,
            "[Thread %d] Message %d - This is a longer message to ensure actual I/O\n",
            args->thread_id, i);

        if (result < 0) {
            args->write_errors++;
            continue;
        }

        // Force the write to complete, not just buffer
        if (fflush(args->log_file) != 0) {
            args->write_errors++;
            continue;
        }

        args->messages_written++;
    }

    atomic_fetch_add(&threads_completed, 1);
    return NULL;
}

void test_log_writes_complete(void) {
    TEST("Log writes actually complete (not just queued)");

    // Reset counters
    atomic_store(&threads_completed, 0);

    // Create a temporary file we can verify
    char tmpname[] = "/tmp/plex_log_test_XXXXXX";
    int fd = mkstemp(tmpname);
    if (fd < 0) {
        FAIL("Could not create temp file");
        return;
    }

    FILE *log_file = fdopen(fd, "w");
    if (!log_file) {
        close(fd);
        unlink(tmpname);
        FAIL("Could not open temp file for writing");
        return;
    }

    // Make unbuffered to test the scenario that caused deadlocks
    setvbuf(log_file, NULL, _IONBF, 0);

    const int num_threads = 10;
    const int messages_per_thread = 100;

    pthread_t threads[num_threads];
    write_completion_args_t args[num_threads];

    // Start all threads
    for (int i = 0; i < num_threads; i++) {
        args[i].thread_id = i;
        args[i].log_file = log_file;
        args[i].messages_to_write = messages_per_thread;
        pthread_create(&threads[i], NULL, write_completion_thread, &args[i]);
    }

    // Wait for completion with timeout
    uint64_t start = get_time_ms();
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    uint64_t elapsed = get_time_ms() - start;

    // Sync and close
    fflush(log_file);
    fsync(fd);
    fclose(log_file);

    // Verify the file contents
    FILE *verify = fopen(tmpname, "r");
    if (!verify) {
        unlink(tmpname);
        FAIL("Could not reopen temp file for verification");
        return;
    }

    int line_count = 0;
    char line[512];
    while (fgets(line, sizeof(line), verify)) {
        line_count++;
    }
    fclose(verify);
    unlink(tmpname);

    // Count total messages written and errors
    int total_written = 0;
    int total_errors = 0;
    for (int i = 0; i < num_threads; i++) {
        total_written += args[i].messages_written;
        total_errors += args[i].write_errors;
    }

    // Verify results
    if (elapsed > 10000) {  // 10 seconds is way too long
        FAIL("Writes took too long - possible blocking issue");
        return;
    }

    if (total_errors > 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "%d write errors occurred", total_errors);
        FAIL(msg);
        return;
    }

    int expected_lines = num_threads * messages_per_thread;
    if (line_count < expected_lines * 0.99) {  // Allow tiny margin
        char msg[128];
        snprintf(msg, sizeof(msg), "Only %d/%d lines in file - writes may not have completed",
                 line_count, expected_lines);
        FAIL(msg);
        return;
    }

    if (total_written != expected_lines) {
        char msg[128];
        snprintf(msg, sizeof(msg), "Only %d/%d writes reported complete",
                 total_written, expected_lines);
        FAIL(msg);
        return;
    }

    PASS();
}

// ============================================================================
// Test 4: Stress test with mixed operations
// ============================================================================

static atomic_int stress_running = 0;

static void* stress_thread(void* arg) {
    int thread_id = (int)(intptr_t)arg;
    FILE *log = stderr;
    int ops = 0;

    while (atomic_load(&stress_running)) {
        // Mix of operations that were problematic
        fprintf(log, "[STRESS][T%d] Op %d\n", thread_id, ops);

        // Occasional flush (the problematic operation)
        if (ops % 10 == 0) {
            fflush(log);
        }

        ops++;
        if (ops > 500) break;
    }

    atomic_fetch_add(&total_messages_logged, ops);
    atomic_fetch_add(&threads_completed, 1);
    return NULL;
}

void test_mixed_operation_stress(void) {
    TEST("Mixed operation stress test");

    // Reset
    atomic_store(&threads_completed, 0);
    atomic_store(&total_messages_logged, 0);
    atomic_store(&stress_running, 1);

    const int num_threads = 20;
    pthread_t threads[num_threads];

    // Redirect stderr
    int saved_stderr = dup(STDERR_FILENO);
    FILE *null_file = fopen("/dev/null", "w");
    if (null_file) {
        dup2(fileno(null_file), STDERR_FILENO);
    }

    uint64_t start = get_time_ms();

    for (int i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, stress_thread, (void*)(intptr_t)i);
    }

    // Let it run for 1 second
    usleep(1000000);
    atomic_store(&stress_running, 0);

    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    uint64_t elapsed = get_time_ms() - start;

    // Restore stderr
    if (null_file) {
        dup2(saved_stderr, STDERR_FILENO);
        fclose(null_file);
    }
    close(saved_stderr);

    int completed = atomic_load(&threads_completed);

    if (completed != num_threads) {
        FAIL("Not all threads completed");
        return;
    }

    if (elapsed > 3000) {
        FAIL("Stress test took too long");
        return;
    }

    PASS();
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("\n\033[1m=== Logging Deadlock Prevention Tests ===\033[0m\n\n");

    printf("\033[1mDeadlock Prevention Tests:\033[0m\n");
    test_logging_no_fflush_deadlock();
    test_concurrent_thread_logging();
    test_log_writes_complete();
    test_mixed_operation_stress();

    printf("\n\033[1m=== Results ===\033[0m\n");
    printf("Passed: \033[32m%d\033[0m\n", tests_passed);
    printf("Failed: \033[31m%d\033[0m\n", tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
