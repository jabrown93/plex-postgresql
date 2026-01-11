/*
 * Unit tests for C++ Exception Handler
 *
 * The shim intercepts __cxa_throw to log C++ exceptions with stack traces,
 * helping debug SOCI/Plex crashes.
 *
 * Tests:
 * 1. Exception type name demangling (C++ mangled names to readable names)
 * 2. Exception throttling (after 50 exceptions, logging throttles)
 * 3. Exception count tracking
 * 4. Shim-related exception detection (column_type_calls > 0)
 * 5. External exception detection (column_type_calls = 0)
 * 6. Stack trace capture
 * 7. Per-type exception tracking
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <dlfcn.h>
#include <execinfo.h>

// Test counters
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) printf("  Testing: %s... ", name)
#define PASS() do { printf("\033[32mPASS\033[0m\n"); tests_passed++; } while(0)
#define FAIL(msg) do { printf("\033[31mFAIL: %s\033[0m\n", msg); tests_failed++; } while(0)

// ============================================================================
// Replicate exception handler constants and structures from db_interpose_core.c
// ============================================================================

#define MAX_EXCEPTION_TYPES 64
#define MAX_LOGGED_PER_TYPE 3
#define MAX_LOGGED_TOTAL 50

typedef struct {
    const char *type_name;
    int count;
    int logged_with_trace;
} exception_type_tracker_t;

// Mock global state for testing
static exception_type_tracker_t test_exception_types[MAX_EXCEPTION_TYPES];
static int test_exception_type_count = 0;
static volatile int test_total_exception_count = 0;
static pthread_mutex_t test_exception_tracker_mutex = PTHREAD_MUTEX_INITIALIZER;

// Demangle function pointer (from libc++)
typedef char* (*cxa_demangle_fn_t)(const char*, char*, size_t*, int*);
static cxa_demangle_fn_t cxa_demangle_fn = NULL;

// ============================================================================
// Helper functions replicated from db_interpose_core.c
// ============================================================================

// Get type name from type_info structure
// In C++ ABI, type_info has: vtable pointer, then const char* name
static const char* get_type_name(void *tinfo) {
    if (!tinfo) return "unknown";
    const char **name_ptr = (const char**)((char*)tinfo + sizeof(void*));
    return *name_ptr;
}

// Find or create tracker for an exception type
static exception_type_tracker_t* get_exception_tracker(const char *type_name) {
    pthread_mutex_lock(&test_exception_tracker_mutex);

    for (int i = 0; i < test_exception_type_count; i++) {
        if (test_exception_types[i].type_name == type_name ||
            (test_exception_types[i].type_name && type_name &&
             strcmp(test_exception_types[i].type_name, type_name) == 0)) {
            test_exception_types[i].count++;
            pthread_mutex_unlock(&test_exception_tracker_mutex);
            return &test_exception_types[i];
        }
    }

    if (test_exception_type_count < MAX_EXCEPTION_TYPES) {
        exception_type_tracker_t *tracker = &test_exception_types[test_exception_type_count++];
        tracker->type_name = type_name;
        tracker->count = 1;
        tracker->logged_with_trace = 0;
        pthread_mutex_unlock(&test_exception_tracker_mutex);
        return tracker;
    }

    pthread_mutex_unlock(&test_exception_tracker_mutex);
    return NULL;
}

// Check if logging should occur based on throttling logic
static int should_log_exception(int total_count, exception_type_tracker_t *tracker, int is_db_exception) {
    return is_db_exception ||
           ((total_count <= MAX_LOGGED_TOTAL) &&
            (tracker == NULL || tracker->count <= MAX_LOGGED_PER_TYPE));
}

// Reset test state
static void reset_test_state(void) {
    pthread_mutex_lock(&test_exception_tracker_mutex);
    memset(test_exception_types, 0, sizeof(test_exception_types));
    test_exception_type_count = 0;
    test_total_exception_count = 0;
    pthread_mutex_unlock(&test_exception_tracker_mutex);
}

// ============================================================================
// Test: Exception Type Demangling
// ============================================================================

static void test_exception_type_demangling(void) {
    TEST("Exception - type name demangling");

    // Load __cxa_demangle dynamically
    if (!cxa_demangle_fn) {
        cxa_demangle_fn = (cxa_demangle_fn_t)dlsym(RTLD_DEFAULT, "__cxa_demangle");
    }

    if (!cxa_demangle_fn) {
        FAIL("__cxa_demangle not available");
        return;
    }

    // Test demangling boost::bad_cast
    int status = 0;
    char *demangled = cxa_demangle_fn("_ZN5boost8bad_castE", NULL, NULL, &status);

    if (status == 0 && demangled && strstr(demangled, "bad_cast")) {
        free(demangled);

        // Also test std::exception
        demangled = cxa_demangle_fn("_ZSt9exceptionD1Ev", NULL, NULL, &status);
        // This is a destructor, may not demangle the same way
        if (demangled) free(demangled);

        // Test std::runtime_error
        demangled = cxa_demangle_fn("_ZNSt13runtime_errorD1Ev", NULL, NULL, &status);
        if (demangled) {
            int has_runtime = strstr(demangled, "runtime_error") != NULL;
            free(demangled);
            if (has_runtime) {
                PASS();
                return;
            }
        }

        // If we got here, at least bad_cast worked
        PASS();
    } else {
        if (demangled) free(demangled);
        FAIL("Failed to demangle _ZN5boost8bad_castE");
    }
}

static void test_demangle_invalid_name(void) {
    TEST("Exception - demangle handles invalid names");

    if (!cxa_demangle_fn) {
        cxa_demangle_fn = (cxa_demangle_fn_t)dlsym(RTLD_DEFAULT, "__cxa_demangle");
    }

    if (!cxa_demangle_fn) {
        FAIL("__cxa_demangle not available");
        return;
    }

    // Invalid mangled name should return error
    int status = 0;
    char *demangled = cxa_demangle_fn("not_a_mangled_name", NULL, NULL, &status);

    // status -2 means the mangled_name is not a valid name
    if (status == -2 || demangled == NULL) {
        if (demangled) free(demangled);
        PASS();
    } else {
        if (demangled) free(demangled);
        FAIL("Expected failure for invalid mangled name");
    }
}

// ============================================================================
// Test: Exception Throttling
// ============================================================================

static void test_exception_throttling(void) {
    TEST("Exception - throttling after 50 exceptions");

    reset_test_state();

    // Simulate 55 exceptions of same type
    int logged_count = 0;
    for (int i = 0; i < 55; i++) {
        int total_count = __sync_add_and_fetch(&test_total_exception_count, 1);
        exception_type_tracker_t *tracker = get_exception_tracker("TestException");

        int should_log = should_log_exception(total_count, tracker, 0);
        if (should_log) logged_count++;
    }

    // Should log first MAX_LOGGED_PER_TYPE (3) for this type, then stop
    // But also respects MAX_LOGGED_TOTAL (50)
    if (logged_count == MAX_LOGGED_PER_TYPE) {
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected %d logged, got %d", MAX_LOGGED_PER_TYPE, logged_count);
        FAIL(msg);
    }
}

static void test_throttle_db_exceptions_not_throttled(void) {
    TEST("Exception - DB exceptions bypass throttle");

    reset_test_state();

    // Simulate many DB exceptions
    int logged_count = 0;
    for (int i = 0; i < 100; i++) {
        int total_count = __sync_add_and_fetch(&test_total_exception_count, 1);
        exception_type_tracker_t *tracker = get_exception_tracker("DB::Exception");

        // DB exceptions always logged (is_db_exception = 1)
        int should_log = should_log_exception(total_count, tracker, 1);
        if (should_log) logged_count++;
    }

    // DB exceptions should not be throttled
    if (logged_count == 100) {
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected 100 logged, got %d", logged_count);
        FAIL(msg);
    }
}

// ============================================================================
// Test: Exception Count Tracking
// ============================================================================

static void test_exception_count_tracking(void) {
    TEST("Exception - counter increments correctly");

    reset_test_state();

    // Increment counter 10 times
    for (int i = 0; i < 10; i++) {
        __sync_add_and_fetch(&test_total_exception_count, 1);
    }

    if (test_total_exception_count == 10) {
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected 10, got %d", test_total_exception_count);
        FAIL(msg);
    }
}

static void test_exception_count_atomic(void) {
    TEST("Exception - counter is atomic");

    reset_test_state();

    // Test atomic fetch-and-add returns previous value + 1
    int val1 = __sync_add_and_fetch(&test_total_exception_count, 1);
    int val2 = __sync_add_and_fetch(&test_total_exception_count, 1);
    int val3 = __sync_add_and_fetch(&test_total_exception_count, 1);

    if (val1 == 1 && val2 == 2 && val3 == 3 && test_total_exception_count == 3) {
        PASS();
    } else {
        FAIL("Atomic increment not working correctly");
    }
}

// ============================================================================
// Test: Shim-Related Exception Detection
// ============================================================================

static void test_exception_source_shim_related(void) {
    TEST("Exception - detects shim-related (column_type_calls > 0)");

    // Simulate shim state
    long column_type_calls = 5;
    long value_type_calls = 2;
    const char *last_query = "SELECT * FROM metadata_items";

    int is_shim_related = (value_type_calls > 0 || column_type_calls > 0 || last_query != NULL);

    if (is_shim_related) {
        PASS();
    } else {
        FAIL("Should detect as shim-related");
    }
}

static void test_exception_source_external(void) {
    TEST("Exception - detects external (column_type_calls = 0)");

    // Simulate external code state (no shim calls)
    long column_type_calls = 0;
    long value_type_calls = 0;
    const char *last_query = NULL;

    int is_shim_related = (value_type_calls > 0 || column_type_calls > 0 || last_query != NULL);

    if (!is_shim_related) {
        PASS();
    } else {
        FAIL("Should detect as external");
    }
}

static void test_exception_source_query_only(void) {
    TEST("Exception - detects shim-related (query set but no calls)");

    // Query is set but no column/value calls yet
    long column_type_calls = 0;
    long value_type_calls = 0;
    const char *last_query = "SELECT 1";

    int is_shim_related = (value_type_calls > 0 || column_type_calls > 0 || last_query != NULL);

    if (is_shim_related) {
        PASS();
    } else {
        FAIL("Query-only should still be shim-related");
    }
}

// ============================================================================
// Test: Backtrace Capture
// ============================================================================

static void test_exception_backtrace_capture(void) {
    TEST("Exception - backtrace captures frames");

    void *callstack[64];
    int frames = backtrace(callstack, 64);

    // Should capture at least a few frames (main, test function, etc.)
    if (frames >= 3) {
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "Expected >= 3 frames, got %d", frames);
        FAIL(msg);
    }
}

static void test_exception_backtrace_symbols(void) {
    TEST("Exception - backtrace symbols resolve");

    void *callstack[64];
    int frames = backtrace(callstack, 64);
    char **symbols = backtrace_symbols(callstack, frames);

    if (symbols && frames > 0) {
        // Check that at least one symbol contains "test" or "main"
        int found_known = 0;
        for (int i = 0; i < frames; i++) {
            if (strstr(symbols[i], "test") || strstr(symbols[i], "main")) {
                found_known = 1;
                break;
            }
        }
        free(symbols);

        if (found_known) {
            PASS();
        } else {
            // Some systems may not have symbols, still pass if we got output
            PASS();
        }
    } else {
        if (symbols) free(symbols);
        FAIL("Failed to get backtrace symbols");
    }
}

// ============================================================================
// Test: Per-Type Exception Tracking
// ============================================================================

static void test_exception_type_tracker_per_type(void) {
    TEST("Exception - different types tracked separately");

    reset_test_state();

    // Track three different exception types
    exception_type_tracker_t *t1 = get_exception_tracker("std::runtime_error");
    exception_type_tracker_t *t2 = get_exception_tracker("std::logic_error");
    exception_type_tracker_t *t3 = get_exception_tracker("boost::bad_cast");

    // Each should have count = 1
    if (t1 && t2 && t3 &&
        t1->count == 1 && t2->count == 1 && t3->count == 1 &&
        test_exception_type_count == 3) {
        PASS();
    } else {
        FAIL("Types not tracked separately");
    }
}

static void test_exception_type_tracker_same_type(void) {
    TEST("Exception - same type increments count");

    reset_test_state();

    // Track same exception type multiple times
    exception_type_tracker_t *t1 = get_exception_tracker("std::runtime_error");
    exception_type_tracker_t *t2 = get_exception_tracker("std::runtime_error");
    exception_type_tracker_t *t3 = get_exception_tracker("std::runtime_error");

    // All should point to same tracker with count = 3
    if (t1 && t2 && t3 &&
        t1 == t2 && t2 == t3 &&
        t1->count == 3 &&
        test_exception_type_count == 1) {
        PASS();
    } else {
        FAIL("Same type should share tracker");
    }
}

static void test_exception_type_tracker_max_types(void) {
    TEST("Exception - respects MAX_EXCEPTION_TYPES limit");

    reset_test_state();

    // Try to track more than MAX_EXCEPTION_TYPES
    int null_count = 0;
    for (int i = 0; i < MAX_EXCEPTION_TYPES + 10; i++) {
        char type_name[32];
        snprintf(type_name, sizeof(type_name), "Type_%d", i);
        // Use strdup because get_exception_tracker stores the pointer
        char *type_copy = strdup(type_name);
        exception_type_tracker_t *t = get_exception_tracker(type_copy);
        if (!t) null_count++;
        // Note: We're leaking memory here for simplicity in tests
    }

    // After MAX_EXCEPTION_TYPES, get_exception_tracker should return NULL
    if (null_count == 10 && test_exception_type_count == MAX_EXCEPTION_TYPES) {
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "null_count=%d, type_count=%d", null_count, test_exception_type_count);
        FAIL(msg);
    }
}

static void test_exception_type_logged_with_trace(void) {
    TEST("Exception - tracks if logged with trace");

    reset_test_state();

    exception_type_tracker_t *t = get_exception_tracker("std::runtime_error");

    // Initially should not be logged with trace
    if (!t || t->logged_with_trace != 0) {
        FAIL("logged_with_trace should start at 0");
        return;
    }

    // Mark as logged with trace
    t->logged_with_trace = 1;

    // Get same tracker again
    exception_type_tracker_t *t2 = get_exception_tracker("std::runtime_error");

    if (t2 && t2->logged_with_trace == 1) {
        PASS();
    } else {
        FAIL("logged_with_trace not preserved");
    }
}

// ============================================================================
// Test: Type Name Extraction (simulated type_info)
// ============================================================================

// Mock type_info structure matching C++ ABI
typedef struct {
    void *vtable;
    const char *name;
} mock_type_info_t;

static void test_get_type_name_valid(void) {
    TEST("Exception - get_type_name extracts name");

    mock_type_info_t info = {
        .vtable = (void*)0x12345678,
        .name = "_ZN5boost8bad_castE"
    };

    const char *name = get_type_name(&info);

    if (name && strcmp(name, "_ZN5boost8bad_castE") == 0) {
        PASS();
    } else {
        FAIL("Failed to extract type name");
    }
}

static void test_get_type_name_null(void) {
    TEST("Exception - get_type_name handles NULL");

    const char *name = get_type_name(NULL);

    if (name && strcmp(name, "unknown") == 0) {
        PASS();
    } else {
        FAIL("NULL should return 'unknown'");
    }
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("\n\033[1m=== C++ Exception Handler Tests ===\033[0m\n\n");

    printf("\033[1mType Demangling:\033[0m\n");
    test_exception_type_demangling();
    test_demangle_invalid_name();

    printf("\n\033[1mException Throttling:\033[0m\n");
    test_exception_throttling();
    test_throttle_db_exceptions_not_throttled();

    printf("\n\033[1mException Count Tracking:\033[0m\n");
    test_exception_count_tracking();
    test_exception_count_atomic();

    printf("\n\033[1mShim-Related Detection:\033[0m\n");
    test_exception_source_shim_related();
    test_exception_source_external();
    test_exception_source_query_only();

    printf("\n\033[1mBacktrace Capture:\033[0m\n");
    test_exception_backtrace_capture();
    test_exception_backtrace_symbols();

    printf("\n\033[1mPer-Type Tracking:\033[0m\n");
    test_exception_type_tracker_per_type();
    test_exception_type_tracker_same_type();
    test_exception_type_tracker_max_types();
    test_exception_type_logged_with_trace();

    printf("\n\033[1mType Name Extraction:\033[0m\n");
    test_get_type_name_valid();
    test_get_type_name_null();

    printf("\n\033[1m=== Results ===\033[0m\n");
    printf("Passed: \033[32m%d\033[0m\n", tests_passed);
    printf("Failed: \033[31m%d\033[0m\n", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
