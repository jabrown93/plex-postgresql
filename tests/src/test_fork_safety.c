/*
 * Unit tests for Fork Safety with pthread_atfork handlers
 *
 * Tests:
 * 1. Child process has no inherited connections after fork
 * 2. Parent process retains its connections after fork
 * 3. Child can create fresh connections after fork
 * 4. Thread-local caches reset in child process
 * 5. pthread_atfork handlers are properly registered
 * 6. Double fork (grandchild) safety
 *
 * IMPORTANT: These tests verify fork safety logic WITHOUT loading the actual shim.
 * They test the state management and cleanup mechanisms in isolation.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <errno.h>

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
    SLOT_FREE = 0,
    SLOT_READY,
    SLOT_RESERVED,
    SLOT_RECONNECTING,
    SLOT_ERROR
} pool_slot_state_t;

typedef struct {
    void *conn;                       // Simulated PG connection pointer
    pthread_t owner_thread;           // Thread that owns this connection
    time_t last_used;
    _Atomic pool_slot_state_t state;
    _Atomic uint32_t generation;
} mock_pool_slot_t;

#define POOL_SIZE_MAX 8
static mock_pool_slot_t mock_pool[POOL_SIZE_MAX];
static pid_t init_pid = 0;

// Thread-local caches (simulating pg_client.c TLS variables)
static __thread void *tls_cached_db = NULL;
static __thread void *tls_cached_conn = NULL;
static __thread int tls_pool_slot = -1;
static __thread uint32_t tls_pool_generation = 0;

// Global state tracking
static int atfork_handlers_registered = 0;
static int atfork_child_called = 0;
static int atfork_parent_called = 0;
static int atfork_prepare_called = 0;

// ============================================================================
// Simulate pg_pool_cleanup_after_fork from pg_client.c
// ============================================================================

static void mock_pool_cleanup_after_fork(void) {
    // Clear all connection pool state WITHOUT closing sockets
    // (parent process still owns them - closing would kill parent's queries)
    for (int i = 0; i < POOL_SIZE_MAX; i++) {
        if (mock_pool[i].conn) {
            // Don't "close" - parent owns these sockets
            // Just clear our references
            mock_pool[i].conn = NULL;
            mock_pool[i].owner_thread = 0;
            mock_pool[i].last_used = 0;
            atomic_store(&mock_pool[i].state, SLOT_FREE);
            atomic_store(&mock_pool[i].generation, 0);
        }
    }

    // Clear thread-local caches (each thread has its own, but child starts fresh)
    tls_cached_db = NULL;
    tls_cached_conn = NULL;
    tls_pool_slot = -1;
    tls_pool_generation = 0;
}

// ============================================================================
// Simulate pthread_atfork handlers from db_interpose_core.c
// ============================================================================

static void atfork_prepare(void) {
    atfork_prepare_called = 1;
    // No action needed - parent continues with its connections
}

static void atfork_parent(void) {
    atfork_parent_called = 1;
    // No action needed - parent keeps its connections
}

static void atfork_child(void) {
    atfork_child_called = 1;
    // CRITICAL: Child process must NOT use parent's PostgreSQL connections
    mock_pool_cleanup_after_fork();
    // Update PID tracking
    init_pid = getpid();
}

static void register_atfork_handlers(void) {
    if (!atfork_handlers_registered) {
        int rc = pthread_atfork(atfork_prepare, atfork_parent, atfork_child);
        if (rc == 0) {
            atfork_handlers_registered = 1;
        }
    }
}

// ============================================================================
// Helper: Initialize mock pool with connections
// ============================================================================

static void setup_mock_pool_with_connections(int num_connections) {
    memset(mock_pool, 0, sizeof(mock_pool));
    init_pid = getpid();

    pthread_t current = pthread_self();

    for (int i = 0; i < num_connections && i < POOL_SIZE_MAX; i++) {
        mock_pool[i].conn = (void*)(uintptr_t)(0xDEAD0000 + i);  // Fake connection pointer
        mock_pool[i].owner_thread = current;
        mock_pool[i].last_used = time(NULL);
        atomic_store(&mock_pool[i].state, SLOT_READY);
        atomic_store(&mock_pool[i].generation, 1);
    }
}

// Helper: Count active connections in pool
static int count_pool_connections(void) {
    int count = 0;
    for (int i = 0; i < POOL_SIZE_MAX; i++) {
        if (mock_pool[i].conn != NULL) {
            count++;
        }
    }
    return count;
}

// Helper: Count READY slots
static int count_ready_slots(void) {
    int count = 0;
    for (int i = 0; i < POOL_SIZE_MAX; i++) {
        if (atomic_load(&mock_pool[i].state) == SLOT_READY) {
            count++;
        }
    }
    return count;
}

// ============================================================================
// Test: Fork - child has no inherited connections
// ============================================================================

static void test_fork_child_no_inherited_connections(void) {
    TEST("Fork - child has no inherited connections");

    // Reset state
    atfork_child_called = 0;
    atfork_parent_called = 0;
    atfork_prepare_called = 0;

    // Setup: parent has 4 active connections
    setup_mock_pool_with_connections(4);

    // Verify parent has connections
    if (count_pool_connections() != 4) {
        FAIL("Parent should have 4 connections before fork");
        return;
    }

    // Register handlers (must be done before fork)
    register_atfork_handlers();

    pid_t pid = fork();
    if (pid < 0) {
        FAIL("fork() failed");
        return;
    }

    if (pid == 0) {
        // Child process
        // atfork_child should have been called, clearing the pool

        int conn_count = count_pool_connections();
        int ready_count = count_ready_slots();

        if (conn_count == 0 && ready_count == 0) {
            _exit(0);  // Pass
        } else {
            fprintf(stderr, "Child: conn_count=%d, ready_count=%d (expected 0, 0)\n",
                    conn_count, ready_count);
            _exit(1);  // Fail
        }
    } else {
        // Parent waits for child
        int status;
        waitpid(pid, &status, 0);

        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
            PASS();
        } else {
            FAIL("Child still had inherited connections");
        }
    }
}

// ============================================================================
// Test: Fork - parent connections preserved
// ============================================================================

static void test_fork_parent_connections_preserved(void) {
    TEST("Fork - parent connections preserved");

    // Setup: parent has 3 active connections
    setup_mock_pool_with_connections(3);

    // Store connection pointers to verify they're unchanged
    void *original_conns[3];
    for (int i = 0; i < 3; i++) {
        original_conns[i] = mock_pool[i].conn;
    }

    register_atfork_handlers();

    pid_t pid = fork();
    if (pid < 0) {
        FAIL("fork() failed");
        return;
    }

    if (pid == 0) {
        // Child just exits
        _exit(0);
    } else {
        // Parent waits
        int status;
        waitpid(pid, &status, 0);

        // Verify parent's connections are unchanged
        int preserved = 1;
        for (int i = 0; i < 3; i++) {
            if (mock_pool[i].conn != original_conns[i]) {
                preserved = 0;
                break;
            }
            if (atomic_load(&mock_pool[i].state) != SLOT_READY) {
                preserved = 0;
                break;
            }
        }

        if (preserved && count_pool_connections() == 3) {
            PASS();
        } else {
            FAIL("Parent connections were modified after fork");
        }
    }
}

// ============================================================================
// Test: Fork - child can create new connections
// ============================================================================

static void test_fork_child_can_create_new_connections(void) {
    TEST("Fork - child can create new connections");

    // Setup: parent has 2 connections
    setup_mock_pool_with_connections(2);
    register_atfork_handlers();

    pid_t pid = fork();
    if (pid < 0) {
        FAIL("fork() failed");
        return;
    }

    if (pid == 0) {
        // Child process - pool should be empty after atfork_child
        int initial_count = count_pool_connections();
        if (initial_count != 0) {
            fprintf(stderr, "Child: initial conn count=%d (expected 0)\n", initial_count);
            _exit(1);
        }

        // Simulate creating a new connection in child
        // Find a free slot and "create" a connection
        for (int i = 0; i < POOL_SIZE_MAX; i++) {
            pool_slot_state_t expected = SLOT_FREE;
            if (atomic_compare_exchange_strong(&mock_pool[i].state,
                                               &expected, SLOT_RESERVED)) {
                mock_pool[i].conn = (void*)0xC41D0001;
                mock_pool[i].owner_thread = pthread_self();
                mock_pool[i].last_used = time(NULL);
                atomic_store(&mock_pool[i].generation, 1);
                atomic_store(&mock_pool[i].state, SLOT_READY);
                break;
            }
        }

        // Verify child has its own connection
        int final_count = count_pool_connections();
        if (final_count == 1) {
            _exit(0);  // Pass
        } else {
            fprintf(stderr, "Child: final conn count=%d (expected 1)\n", final_count);
            _exit(2);
        }
    } else {
        // Parent waits
        int status;
        waitpid(pid, &status, 0);

        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
            PASS();
        } else {
            FAIL("Child failed to create new connections");
        }
    }
}

// ============================================================================
// Test: Fork - TLS reset in child
// ============================================================================

static void test_fork_tls_reset_in_child(void) {
    TEST("Fork - TLS caches reset in child");

    // Setup: set TLS values in parent
    setup_mock_pool_with_connections(2);
    tls_cached_db = (void*)0xDB123456;
    tls_cached_conn = (void*)0xC0117890;
    tls_pool_slot = 5;
    tls_pool_generation = 42;

    register_atfork_handlers();

    pid_t pid = fork();
    if (pid < 0) {
        FAIL("fork() failed");
        return;
    }

    if (pid == 0) {
        // Child process - TLS should be reset after atfork_child

        int pass = 1;
        if (tls_cached_db != NULL) {
            fprintf(stderr, "Child: tls_cached_db=%p (expected NULL)\n", tls_cached_db);
            pass = 0;
        }
        if (tls_cached_conn != NULL) {
            fprintf(stderr, "Child: tls_cached_conn=%p (expected NULL)\n", tls_cached_conn);
            pass = 0;
        }
        if (tls_pool_slot != -1) {
            fprintf(stderr, "Child: tls_pool_slot=%d (expected -1)\n", tls_pool_slot);
            pass = 0;
        }
        if (tls_pool_generation != 0) {
            fprintf(stderr, "Child: tls_pool_generation=%u (expected 0)\n", tls_pool_generation);
            pass = 0;
        }

        _exit(pass ? 0 : 1);
    } else {
        // Parent waits
        int status;
        waitpid(pid, &status, 0);

        // Verify parent TLS is unchanged
        if (tls_cached_db != (void*)0xDB123456 ||
            tls_cached_conn != (void*)0xC0117890 ||
            tls_pool_slot != 5 ||
            tls_pool_generation != 42) {
            FAIL("Parent TLS was incorrectly modified");
            return;
        }

        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
            PASS();
        } else {
            FAIL("Child TLS was not properly reset");
        }
    }
}

// ============================================================================
// Test: atfork handlers registered
// ============================================================================

static void test_fork_atfork_handlers_registered(void) {
    TEST("Fork - pthread_atfork handlers registered");

    // Reset flags
    atfork_prepare_called = 0;
    atfork_parent_called = 0;
    atfork_child_called = 0;

    setup_mock_pool_with_connections(1);
    register_atfork_handlers();

    if (!atfork_handlers_registered) {
        FAIL("pthread_atfork registration failed");
        return;
    }

    pid_t pid = fork();
    if (pid < 0) {
        FAIL("fork() failed");
        return;
    }

    if (pid == 0) {
        // Child - verify atfork_child was called
        if (atfork_child_called) {
            _exit(0);
        } else {
            _exit(1);
        }
    } else {
        // Parent waits
        int status;
        waitpid(pid, &status, 0);

        // In parent, atfork_prepare and atfork_parent should have been called
        int parent_ok = atfork_prepare_called && atfork_parent_called;
        int child_ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;

        if (parent_ok && child_ok) {
            PASS();
        } else {
            char msg[128];
            snprintf(msg, sizeof(msg),
                     "prepare=%d, parent=%d, child_exit=%d",
                     atfork_prepare_called, atfork_parent_called,
                     WIFEXITED(status) ? WEXITSTATUS(status) : -1);
            FAIL(msg);
        }
    }
}

// ============================================================================
// Test: Double fork (grandchild) safety
// ============================================================================

static void test_double_fork_safety(void) {
    TEST("Fork - double fork (grandchild) safety");

    // Setup: parent has 4 connections
    setup_mock_pool_with_connections(4);
    register_atfork_handlers();

    pid_t pid1 = fork();
    if (pid1 < 0) {
        FAIL("First fork() failed");
        return;
    }

    if (pid1 == 0) {
        // Child process
        int child_conns = count_pool_connections();
        if (child_conns != 0) {
            fprintf(stderr, "Child: conn_count=%d (expected 0)\n", child_conns);
            _exit(1);
        }

        // Child creates its own connection
        mock_pool[0].conn = (void*)0xC41D0001;
        mock_pool[0].owner_thread = pthread_self();
        atomic_store(&mock_pool[0].state, SLOT_READY);
        atomic_store(&mock_pool[0].generation, 1);

        // Now fork again (grandchild)
        pid_t pid2 = fork();
        if (pid2 < 0) {
            _exit(2);  // Fork failed
        }

        if (pid2 == 0) {
            // Grandchild process
            int grandchild_conns = count_pool_connections();
            if (grandchild_conns != 0) {
                fprintf(stderr, "Grandchild: conn_count=%d (expected 0)\n", grandchild_conns);
                _exit(3);
            }

            // Grandchild can also create its own connection
            mock_pool[0].conn = (void*)0x68A1D001;
            mock_pool[0].owner_thread = pthread_self();
            atomic_store(&mock_pool[0].state, SLOT_READY);
            atomic_store(&mock_pool[0].generation, 1);

            int final = count_pool_connections();
            _exit(final == 1 ? 0 : 4);
        } else {
            // Child waits for grandchild
            int status;
            waitpid(pid2, &status, 0);

            // Child's connection should still be there
            if (count_pool_connections() != 1) {
                _exit(5);
            }

            if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
                _exit(0);  // Pass
            } else {
                _exit(6);
            }
        }
    } else {
        // Parent waits
        int status;
        waitpid(pid1, &status, 0);

        // Parent should still have its 4 connections
        if (count_pool_connections() != 4) {
            FAIL("Parent lost connections after double fork");
            return;
        }

        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
            PASS();
        } else {
            char msg[64];
            snprintf(msg, sizeof(msg), "Child/grandchild failed with status %d",
                     WIFEXITED(status) ? WEXITSTATUS(status) : -1);
            FAIL(msg);
        }
    }
}

// ============================================================================
// Test: Concurrent fork from multiple threads
// ============================================================================

typedef struct {
    int thread_id;
    int fork_result;  // 0 = pass, non-zero = fail
} thread_fork_data_t;

static void* thread_fork_func(void *arg) {
    thread_fork_data_t *data = (thread_fork_data_t *)arg;

    // Small delay to stagger forks
    usleep(data->thread_id * 1000);

    pid_t pid = fork();
    if (pid < 0) {
        data->fork_result = -1;
        return NULL;
    }

    if (pid == 0) {
        // Child - verify pool is clean
        int conns = count_pool_connections();
        _exit(conns == 0 ? 0 : 1);
    } else {
        // Parent waits
        int status;
        waitpid(pid, &status, 0);

        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
            data->fork_result = 0;
        } else {
            data->fork_result = 1;
        }
    }

    return NULL;
}

static void test_concurrent_fork_from_threads(void) {
    TEST("Fork - concurrent forks from multiple threads");

    // Setup pool with connections
    setup_mock_pool_with_connections(4);
    register_atfork_handlers();

    // Launch threads that will each fork
    pthread_t threads[4];
    thread_fork_data_t data[4];

    for (int i = 0; i < 4; i++) {
        data[i].thread_id = i;
        data[i].fork_result = -1;
        pthread_create(&threads[i], NULL, thread_fork_func, &data[i]);
    }

    // Wait for all threads
    for (int i = 0; i < 4; i++) {
        pthread_join(threads[i], NULL);
    }

    // Check results
    int all_pass = 1;
    for (int i = 0; i < 4; i++) {
        if (data[i].fork_result != 0) {
            all_pass = 0;
            break;
        }
    }

    // Parent should still have its connections
    if (count_pool_connections() != 4) {
        FAIL("Parent lost connections during concurrent forks");
        return;
    }

    if (all_pass) {
        PASS();
    } else {
        FAIL("Some child processes had inherited connections");
    }
}

// ============================================================================
// Test: PID tracking for posix_spawn detection
// ============================================================================

static void test_pid_tracking(void) {
    TEST("Fork - PID tracking updated in child");

    setup_mock_pool_with_connections(1);
    register_atfork_handlers();

    pid_t parent_pid = getpid();
    pid_t parent_init_pid = init_pid;

    if (parent_init_pid != parent_pid) {
        FAIL("init_pid should match parent's PID before fork");
        return;
    }

    pid_t pid = fork();
    if (pid < 0) {
        FAIL("fork() failed");
        return;
    }

    if (pid == 0) {
        // Child - init_pid should be updated to child's PID
        pid_t child_pid = getpid();
        if (init_pid == child_pid && init_pid != parent_pid) {
            _exit(0);
        } else {
            fprintf(stderr, "Child: init_pid=%d, child_pid=%d, parent_pid=%d\n",
                    init_pid, child_pid, parent_pid);
            _exit(1);
        }
    } else {
        // Parent waits
        int status;
        waitpid(pid, &status, 0);

        // Parent's init_pid should be unchanged
        if (init_pid != parent_pid) {
            FAIL("Parent's init_pid was modified");
            return;
        }

        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
            PASS();
        } else {
            FAIL("Child's init_pid was not properly updated");
        }
    }
}

// ============================================================================
// Test: Slot state transitions after fork
// ============================================================================

static void test_slot_state_transitions_after_fork(void) {
    TEST("Fork - slot states transition to FREE");

    setup_mock_pool_with_connections(0);  // Start empty

    // Set up slots in various states
    mock_pool[0].conn = (void*)0x1;
    atomic_store(&mock_pool[0].state, SLOT_READY);

    mock_pool[1].conn = (void*)0x2;
    atomic_store(&mock_pool[1].state, SLOT_RESERVED);

    mock_pool[2].conn = (void*)0x3;
    atomic_store(&mock_pool[2].state, SLOT_RECONNECTING);

    mock_pool[3].conn = (void*)0x4;
    atomic_store(&mock_pool[3].state, SLOT_ERROR);

    // Slot 4 stays FREE with no connection
    mock_pool[4].conn = NULL;
    atomic_store(&mock_pool[4].state, SLOT_FREE);

    register_atfork_handlers();

    pid_t pid = fork();
    if (pid < 0) {
        FAIL("fork() failed");
        return;
    }

    if (pid == 0) {
        // Child - all slots should be FREE with no connections
        int all_free = 1;
        int all_null = 1;

        for (int i = 0; i < 5; i++) {
            if (atomic_load(&mock_pool[i].state) != SLOT_FREE) {
                fprintf(stderr, "Child: slot %d state=%d (expected FREE=0)\n",
                        i, atomic_load(&mock_pool[i].state));
                all_free = 0;
            }
            if (mock_pool[i].conn != NULL) {
                fprintf(stderr, "Child: slot %d conn=%p (expected NULL)\n",
                        i, mock_pool[i].conn);
                all_null = 0;
            }
        }

        _exit((all_free && all_null) ? 0 : 1);
    } else {
        // Parent waits
        int status;
        waitpid(pid, &status, 0);

        // Parent's slots should be unchanged
        if (atomic_load(&mock_pool[0].state) != SLOT_READY ||
            atomic_load(&mock_pool[1].state) != SLOT_RESERVED ||
            atomic_load(&mock_pool[2].state) != SLOT_RECONNECTING ||
            atomic_load(&mock_pool[3].state) != SLOT_ERROR ||
            atomic_load(&mock_pool[4].state) != SLOT_FREE) {
            FAIL("Parent's slot states were modified");
            return;
        }

        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
            PASS();
        } else {
            FAIL("Child's slot states not properly reset to FREE");
        }
    }
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("\n\033[1m=== Fork Safety Tests ===\033[0m\n\n");

    // Run basic tests first
    printf("\033[1mBasic Fork Safety:\033[0m\n");
    test_fork_atfork_handlers_registered();
    test_fork_child_no_inherited_connections();
    test_fork_parent_connections_preserved();

    printf("\n\033[1mChild Process Operations:\033[0m\n");
    test_fork_child_can_create_new_connections();
    test_fork_tls_reset_in_child();

    printf("\n\033[1mAdvanced Fork Scenarios:\033[0m\n");
    test_double_fork_safety();
    test_pid_tracking();
    test_slot_state_transitions_after_fork();

    printf("\n\033[1mConcurrency:\033[0m\n");
    test_concurrent_fork_from_threads();

    printf("\n\033[1m=== Results ===\033[0m\n");
    printf("Passed: \033[32m%d\033[0m\n", tests_passed);
    printf("Failed: \033[31m%d\033[0m\n", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
