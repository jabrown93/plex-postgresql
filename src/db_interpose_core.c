/*
 * Plex PostgreSQL Interposing Shim - Core Module
 *
 * This is the main entry point containing:
 * - Global shared state definitions
 * - Original SQLite function pointers (for fishhook rebinding)
 * - Worker thread implementation
 * - Constructor/destructor with fishhook initialization
 * - C++ exception interception for debugging
 */

#include "db_interpose.h"
#include "pg_query_cache.h"
#include "fishhook.h"
#include <execinfo.h>
#include <signal.h>

// ============================================================================
// C++ Exception Interception (macOS via fishhook)
// ============================================================================

// Limits for exception logging to prevent log explosion
#define MAX_EXCEPTION_TYPES 64
#define MAX_LOGGED_PER_TYPE 3
#define MAX_LOGGED_TOTAL 50

// Original __cxa_throw function pointer - must use noreturn attribute to match ABI
typedef void (*cxa_throw_fn)(void*, void*, void(*)(void*)) __attribute__((noreturn));
static cxa_throw_fn orig_cxa_throw = NULL;

// Thread-local recursion prevention
static __thread int in_exception_handler = 0;

// Exception type tracking
typedef struct {
    const char *type_name;
    int count;
    int logged_with_trace;
} exception_type_tracker_t;

static exception_type_tracker_t exception_types[MAX_EXCEPTION_TYPES];
static int exception_type_count = 0;
static volatile int total_exception_count = 0;
static pthread_mutex_t exception_tracker_mutex = PTHREAD_MUTEX_INITIALIZER;

// Thread-local counters for more accurate debugging (declared early for use in print_exception_info)
static __thread long tls_value_type_calls = 0;
static __thread long tls_column_type_calls = 0;
static __thread const char *tls_last_query = NULL;

// Demangle function pointer
static char* (*cxa_demangle_fn)(const char*, char*, size_t*, int*) = NULL;

// Get type name from type_info
static const char* get_type_name(void *tinfo) {
    if (!tinfo) return "unknown";
    // type_info layout: vtable pointer, then const char* name
    const char **name_ptr = (const char**)((char*)tinfo + sizeof(void*));
    return *name_ptr;
}

// Find or create tracker for an exception type
static exception_type_tracker_t* get_exception_tracker(const char *type_name) {
    pthread_mutex_lock(&exception_tracker_mutex);
    
    for (int i = 0; i < exception_type_count; i++) {
        if (exception_types[i].type_name == type_name ||
            (exception_types[i].type_name && type_name &&
             strcmp(exception_types[i].type_name, type_name) == 0)) {
            exception_types[i].count++;
            pthread_mutex_unlock(&exception_tracker_mutex);
            return &exception_types[i];
        }
    }
    
    if (exception_type_count < MAX_EXCEPTION_TYPES) {
        exception_type_tracker_t *tracker = &exception_types[exception_type_count++];
        tracker->type_name = type_name;
        tracker->count = 1;
        tracker->logged_with_trace = 0;
        pthread_mutex_unlock(&exception_tracker_mutex);
        return tracker;
    }
    
    pthread_mutex_unlock(&exception_tracker_mutex);
    return NULL;
}

// ============================================================================
// Crash/Exit Handler for Debugging
// ============================================================================

// Try to extract exception message from std::exception or derived types
// Returns pointer to static buffer or NULL
// SAFE version that avoids calling virtual functions that might cause issues
static const char* try_get_exception_message(void *thrown_exception, void *tinfo) {
    (void)tinfo;  // Unused for now
    if (!thrown_exception) return NULL;
    
    // The safest approach is to not call what() at all during exception handling
    // as it can cause issues with exception propagation.
    // Instead, we just return NULL and let the exception type speak for itself.
    
    // NOTE: Previously tried to call vtable[2] for what(), but this was causing
    // issues with exception handling in boost::asio threads.
    
    return NULL;
}

static void print_backtrace_demangled(const char *reason, int skip_frames) {
    void *callstack[64];
    int frames = backtrace(callstack, 64);
    char **symbols = backtrace_symbols(callstack, frames);
    
    fprintf(stderr, "\n");
    fprintf(stderr, "╔══════════════════════════════════════════════════════════════════════════════╗\n");
    fprintf(stderr, "║ BACKTRACE: %-67s ║\n", reason);
    fprintf(stderr, "╠══════════════════════════════════════════════════════════════════════════════╣\n");
    LOG_ERROR("=== BACKTRACE (%s) ===", reason);
    
    // Skip our own frames (print_backtrace, my_cxa_throw, etc.)
    int start = skip_frames;
    int printed = 0;
    
    for (int i = start; i < frames && printed < 25; i++) {
        // Parse the symbol line to extract the mangled name
        // Format: "index  library  address  symbol + offset"
        char *symbol = symbols[i];
        
        // Find the mangled symbol name (starts after the address)
        char *name_start = NULL;
        char *plus_sign = strrchr(symbol, '+');
        if (plus_sign) {
            // Work backwards from + to find start of symbol name
            char *p = plus_sign - 1;
            while (p > symbol && *p == ' ') p--;
            while (p > symbol && *p != ' ') p--;
            if (*p == ' ') name_start = p + 1;
        }
        
        char demangled_line[256] = {0};
        if (name_start && cxa_demangle_fn) {
            // Extract just the mangled name
            size_t name_len = plus_sign - name_start - 1;
            char mangled[256];
            if (name_len < sizeof(mangled)) {
                strncpy(mangled, name_start, name_len);
                mangled[name_len] = '\0';
                
                int status = 0;
                char *demangled = cxa_demangle_fn(mangled, NULL, NULL, &status);
                if (demangled && status == 0) {
                    // Truncate long names
                    if (strlen(demangled) > 70) {
                        demangled[67] = '.';
                        demangled[68] = '.';
                        demangled[69] = '.';
                        demangled[70] = '\0';
                    }
                    snprintf(demangled_line, sizeof(demangled_line), "[%2d] %s", i - start, demangled);
                    free(demangled);
                }
            }
        }
        
        if (demangled_line[0] == '\0') {
            // Fallback to raw symbol (truncated)
            snprintf(demangled_line, sizeof(demangled_line), "[%2d] %.72s", i - start, symbol);
        }
        
        fprintf(stderr, "║ %-78s ║\n", demangled_line);
        LOG_ERROR("  %s", demangled_line);
        printed++;
    }
    
    if (frames > start + 25) {
        fprintf(stderr, "║ ... and %d more frames                                                         ║\n", frames - start - 25);
    }
    fprintf(stderr, "╚══════════════════════════════════════════════════════════════════════════════╝\n");
    fflush(stderr);
    
    free(symbols);
}

// Legacy function for compatibility
static void print_backtrace(const char *reason) {
    print_backtrace_demangled(reason, 0);
}

// Print exception with context - now takes thrown_exception to extract message
static void print_exception_info_full(void *thrown_exception, void *tinfo, const char *type_name, int count) {
    // Demangle type name
    char *demangled = NULL;
    if (!cxa_demangle_fn) {
        cxa_demangle_fn = (char* (*)(const char*, char*, size_t*, int*))dlsym(RTLD_DEFAULT, "__cxa_demangle");
    }
    if (cxa_demangle_fn && type_name) {
        int status = 0;
        demangled = cxa_demangle_fn(type_name, NULL, NULL, &status);
    }
    const char *readable_name = demangled ? demangled : (type_name ? type_name : "unknown");
    
    // Try to get exception message
    const char *exc_message = try_get_exception_message(thrown_exception, tinfo);
    
    // Get shim context
    const char *ctx_query = last_query_being_processed;
    const char *ctx_column = last_column_being_accessed;
    long ctx_value_calls = global_value_type_calls;
    long ctx_column_calls = global_column_type_calls;
    int is_shim_related = (ctx_value_calls > 0 || ctx_column_calls > 0 || ctx_query != NULL);
    
    // Check if this specific thread has done any shim work
    int tls_is_shim_related = (tls_column_type_calls > 0 || tls_value_type_calls > 0 || tls_last_query != NULL);
    
    pthread_t tid = pthread_self();
    fprintf(stderr, "\n");
    fprintf(stderr, "╔══════════════════════════════════════════════════════════════════════════════╗\n");
    fprintf(stderr, "║ C++ EXCEPTION #%-4d                                                          ║\n", count);
    fprintf(stderr, "╠══════════════════════════════════════════════════════════════════════════════╣\n");
    fprintf(stderr, "║ Type: %-72s ║\n", readable_name);
    fprintf(stderr, "║ PID: %-6d  Thread: 0x%-54llx ║\n", getpid(), (unsigned long long)tid);
    
    // Print exception message if available
    if (exc_message && exc_message[0]) {
        fprintf(stderr, "╠══════════════════════════════════════════════════════════════════════════════╣\n");
        fprintf(stderr, "║ MESSAGE:                                                                     ║\n");
        // Print message, handling long messages with word wrap
        const char *p = exc_message;
        while (*p) {
            char line[76];
            int len = 0;
            while (*p && len < 74) {
                if (*p == '\n') { p++; break; }
                line[len++] = *p++;
            }
            line[len] = '\0';
            fprintf(stderr, "║   %-75s ║\n", line);
        }
    }
    
    fprintf(stderr, "╠══════════════════════════════════════════════════════════════════════════════╣\n");
    
    if (is_shim_related) {
        fprintf(stderr, "║ SHIM STATE:                                                                  ║\n");
        fprintf(stderr, "║   Global: col_type=%ld, val_type=%ld                                         ║\n",
                ctx_column_calls, ctx_value_calls);
        fprintf(stderr, "║   Thread: col_type=%ld, val_type=%ld (this_thread_used_shim=%s)              ║\n",
                tls_column_type_calls, tls_value_type_calls, tls_is_shim_related ? "YES" : "NO");
        if (!tls_is_shim_related) {
            fprintf(stderr, "║   NOTE: This thread has NOT made any SQLite calls through shim!            ║\n");
        }
        if (ctx_query && ctx_query[0]) {
            fprintf(stderr, "║   Last Query (any thread): %.50s ║\n", ctx_query);
        }
        if (ctx_column && ctx_column[0]) {
            fprintf(stderr, "║   Last Column: %-62s ║\n", ctx_column);
        }
    } else {
        fprintf(stderr, "║ NOT SHIM-RELATED: No SQLite calls have been made through the shim           ║\n");
    }
    
    // Log to file with full message
    LOG_ERROR("EXCEPTION #%d [%s]: msg=%s shim=%s tls_shim=%s col=%ld val=%ld",
              count, readable_name, 
              exc_message ? exc_message : "(no message)",
              is_shim_related ? "YES" : "NO",
              tls_is_shim_related ? "YES" : "NO",
              ctx_column_calls, ctx_value_calls);
    
    if (demangled) free(demangled);
}

// Our __cxa_throw interceptor - MUST be noreturn to match original ABI
__attribute__((noreturn))
static void my_cxa_throw(void *thrown_exception, void *tinfo, void (*dest)(void*)) {
    // Prevent recursion
    if (in_exception_handler) {
        if (orig_cxa_throw) {
            orig_cxa_throw(thrown_exception, tinfo, dest);
        }
        abort();
    }
    in_exception_handler = 1;
    
    int total_count = __sync_add_and_fetch(&total_exception_count, 1);
    const char *type_name = get_type_name(tinfo);
    exception_type_tracker_t *tracker = get_exception_tracker(type_name);
    
    // Determine if we should log this exception
    // Always log DB::Exception and similar database-related exceptions
    int is_db_exception = (type_name && (strstr(type_name, "DB") || strstr(type_name, "Exception") || 
                           strstr(type_name, "exception") || strstr(type_name, "Error")));
    
    int should_log = is_db_exception || 
                     ((total_count <= MAX_LOGGED_TOTAL) &&
                      (tracker == NULL || tracker->count <= MAX_LOGGED_PER_TYPE));
    int should_trace = is_db_exception || (tracker && !tracker->logged_with_trace);
    
    if (should_log) {
        print_exception_info_full(thrown_exception, tinfo, type_name, total_count);
        
        if (should_trace) {
            if (tracker) tracker->logged_with_trace = 1;
            print_backtrace_demangled("Exception Stack Trace", 2);  // Skip my_cxa_throw frames
        }
        
        fprintf(stderr, "╚══════════════════════════════════════════════════════════════════════════════╝\n");
        fflush(stderr);
    } else if (total_count == MAX_LOGGED_TOTAL + 1) {
        fprintf(stderr, "\n╔══════════════════════════════════════════════════════════════════════════════╗\n");
        fprintf(stderr, "║ [THROTTLE] Exception logging limited (>%d). Summary in log file.              ║\n", MAX_LOGGED_TOTAL);
        fprintf(stderr, "╚══════════════════════════════════════════════════════════════════════════════╝\n");
        
        // Log summary
        LOG_ERROR("=== EXCEPTION SUMMARY ===");
        for (int i = 0; i < exception_type_count; i++) {
            char *demangled = NULL;
            if (cxa_demangle_fn && exception_types[i].type_name) {
                int status = 0;
                demangled = cxa_demangle_fn(exception_types[i].type_name, NULL, NULL, &status);
            }
            LOG_ERROR("  %s: %d occurrences",
                      demangled ? demangled : exception_types[i].type_name,
                      exception_types[i].count);
            if (demangled) free(demangled);
        }
    }
    
    in_exception_handler = 0;
    
    // Call original - MUST call this for exception to propagate correctly
    if (orig_cxa_throw) {
        // Debug: log the call
        fprintf(stderr, "[EXCEPTION] Calling orig_cxa_throw at %p\n", (void*)orig_cxa_throw);
        fflush(stderr);
        orig_cxa_throw(thrown_exception, tinfo, dest);
    } else {
        fprintf(stderr, "[EXCEPTION] FATAL: orig_cxa_throw is NULL! Cannot propagate exception.\n");
        fflush(stderr);
    }
    
    // Should never reach here - __cxa_throw is [[noreturn]]
    fprintf(stderr, "[EXCEPTION] FATAL: orig_cxa_throw returned! This should never happen.\n");
    fflush(stderr);
    abort();
}

static void signal_handler(int sig) {
    const char *sig_name = "UNKNOWN";
    const char *sig_desc = "Unknown signal";
    switch(sig) {
        case SIGSEGV: sig_name = "SIGSEGV"; sig_desc = "Segmentation fault"; break;
        case SIGABRT: sig_name = "SIGABRT"; sig_desc = "Abort"; break;
        case SIGBUS:  sig_name = "SIGBUS";  sig_desc = "Bus error"; break;
        case SIGFPE:  sig_name = "SIGFPE";  sig_desc = "Floating point exception"; break;
        case SIGILL:  sig_name = "SIGILL";  sig_desc = "Illegal instruction"; break;
    }
    
    // Get shim context
    const char *ctx_query = last_query_being_processed;
    const char *ctx_column = last_column_being_accessed;
    
    fprintf(stderr, "\n");
    fprintf(stderr, "╔══════════════════════════════════════════════════════════════════════╗\n");
    fprintf(stderr, "║ FATAL SIGNAL: %s (%s)                                       ║\n", sig_name, sig_desc);
    fprintf(stderr, "╠══════════════════════════════════════════════════════════════════════╣\n");
    if (ctx_query) {
        char q[60];
        snprintf(q, sizeof(q), "%.59s", ctx_query);
        fprintf(stderr, "║ Last Query: %-59s ║\n", q);
    }
    if (ctx_column) {
        fprintf(stderr, "║ Last Column: %-58s ║\n", ctx_column);
    }
    fprintf(stderr, "╚══════════════════════════════════════════════════════════════════════╝\n");
    
    print_backtrace(sig_name);
    
    // Re-raise to get default behavior
    signal(sig, SIG_DFL);
    raise(sig);
}

// Exit handler - tracks clean (non-signal) exits
static void exit_handler(void) {
    const char *ctx_query = last_query_being_processed;
    const char *ctx_column = last_column_being_accessed;
    
    fprintf(stderr, "\n");
    fprintf(stderr, "╔══════════════════════════════════════════════════════════════════════╗\n");
    fprintf(stderr, "║ EXIT_HANDLER: Process exiting normally (atexit)                      ║\n");
    fprintf(stderr, "╠══════════════════════════════════════════════════════════════════════╣\n");
    fprintf(stderr, "║ PID: %-66d ║\n", getpid());
    fprintf(stderr, "║ value_type calls: %-53ld ║\n", global_value_type_calls);
    fprintf(stderr, "║ column_type calls: %-52ld ║\n", global_column_type_calls);
    if (ctx_query) {
        char q[60];
        snprintf(q, sizeof(q), "%.59s", ctx_query);
        fprintf(stderr, "║ Last Query: %-59s ║\n", q);
    }
    if (ctx_column) {
        fprintf(stderr, "║ Last Column: %-58s ║\n", ctx_column);
    }
    fprintf(stderr, "╚══════════════════════════════════════════════════════════════════════╝\n");
    
    print_backtrace("EXIT");
}

// ============================================================================
// Global State Definitions (exported via db_interpose.h)
// ============================================================================

// Recursion prevention
__thread int in_interpose_call = 0;
__thread int prepare_v2_depth = 0;

// SQLite library handle for dlsym fallback
void *sqlite_handle = NULL;

// Original SQLite function pointers (populated by fishhook rebind_symbols)
// These are called by our my_* implementations to invoke the real SQLite
// CRITICAL: VISIBLE attribute ensures child processes can resolve these symbols
VISIBLE int (*orig_sqlite3_open)(const char*, sqlite3**) = NULL;
VISIBLE int (*orig_sqlite3_open_v2)(const char*, sqlite3**, int, const char*) = NULL;
VISIBLE int (*orig_sqlite3_close)(sqlite3*) = NULL;
VISIBLE int (*orig_sqlite3_close_v2)(sqlite3*) = NULL;
VISIBLE int (*orig_sqlite3_exec)(sqlite3*, const char*, int(*)(void*,int,char**,char**), void*, char**) = NULL;
VISIBLE int (*orig_sqlite3_changes)(sqlite3*) = NULL;
VISIBLE sqlite3_int64 (*orig_sqlite3_changes64)(sqlite3*) = NULL;
VISIBLE sqlite3_int64 (*orig_sqlite3_last_insert_rowid)(sqlite3*) = NULL;
VISIBLE int (*orig_sqlite3_get_table)(sqlite3*, const char*, char***, int*, int*, char**) = NULL;

VISIBLE const char* (*orig_sqlite3_errmsg)(sqlite3*) = NULL;
VISIBLE int (*orig_sqlite3_errcode)(sqlite3*) = NULL;
VISIBLE int (*orig_sqlite3_extended_errcode)(sqlite3*) = NULL;

VISIBLE int (*orig_sqlite3_prepare)(sqlite3*, const char*, int, sqlite3_stmt**, const char**) = NULL;
VISIBLE int (*orig_sqlite3_prepare_v2)(sqlite3*, const char*, int, sqlite3_stmt**, const char**) = NULL;
VISIBLE int (*orig_sqlite3_prepare_v3)(sqlite3*, const char*, int, unsigned int, sqlite3_stmt**, const char**) = NULL;
VISIBLE int (*orig_sqlite3_prepare16_v2)(sqlite3*, const void*, int, sqlite3_stmt**, const void**) = NULL;

VISIBLE int (*orig_sqlite3_bind_int)(sqlite3_stmt*, int, int) = NULL;
VISIBLE int (*orig_sqlite3_bind_int64)(sqlite3_stmt*, int, sqlite3_int64) = NULL;
VISIBLE int (*orig_sqlite3_bind_double)(sqlite3_stmt*, int, double) = NULL;
VISIBLE int (*orig_sqlite3_bind_text)(sqlite3_stmt*, int, const char*, int, void(*)(void*)) = NULL;
VISIBLE int (*orig_sqlite3_bind_text64)(sqlite3_stmt*, int, const char*, sqlite3_uint64, void(*)(void*), unsigned char) = NULL;
VISIBLE int (*orig_sqlite3_bind_blob)(sqlite3_stmt*, int, const void*, int, void(*)(void*)) = NULL;
VISIBLE int (*orig_sqlite3_bind_blob64)(sqlite3_stmt*, int, const void*, sqlite3_uint64, void(*)(void*)) = NULL;
VISIBLE int (*orig_sqlite3_bind_value)(sqlite3_stmt*, int, const sqlite3_value*) = NULL;
VISIBLE int (*orig_sqlite3_bind_null)(sqlite3_stmt*, int) = NULL;

VISIBLE int (*orig_sqlite3_step)(sqlite3_stmt*) = NULL;
VISIBLE int (*orig_sqlite3_reset)(sqlite3_stmt*) = NULL;
VISIBLE int (*orig_sqlite3_finalize)(sqlite3_stmt*) = NULL;
VISIBLE int (*orig_sqlite3_clear_bindings)(sqlite3_stmt*) = NULL;

VISIBLE int (*orig_sqlite3_column_count)(sqlite3_stmt*) = NULL;
VISIBLE int (*orig_sqlite3_column_type)(sqlite3_stmt*, int) = NULL;
VISIBLE int (*orig_sqlite3_column_int)(sqlite3_stmt*, int) = NULL;
VISIBLE sqlite3_int64 (*orig_sqlite3_column_int64)(sqlite3_stmt*, int) = NULL;
VISIBLE double (*orig_sqlite3_column_double)(sqlite3_stmt*, int) = NULL;
VISIBLE const unsigned char* (*orig_sqlite3_column_text)(sqlite3_stmt*, int) = NULL;
VISIBLE const void* (*orig_sqlite3_column_blob)(sqlite3_stmt*, int) = NULL;
VISIBLE int (*orig_sqlite3_column_bytes)(sqlite3_stmt*, int) = NULL;
VISIBLE const char* (*orig_sqlite3_column_name)(sqlite3_stmt*, int) = NULL;
VISIBLE const char* (*orig_sqlite3_column_decltype)(sqlite3_stmt*, int) = NULL;
VISIBLE sqlite3_value* (*orig_sqlite3_column_value)(sqlite3_stmt*, int) = NULL;
VISIBLE int (*orig_sqlite3_data_count)(sqlite3_stmt*) = NULL;

VISIBLE int (*orig_sqlite3_value_type)(sqlite3_value*) = NULL;
VISIBLE const unsigned char* (*orig_sqlite3_value_text)(sqlite3_value*) = NULL;
VISIBLE int (*orig_sqlite3_value_int)(sqlite3_value*) = NULL;
VISIBLE sqlite3_int64 (*orig_sqlite3_value_int64)(sqlite3_value*) = NULL;
VISIBLE double (*orig_sqlite3_value_double)(sqlite3_value*) = NULL;
VISIBLE int (*orig_sqlite3_value_bytes)(sqlite3_value*) = NULL;
VISIBLE const void* (*orig_sqlite3_value_blob)(sqlite3_value*) = NULL;

VISIBLE int (*orig_sqlite3_create_collation)(sqlite3*, const char*, int, void*, int(*)(void*,int,const void*,int,const void*)) = NULL;
VISIBLE int (*orig_sqlite3_create_collation_v2)(sqlite3*, const char*, int, void*, int(*)(void*,int,const void*,int,const void*), void(*)(void*)) = NULL;

// New SQLite API functions
VISIBLE void (*orig_sqlite3_free)(void*) = NULL;
VISIBLE void* (*orig_sqlite3_malloc)(int) = NULL;
VISIBLE sqlite3* (*orig_sqlite3_db_handle)(sqlite3_stmt*) = NULL;
VISIBLE const char* (*orig_sqlite3_sql)(sqlite3_stmt*) = NULL;
VISIBLE char* (*orig_sqlite3_expanded_sql)(sqlite3_stmt*) = NULL;
VISIBLE int (*orig_sqlite3_bind_parameter_count)(sqlite3_stmt*) = NULL;
VISIBLE int (*orig_sqlite3_bind_parameter_index)(sqlite3_stmt*, const char*) = NULL;
VISIBLE int (*orig_sqlite3_stmt_readonly)(sqlite3_stmt*) = NULL;
VISIBLE int (*orig_sqlite3_stmt_busy)(sqlite3_stmt*) = NULL;
VISIBLE int (*orig_sqlite3_stmt_status)(sqlite3_stmt*, int, int) = NULL;
VISIBLE const char* (*orig_sqlite3_bind_parameter_name)(sqlite3_stmt*, int) = NULL;

// Aliases for backward compatibility (used by prepare module)
int (*real_sqlite3_prepare_v2)(sqlite3*, const char*, int, sqlite3_stmt**, const char**) = NULL;
const char* (*real_sqlite3_errmsg)(sqlite3*) = NULL;
int (*real_sqlite3_errcode)(sqlite3*) = NULL;

// Worker thread state
pthread_t worker_thread;
pthread_mutex_t worker_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t worker_cond_request = PTHREAD_COND_INITIALIZER;
pthread_cond_t worker_cond_response = PTHREAD_COND_INITIALIZER;
worker_request_t worker_request;
volatile int worker_running = 0;

// Fake value pool for sqlite3_column_value
pg_fake_value_t fake_value_pool[MAX_FAKE_VALUES];
unsigned int fake_value_next = 0;
pthread_mutex_t fake_value_mutex = PTHREAD_MUTEX_INITIALIZER;

// Initialization flag
int shim_initialized = 0;

// Global context tracking for exception debugging
// VISIBLE needed so other modules can reference these symbols
VISIBLE const char * volatile last_query_being_processed = NULL;
VISIBLE const char * volatile last_column_being_accessed = NULL;

// Global counters for debugging
volatile long global_value_type_calls = 0;
volatile long global_column_type_calls = 0;

// Thread-local counters moved to top of file (before print_exception_info)

// ============================================================================
// Helper Functions
// ============================================================================

// Check if a pointer is one of our fake values
pg_fake_value_t* pg_check_fake_value(sqlite3_value *pVal) {
    if (!pVal) return NULL;

    // Check if pointer is in our pool
    uintptr_t ptr = (uintptr_t)pVal;
    uintptr_t pool_start = (uintptr_t)&fake_value_pool[0];
    uintptr_t pool_end = (uintptr_t)&fake_value_pool[MAX_FAKE_VALUES];

    if (ptr >= pool_start && ptr < pool_end) {
        pg_fake_value_t *fake = (pg_fake_value_t*)pVal;
        if (fake->magic == PG_FAKE_VALUE_MAGIC) {
            return fake;
        }
    }
    return NULL;
}

// Helper to check if path is library.db
int is_library_db_path(const char *path) {
    return path && strstr(path, "com.plexapp.plugins.library.db") != NULL;
}

// Simple string replace helper
char* simple_str_replace(const char *str, const char *old, const char *new_str) {
    if (!str || !old || !new_str) return NULL;

    const char *pos = strstr(str, old);
    if (!pos) return NULL;

    size_t old_len = strlen(old);
    size_t new_len = strlen(new_str);
    size_t result_len = strlen(str) - old_len + new_len;

    char *result = malloc(result_len + 1);
    if (!result) return NULL;

    size_t prefix_len = pos - str;
    memcpy(result, str, prefix_len);
    memcpy(result + prefix_len, new_str, new_len);
    strcpy(result + prefix_len + new_len, pos + old_len);

    return result;
}

// ============================================================================
// Lazy Init for Real SQLite Functions (fallback if fishhook fails)
// ============================================================================

void ensure_real_sqlite_loaded(void) {
    if (real_sqlite3_prepare_v2) return;  // Already loaded

    const char *sqlite_paths[] = {
        "/Applications/Plex Media Server.app/Contents/Frameworks/libsqlite3_orig.dylib",
        "/Applications/Plex Media Server.app/Contents/Frameworks/libsqlite3.dylib",
        "/usr/lib/libsqlite3.dylib",
        NULL
    };

    for (int i = 0; sqlite_paths[i] != NULL && !sqlite_handle; i++) {
        sqlite_handle = dlopen(sqlite_paths[i], RTLD_LAZY | RTLD_LOCAL);
    }

    if (sqlite_handle) {
        real_sqlite3_prepare_v2 = dlsym(sqlite_handle, "sqlite3_prepare_v2");
        real_sqlite3_errmsg = dlsym(sqlite_handle, "sqlite3_errmsg");
        real_sqlite3_errcode = dlsym(sqlite_handle, "sqlite3_errcode");
    }
}

// ============================================================================
// Symbol Resolution Safety Check
// ============================================================================
// 
// Called at the start of critical interpose functions to ensure the shim
// is fully initialized. This catches any race condition where an interposed
// function is called before the constructor completes.
//
// Returns 1 if safe to proceed, 0 if symbols are not ready.
// When returning 0, the caller should fall back to real SQLite if possible.

static volatile int symbols_verified = 0;

int shim_ensure_ready(void) {
    // Fast path: already verified
    if (symbols_verified) return 1;
    
    // Memory barrier to ensure we see latest values
    __sync_synchronize();
    
    // Check if constructor has completed
    if (!shim_initialized) {
        // Constructor hasn't finished - this shouldn't happen with proper delays
        // but could occur if delay is disabled or too short
        fprintf(stderr, "[SHIM] WARNING: shim_ensure_ready called before shim_initialized!\n");
        fflush(stderr);
        return 0;
    }
    
    // Verify critical function pointers
    if (!orig_sqlite3_open || !orig_sqlite3_prepare_v2 || !orig_sqlite3_step) {
        // Symbols not resolved yet - try to load via dlsym as emergency fallback
        fprintf(stderr, "[SHIM] WARNING: Critical symbols NULL, attempting dlsym fallback...\n");
        fflush(stderr);
        
        if (sqlite_handle) {
            if (!orig_sqlite3_open) 
                orig_sqlite3_open = dlsym(sqlite_handle, "sqlite3_open");
            if (!orig_sqlite3_prepare_v2) 
                orig_sqlite3_prepare_v2 = dlsym(sqlite_handle, "sqlite3_prepare_v2");
            if (!orig_sqlite3_step) 
                orig_sqlite3_step = dlsym(sqlite_handle, "sqlite3_step");
        }
        
        // Check again
        if (!orig_sqlite3_open || !orig_sqlite3_prepare_v2 || !orig_sqlite3_step) {
            fprintf(stderr, "[SHIM] FATAL: Cannot resolve critical SQLite symbols!\n");
            fflush(stderr);
            return 0;
        }
    }
    
    // All checks passed - mark as verified
    symbols_verified = 1;
    return 1;
}

// ============================================================================
// Worker Thread Implementation
// ============================================================================

static void* worker_thread_func(void *arg) {
    (void)arg;
    LOG_INFO("WORKER: Thread started with %d MB stack", WORKER_STACK_SIZE / (1024*1024));

    while (1) {
        pthread_mutex_lock(&worker_mutex);

        // Wait for work
        while (!worker_request.work_ready && worker_running) {
            pthread_cond_wait(&worker_cond_request, &worker_mutex);
        }

        if (!worker_running) {
            pthread_mutex_unlock(&worker_mutex);
            break;
        }

        worker_request.work_ready = 0;

        // Handle the request
        if (worker_request.type == WORK_SHUTDOWN) {
            worker_request.work_done = 1;
            pthread_cond_signal(&worker_cond_response);
            pthread_mutex_unlock(&worker_mutex);
            break;
        }

        if (worker_request.type == WORK_PREPARE_V2) {
            sqlite3_stmt *stmt = NULL;
            const char *tail = NULL;

            // Call internal prepare with from_worker=1 to avoid recursion
            int rc = my_sqlite3_prepare_v2_internal(
                worker_request.db,
                worker_request.zSql,
                worker_request.nByte,
                &stmt,
                &tail,
                1  // from_worker - prevents re-delegation
            );

            worker_request.stmt = stmt;
            worker_request.tail = tail;
            worker_request.result = rc;
        }

        worker_request.work_done = 1;
        pthread_cond_signal(&worker_cond_response);
        pthread_mutex_unlock(&worker_mutex);
    }

    LOG_INFO("WORKER: Thread exiting");
    return NULL;
}

int worker_init(void) {
    pthread_attr_t attr;
    if (pthread_attr_init(&attr) != 0) {
        LOG_ERROR("WORKER: Failed to init thread attributes");
        return -1;
    }

    // Set 8MB stack size
    if (pthread_attr_setstacksize(&attr, WORKER_STACK_SIZE) != 0) {
        LOG_ERROR("WORKER: Failed to set stack size");
        pthread_attr_destroy(&attr);
        return -1;
    }

    worker_running = 1;
    memset(&worker_request, 0, sizeof(worker_request));

    if (pthread_create(&worker_thread, &attr, worker_thread_func, NULL) != 0) {
        LOG_ERROR("WORKER: Failed to create thread");
        worker_running = 0;
        pthread_attr_destroy(&attr);
        return -1;
    }

    pthread_attr_destroy(&attr);
    LOG_INFO("WORKER: Initialized with %d MB stack", WORKER_STACK_SIZE / (1024*1024));
    return 0;
}

void worker_cleanup(void) {
    if (!worker_running) return;

    pthread_mutex_lock(&worker_mutex);
    worker_request.type = WORK_SHUTDOWN;
    worker_request.work_ready = 1;
    worker_running = 0;
    pthread_cond_signal(&worker_cond_request);
    pthread_mutex_unlock(&worker_mutex);

    pthread_join(worker_thread, NULL);
    LOG_INFO("WORKER: Cleaned up");
}

// Delegate prepare_v2 to worker thread (called when stack is low)
int delegate_prepare_to_worker(sqlite3 *db, const char *zSql, int nByte,
                               sqlite3_stmt **ppStmt, const char **pzTail) {
    if (!worker_running) {
        LOG_ERROR("WORKER: Not running, cannot delegate");
        return SQLITE_ERROR;
    }

    LOG_DEBUG("WORKER: Delegating query (%.100s)", zSql ? zSql : "NULL");

    pthread_mutex_lock(&worker_mutex);

    // Set up request
    worker_request.type = WORK_PREPARE_V2;
    worker_request.db = db;
    worker_request.zSql = zSql;
    worker_request.nByte = nByte;
    worker_request.stmt = NULL;
    worker_request.tail = NULL;
    worker_request.result = SQLITE_ERROR;
    worker_request.work_done = 0;
    worker_request.work_ready = 1;

    // Signal worker
    pthread_cond_signal(&worker_cond_request);

    // Wait for response
    while (!worker_request.work_done) {
        pthread_cond_wait(&worker_cond_response, &worker_mutex);
    }

    // Get results
    if (ppStmt) *ppStmt = worker_request.stmt;
    if (pzTail) *pzTail = worker_request.tail;
    int result = worker_request.result;

    pthread_mutex_unlock(&worker_mutex);

    LOG_DEBUG("WORKER: Delegation complete, rc=%d", result);
    return result;
}

// ============================================================================
// fishhook Rebinding Setup
// ============================================================================

static void setup_fishhook_rebindings(void) {
    fprintf(stderr, "[SHIM_INIT] Setting up fishhook rebindings...\n");

    // Define all rebindings - fishhook will store the original function pointers
    struct rebinding rebindings[] = {
        // Open/Close
        {"sqlite3_open", my_sqlite3_open, (void**)&orig_sqlite3_open},
        {"sqlite3_open_v2", my_sqlite3_open_v2, (void**)&orig_sqlite3_open_v2},
        {"sqlite3_close", my_sqlite3_close, (void**)&orig_sqlite3_close},
        {"sqlite3_close_v2", my_sqlite3_close_v2, (void**)&orig_sqlite3_close_v2},

        // Exec
        {"sqlite3_exec", my_sqlite3_exec, (void**)&orig_sqlite3_exec},
        {"sqlite3_get_table", my_sqlite3_get_table, (void**)&orig_sqlite3_get_table},

        // Metadata
        {"sqlite3_changes", my_sqlite3_changes, (void**)&orig_sqlite3_changes},
        {"sqlite3_changes64", my_sqlite3_changes64, (void**)&orig_sqlite3_changes64},
        {"sqlite3_last_insert_rowid", my_sqlite3_last_insert_rowid, (void**)&orig_sqlite3_last_insert_rowid},
        {"sqlite3_errmsg", my_sqlite3_errmsg, (void**)&orig_sqlite3_errmsg},
        {"sqlite3_errcode", my_sqlite3_errcode, (void**)&orig_sqlite3_errcode},
        {"sqlite3_extended_errcode", my_sqlite3_extended_errcode, (void**)&orig_sqlite3_extended_errcode},

        // Prepare
        {"sqlite3_prepare", my_sqlite3_prepare, (void**)&orig_sqlite3_prepare},
        {"sqlite3_prepare_v2", my_sqlite3_prepare_v2, (void**)&orig_sqlite3_prepare_v2},
        {"sqlite3_prepare_v3", my_sqlite3_prepare_v3, (void**)&orig_sqlite3_prepare_v3},
        {"sqlite3_prepare16_v2", my_sqlite3_prepare16_v2, (void**)&orig_sqlite3_prepare16_v2},

        // Bind
        {"sqlite3_bind_int", my_sqlite3_bind_int, (void**)&orig_sqlite3_bind_int},
        {"sqlite3_bind_int64", my_sqlite3_bind_int64, (void**)&orig_sqlite3_bind_int64},
        {"sqlite3_bind_double", my_sqlite3_bind_double, (void**)&orig_sqlite3_bind_double},
        {"sqlite3_bind_text", my_sqlite3_bind_text, (void**)&orig_sqlite3_bind_text},
        {"sqlite3_bind_text64", my_sqlite3_bind_text64, (void**)&orig_sqlite3_bind_text64},
        {"sqlite3_bind_blob", my_sqlite3_bind_blob, (void**)&orig_sqlite3_bind_blob},
        {"sqlite3_bind_blob64", my_sqlite3_bind_blob64, (void**)&orig_sqlite3_bind_blob64},
        {"sqlite3_bind_value", my_sqlite3_bind_value, (void**)&orig_sqlite3_bind_value},
        {"sqlite3_bind_null", my_sqlite3_bind_null, (void**)&orig_sqlite3_bind_null},

        // Step/Reset/Finalize
        {"sqlite3_step", my_sqlite3_step, (void**)&orig_sqlite3_step},
        {"sqlite3_reset", my_sqlite3_reset, (void**)&orig_sqlite3_reset},
        {"sqlite3_finalize", my_sqlite3_finalize, (void**)&orig_sqlite3_finalize},
        {"sqlite3_clear_bindings", my_sqlite3_clear_bindings, (void**)&orig_sqlite3_clear_bindings},

        // Column access
        {"sqlite3_column_count", my_sqlite3_column_count, (void**)&orig_sqlite3_column_count},
        {"sqlite3_column_type", my_sqlite3_column_type, (void**)&orig_sqlite3_column_type},
        {"sqlite3_column_int", my_sqlite3_column_int, (void**)&orig_sqlite3_column_int},
        {"sqlite3_column_int64", my_sqlite3_column_int64, (void**)&orig_sqlite3_column_int64},
        {"sqlite3_column_double", my_sqlite3_column_double, (void**)&orig_sqlite3_column_double},
        {"sqlite3_column_text", my_sqlite3_column_text, (void**)&orig_sqlite3_column_text},
        {"sqlite3_column_blob", my_sqlite3_column_blob, (void**)&orig_sqlite3_column_blob},
        {"sqlite3_column_bytes", my_sqlite3_column_bytes, (void**)&orig_sqlite3_column_bytes},
        {"sqlite3_column_name", my_sqlite3_column_name, (void**)&orig_sqlite3_column_name},
        // DISABLED: Let SOCI call original decltype which returns NULL, forcing fallback to column_type()
        // {"sqlite3_column_decltype", my_sqlite3_column_decltype, (void**)&orig_sqlite3_column_decltype},
        {"sqlite3_column_value", my_sqlite3_column_value, (void**)&orig_sqlite3_column_value},
        {"sqlite3_data_count", my_sqlite3_data_count, (void**)&orig_sqlite3_data_count},

        // Value access
        {"sqlite3_value_type", my_sqlite3_value_type, (void**)&orig_sqlite3_value_type},
        {"sqlite3_value_text", my_sqlite3_value_text, (void**)&orig_sqlite3_value_text},
        {"sqlite3_value_int", my_sqlite3_value_int, (void**)&orig_sqlite3_value_int},
        {"sqlite3_value_int64", my_sqlite3_value_int64, (void**)&orig_sqlite3_value_int64},
        {"sqlite3_value_double", my_sqlite3_value_double, (void**)&orig_sqlite3_value_double},
        {"sqlite3_value_bytes", my_sqlite3_value_bytes, (void**)&orig_sqlite3_value_bytes},
        {"sqlite3_value_blob", my_sqlite3_value_blob, (void**)&orig_sqlite3_value_blob},

        // Collation
        {"sqlite3_create_collation", my_sqlite3_create_collation, (void**)&orig_sqlite3_create_collation},
        {"sqlite3_create_collation_v2", my_sqlite3_create_collation_v2, (void**)&orig_sqlite3_create_collation_v2},

        // Memory and statement info
        {"sqlite3_free", my_sqlite3_free, (void**)&orig_sqlite3_free},
        {"sqlite3_malloc", my_sqlite3_malloc, (void**)&orig_sqlite3_malloc},
        {"sqlite3_db_handle", my_sqlite3_db_handle, (void**)&orig_sqlite3_db_handle},
        {"sqlite3_sql", my_sqlite3_sql, (void**)&orig_sqlite3_sql},
        {"sqlite3_expanded_sql", my_sqlite3_expanded_sql, (void**)&orig_sqlite3_expanded_sql},
        {"sqlite3_bind_parameter_count", my_sqlite3_bind_parameter_count, (void**)&orig_sqlite3_bind_parameter_count},
        {"sqlite3_bind_parameter_index", my_sqlite3_bind_parameter_index, (void**)&orig_sqlite3_bind_parameter_index},
        {"sqlite3_stmt_readonly", my_sqlite3_stmt_readonly, (void**)&orig_sqlite3_stmt_readonly},
        {"sqlite3_stmt_busy", my_sqlite3_stmt_busy, (void**)&orig_sqlite3_stmt_busy},
        {"sqlite3_stmt_status", my_sqlite3_stmt_status, (void**)&orig_sqlite3_stmt_status},
        {"sqlite3_bind_parameter_name", my_sqlite3_bind_parameter_name, (void**)&orig_sqlite3_bind_parameter_name},
        
        // C++ exception interception DISABLED - causes crash regardless of noreturn
        // {"__cxa_throw", (void*)my_cxa_throw, (void**)&orig_cxa_throw},
    };

    int count = sizeof(rebindings) / sizeof(rebindings[0]);
    int result = rebind_symbols(rebindings, count);

    if (result == 0) {
        fprintf(stderr, "[SHIM_INIT] fishhook rebind_symbols succeeded for %d functions\n", count);
        
        // Report exception interception status
        if (orig_cxa_throw) {
            fprintf(stderr, "[SHIM_INIT] C++ exception interception ENABLED (orig_cxa_throw = %p)\n", (void*)orig_cxa_throw);
        } else {
            fprintf(stderr, "[SHIM_INIT] WARNING: C++ exception interception NOT available\n");
        }

        // Set up aliases for backward compatibility
        real_sqlite3_prepare_v2 = orig_sqlite3_prepare_v2;
        real_sqlite3_errmsg = orig_sqlite3_errmsg;
        real_sqlite3_errcode = orig_sqlite3_errcode;

        // Verify critical functions were bound
        if (orig_sqlite3_open) {
            fprintf(stderr, "[SHIM_INIT] orig_sqlite3_open = %p\n", (void*)orig_sqlite3_open);
        } else {
            fprintf(stderr, "[SHIM_INIT] WARNING: orig_sqlite3_open is NULL!\n");
        }
        if (orig_sqlite3_prepare_v2) {
            fprintf(stderr, "[SHIM_INIT] orig_sqlite3_prepare_v2 = %p\n", (void*)orig_sqlite3_prepare_v2);
        } else {
            fprintf(stderr, "[SHIM_INIT] WARNING: orig_sqlite3_prepare_v2 is NULL!\n");
        }
    } else {
        fprintf(stderr, "[SHIM_INIT] ERROR: fishhook rebind_symbols failed with code %d\n", result);
    }
}

// ============================================================================
// Fork Handlers - Critical for Connection Pool Safety
// ============================================================================

// Called in PARENT before fork()
static void atfork_prepare(void) {
    // No action needed - parent continues with its connections
}

// Called in PARENT after fork()
static void atfork_parent(void) {
    // No action needed - parent keeps its connections
}

// Called in CHILD after fork()
static void atfork_child(void) {
    // CRITICAL: Child process must NOT use parent's PostgreSQL connections
    // The PostgreSQL protocol is not fork-safe - sockets are in the middle of I/O
    // This prevents the crash where parent's query on slot N hangs while child
    // initializes and creates connections in adjacent slots

    // Use fprintf since logging may not be initialized yet
    fprintf(stderr, "[FORK_CHILD] Cleaning up inherited connection pool (child PID %d)\n", getpid());
    fflush(stderr);

    // Clear exception context - parent's pointers are not valid in child
    last_query_being_processed = NULL;
    last_column_being_accessed = NULL;
    global_value_type_calls = 0;
    global_column_type_calls = 0;
    
    // Reset exception tracking for child process
    total_exception_count = 0;
    exception_type_count = 0;
    
    // Reset symbol verification - child needs to re-verify
    // (symbols may need re-resolution in child's address space)
    symbols_verified = 0;

    // Call pg_client cleanup function to clear pool state
    extern void pg_pool_cleanup_after_fork(void);
    pg_pool_cleanup_after_fork();
    
    // Reset logging to prevent mutex deadlock
    // After fork, the child inherits parent's mutex state which may be locked
    extern void pg_logging_reset_after_fork(void);
    pg_logging_reset_after_fork();

    fprintf(stderr, "[FORK_CHILD] Pool and logging reset, child will reinitialize\n");
    fflush(stderr);
}

// ============================================================================
// Constructor/Destructor
// ============================================================================

// Track if shim was initialized (separate from shim_initialized which tracks completion)
static pid_t shim_init_pid = 0;

__attribute__((constructor))
static void shim_init(void) {
    // Write to stderr first to verify constructor runs
    fprintf(stderr, "[SHIM_INIT] Constructor starting...\n");
    fflush(stderr);
    
    // Detect if we're in a forked child process
    // pthread_once doesn't reset after fork, so we need to detect this
    pid_t current_pid = getpid();
    if (shim_init_pid != 0 && shim_init_pid != current_pid) {
        fprintf(stderr, "[SHIM_INIT] Detected fork (parent PID %d, our PID %d) - resetting state\n",
                shim_init_pid, current_pid);
        fflush(stderr);
        // We're in a forked child - clear state and let initialization happen fresh
        shim_initialized = 0;
        // Reset the global context
        last_query_being_processed = NULL;
        last_column_being_accessed = NULL;
        global_value_type_calls = 0;
        global_column_type_calls = 0;
        total_exception_count = 0;
        exception_type_count = 0;
    }
    shim_init_pid = current_pid;

    // Install signal handlers for crash debugging (only in debug mode)
    #ifdef DEBUG
    signal(SIGSEGV, signal_handler);
    signal(SIGABRT, signal_handler);
    signal(SIGBUS, signal_handler);
    signal(SIGFPE, signal_handler);
    signal(SIGILL, signal_handler);
    #endif

    // CRITICAL: Install fork handlers BEFORE any PostgreSQL connections are made
    // This ensures child processes don't inherit parent's active connections
    pthread_atfork(atfork_prepare, atfork_parent, atfork_child);
    fprintf(stderr, "[SHIM_INIT] Registered pthread_atfork handlers for connection pool safety\n");
    fflush(stderr);

    pg_logging_init();
    LOG_INFO("=== Plex PostgreSQL Interpose Shim loaded ===");
    LOG_INFO("SHIM_CONSTRUCTOR: Initialization complete");

    // Also write to stderr after logging init
    fprintf(stderr, "[SHIM_INIT] Logging initialized\n");
    fflush(stderr);

    // Use fishhook to rebind SQLite symbols at runtime
    // This works with modular compilation (unlike DYLD_INTERPOSE)
    setup_fishhook_rebindings();

    // Fallback: Load SQLite library directly for functions that fishhook might miss
    const char *sqlite_paths[] = {
        "/Applications/Plex Media Server.app/Contents/Frameworks/libsqlite3_orig.dylib",
        "/Applications/Plex Media Server.app/Contents/Frameworks/libsqlite3.dylib",
        "/usr/lib/libsqlite3.dylib",
        NULL
    };

    for (int i = 0; sqlite_paths[i] != NULL && sqlite_handle == NULL; i++) {
        sqlite_handle = dlopen(sqlite_paths[i], RTLD_LAZY | RTLD_LOCAL);
        if (sqlite_handle) {
            fprintf(stderr, "[SHIM_INIT] Loaded SQLite fallback from: %s\n", sqlite_paths[i]);
            break;
        }
    }

    // CRITICAL FIX: If fishhook didn't set up ANY pointers, use dlsym as fallback
    // This happens when fishhook returns success but fails to actually rebind symbols
    // We need to populate BOTH real_* (for recursion prevention) AND orig_* (for actual calls)
    if (sqlite_handle && (!real_sqlite3_prepare_v2 || !orig_sqlite3_prepare_v2)) {
        fprintf(stderr, "[SHIM_INIT] Fishhook incomplete, using dlsym fallback for ALL functions\n");

        // Recursion prevention pointers (used in prepare module)
        real_sqlite3_prepare_v2 = dlsym(sqlite_handle, "sqlite3_prepare_v2");
        real_sqlite3_errmsg = dlsym(sqlite_handle, "sqlite3_errmsg");
        real_sqlite3_errcode = dlsym(sqlite_handle, "sqlite3_errcode");

        // CRITICAL: Also populate orig_* pointers that are actually called!
        if (!orig_sqlite3_open) orig_sqlite3_open = dlsym(sqlite_handle, "sqlite3_open");
        if (!orig_sqlite3_open_v2) orig_sqlite3_open_v2 = dlsym(sqlite_handle, "sqlite3_open_v2");
        if (!orig_sqlite3_close) orig_sqlite3_close = dlsym(sqlite_handle, "sqlite3_close");
        if (!orig_sqlite3_close_v2) orig_sqlite3_close_v2 = dlsym(sqlite_handle, "sqlite3_close_v2");
        if (!orig_sqlite3_exec) orig_sqlite3_exec = dlsym(sqlite_handle, "sqlite3_exec");
        if (!orig_sqlite3_get_table) orig_sqlite3_get_table = dlsym(sqlite_handle, "sqlite3_get_table");

        if (!orig_sqlite3_changes) orig_sqlite3_changes = dlsym(sqlite_handle, "sqlite3_changes");
        if (!orig_sqlite3_changes64) orig_sqlite3_changes64 = dlsym(sqlite_handle, "sqlite3_changes64");
        if (!orig_sqlite3_last_insert_rowid) orig_sqlite3_last_insert_rowid = dlsym(sqlite_handle, "sqlite3_last_insert_rowid");
        if (!orig_sqlite3_errmsg) orig_sqlite3_errmsg = dlsym(sqlite_handle, "sqlite3_errmsg");
        if (!orig_sqlite3_errcode) orig_sqlite3_errcode = dlsym(sqlite_handle, "sqlite3_errcode");
        if (!orig_sqlite3_extended_errcode) orig_sqlite3_extended_errcode = dlsym(sqlite_handle, "sqlite3_extended_errcode");

        if (!orig_sqlite3_prepare) orig_sqlite3_prepare = dlsym(sqlite_handle, "sqlite3_prepare");
        if (!orig_sqlite3_prepare_v2) orig_sqlite3_prepare_v2 = dlsym(sqlite_handle, "sqlite3_prepare_v2");
        if (!orig_sqlite3_prepare_v3) orig_sqlite3_prepare_v3 = dlsym(sqlite_handle, "sqlite3_prepare_v3");
        if (!orig_sqlite3_prepare16_v2) orig_sqlite3_prepare16_v2 = dlsym(sqlite_handle, "sqlite3_prepare16_v2");

        if (!orig_sqlite3_bind_int) orig_sqlite3_bind_int = dlsym(sqlite_handle, "sqlite3_bind_int");
        if (!orig_sqlite3_bind_int64) orig_sqlite3_bind_int64 = dlsym(sqlite_handle, "sqlite3_bind_int64");
        if (!orig_sqlite3_bind_double) orig_sqlite3_bind_double = dlsym(sqlite_handle, "sqlite3_bind_double");
        if (!orig_sqlite3_bind_text) orig_sqlite3_bind_text = dlsym(sqlite_handle, "sqlite3_bind_text");
        if (!orig_sqlite3_bind_text64) orig_sqlite3_bind_text64 = dlsym(sqlite_handle, "sqlite3_bind_text64");
        if (!orig_sqlite3_bind_blob) orig_sqlite3_bind_blob = dlsym(sqlite_handle, "sqlite3_bind_blob");
        if (!orig_sqlite3_bind_blob64) orig_sqlite3_bind_blob64 = dlsym(sqlite_handle, "sqlite3_bind_blob64");
        if (!orig_sqlite3_bind_value) orig_sqlite3_bind_value = dlsym(sqlite_handle, "sqlite3_bind_value");
        if (!orig_sqlite3_bind_null) orig_sqlite3_bind_null = dlsym(sqlite_handle, "sqlite3_bind_null");

        if (!orig_sqlite3_step) orig_sqlite3_step = dlsym(sqlite_handle, "sqlite3_step");
        if (!orig_sqlite3_reset) orig_sqlite3_reset = dlsym(sqlite_handle, "sqlite3_reset");
        if (!orig_sqlite3_finalize) orig_sqlite3_finalize = dlsym(sqlite_handle, "sqlite3_finalize");
        if (!orig_sqlite3_clear_bindings) orig_sqlite3_clear_bindings = dlsym(sqlite_handle, "sqlite3_clear_bindings");

        if (!orig_sqlite3_column_count) orig_sqlite3_column_count = dlsym(sqlite_handle, "sqlite3_column_count");
        if (!orig_sqlite3_column_type) orig_sqlite3_column_type = dlsym(sqlite_handle, "sqlite3_column_type");
        if (!orig_sqlite3_column_int) orig_sqlite3_column_int = dlsym(sqlite_handle, "sqlite3_column_int");
        if (!orig_sqlite3_column_int64) orig_sqlite3_column_int64 = dlsym(sqlite_handle, "sqlite3_column_int64");
        if (!orig_sqlite3_column_double) orig_sqlite3_column_double = dlsym(sqlite_handle, "sqlite3_column_double");
        if (!orig_sqlite3_column_text) orig_sqlite3_column_text = dlsym(sqlite_handle, "sqlite3_column_text");
        if (!orig_sqlite3_column_blob) orig_sqlite3_column_blob = dlsym(sqlite_handle, "sqlite3_column_blob");
        if (!orig_sqlite3_column_bytes) orig_sqlite3_column_bytes = dlsym(sqlite_handle, "sqlite3_column_bytes");
        if (!orig_sqlite3_column_name) orig_sqlite3_column_name = dlsym(sqlite_handle, "sqlite3_column_name");
        if (!orig_sqlite3_column_decltype) orig_sqlite3_column_decltype = dlsym(sqlite_handle, "sqlite3_column_decltype");
        if (!orig_sqlite3_column_value) orig_sqlite3_column_value = dlsym(sqlite_handle, "sqlite3_column_value");
        if (!orig_sqlite3_data_count) orig_sqlite3_data_count = dlsym(sqlite_handle, "sqlite3_data_count");
        if (!orig_sqlite3_db_handle) orig_sqlite3_db_handle = dlsym(sqlite_handle, "sqlite3_db_handle");
        if (!orig_sqlite3_expanded_sql) orig_sqlite3_expanded_sql = dlsym(sqlite_handle, "sqlite3_expanded_sql");
        if (!orig_sqlite3_sql) orig_sqlite3_sql = dlsym(sqlite_handle, "sqlite3_sql");
        if (!orig_sqlite3_free) orig_sqlite3_free = dlsym(sqlite_handle, "sqlite3_free");
        if (!orig_sqlite3_bind_parameter_name) orig_sqlite3_bind_parameter_name = dlsym(sqlite_handle, "sqlite3_bind_parameter_name");

        if (!orig_sqlite3_value_type) orig_sqlite3_value_type = dlsym(sqlite_handle, "sqlite3_value_type");
        if (!orig_sqlite3_value_text) orig_sqlite3_value_text = dlsym(sqlite_handle, "sqlite3_value_text");
        if (!orig_sqlite3_value_int) orig_sqlite3_value_int = dlsym(sqlite_handle, "sqlite3_value_int");
        if (!orig_sqlite3_value_int64) orig_sqlite3_value_int64 = dlsym(sqlite_handle, "sqlite3_value_int64");
        if (!orig_sqlite3_value_double) orig_sqlite3_value_double = dlsym(sqlite_handle, "sqlite3_value_double");
        if (!orig_sqlite3_value_bytes) orig_sqlite3_value_bytes = dlsym(sqlite_handle, "sqlite3_value_bytes");
        if (!orig_sqlite3_value_blob) orig_sqlite3_value_blob = dlsym(sqlite_handle, "sqlite3_value_blob");

        if (!orig_sqlite3_create_collation) orig_sqlite3_create_collation = dlsym(sqlite_handle, "sqlite3_create_collation");
        if (!orig_sqlite3_create_collation_v2) orig_sqlite3_create_collation_v2 = dlsym(sqlite_handle, "sqlite3_create_collation_v2");

        if (!orig_sqlite3_malloc) orig_sqlite3_malloc = dlsym(sqlite_handle, "sqlite3_malloc");
        if (!orig_sqlite3_bind_parameter_count) orig_sqlite3_bind_parameter_count = dlsym(sqlite_handle, "sqlite3_bind_parameter_count");
        if (!orig_sqlite3_bind_parameter_index) orig_sqlite3_bind_parameter_index = dlsym(sqlite_handle, "sqlite3_bind_parameter_index");
        if (!orig_sqlite3_stmt_readonly) orig_sqlite3_stmt_readonly = dlsym(sqlite_handle, "sqlite3_stmt_readonly");
        if (!orig_sqlite3_stmt_busy) orig_sqlite3_stmt_busy = dlsym(sqlite_handle, "sqlite3_stmt_busy");
        if (!orig_sqlite3_stmt_status) orig_sqlite3_stmt_status = dlsym(sqlite_handle, "sqlite3_stmt_status");

        fprintf(stderr, "[SHIM_INIT] dlsym fallback complete - orig_sqlite3_prepare_v2 = %p\n", (void*)orig_sqlite3_prepare_v2);
    }

    LOG_INFO("Resolved real SQLite functions for recursion prevention");

    pg_config_init();
    pg_client_init();
    pg_statement_init();
    pg_query_cache_init();  // Initialize query result cache
    sql_translator_init();

    // Start worker thread with 8MB stack for heavy queries
    worker_init();

    shim_initialized = 1;

    fprintf(stderr, "[SHIM_INIT] All modules initialized\n");
    fflush(stderr);
    
    // ========================================================================
    // TIMING FIX: dyld symbol resolution race condition
    // ========================================================================
    // 
    // PROBLEM: When Plex forks child processes (Scanner, Transcoder, etc.),
    // the child inherits our dylib via DYLD_INSERT_LIBRARIES. However, there's
    // a race condition where the child's constructor runs before dyld has
    // finished resolving all symbols (orig_sqlite3_* function pointers).
    // This causes crashes ~80% of the time.
    //
    // EVIDENCE:
    // - Adding 200ms delay: fixes crash 100%
    // - DYLD_PRINT_BINDINGS=1 (slows dyld): fixes crash 100%
    // - Running under lldb: process hangs (debugger delays prevent crash)
    //
    // SOLUTION: Default-on delay of 200ms to ensure dyld completes symbol
    // resolution before we return from constructor. Can be tuned or disabled.
    //
    // Environment variables:
    //   PLEX_PG_INIT_DELAY_MS=N  - Custom delay in milliseconds (overrides default)
    //   PLEX_PG_NO_INIT_DELAY=1  - Disable delay entirely (for debugging)
    // ========================================================================
    
    // Check if delay is explicitly disabled
    const char *no_delay = getenv("PLEX_PG_NO_INIT_DELAY");
    if (no_delay && (no_delay[0] == '1' || no_delay[0] == 'y' || no_delay[0] == 'Y')) {
        fprintf(stderr, "[SHIM_INIT] Init delay DISABLED via PLEX_PG_NO_INIT_DELAY\n");
        fflush(stderr);
    } else {
        // Default 200ms delay, can be overridden
        const char *delay_str = getenv("PLEX_PG_INIT_DELAY_MS");
        int delay_ms = delay_str ? atoi(delay_str) : 200;  // Default 200ms
        
        if (delay_ms > 0) {
            fprintf(stderr, "[SHIM_INIT] Waiting %d ms for dyld symbol resolution (PID %d)...\n", 
                    delay_ms, current_pid);
            fflush(stderr);
            
            // Use memory barrier to ensure all writes are visible
            __sync_synchronize();
            
            usleep(delay_ms * 1000);
            
            // Another barrier after delay
            __sync_synchronize();
            
            // Verify critical function pointers are valid
            if (!orig_sqlite3_open || !orig_sqlite3_prepare_v2 || !orig_sqlite3_step) {
                fprintf(stderr, "[SHIM_INIT] WARNING: Critical function pointers still NULL after delay!\n");
                fprintf(stderr, "[SHIM_INIT]   orig_sqlite3_open = %p\n", (void*)orig_sqlite3_open);
                fprintf(stderr, "[SHIM_INIT]   orig_sqlite3_prepare_v2 = %p\n", (void*)orig_sqlite3_prepare_v2);
                fprintf(stderr, "[SHIM_INIT]   orig_sqlite3_step = %p\n", (void*)orig_sqlite3_step);
                fflush(stderr);
            }
        }
    }
    
    fprintf(stderr, "[SHIM_INIT] Constructor complete (PID %d)\n", current_pid);
    fflush(stderr);
}

__attribute__((destructor))
static void shim_cleanup(void) {
    LOG_INFO("=== Plex PostgreSQL Interpose Shim unloading ===");
    worker_cleanup();  // Stop worker thread first
    pg_statement_cleanup();
    pg_client_cleanup();
    sql_translator_cleanup();
    pg_logging_cleanup();
}
