/*
 * Plex PostgreSQL Interposing Shim - Core Module (Linux)
 *
 * This is the Linux version of the core module containing:
 * - Global shared state definitions
 * - Original SQLite function pointers (via dlsym RTLD_NEXT)
 * - Worker thread implementation
 * - Constructor/destructor
 * - LD_PRELOAD wrapper functions that call my_sqlite3_* from shared modules
 */

#define _GNU_SOURCE
#include "db_interpose.h"
#include "pg_query_cache.h"
#include "sql_translator.h"
#include <signal.h>
#include <dlfcn.h>

// execinfo.h is glibc-specific, not available on musl
#ifdef __GLIBC__
#include <execinfo.h>
#define HAS_BACKTRACE 1
#else
#define HAS_BACKTRACE 0
#endif

// ============================================================================
// C++ Exception Interception (C-compatible)
// ============================================================================
// Intercept __cxa_throw to catch ALL C++ exceptions and log them with backtraces.
// This is essential for debugging std::bad_cast and other exceptions.

// Original __cxa_throw function pointer (noreturn attribute removed for cast compatibility)
static void (*orig_cxa_throw)(void*, void*, void(*)(void*)) = NULL;

// __cxa_demangle function pointer (loaded via dlsym)
static char* (*cxa_demangle_fn)(const char*, char*, size_t*, int*) = NULL;

// Counter for exception logging (to avoid infinite loops)
static __thread int in_exception_handler = 0;

// Global tracking of last query being processed (for exception context)
// NOT thread-local because musl TLS doesn't work reliably across exception boundaries
const char * volatile last_query_being_processed = NULL;
const char * volatile last_column_being_accessed = NULL;

// Global counters for debugging (NOT thread-local so visible across threads)
volatile long global_value_type_calls = 0;
volatile long global_column_type_calls = 0;

// C++ std::type_info has a virtual method name() - we access it via the vtable
// The first pointer in the object is the vtable, and name() is typically the first virtual method
typedef struct {
    void **vtable;  // Virtual table pointer
} fake_type_info;

static const char* get_type_name(void *tinfo) {
    if (!tinfo) return "unknown";
    // The name is stored after the vtable pointer in most implementations
    // For Itanium ABI (used by GCC/Clang), the mangled name follows the vtable ptr
    fake_type_info *ti = (fake_type_info *)tinfo;
    // The name pointer is at offset 1 (after vtable)
    const char **name_ptr = (const char **)((char *)ti + sizeof(void*));
    return *name_ptr ? *name_ptr : "unknown";
}

// ============================================================================
// Robust Stack Trace Analyzer
// ============================================================================
// Automatically resolves stack addresses to function names using:
// 1. /proc/self/maps for library identification
// 2. addr2line for symbol resolution (if available)
// 3. dladdr as fallback for dynamic symbols

#define MAX_STACK_FRAMES 32
#define MAX_MAPS_ENTRIES 256

// Memory map entry structure
typedef struct {
    unsigned long start;
    unsigned long end;
    unsigned long offset;  // File offset for the mapping
    char perms[8];
    char path[256];
} map_entry_t;

// Cached memory map (reloaded on each stack trace)
static map_entry_t memory_map[MAX_MAPS_ENTRIES];
static int memory_map_count = 0;

// Parse hex number from string, returns pointer to next char after number
static const char* parse_hex(const char *s, unsigned long *out) {
    unsigned long val = 0;
    while (*s) {
        char c = *s;
        if (c >= '0' && c <= '9') {
            val = (val << 4) | (c - '0');
        } else if (c >= 'a' && c <= 'f') {
            val = (val << 4) | (c - 'a' + 10);
        } else if (c >= 'A' && c <= 'F') {
            val = (val << 4) | (c - 'A' + 10);
        } else {
            break;
        }
        s++;
    }
    *out = val;
    return s;
}

// Skip whitespace
static const char* skip_ws(const char *s) {
    while (*s == ' ' || *s == '\t') s++;
    return s;
}

// Load memory map from /proc/self/maps (using manual parsing to avoid sscanf/glibc issues)
static void load_memory_map(void) {
    memory_map_count = 0;

    FILE *maps = fopen("/proc/self/maps", "r");
    if (!maps) return;

    char line[512];
    while (fgets(line, sizeof(line), maps) && memory_map_count < MAX_MAPS_ENTRIES) {
        map_entry_t *e = &memory_map[memory_map_count];

        // Parse: start-end perms offset dev inode path
        // Example: ffff80cf3000-ffff818ce000 r-xp 005c3000 00:50 252769 /usr/lib/plexmediaserver/Plex Media Server
        const char *p = line;

        // Parse start address
        unsigned long start = 0;
        p = parse_hex(p, &start);
        if (*p != '-') continue;  // Invalid format
        p++;

        // Parse end address
        unsigned long end = 0;
        p = parse_hex(p, &end);
        p = skip_ws(p);

        // Parse permissions (4 chars like "r-xp")
        char perms[8] = {0};
        int pi = 0;
        while (*p && *p != ' ' && pi < 7) {
            perms[pi++] = *p++;
        }
        p = skip_ws(p);

        // Parse offset
        unsigned long offset = 0;
        p = parse_hex(p, &offset);
        p = skip_ws(p);

        // Skip dev (xx:xx)
        while (*p && *p != ' ') p++;
        p = skip_ws(p);

        // Skip inode
        while (*p && *p != ' ') p++;
        p = skip_ws(p);

        // Rest is path (may be empty)
        char path[256] = {0};
        if (*p && *p != '\n') {
            int plen = 0;
            while (*p && *p != '\n' && plen < 255) {
                path[plen++] = *p++;
            }
            path[plen] = '\0';
        }

        // Store entry
        e->start = start;
        e->end = end;
        e->offset = offset;
        strncpy(e->perms, perms, sizeof(e->perms) - 1);
        if (path[0]) {
            strncpy(e->path, path, sizeof(e->path) - 1);
        } else {
            strcpy(e->path, "[anonymous]");
        }
        memory_map_count++;
    }

    fclose(maps);
}

// Find which library contains an address
static map_entry_t* find_map_entry(unsigned long addr) {
    for (int i = 0; i < memory_map_count; i++) {
        if (addr >= memory_map[i].start && addr < memory_map[i].end) {
            return &memory_map[i];
        }
    }
    return NULL;
}

// Get just the filename from a path
static const char* get_basename(const char *path) {
    const char *base = strrchr(path, '/');
    return base ? base + 1 : path;
}

// Try to resolve symbol using addr2line (via popen)
static int resolve_with_addr2line(const char *path, unsigned long offset,
                                   char *func_out, size_t func_size,
                                   char *file_out, size_t file_size) {
    func_out[0] = '\0';
    file_out[0] = '\0';

    // Build addr2line command
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "addr2line -f -e '%s' 0x%lx 2>/dev/null", path, offset);

    FILE *p = popen(cmd, "r");
    if (!p) return 0;

    // First line is function name
    if (fgets(func_out, func_size, p)) {
        // Remove trailing newline
        size_t len = strlen(func_out);
        if (len > 0 && func_out[len-1] == '\n') func_out[len-1] = '\0';

        // Check if it's a valid result (not "??")
        if (strcmp(func_out, "??") == 0) {
            func_out[0] = '\0';
        }
    }

    // Second line is file:line
    if (fgets(file_out, file_size, p)) {
        size_t len = strlen(file_out);
        if (len > 0 && file_out[len-1] == '\n') file_out[len-1] = '\0';

        // Check if it's a valid result (not "??:0" or "??:?")
        if (strncmp(file_out, "??:", 3) == 0) {
            file_out[0] = '\0';
        }
    }

    pclose(p);
    return func_out[0] != '\0';
}

// Try to resolve symbol using dladdr (for dynamic symbols)
static int resolve_with_dladdr(void *addr, char *func_out, size_t func_size,
                                char *lib_out, size_t lib_size) {
    func_out[0] = '\0';
    lib_out[0] = '\0';

    Dl_info info;
    if (dladdr(addr, &info)) {
        if (info.dli_sname) {
            // Try to demangle C++ names
            if (cxa_demangle_fn) {
                int status = 0;
                char *demangled = cxa_demangle_fn(info.dli_sname, NULL, NULL, &status);
                if (demangled) {
                    strncpy(func_out, demangled, func_size - 1);
                    free(demangled);
                } else {
                    strncpy(func_out, info.dli_sname, func_size - 1);
                }
            } else {
                strncpy(func_out, info.dli_sname, func_size - 1);
            }
        }
        if (info.dli_fname) {
            strncpy(lib_out, get_basename(info.dli_fname), lib_size - 1);
        }
        return func_out[0] != '\0';
    }
    return 0;
}

// Collect stack frames using frame pointer walking
static int collect_stack_frames(void **frames, int max_frames) {
    int depth = 0;

    // Method 1: Frame pointer walking (works on ARM64 with frame pointers)
    void **fp = __builtin_frame_address(0);

    // Safety: limit iterations and validate pointers
    int iterations = 0;
    while (fp && depth < max_frames && iterations < 100) {
        iterations++;

        // Validate frame pointer is in reasonable range
        if ((unsigned long)fp < 0x1000 || (unsigned long)fp > 0xffffffffffff) break;

        // On ARM64, return address is at fp+8 (LR saved after FP)
        // On x86_64, return address is at fp+8 (after saved RBP)
        void *ret_addr = NULL;

        // Try to safely read the return address
        // The layout is: [saved_fp][return_addr]
        #if defined(__aarch64__)
        ret_addr = *((void**)((char*)fp + 8));  // ARM64: LR at fp+8
        #elif defined(__x86_64__)
        ret_addr = *((void**)((char*)fp + 8));  // x86_64: return addr at rbp+8
        #else
        ret_addr = fp[1];  // Generic fallback
        #endif

        if (!ret_addr || (unsigned long)ret_addr < 0x1000) break;

        frames[depth++] = ret_addr;

        // Move to next frame
        void **next_fp = (void**)*fp;

        // Sanity checks
        if (!next_fp) break;
        if (next_fp <= fp) break;  // Stack grows down, FP should increase
        if ((unsigned long)next_fp - (unsigned long)fp > 0x100000) break;  // Max 1MB frame

        fp = next_fp;
    }

    return depth;
}

// Print a fully analyzed stack trace
static void print_analyzed_stack_trace(const char *reason) {
    void *frames[MAX_STACK_FRAMES];
    int depth;

    // Collect stack frames
    depth = collect_stack_frames(frames, MAX_STACK_FRAMES);

    if (depth == 0) {
        fprintf(stderr, "\n  [Stack trace unavailable - frame pointer walking failed]\n");
        return;
    }

    fprintf(stderr, "\n┌─────────────────────────────────────────────────────────────────────┐\n");
    fprintf(stderr, "│ STACK TRACE: %-55s │\n", reason ? reason : "Exception");
    fprintf(stderr, "├─────────────────────────────────────────────────────────────────────┤\n");

    // Load memory map for addr2line offset calculation
    load_memory_map();

    int frames_shown = 0;
    for (int i = 0; i < depth && frames_shown < 15; i++) {
        unsigned long addr = (unsigned long)frames[i];
        char func_name[256] = {0};
        char lib_name[256] = {0};
        char lib_display[30] = {0};
        int resolved = 0;

        // PRIMARY: Use dladdr - works reliably with runtime linker
        Dl_info info;
        if (dladdr(frames[i], &info)) {
            // Get library name
            if (info.dli_fname) {
                strncpy(lib_name, get_basename(info.dli_fname), sizeof(lib_name) - 1);
            }

            // Get symbol name (try to demangle)
            if (info.dli_sname) {
                if (cxa_demangle_fn) {
                    int status = 0;
                    char *demangled = cxa_demangle_fn(info.dli_sname, NULL, NULL, &status);
                    if (demangled) {
                        strncpy(func_name, demangled, sizeof(func_name) - 1);
                        free(demangled);
                    } else {
                        strncpy(func_name, info.dli_sname, sizeof(func_name) - 1);
                    }
                } else {
                    strncpy(func_name, info.dli_sname, sizeof(func_name) - 1);
                }
                resolved = 1;
            }

            // SECONDARY: Try addr2line for more detail if dladdr didn't give symbol
            if (!resolved && info.dli_fname) {
                // Calculate offset for addr2line
                map_entry_t *entry = find_map_entry(addr);
                if (entry) {
                    unsigned long file_offset = entry->offset + (addr - entry->start);
                    char file_loc[256];
                    resolve_with_addr2line(entry->path, file_offset,
                                          func_name, sizeof(func_name),
                                          file_loc, sizeof(file_loc));
                    if (func_name[0]) resolved = 1;
                }
            }
        }

        // If dladdr failed, try to find in memory map
        if (!lib_name[0]) {
            map_entry_t *entry = find_map_entry(addr);
            if (entry) {
                strncpy(lib_name, get_basename(entry->path), sizeof(lib_name) - 1);
            } else {
                strcpy(lib_name, "[unknown]");
            }
        }

        // Truncate display names
        snprintf(lib_display, sizeof(lib_display), "%.28s", lib_name);

        // Truncate long function names
        if (strlen(func_name) > 38) {
            func_name[35] = '.';
            func_name[36] = '.';
            func_name[37] = '.';
            func_name[38] = '\0';
        }

        if (func_name[0]) {
            fprintf(stderr, "│ %2d: %-28s  %-38s │\n",
                    frames_shown, lib_display, func_name);
        } else {
            // No symbol - show raw address
            fprintf(stderr, "│ %2d: %-28s  [%p]%*s│\n",
                    frames_shown, lib_display, frames[i], 20, "");
        }

        frames_shown++;
    }

    if (depth > 15) {
        fprintf(stderr, "│     ... %d more frames ...%*s│\n", depth - 15, 37, "");
    }

    fprintf(stderr, "└─────────────────────────────────────────────────────────────────────┘\n");
    fflush(stderr);
}

// Legacy function for backwards compatibility (redirects to new analyzer)
static void print_stack_addresses(void) {
    print_analyzed_stack_trace("Legacy call");
}

// ============================================================================
// Exception Tracking (per-type counters for smart stack trace printing)
// ============================================================================

#define MAX_EXCEPTION_TYPES 32
#define MAX_LOGGED_PER_TYPE 3
#define MAX_LOGGED_TOTAL 50

typedef struct {
    const char *type_name;      // Mangled type name (pointer comparison ok)
    int count;                  // Times this type was thrown
    int logged_with_trace;      // Whether we've logged a full trace for this type
} exception_type_tracker_t;

static exception_type_tracker_t exception_types[MAX_EXCEPTION_TYPES];
static int exception_type_count = 0;
static volatile int total_exception_count = 0;
static pthread_mutex_t exception_tracker_mutex = PTHREAD_MUTEX_INITIALIZER;

// Find or create tracker for an exception type
static exception_type_tracker_t* get_exception_tracker(const char *type_name) {
    pthread_mutex_lock(&exception_tracker_mutex);

    // Look for existing tracker
    for (int i = 0; i < exception_type_count; i++) {
        if (exception_types[i].type_name == type_name ||
            (exception_types[i].type_name && type_name &&
             strcmp(exception_types[i].type_name, type_name) == 0)) {
            exception_types[i].count++;
            pthread_mutex_unlock(&exception_tracker_mutex);
            return &exception_types[i];
        }
    }

    // Create new tracker if space available
    if (exception_type_count < MAX_EXCEPTION_TYPES) {
        exception_type_tracker_t *tracker = &exception_types[exception_type_count++];
        tracker->type_name = type_name;
        tracker->count = 1;
        tracker->logged_with_trace = 0;
        pthread_mutex_unlock(&exception_tracker_mutex);
        return tracker;
    }

    pthread_mutex_unlock(&exception_tracker_mutex);
    return NULL;  // No space for new types
}

// Our __cxa_throw interceptor - catches ALL C++ exceptions
void __cxa_throw(void *thrown_exception, void *tinfo, void (*dest)(void*)) {
    // Prevent recursion
    if (in_exception_handler) {
        if (orig_cxa_throw) {
            orig_cxa_throw(thrown_exception, tinfo, dest);
        }
        abort();  // Should never reach here
    }
    in_exception_handler = 1;

    int total_count = __sync_add_and_fetch(&total_exception_count, 1);

    // Get exception type info
    const char *type_name = get_type_name(tinfo);
    exception_type_tracker_t *tracker = get_exception_tracker(type_name);

    // Determine if we should log this exception
    int should_log = (total_count <= MAX_LOGGED_TOTAL) &&
                     (tracker == NULL || tracker->count <= MAX_LOGGED_PER_TYPE);
    int should_trace = tracker && !tracker->logged_with_trace;

    if (should_log) {
        // Try to demangle the type name
        char *demangled = NULL;
        if (!cxa_demangle_fn) {
            cxa_demangle_fn = (char* (*)(const char*, char*, size_t*, int*))dlsym(RTLD_DEFAULT, "__cxa_demangle");
        }
        if (cxa_demangle_fn && type_name) {
            int status = 0;
            demangled = cxa_demangle_fn(type_name, NULL, NULL, &status);
        }
        const char *readable_name = demangled ? demangled : type_name;

        // Capture context FIRST (before any calls that might fail)
        const char *ctx_query = last_query_being_processed;
        const char *ctx_column = last_column_being_accessed;
        long ctx_value_calls = global_value_type_calls;
        long ctx_column_calls = global_column_type_calls;

        // Determine if this is shim-related (our code was in the call path)
        int is_shim_related = (ctx_value_calls > 0 || ctx_column_calls > 0 || ctx_query != NULL);

        // Build a concise summary
        fprintf(stderr, "\n");
        fprintf(stderr, "╔══════════════════════════════════════════════════════════════════════╗\n");
        fprintf(stderr, "║ EXCEPTION: %-50s      #%-4d ║\n",
                readable_name ? readable_name : "unknown",
                tracker ? tracker->count : total_count);
        fprintf(stderr, "╠══════════════════════════════════════════════════════════════════════╣\n");

        if (is_shim_related) {
            fprintf(stderr, "║ Source: SHIM-RELATED (column_type=%ld, value_type=%ld)%*s║\n",
                    ctx_column_calls, ctx_value_calls, 16, "");
            if (ctx_query) {
                // Truncate long queries
                char query_snippet[55];
                snprintf(query_snippet, sizeof(query_snippet), "%.54s", ctx_query);
                fprintf(stderr, "║ Query: %-64s ║\n", query_snippet);
            }
            if (ctx_column) {
                fprintf(stderr, "║ Column: %-63s ║\n", ctx_column);
            }
        } else {
            fprintf(stderr, "║ Source: NOT SHIM-RELATED (external C++ code)%*s║\n", 26, "");
        }

        // Print stack trace for first occurrence of each exception type
        if (should_trace) {
            tracker->logged_with_trace = 1;
            fprintf(stderr, "╠══════════════════════════════════════════════════════════════════════╣\n");
            print_analyzed_stack_trace(readable_name);
        }

        fprintf(stderr, "╚══════════════════════════════════════════════════════════════════════╝\n");
        fflush(stderr);

        // Also log to our log file (concise version)
        LOG_ERROR("EXCEPTION #%d [%s]: %s | shim=%s | col_calls=%ld | query=%.60s",
                  total_count,
                  readable_name ? readable_name : "?",
                  should_trace ? "FIRST_OF_TYPE" : "repeat",
                  is_shim_related ? "YES" : "NO",
                  ctx_column_calls,
                  ctx_query ? ctx_query : "(none)");

        if (demangled) free(demangled);

    } else if (total_count == MAX_LOGGED_TOTAL + 1) {
        fprintf(stderr, "\n╔══════════════════════════════════════════════════════════════════════╗\n");
        fprintf(stderr, "║ [INFO] Exception logging throttled (>%d total). Check logs for stats. ║\n", MAX_LOGGED_TOTAL);
        fprintf(stderr, "╚══════════════════════════════════════════════════════════════════════╝\n");
        fflush(stderr);

        // Log exception type summary
        LOG_ERROR("=== EXCEPTION SUMMARY (throttling active) ===");
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

    // Call original __cxa_throw to continue exception handling
    if (!orig_cxa_throw) {
        orig_cxa_throw = (void (*)(void*, void*, void(*)(void*)))dlsym(RTLD_NEXT, "__cxa_throw");
    }
    if (orig_cxa_throw) {
        orig_cxa_throw(thrown_exception, tinfo, dest);
    }

    // Should never reach here
    abort();
}

// ============================================================================
// Crash/Exit Handler for Debugging
// ============================================================================
static void print_backtrace(const char *reason) {
    fprintf(stderr, "\n");
    fprintf(stderr, "╔══════════════════════════════════════════════════════════════════════╗\n");
    fprintf(stderr, "║ FATAL SIGNAL: %-57s ║\n", reason);
    fprintf(stderr, "╚══════════════════════════════════════════════════════════════════════╝\n");

    // Use our robust stack trace analyzer
    print_analyzed_stack_trace(reason);

    // Also log to file
    LOG_ERROR("=== FATAL SIGNAL: %s ===", reason);
    fflush(stderr);
}

static void signal_handler(int sig) {
    const char *sig_name = "UNKNOWN";
    const char *sig_desc = "";
    switch(sig) {
        case SIGSEGV: sig_name = "SIGSEGV"; sig_desc = "Segmentation fault"; break;
        case SIGBUS:  sig_name = "SIGBUS";  sig_desc = "Bus error"; break;
        case SIGFPE:  sig_name = "SIGFPE";  sig_desc = "Floating point exception"; break;
        case SIGILL:  sig_name = "SIGILL";  sig_desc = "Illegal instruction"; break;
    }

    char reason[64];
    snprintf(reason, sizeof(reason), "%s (%s)", sig_name, sig_desc);
    print_backtrace(reason);

    // Re-raise to get default behavior
    signal(sig, SIG_DFL);
    raise(sig);
}

// Only log exits if we actually crashed (non-zero exit status)
// Remove atexit handler as it triggers on all subprocess exits

// ============================================================================
// Global State Definitions (exported via db_interpose.h)
// ============================================================================

// Recursion prevention
__thread int in_interpose_call = 0;
__thread int prepare_v2_depth = 0;

// SQLite library handle for dlsym fallback
void *sqlite_handle = NULL;

// Original SQLite function pointers (populated by dlsym RTLD_NEXT)
// These are called by our my_* implementations to invoke the real SQLite
int (*orig_sqlite3_open)(const char*, sqlite3**) = NULL;
int (*orig_sqlite3_open_v2)(const char*, sqlite3**, int, const char*) = NULL;
int (*orig_sqlite3_close)(sqlite3*) = NULL;
int (*orig_sqlite3_close_v2)(sqlite3*) = NULL;
int (*orig_sqlite3_exec)(sqlite3*, const char*, int(*)(void*,int,char**,char**), void*, char**) = NULL;
int (*orig_sqlite3_changes)(sqlite3*) = NULL;
sqlite3_int64 (*orig_sqlite3_changes64)(sqlite3*) = NULL;
sqlite3_int64 (*orig_sqlite3_last_insert_rowid)(sqlite3*) = NULL;
int (*orig_sqlite3_get_table)(sqlite3*, const char*, char***, int*, int*, char**) = NULL;

const char* (*orig_sqlite3_errmsg)(sqlite3*) = NULL;
int (*orig_sqlite3_errcode)(sqlite3*) = NULL;
int (*orig_sqlite3_extended_errcode)(sqlite3*) = NULL;

int (*orig_sqlite3_prepare)(sqlite3*, const char*, int, sqlite3_stmt**, const char**) = NULL;
int (*orig_sqlite3_prepare_v2)(sqlite3*, const char*, int, sqlite3_stmt**, const char**) = NULL;
int (*orig_sqlite3_prepare_v3)(sqlite3*, const char*, int, unsigned int, sqlite3_stmt**, const char**) = NULL;
int (*orig_sqlite3_prepare16_v2)(sqlite3*, const void*, int, sqlite3_stmt**, const void**) = NULL;

int (*orig_sqlite3_bind_int)(sqlite3_stmt*, int, int) = NULL;
int (*orig_sqlite3_bind_int64)(sqlite3_stmt*, int, sqlite3_int64) = NULL;
int (*orig_sqlite3_bind_double)(sqlite3_stmt*, int, double) = NULL;
int (*orig_sqlite3_bind_text)(sqlite3_stmt*, int, const char*, int, void(*)(void*)) = NULL;
int (*orig_sqlite3_bind_text64)(sqlite3_stmt*, int, const char*, sqlite3_uint64, void(*)(void*), unsigned char) = NULL;
int (*orig_sqlite3_bind_blob)(sqlite3_stmt*, int, const void*, int, void(*)(void*)) = NULL;
int (*orig_sqlite3_bind_blob64)(sqlite3_stmt*, int, const void*, sqlite3_uint64, void(*)(void*)) = NULL;
int (*orig_sqlite3_bind_value)(sqlite3_stmt*, int, const sqlite3_value*) = NULL;
int (*orig_sqlite3_bind_null)(sqlite3_stmt*, int) = NULL;

int (*orig_sqlite3_step)(sqlite3_stmt*) = NULL;
int (*orig_sqlite3_reset)(sqlite3_stmt*) = NULL;
int (*orig_sqlite3_finalize)(sqlite3_stmt*) = NULL;
int (*orig_sqlite3_clear_bindings)(sqlite3_stmt*) = NULL;

int (*orig_sqlite3_column_count)(sqlite3_stmt*) = NULL;
int (*orig_sqlite3_column_type)(sqlite3_stmt*, int) = NULL;
int (*orig_sqlite3_column_int)(sqlite3_stmt*, int) = NULL;
sqlite3_int64 (*orig_sqlite3_column_int64)(sqlite3_stmt*, int) = NULL;
double (*orig_sqlite3_column_double)(sqlite3_stmt*, int) = NULL;
const unsigned char* (*orig_sqlite3_column_text)(sqlite3_stmt*, int) = NULL;
const void* (*orig_sqlite3_column_blob)(sqlite3_stmt*, int) = NULL;
int (*orig_sqlite3_column_bytes)(sqlite3_stmt*, int) = NULL;
const char* (*orig_sqlite3_column_name)(sqlite3_stmt*, int) = NULL;
sqlite3_value* (*orig_sqlite3_column_value)(sqlite3_stmt*, int) = NULL;
int (*orig_sqlite3_data_count)(sqlite3_stmt*) = NULL;
sqlite3* (*orig_sqlite3_db_handle)(sqlite3_stmt*) = NULL;
char* (*orig_sqlite3_expanded_sql)(sqlite3_stmt*) = NULL;
const char* (*orig_sqlite3_sql)(sqlite3_stmt*) = NULL;
void (*orig_sqlite3_free)(void*) = NULL;
const char* (*orig_sqlite3_bind_parameter_name)(sqlite3_stmt*, int) = NULL;
int (*orig_sqlite3_bind_parameter_index)(sqlite3_stmt*, const char*) = NULL;
const char* (*orig_sqlite3_column_decltype)(sqlite3_stmt*, int) = NULL;

int (*orig_sqlite3_value_type)(sqlite3_value*) = NULL;
const unsigned char* (*orig_sqlite3_value_text)(sqlite3_value*) = NULL;
int (*orig_sqlite3_value_int)(sqlite3_value*) = NULL;
sqlite3_int64 (*orig_sqlite3_value_int64)(sqlite3_value*) = NULL;
double (*orig_sqlite3_value_double)(sqlite3_value*) = NULL;
int (*orig_sqlite3_value_bytes)(sqlite3_value*) = NULL;
const void* (*orig_sqlite3_value_blob)(sqlite3_value*) = NULL;

int (*orig_sqlite3_create_collation)(sqlite3*, const char*, int, void*, int(*)(void*,int,const void*,int,const void*)) = NULL;
int (*orig_sqlite3_create_collation_v2)(sqlite3*, const char*, int, void*, int(*)(void*,int,const void*,int,const void*), void(*)(void*)) = NULL;

// Additional SQLite API functions (free, db_handle, sql already defined above)
void* (*orig_sqlite3_malloc)(int) = NULL;
int (*orig_sqlite3_bind_parameter_count)(sqlite3_stmt*) = NULL;
int (*orig_sqlite3_stmt_readonly)(sqlite3_stmt*) = NULL;
int (*orig_sqlite3_stmt_busy)(sqlite3_stmt*) = NULL;
int (*orig_sqlite3_stmt_status)(sqlite3_stmt*, int, int) = NULL;

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
// Lazy Init for Real SQLite Functions
// ============================================================================

void ensure_real_sqlite_loaded(void) {
    if (real_sqlite3_prepare_v2) return;  // Already loaded

    // On Linux, use RTLD_NEXT to get the real functions
    real_sqlite3_prepare_v2 = orig_sqlite3_prepare_v2;
    real_sqlite3_errmsg = orig_sqlite3_errmsg;
    real_sqlite3_errcode = orig_sqlite3_errcode;
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
// Load Original SQLite Functions
// ============================================================================
// First try to load from explicit path (for when we REPLACE Plex's SQLite),
// then fall back to RTLD_NEXT (for LD_PRELOAD mode)

static void *real_sqlite_handle = NULL;

static void load_original_functions(void) {
    // Try to load real SQLite from explicit path first
    // This is needed when we replace Plex's libsqlite3.so with our shim
    const char *sqlite_paths[] = {
        "/usr/local/lib/plex-postgresql/libsqlite3_real.so",
        "/usr/lib/plexmediaserver/lib/libsqlite3.so.original",
        NULL
    };

    void *handle = NULL;
    for (int i = 0; sqlite_paths[i] != NULL; i++) {
        handle = dlopen(sqlite_paths[i], RTLD_NOW | RTLD_LOCAL);
        if (handle) {
            fprintf(stderr, "[SHIM_INIT] Loaded real SQLite from %s\n", sqlite_paths[i]);
            real_sqlite_handle = handle;
            break;
        }
    }

    if (handle) {
        // Load from explicit library
        fprintf(stderr, "[SHIM_INIT] Loading original SQLite functions from explicit library...\n");
        orig_sqlite3_open = dlsym(handle, "sqlite3_open");
    } else {
        // Fall back to RTLD_NEXT (LD_PRELOAD mode)
        fprintf(stderr, "[SHIM_INIT] Loading original SQLite functions via RTLD_NEXT...\n");
        handle = RTLD_NEXT;
        orig_sqlite3_open = dlsym(handle, "sqlite3_open");
    }

    // Use the selected handle (either explicit or RTLD_NEXT) for remaining functions
    #define LOAD_SYM(name) orig_##name = dlsym(handle, #name)

    orig_sqlite3_open = dlsym(handle, "sqlite3_open");
    orig_sqlite3_open_v2 = dlsym(handle, "sqlite3_open_v2");
    orig_sqlite3_close = dlsym(handle, "sqlite3_close");
    orig_sqlite3_close_v2 = dlsym(handle, "sqlite3_close_v2");
    orig_sqlite3_exec = dlsym(handle, "sqlite3_exec");
    orig_sqlite3_changes = dlsym(handle, "sqlite3_changes");
    orig_sqlite3_changes64 = dlsym(handle, "sqlite3_changes64");
    orig_sqlite3_last_insert_rowid = dlsym(handle, "sqlite3_last_insert_rowid");
    orig_sqlite3_get_table = dlsym(handle, "sqlite3_get_table");

    orig_sqlite3_errmsg = dlsym(handle, "sqlite3_errmsg");
    orig_sqlite3_errcode = dlsym(handle, "sqlite3_errcode");
    orig_sqlite3_extended_errcode = dlsym(handle, "sqlite3_extended_errcode");

    orig_sqlite3_prepare = dlsym(handle, "sqlite3_prepare");
    orig_sqlite3_prepare_v2 = dlsym(handle, "sqlite3_prepare_v2");
    orig_sqlite3_prepare_v3 = dlsym(handle, "sqlite3_prepare_v3");
    orig_sqlite3_prepare16_v2 = dlsym(handle, "sqlite3_prepare16_v2");

    orig_sqlite3_bind_int = dlsym(handle, "sqlite3_bind_int");
    orig_sqlite3_bind_int64 = dlsym(handle, "sqlite3_bind_int64");
    orig_sqlite3_bind_double = dlsym(handle, "sqlite3_bind_double");
    orig_sqlite3_bind_text = dlsym(handle, "sqlite3_bind_text");
    orig_sqlite3_bind_text64 = dlsym(handle, "sqlite3_bind_text64");
    orig_sqlite3_bind_blob = dlsym(handle, "sqlite3_bind_blob");
    orig_sqlite3_bind_blob64 = dlsym(handle, "sqlite3_bind_blob64");
    orig_sqlite3_bind_value = dlsym(handle, "sqlite3_bind_value");
    orig_sqlite3_bind_null = dlsym(handle, "sqlite3_bind_null");

    orig_sqlite3_step = dlsym(handle, "sqlite3_step");
    orig_sqlite3_reset = dlsym(handle, "sqlite3_reset");
    orig_sqlite3_finalize = dlsym(handle, "sqlite3_finalize");
    orig_sqlite3_clear_bindings = dlsym(handle, "sqlite3_clear_bindings");

    orig_sqlite3_column_count = dlsym(handle, "sqlite3_column_count");
    orig_sqlite3_column_type = dlsym(handle, "sqlite3_column_type");
    orig_sqlite3_column_int = dlsym(handle, "sqlite3_column_int");
    orig_sqlite3_column_int64 = dlsym(handle, "sqlite3_column_int64");
    orig_sqlite3_column_double = dlsym(handle, "sqlite3_column_double");
    orig_sqlite3_column_text = dlsym(handle, "sqlite3_column_text");
    orig_sqlite3_column_blob = dlsym(handle, "sqlite3_column_blob");
    orig_sqlite3_column_bytes = dlsym(handle, "sqlite3_column_bytes");
    orig_sqlite3_column_name = dlsym(handle, "sqlite3_column_name");
    orig_sqlite3_column_value = dlsym(handle, "sqlite3_column_value");
    orig_sqlite3_data_count = dlsym(handle, "sqlite3_data_count");
    orig_sqlite3_db_handle = dlsym(handle, "sqlite3_db_handle");
    orig_sqlite3_expanded_sql = dlsym(handle, "sqlite3_expanded_sql");
    orig_sqlite3_sql = dlsym(handle, "sqlite3_sql");
    orig_sqlite3_free = dlsym(handle, "sqlite3_free");
    orig_sqlite3_bind_parameter_name = dlsym(handle, "sqlite3_bind_parameter_name");
    orig_sqlite3_bind_parameter_index = dlsym(handle, "sqlite3_bind_parameter_index");
    orig_sqlite3_column_decltype = dlsym(handle, "sqlite3_column_decltype");

    orig_sqlite3_value_type = dlsym(handle, "sqlite3_value_type");
    orig_sqlite3_value_text = dlsym(handle, "sqlite3_value_text");
    orig_sqlite3_value_int = dlsym(handle, "sqlite3_value_int");
    orig_sqlite3_value_int64 = dlsym(handle, "sqlite3_value_int64");
    orig_sqlite3_value_double = dlsym(handle, "sqlite3_value_double");
    orig_sqlite3_value_bytes = dlsym(handle, "sqlite3_value_bytes");
    orig_sqlite3_value_blob = dlsym(handle, "sqlite3_value_blob");

    orig_sqlite3_create_collation = dlsym(handle, "sqlite3_create_collation");
    orig_sqlite3_create_collation_v2 = dlsym(handle, "sqlite3_create_collation_v2");

    // Additional SQLite API functions
    orig_sqlite3_malloc = dlsym(handle, "sqlite3_malloc");
    orig_sqlite3_bind_parameter_count = dlsym(handle, "sqlite3_bind_parameter_count");
    orig_sqlite3_stmt_readonly = dlsym(handle, "sqlite3_stmt_readonly");
    orig_sqlite3_stmt_busy = dlsym(handle, "sqlite3_stmt_busy");
    orig_sqlite3_stmt_status = dlsym(handle, "sqlite3_stmt_status");

    // Set up aliases for backward compatibility
    real_sqlite3_prepare_v2 = orig_sqlite3_prepare_v2;
    real_sqlite3_errmsg = orig_sqlite3_errmsg;
    real_sqlite3_errcode = orig_sqlite3_errcode;

    // Verify critical functions were found
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

    fprintf(stderr, "[SHIM_INIT] Original SQLite functions loaded\n");
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
    // This prevents the crash where parent's query hangs while child creates connections

    // Use fprintf since logging may not be initialized yet
    fprintf(stderr, "[FORK_CHILD] Cleaning up inherited connection pool\n");
    fflush(stderr);

    // Call pg_client cleanup function to clear pool state
    extern void pg_pool_cleanup_after_fork(void);
    pg_pool_cleanup_after_fork();

    fprintf(stderr, "[FORK_CHILD] Pool cleared, child will create new connections\n");
    fflush(stderr);
}

// ============================================================================
// Constructor/Destructor
// ============================================================================

__attribute__((constructor))
static void shim_init(void) {
    // Write to stderr first to verify constructor runs
    fprintf(stderr, "[SHIM_INIT] Constructor starting (Linux)...\n");
    fflush(stderr);

    // Install signal handlers for crash debugging (only actual crashes, not SIGABRT which is often legitimate)
    signal(SIGSEGV, signal_handler);
    signal(SIGBUS, signal_handler);
    signal(SIGFPE, signal_handler);
    signal(SIGILL, signal_handler);
    // Note: No SIGABRT handler (used by legitimate assert/abort) and no atexit (triggers on subprocesses)

    // CRITICAL: Install fork handlers BEFORE any PostgreSQL connections are made
    // This ensures child processes don't inherit parent's active connections
    pthread_atfork(atfork_prepare, atfork_parent, atfork_child);
    fprintf(stderr, "[SHIM_INIT] Registered pthread_atfork handlers for connection pool safety\n");
    fflush(stderr);

    // Load original SQLite functions via RTLD_NEXT
    load_original_functions();

    // Skip full initialization if SQLite isn't loaded (e.g., in /bin/sh, nc, etc.)
    // This prevents crashes in processes that don't use SQLite
    if (!orig_sqlite3_open || !orig_sqlite3_prepare_v2) {
        fprintf(stderr, "[SHIM_INIT] SQLite not found in this process, skipping initialization\n");
        fflush(stderr);
        return;
    }

    pg_logging_init();
    LOG_INFO("=== Plex PostgreSQL Interpose Shim loaded (Linux) ===");
    LOG_ERROR("SHIM_CONSTRUCTOR: Initialization complete");

    // Also write to stderr after logging init
    fprintf(stderr, "[SHIM_INIT] Logging initialized\n");
    fflush(stderr);

    pg_config_init();
    pg_client_init();
    pg_statement_init();
    pg_query_cache_init();  // Initialize query result cache
    sql_translator_init();

    // Start worker thread with 8MB stack for heavy queries
    worker_init();

    shim_initialized = 1;

    fprintf(stderr, "[SHIM_INIT] All modules initialized (Linux)\n");
    fflush(stderr);
}

__attribute__((destructor))
static void shim_cleanup(void) {
    // Only cleanup if we actually initialized
    if (!shim_initialized) return;

    LOG_INFO("=== Plex PostgreSQL Interpose Shim unloading ===");
    worker_cleanup();  // Stop worker thread first
    pg_statement_cleanup();
    pg_client_cleanup();
    sql_translator_cleanup();
    pg_logging_cleanup();
}

// ============================================================================
// LD_PRELOAD Wrapper Functions
// These have the exact sqlite3_* names so they intercept via LD_PRELOAD.
// They call the my_sqlite3_* implementations from the shared modules.
// ============================================================================

// Open/Close
int sqlite3_open(const char *filename, sqlite3 **ppDb) {
    return my_sqlite3_open(filename, ppDb);
}

int sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags, const char *zVfs) {
    return my_sqlite3_open_v2(filename, ppDb, flags, zVfs);
}

int sqlite3_close(sqlite3 *db) {
    return my_sqlite3_close(db);
}

int sqlite3_close_v2(sqlite3 *db) {
    return my_sqlite3_close_v2(db);
}

// Exec
int sqlite3_exec(sqlite3 *db, const char *sql,
                 int (*callback)(void*,int,char**,char**),
                 void *arg, char **errmsg) {
    return my_sqlite3_exec(db, sql, callback, arg, errmsg);
}

int sqlite3_get_table(sqlite3 *db, const char *sql, char ***pazResult,
                      int *pnRow, int *pnColumn, char **pzErrMsg) {
    return my_sqlite3_get_table(db, sql, pazResult, pnRow, pnColumn, pzErrMsg);
}

// Metadata
int sqlite3_changes(sqlite3 *db) {
    return my_sqlite3_changes(db);
}

sqlite3_int64 sqlite3_changes64(sqlite3 *db) {
    return my_sqlite3_changes64(db);
}

sqlite3_int64 sqlite3_last_insert_rowid(sqlite3 *db) {
    return my_sqlite3_last_insert_rowid(db);
}

const char* sqlite3_errmsg(sqlite3 *db) {
    return my_sqlite3_errmsg(db);
}

int sqlite3_errcode(sqlite3 *db) {
    return my_sqlite3_errcode(db);
}

int sqlite3_extended_errcode(sqlite3 *db) {
    return my_sqlite3_extended_errcode(db);
}

// Prepare
int sqlite3_prepare(sqlite3 *db, const char *zSql, int nByte,
                    sqlite3_stmt **ppStmt, const char **pzTail) {
    return my_sqlite3_prepare(db, zSql, nByte, ppStmt, pzTail);
}

int sqlite3_prepare_v2(sqlite3 *db, const char *zSql, int nByte,
                       sqlite3_stmt **ppStmt, const char **pzTail) {
    return my_sqlite3_prepare_v2(db, zSql, nByte, ppStmt, pzTail);
}

int sqlite3_prepare_v3(sqlite3 *db, const char *zSql, int nByte, unsigned int prepFlags,
                       sqlite3_stmt **ppStmt, const char **pzTail) {
    return my_sqlite3_prepare_v3(db, zSql, nByte, prepFlags, ppStmt, pzTail);
}

int sqlite3_prepare16_v2(sqlite3 *db, const void *zSql, int nByte,
                         sqlite3_stmt **ppStmt, const void **pzTail) {
    return my_sqlite3_prepare16_v2(db, zSql, nByte, ppStmt, pzTail);
}

// Bind
int sqlite3_bind_int(sqlite3_stmt *pStmt, int idx, int val) {
    return my_sqlite3_bind_int(pStmt, idx, val);
}

int sqlite3_bind_int64(sqlite3_stmt *pStmt, int idx, sqlite3_int64 val) {
    return my_sqlite3_bind_int64(pStmt, idx, val);
}

int sqlite3_bind_double(sqlite3_stmt *pStmt, int idx, double val) {
    return my_sqlite3_bind_double(pStmt, idx, val);
}

int sqlite3_bind_text(sqlite3_stmt *pStmt, int idx, const char *val,
                      int nByte, void (*xDel)(void*)) {
    return my_sqlite3_bind_text(pStmt, idx, val, nByte, xDel);
}

int sqlite3_bind_text64(sqlite3_stmt *pStmt, int idx, const char *val,
                        sqlite3_uint64 nByte, void (*xDel)(void*), unsigned char enc) {
    return my_sqlite3_bind_text64(pStmt, idx, val, nByte, xDel, enc);
}

int sqlite3_bind_blob(sqlite3_stmt *pStmt, int idx, const void *val,
                      int nByte, void (*xDel)(void*)) {
    return my_sqlite3_bind_blob(pStmt, idx, val, nByte, xDel);
}

int sqlite3_bind_blob64(sqlite3_stmt *pStmt, int idx, const void *val,
                        sqlite3_uint64 nByte, void (*xDel)(void*)) {
    return my_sqlite3_bind_blob64(pStmt, idx, val, nByte, xDel);
}

int sqlite3_bind_value(sqlite3_stmt *pStmt, int idx, const sqlite3_value *pValue) {
    return my_sqlite3_bind_value(pStmt, idx, pValue);
}

int sqlite3_bind_null(sqlite3_stmt *pStmt, int idx) {
    return my_sqlite3_bind_null(pStmt, idx);
}

// Step/Reset/Finalize
int sqlite3_step(sqlite3_stmt *pStmt) {
    return my_sqlite3_step(pStmt);
}

int sqlite3_reset(sqlite3_stmt *pStmt) {
    return my_sqlite3_reset(pStmt);
}

int sqlite3_finalize(sqlite3_stmt *pStmt) {
    return my_sqlite3_finalize(pStmt);
}

int sqlite3_clear_bindings(sqlite3_stmt *pStmt) {
    return my_sqlite3_clear_bindings(pStmt);
}

// Column access
int sqlite3_column_count(sqlite3_stmt *pStmt) {
    return my_sqlite3_column_count(pStmt);
}

int sqlite3_column_type(sqlite3_stmt *pStmt, int idx) {
    return my_sqlite3_column_type(pStmt, idx);
}

int sqlite3_column_int(sqlite3_stmt *pStmt, int idx) {
    return my_sqlite3_column_int(pStmt, idx);
}

sqlite3_int64 sqlite3_column_int64(sqlite3_stmt *pStmt, int idx) {
    return my_sqlite3_column_int64(pStmt, idx);
}

double sqlite3_column_double(sqlite3_stmt *pStmt, int idx) {
    return my_sqlite3_column_double(pStmt, idx);
}

const unsigned char* sqlite3_column_text(sqlite3_stmt *pStmt, int idx) {
    return my_sqlite3_column_text(pStmt, idx);
}

const void* sqlite3_column_blob(sqlite3_stmt *pStmt, int idx) {
    return my_sqlite3_column_blob(pStmt, idx);
}

int sqlite3_column_bytes(sqlite3_stmt *pStmt, int idx) {
    return my_sqlite3_column_bytes(pStmt, idx);
}

const char* sqlite3_column_name(sqlite3_stmt *pStmt, int idx) {
    return my_sqlite3_column_name(pStmt, idx);
}

sqlite3_value* sqlite3_column_value(sqlite3_stmt *pStmt, int idx) {
    return my_sqlite3_column_value(pStmt, idx);
}

int sqlite3_data_count(sqlite3_stmt *pStmt) {
    return my_sqlite3_data_count(pStmt);
}

// sqlite3_db_handle - use my_ implementation for PG statement support
sqlite3* sqlite3_db_handle(sqlite3_stmt *pStmt) {
    return my_sqlite3_db_handle(pStmt);
}

// sqlite3_expanded_sql - use my_ implementation for PG statement support
char* sqlite3_expanded_sql(sqlite3_stmt *pStmt) {
    return my_sqlite3_expanded_sql(pStmt);
}

// sqlite3_sql - use my_ implementation for PG statement support
const char* sqlite3_sql(sqlite3_stmt *pStmt) {
    return my_sqlite3_sql(pStmt);
}

// sqlite3_free - use my_ implementation
void sqlite3_free(void *p) {
    my_sqlite3_free(p);
}

// sqlite3_malloc - use my_ implementation
void* sqlite3_malloc(int n) {
    return my_sqlite3_malloc(n);
}

// sqlite3_bind_parameter_count - use my_ implementation for PG statement support
int sqlite3_bind_parameter_count(sqlite3_stmt *pStmt) {
    return my_sqlite3_bind_parameter_count(pStmt);
}

// sqlite3_stmt_readonly - use my_ implementation for PG statement support
int sqlite3_stmt_readonly(sqlite3_stmt *pStmt) {
    return my_sqlite3_stmt_readonly(pStmt);
}

// sqlite3_stmt_busy - use my_ implementation for PG statement support
int sqlite3_stmt_busy(sqlite3_stmt *pStmt) {
    return my_sqlite3_stmt_busy(pStmt);
}

// sqlite3_stmt_status - use my_ implementation for PG statement support
int sqlite3_stmt_status(sqlite3_stmt *pStmt, int op, int resetFlg) {
    return my_sqlite3_stmt_status(pStmt, op, resetFlg);
}

// sqlite3_bind_parameter_name - use my_ implementation for PG statement support
const char* sqlite3_bind_parameter_name(sqlite3_stmt *pStmt, int idx) {
    return my_sqlite3_bind_parameter_name(pStmt, idx);
}

// sqlite3_bind_parameter_index - use my_ implementation for named parameter support
int sqlite3_bind_parameter_index(sqlite3_stmt *pStmt, const char *zName) {
    return my_sqlite3_bind_parameter_index(pStmt, zName);
}

// sqlite3_column_decltype - use my_ implementation for PG type mapping
const char* sqlite3_column_decltype(sqlite3_stmt *pStmt, int idx) {
    return my_sqlite3_column_decltype(pStmt, idx);
}

// Value access
int sqlite3_value_type(sqlite3_value *pVal) {
    return my_sqlite3_value_type(pVal);
}

const unsigned char* sqlite3_value_text(sqlite3_value *pVal) {
    return my_sqlite3_value_text(pVal);
}

int sqlite3_value_int(sqlite3_value *pVal) {
    return my_sqlite3_value_int(pVal);
}

sqlite3_int64 sqlite3_value_int64(sqlite3_value *pVal) {
    return my_sqlite3_value_int64(pVal);
}

double sqlite3_value_double(sqlite3_value *pVal) {
    return my_sqlite3_value_double(pVal);
}

int sqlite3_value_bytes(sqlite3_value *pVal) {
    return my_sqlite3_value_bytes(pVal);
}

const void* sqlite3_value_blob(sqlite3_value *pVal) {
    return my_sqlite3_value_blob(pVal);
}

// Collation
int sqlite3_create_collation(sqlite3 *db, const char *zName, int eTextRep,
                              void *pArg,
                              int(*xCompare)(void*,int,const void*,int,const void*)) {
    return my_sqlite3_create_collation(db, zName, eTextRep, pArg, xCompare);
}

int sqlite3_create_collation_v2(sqlite3 *db, const char *zName, int eTextRep,
                                 void *pArg,
                                 int(*xCompare)(void*,int,const void*,int,const void*),
                                 void(*xDestroy)(void*)) {
    return my_sqlite3_create_collation_v2(db, zName, eTextRep, pArg, xCompare, xDestroy);
}
