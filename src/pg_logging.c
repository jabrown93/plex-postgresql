#include "pg_logging.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#define DEFAULT_LOG_FILE "/tmp/plex_redirect_pg.log"

static FILE *log_file = NULL;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
static int current_log_level = PG_LOG_INFO; // Default to INFO
static int logging_initialized = 0;

void init_logging(void) {
    if (logging_initialized) return;
    
    // Log Level
    const char *level_env = getenv("PLEX_PG_LOG_LEVEL");
    if (level_env) {
        if (strcasecmp(level_env, "DEBUG") == 0) current_log_level = PG_LOG_DEBUG;
        else if (strcasecmp(level_env, "ERROR") == 0) current_log_level = PG_LOG_ERROR;
        else current_log_level = PG_LOG_INFO;
    }

    // Log File
    const char *file_env = getenv("PLEX_PG_LOG_FILE");
    if (file_env) {
        if (strcasecmp(file_env, "stdout") == 0) {
            log_file = stdout;
        } else if (strcasecmp(file_env, "stderr") == 0) {
            log_file = stderr;
        } else {
            log_file = fopen(file_env, "a");
        }
    } else {
        log_file = fopen(DEFAULT_LOG_FILE, "a");
    }

    if (!log_file && log_file != stdout && log_file != stderr) {
        // Fallback to stderr if file opening fails
        log_file = stderr;
        fprintf(stderr, "[PG_SHIM] Failed to open log file, falling back to stderr\n");
    }

    if (log_file != stdout && log_file != stderr) {
        setbuf(log_file, NULL); // Unbuffered
    }

    logging_initialized = 1;
    log_message(PG_LOG_INFO, "Logging initialized. Level: %d", current_log_level);
}

void log_message(int level, const char *fmt, ...) {
    if (!logging_initialized) init_logging();
    if (level > current_log_level) return;

    pthread_mutex_lock(&log_mutex);

    time_t now = time(NULL);
    struct tm *tm = localtime(&now);
    
    // Timestamp
    if (log_file) {
        fprintf(log_file, "[%04d-%02d-%02d %02d:%02d:%02d] ",
                tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
                tm->tm_hour, tm->tm_min, tm->tm_sec);

        // Level tag
        switch(level) {
            case PG_LOG_ERROR: fprintf(log_file, "[ERROR] "); break;
            case PG_LOG_INFO:  fprintf(log_file, "[INFO] "); break;
            case PG_LOG_DEBUG: fprintf(log_file, "[DEBUG] "); break;
        }

        va_list args;
        va_start(args, fmt);
        vfprintf(log_file, fmt, args);
        va_end(args);

        fprintf(log_file, "\n");
        fflush(log_file);
    }

    pthread_mutex_unlock(&log_mutex);
}
