#ifndef PG_LOGGING_H
#define PG_LOGGING_H

// Log levels
typedef enum {
    PG_LOG_ERROR = 0,
    PG_LOG_INFO = 1,
    PG_LOG_DEBUG = 2
} pg_log_level_t;

void init_logging(void);
void log_message(int level, const char *fmt, ...);

// Legacy/Compatibility macro - defaults to INFO
#define LOG_INFO(fmt, ...) log_message(PG_LOG_INFO, fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) log_message(PG_LOG_ERROR, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(fmt, ...) log_message(PG_LOG_DEBUG, fmt, ##__VA_ARGS__)

#endif // PG_LOGGING_H
