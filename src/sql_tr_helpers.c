/*
 * SQL Translator - Helper Functions
 * String manipulation utilities for SQL translation
 */

#include "sql_translator_internal.h"

// ============================================================================
// String Replace (case-sensitive)
// ============================================================================

char* str_replace(const char *str, const char *old, const char *new_str) {
    if (!str || !old || !new_str) return NULL;

    size_t old_len = strlen(old);
    size_t new_len = strlen(new_str);

    // Count occurrences
    int count = 0;
    const char *p = str;
    while ((p = strstr(p, old)) != NULL) {
        count++;
        p += old_len;
    }

    if (count == 0) return strdup(str);

    // Allocate result
    size_t result_len = strlen(str) + count * (new_len - old_len) + 1;
    char *result = malloc(result_len);
    if (!result) return NULL;

    char *out = result;
    p = str;
    while (*p) {
        if (strncmp(p, old, old_len) == 0) {
            memcpy(out, new_str, new_len);
            out += new_len;
            p += old_len;
        } else {
            *out++ = *p++;
        }
    }
    *out = '\0';

    return result;
}

// ============================================================================
// String Replace (case-insensitive)
// ============================================================================

char* str_replace_nocase(const char *str, const char *old, const char *new_str) {
    if (!str || !old || !new_str) return NULL;

    size_t old_len = strlen(old);
    size_t new_len = strlen(new_str);
    size_t str_len = strlen(str);

    // Find all occurrences (case insensitive)
    char *result = malloc(str_len * 2 + new_len * 10 + 1);
    if (!result) return NULL;

    char *out = result;
    const char *p = str;

    while (*p) {
        if (strncasecmp(p, old, old_len) == 0) {
            memcpy(out, new_str, new_len);
            out += new_len;
            p += old_len;
        } else {
            *out++ = *p++;
        }
    }
    *out = '\0';

    return result;
}

// ============================================================================
// Skip Whitespace
// ============================================================================

const char* skip_ws(const char *p) {
    while (*p && isspace(*p)) p++;
    return p;
}

// ============================================================================
// Check if Character is Part of Identifier
// ============================================================================

int is_ident_char(char c) {
    return isalnum(c) || c == '_';
}

// ============================================================================
// Extract Function Argument (handles nested parentheses)
// ============================================================================

const char* extract_arg(const char *start, char *buf, size_t bufsize) {
    const char *p = start;
    int depth = 0;
    size_t i = 0;

    p = skip_ws(p);

    while (*p && i < bufsize - 1) {
        if (*p == '(') depth++;
        else if (*p == ')') {
            if (depth == 0) break;
            depth--;
        }
        else if (*p == ',' && depth == 0) break;

        buf[i++] = *p++;
    }

    // Trim trailing whitespace
    while (i > 0 && isspace(buf[i-1])) i--;
    buf[i] = '\0';

    return p;
}
