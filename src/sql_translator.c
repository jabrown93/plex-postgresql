/*
 * SQL Translator - SQLite to PostgreSQL
 *
 * Comprehensive translation of SQLite SQL to PostgreSQL.
 */

#include "sql_translator.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>  // for strcasestr
#include <ctype.h>
#include <regex.h>

#define MAX_SQL_LEN 131072
#define MAX_PARAMS 512
#define MAX_PARAM_NAME 64

// ============================================================================
// Helper Functions
// ============================================================================

static char* str_replace(const char *str, const char *old, const char *new_str) {
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

static char* str_replace_nocase(const char *str, const char *old, const char *new_str) {
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

// Skip whitespace
static const char* skip_ws(const char *p) {
    while (*p && isspace(*p)) p++;
    return p;
}

// Check if character is part of identifier
static int is_ident_char(char c) {
    return isalnum(c) || c == '_';
}

// Extract function argument (handles nested parentheses)
static const char* extract_arg(const char *start, char *buf, size_t bufsize) {
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

// ============================================================================
// Placeholder Translation (? and :name -> $1, $2, ...)
// ============================================================================

char* sql_translate_placeholders(const char *sql, char ***param_names_out, int *param_count_out) {
    if (!sql) return NULL;

    size_t sql_len = strlen(sql);
    char *result = malloc(sql_len * 2 + 1);
    if (!result) return NULL;

    char **param_names = NULL;
    int param_count = 0;
    int param_capacity = 0;

    char *out = result;
    const char *p = sql;
    int in_string = 0;
    char string_char = 0;

    while (*p) {
        // Track string literals
        if ((*p == '\'' || *p == '"') && (p == sql || *(p-1) != '\\')) {
            if (!in_string) {
                in_string = 1;
                string_char = *p;
            } else if (*p == string_char) {
                in_string = 0;
            }
            *out++ = *p++;
            continue;
        }

        if (in_string) {
            *out++ = *p++;
            continue;
        }

        // Handle ? placeholder
        if (*p == '?') {
            param_count++;
            out += sprintf(out, "$%d", param_count);
            p++;
            // Add space if next char is a letter (to avoid $1left instead of $1 left)
            if (isalpha(*p)) {
                *out++ = ' ';
            }
            continue;
        }

        // Handle :name placeholder
        if (*p == ':' && (p == sql || !is_ident_char(*(p-1)))) {
            const char *name_start = p + 1;
            if (isalpha(*name_start) || *name_start == '_') {
                const char *name_end = name_start;
                while (is_ident_char(*name_end)) name_end++;

                size_t name_len = name_end - name_start;

                // Check if we've seen this name before
                int found_idx = -1;
                for (int i = 0; i < param_count; i++) {
                    if (param_names && param_names[i] &&
                        strncmp(param_names[i], name_start, name_len) == 0 &&
                        param_names[i][name_len] == '\0') {
                        found_idx = i;
                        break;
                    }
                }

                if (found_idx >= 0) {
                    // Reuse existing parameter index
                    out += sprintf(out, "$%d", found_idx + 1);
                } else {
                    // New parameter
                    if (param_count >= param_capacity) {
                        param_capacity = param_capacity ? param_capacity * 2 : 16;
                        param_names = realloc(param_names, param_capacity * sizeof(char*));
                    }

                    param_names[param_count] = malloc(name_len + 1);
                    memcpy(param_names[param_count], name_start, name_len);
                    param_names[param_count][name_len] = '\0';

                    param_count++;
                    out += sprintf(out, "$%d", param_count);
                }

                p = name_end;
                continue;
            }
        }

        *out++ = *p++;
    }

    *out = '\0';

    if (param_names_out) *param_names_out = param_names;
    else if (param_names) {
        for (int i = 0; i < param_count; i++) free(param_names[i]);
        free(param_names);
    }

    if (param_count_out) *param_count_out = param_count;

    return result;
}

// ============================================================================
// Function Translation
// ============================================================================

// Translate iif(cond, true_val, false_val) -> CASE WHEN cond THEN true_val ELSE false_val END
static char* translate_iif(const char *sql) {
    char *result = malloc(MAX_SQL_LEN);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;

    while (*p) {
        // Find iif( case-insensitively
        if (strncasecmp(p, "iif(", 4) == 0) {
            const char *args_start = p + 4;
            char cond[4096] = {0};
            char true_val[4096] = {0};
            char false_val[4096] = {0};

            // Extract condition
            const char *next = extract_arg(args_start, cond, sizeof(cond));
            if (*next == ',') next++;

            // Extract true value
            next = extract_arg(next, true_val, sizeof(true_val));
            if (*next == ',') next++;

            // Extract false value
            next = extract_arg(next, false_val, sizeof(false_val));
            if (*next == ')') next++;

            // Write CASE WHEN
            out += sprintf(out, "CASE WHEN %s THEN %s ELSE %s END", cond, true_val, false_val);
            p = next;
        } else {
            *out++ = *p++;
        }
    }

    *out = '\0';
    return result;
}

// Translate typeof(x) -> pg_typeof(x)::text
static char* translate_typeof(const char *sql) {
    // Also need to translate the comparison values
    // SQLite typeof() returns: 'integer', 'real', 'text', 'blob', 'null'
    // PostgreSQL pg_typeof() returns: 'integer', 'bigint', 'double precision', etc.

    char *temp = str_replace_nocase(sql, "typeof(", "pg_typeof(");
    if (!temp) return NULL;

    // Add ::text cast after pg_typeof(...)
    // This is complex because we need to find the matching )
    char *result = malloc(MAX_SQL_LEN);
    if (!result) { free(temp); return NULL; }

    char *out = result;
    const char *p = temp;

    while (*p) {
        if (strncasecmp(p, "pg_typeof(", 10) == 0) {
            // Copy pg_typeof(
            memcpy(out, p, 10);
            out += 10;
            p += 10;

            // Find matching )
            int depth = 1;
            while (*p && depth > 0) {
                if (*p == '(') depth++;
                else if (*p == ')') depth--;
                *out++ = *p++;
            }

            // Add ::text
            memcpy(out, "::text", 6);
            out += 6;
        } else {
            *out++ = *p++;
        }
    }

    *out = '\0';
    free(temp);

    // Now translate the type names in comparisons
    char *temp2 = str_replace(result, "'integer'", "'integer'");  // same
    free(result);
    temp2 = str_replace(temp2, "'real'", "'double precision'");
    // Note: SQLite 'integer' could be 'integer' or 'bigint' in PG

    return temp2;
}

// Translate strftime('%s', x, 'utc') -> EXTRACT(EPOCH FROM x)::bigint
static char* translate_strftime(const char *sql) {
    char *result = malloc(MAX_SQL_LEN);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;

    while (*p) {
        if (strncasecmp(p, "strftime(", 9) == 0) {
            const char *args_start = p + 9;
            char format[256] = {0};
            char value[4096] = {0};
            char extra[256] = {0};

            // Extract format string
            const char *next = extract_arg(args_start, format, sizeof(format));
            if (*next == ',') next++;

            // Extract value
            next = extract_arg(next, value, sizeof(value));

            // Check for extra argument (like 'utc')
            if (*next == ',') {
                next++;
                next = extract_arg(next, extra, sizeof(extra));
            }
            if (*next == ')') next++;

            // Translate based on format
            if (strcmp(format, "'%s'") == 0) {
                // Unix timestamp
                out += sprintf(out, "EXTRACT(EPOCH FROM %s)::bigint", value);
            } else if (strcmp(format, "'%Y-%m-%d'") == 0) {
                out += sprintf(out, "TO_CHAR(%s, 'YYYY-MM-DD')", value);
            } else if (strcmp(format, "'%Y-%m-%d %H:%M:%S'") == 0) {
                out += sprintf(out, "TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS')", value);
            } else {
                // Generic: try to convert format
                out += sprintf(out, "TO_CHAR(%s, %s)", value, format);
            }

            p = next;
        } else {
            *out++ = *p++;
        }
    }

    *out = '\0';
    return result;
}

// Translate unixepoch('now', '-7 day') -> EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days')::bigint
static char* translate_unixepoch(const char *sql) {
    char *result = malloc(MAX_SQL_LEN);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;

    while (*p) {
        if (strncasecmp(p, "unixepoch(", 10) == 0) {
            const char *args_start = p + 10;
            char arg1[256] = {0};
            char arg2[256] = {0};

            // Extract first argument
            const char *next = extract_arg(args_start, arg1, sizeof(arg1));

            // Check for second argument
            if (*next == ',') {
                next++;
                next = extract_arg(next, arg2, sizeof(arg2));
            }
            if (*next == ')') next++;

            // Parse interval from arg2 if present
            if (strcasecmp(arg1, "'now'") == 0) {
                if (arg2[0]) {
                    // Parse interval like '-7 day' or '+1 hour'
                    // Remove quotes
                    char interval[256];
                    if (arg2[0] == '\'') {
                        strncpy(interval, arg2 + 1, sizeof(interval) - 1);
                        char *q = strrchr(interval, '\'');
                        if (q) *q = '\0';
                    } else {
                        strncpy(interval, arg2, sizeof(interval) - 1);
                    }

                    // Convert to PostgreSQL interval
                    // SQLite: '-7 day' -> PostgreSQL: INTERVAL '-7 days'
                    out += sprintf(out, "EXTRACT(EPOCH FROM NOW() + INTERVAL '%s')::bigint", interval);
                } else {
                    out += sprintf(out, "EXTRACT(EPOCH FROM NOW())::bigint");
                }
            } else {
                // Generic case
                out += sprintf(out, "EXTRACT(EPOCH FROM %s)::bigint", arg1);
            }

            p = next;
        } else {
            *out++ = *p++;
        }
    }

    *out = '\0';
    return result;
}

// Translate datetime('now') -> NOW()
static char* translate_datetime(const char *sql) {
    char *result = str_replace_nocase(sql, "datetime('now')", "NOW()");
    return result;
}

// Add alias to subqueries in FROM clause
// PostgreSQL requires: FROM (SELECT ...) AS alias
// SQLite allows: FROM (SELECT ...)
static char* add_subquery_alias(const char *sql) {
    if (!sql) return NULL;

    // Look for pattern: FROM (SELECT ... ) followed by ) or ORDER or WHERE etc
    char *result = malloc(strlen(sql) * 2 + 100);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;
    int alias_counter = 0;

    while (*p) {
        // Look for "from (" or "FROM ("
        if ((strncasecmp(p, "from (", 6) == 0 || strncasecmp(p, "from  (", 7) == 0) &&
            (p == sql || !is_ident_char(*(p-1)))) {

            // Copy "from ("
            while (*p && *p != '(') *out++ = *p++;
            if (*p == '(') *out++ = *p++;

            // Check if this is a subquery (starts with SELECT)
            const char *after_paren = skip_ws(p);
            if (strncasecmp(after_paren, "select", 6) == 0) {
                // Find matching closing paren
                int depth = 1;
                while (*p && depth > 0) {
                    if (*p == '(') depth++;
                    else if (*p == ')') depth--;
                    if (depth > 0) *out++ = *p++;
                }

                if (*p == ')') {
                    // Check if already has alias (next non-space is AS or identifier)
                    const char *after_close = skip_ws(p + 1);
                    if (strncasecmp(after_close, "as ", 3) != 0 &&
                        strncasecmp(after_close, ")", 1) != 0 &&
                        strncasecmp(after_close, "order", 5) != 0 &&
                        strncasecmp(after_close, "where", 5) != 0 &&
                        strncasecmp(after_close, "group", 5) != 0 &&
                        strncasecmp(after_close, "having", 6) != 0 &&
                        strncasecmp(after_close, "limit", 5) != 0 &&
                        strncasecmp(after_close, "union", 5) != 0 &&
                        strncasecmp(after_close, ";", 1) != 0 &&
                        *after_close != '\0') {
                        // Has identifier after - probably already has alias
                        *out++ = *p++;
                    } else {
                        // No alias - add one
                        *out++ = *p++;  // copy )
                        out += sprintf(out, " AS subq%d", alias_counter++);
                    }
                }
            } else {
                // Not a subquery, just copy
            }
        } else {
            *out++ = *p++;
        }
    }

    *out = '\0';
    return result;
}

// Translate CASE THEN 0 ELSE 1 -> CASE THEN FALSE ELSE TRUE (and vice versa)
// SQLite uses integers in boolean context, PostgreSQL requires actual booleans
static char* translate_case_booleans(const char *sql) {
    if (!sql) return NULL;

    char *current = strdup(sql);
    if (!current) return NULL;

    char *temp;

    // THEN 0 ELSE 1 END -> THEN FALSE ELSE TRUE END
    temp = str_replace_nocase(current, " then 0 else 1 end", " THEN FALSE ELSE TRUE END");
    free(current);
    current = temp;

    // THEN 1 ELSE 0 END -> THEN TRUE ELSE FALSE END
    temp = str_replace_nocase(current, " then 1 else 0 end", " THEN TRUE ELSE FALSE END");
    free(current);
    current = temp;

    return current;
}

// Translate max(a, b) -> GREATEST(a, b) when used with 2+ arguments
// SQLite's max() with multiple args returns the largest, PostgreSQL needs GREATEST()
static char* translate_max_to_greatest(const char *sql) {
    if (!sql) return NULL;

    char *result = malloc(strlen(sql) * 2 + 1);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;

    while (*p) {
        // Look for max( - case insensitive
        if ((p == sql || !is_ident_char(*(p-1))) &&
            strncasecmp(p, "max(", 4) == 0) {

            const char *start = p;
            p += 4;  // skip "max("

            // Extract the content inside parentheses
            int depth = 1;
            const char *content_start = p;
            while (*p && depth > 0) {
                if (*p == '(') depth++;
                else if (*p == ')') depth--;
                if (depth > 0) p++;
            }

            // Check if there's a comma (meaning 2+ args)
            int has_comma = 0;
            int inner_depth = 0;
            for (const char *c = content_start; c < p; c++) {
                if (*c == '(') inner_depth++;
                else if (*c == ')') inner_depth--;
                else if (*c == ',' && inner_depth == 0) {
                    has_comma = 1;
                    break;
                }
            }

            if (has_comma) {
                // Multiple args - use GREATEST
                memcpy(out, "GREATEST(", 9);
                out += 9;
                size_t content_len = p - content_start;
                memcpy(out, content_start, content_len);
                out += content_len;
                *out++ = ')';
                p++;  // skip closing )
            } else {
                // Single arg - keep as max() (aggregate)
                size_t len = (p + 1) - start;
                memcpy(out, start, len);
                out += len;
                p++;  // skip closing )
            }
        } else {
            *out++ = *p++;
        }
    }

    *out = '\0';
    return result;
}

// Translate min(a, b) -> LEAST(a, b) when used with 2+ arguments
static char* translate_min_to_least(const char *sql) {
    if (!sql) return NULL;

    char *result = malloc(strlen(sql) * 2 + 1);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;

    while (*p) {
        // Look for min( - case insensitive
        if ((p == sql || !is_ident_char(*(p-1))) &&
            strncasecmp(p, "min(", 4) == 0) {

            const char *start = p;
            p += 4;  // skip "min("

            // Extract the content inside parentheses
            int depth = 1;
            const char *content_start = p;
            while (*p && depth > 0) {
                if (*p == '(') depth++;
                else if (*p == ')') depth--;
                if (depth > 0) p++;
            }

            // Check if there's a comma (meaning 2+ args)
            int has_comma = 0;
            int inner_depth = 0;
            for (const char *c = content_start; c < p; c++) {
                if (*c == '(') inner_depth++;
                else if (*c == ')') inner_depth--;
                else if (*c == ',' && inner_depth == 0) {
                    has_comma = 1;
                    break;
                }
            }

            if (has_comma) {
                // Multiple args - use LEAST
                memcpy(out, "LEAST(", 6);
                out += 6;
                size_t content_len = p - content_start;
                memcpy(out, content_start, content_len);
                out += content_len;
                *out++ = ')';
                p++;  // skip closing )
            } else {
                // Single arg - keep as min() (aggregate)
                size_t len = (p + 1) - start;
                memcpy(out, start, len);
                out += len;
                p++;  // skip closing )
            }
        } else {
            *out++ = *p++;
        }
    }

    *out = '\0';
    return result;
}

// Main function translator
char* sql_translate_functions(const char *sql) {
    if (!sql) return NULL;

    char *current = strdup(sql);
    if (!current) return NULL;

    // Apply translations in order
    char *temp;

    // 1. iif() -> CASE WHEN
    temp = translate_iif(current);
    free(current);
    if (!temp) return NULL;
    current = temp;

    // 2. typeof() -> pg_typeof()::text
    temp = translate_typeof(current);
    free(current);
    if (!temp) return NULL;
    current = temp;

    // 3. strftime() -> EXTRACT/TO_CHAR
    temp = translate_strftime(current);
    free(current);
    if (!temp) return NULL;
    current = temp;

    // 4. unixepoch() -> EXTRACT(EPOCH FROM ...)
    temp = translate_unixepoch(current);
    free(current);
    if (!temp) return NULL;
    current = temp;

    // 5. datetime('now') -> NOW()
    temp = translate_datetime(current);
    free(current);
    if (!temp) return NULL;
    current = temp;

    // 6. IFNULL -> COALESCE
    temp = str_replace_nocase(current, "IFNULL(", "COALESCE(");
    free(current);
    if (!temp) return NULL;
    current = temp;

    // 7. SUBSTR -> SUBSTRING
    temp = str_replace_nocase(current, "SUBSTR(", "SUBSTRING(");
    free(current);
    if (!temp) return NULL;
    current = temp;

    // 8. RANDOM() is the same in both

    // 9. LENGTH() is the same in both

    // 10. INSTR(str, substr) -> POSITION(substr IN str)
    // This is complex, skip for now

    // 11. max(a, b) with 2+ args -> GREATEST(a, b)
    // SQLite's max() can be used as scalar function with multiple args
    // PostgreSQL's max() is only aggregate, use GREATEST() for scalar
    temp = translate_max_to_greatest(current);
    free(current);
    if (!temp) return NULL;
    current = temp;

    // 12. min(a, b) with 2+ args -> LEAST(a, b)
    temp = translate_min_to_least(current);
    free(current);
    if (!temp) return NULL;
    current = temp;

    // 13. CASE THEN 0/1 -> THEN FALSE/TRUE (for boolean context)
    temp = translate_case_booleans(current);
    free(current);
    if (!temp) return NULL;
    current = temp;

    // 14. Add alias to subqueries in FROM clause
    temp = add_subquery_alias(current);
    free(current);
    if (!temp) return NULL;
    current = temp;

    return current;
}

// ============================================================================
// Type Translation
// ============================================================================

char* sql_translate_types(const char *sql) {
    if (!sql) return NULL;

    char *current = strdup(sql);
    if (!current) return NULL;

    char *temp;

    // INTEGER PRIMARY KEY AUTOINCREMENT -> SERIAL PRIMARY KEY
    temp = str_replace_nocase(current, "INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY");
    free(current);
    current = temp;

    // Remove AUTOINCREMENT (handled by SERIAL)
    temp = str_replace_nocase(current, "AUTOINCREMENT", "");
    free(current);
    current = temp;

    // dt_integer(8) -> BIGINT
    temp = str_replace_nocase(current, "dt_integer(8)", "BIGINT");
    free(current);
    current = temp;

    // integer(8) -> BIGINT
    temp = str_replace_nocase(current, "integer(8)", "BIGINT");
    free(current);
    current = temp;

    // varchar(255) -> VARCHAR(255) (same, but normalize case)
    // TEXT is the same
    // BLOB -> BYTEA
    temp = str_replace_nocase(current, " BLOB", " BYTEA");
    free(current);
    current = temp;

    // boolean DEFAULT 't' -> boolean DEFAULT TRUE
    temp = str_replace(current, "DEFAULT 't'", "DEFAULT TRUE");
    free(current);
    current = temp;

    temp = str_replace(current, "DEFAULT 'f'", "DEFAULT FALSE");
    free(current);
    current = temp;

    return current;
}

// ============================================================================
// Alias Quote Translation
// ============================================================================

// Translate backticks `column` -> "column"
// SQLite/MySQL use backticks for identifiers, PostgreSQL uses double quotes
static char* translate_backticks(const char *sql) {
    if (!sql) return NULL;

    char *result = malloc(strlen(sql) + 1);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;

    while (*p) {
        if (*p == '`') {
            *out++ = '"';  // Replace backtick with double quote
        } else {
            *out++ = *p;
        }
        p++;
    }

    *out = '\0';
    return result;
}

// Translate table.'column' -> table."column"
// SQLite allows single quotes for column names, PostgreSQL requires double quotes
static char* translate_column_quotes(const char *sql) {
    if (!sql) return NULL;

    char *result = malloc(strlen(sql) * 2 + 1);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;
    int in_string = 0;

    while (*p) {
        // Check for table.'column' pattern: preceded by dot
        if (*p == '\'' && p > sql && *(p-1) == '.') {
            *out++ = '"';  // Replace opening single with double
            p++;

            // Copy content until closing single quote
            while (*p && *p != '\'') {
                *out++ = *p++;
            }

            if (*p == '\'') {
                *out++ = '"';  // Replace closing single with double
                p++;
            }
            continue;
        }

        // Track regular string literals
        if (*p == '\'' && !in_string) {
            in_string = 1;
            *out++ = *p++;
            continue;
        }
        if (*p == '\'' && in_string) {
            // Check for escaped single quote ('')
            if (*(p+1) == '\'') {
                *out++ = *p++;
                *out++ = *p++;
                continue;
            }
            in_string = 0;
        }

        *out++ = *p++;
    }

    *out = '\0';
    return result;
}

// Translate AS 'alias' -> AS "alias"
// SQLite allows single quotes for aliases, PostgreSQL requires double quotes
static char* translate_alias_quotes(const char *sql) {
    if (!sql) return NULL;

    char *result = malloc(strlen(sql) * 2 + 1);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;
    int in_string = 0;
    char string_char = 0;

    while (*p) {
        // Track string literals (not after AS)
        if ((*p == '\'' || *p == '"') && !in_string) {
            // Check if this is after AS (alias context)
            const char *back = p - 1;
            while (back > sql && isspace(*back)) back--;

            // Check if preceded by AS
            if (back >= sql + 1 &&
                (back[-1] == 'a' || back[-1] == 'A') &&
                (back[0] == 's' || back[0] == 'S') &&
                (back == sql + 1 || !is_ident_char(back[-2]))) {

                // This is an alias - if single quote, convert to double
                if (*p == '\'') {
                    *out++ = '"';  // Replace opening single with double
                    p++;

                    // Copy content until closing single quote
                    while (*p && *p != '\'') {
                        *out++ = *p++;
                    }

                    if (*p == '\'') {
                        *out++ = '"';  // Replace closing single with double
                        p++;
                    }
                    continue;
                }
            }

            // Not an alias context - track string
            in_string = 1;
            string_char = *p;
            *out++ = *p++;
            continue;
        }

        if (in_string && *p == string_char) {
            // Check for escaped quotes (doubled)
            if (*(p+1) == string_char) {
                *out++ = *p++;
                *out++ = *p++;
                continue;
            }
            in_string = 0;
        }

        *out++ = *p++;
    }

    *out = '\0';
    return result;
}

// ============================================================================
// Keyword Translation
// ============================================================================

char* sql_translate_keywords(const char *sql) {
    if (!sql) return NULL;

    char *current = strdup(sql);
    if (!current) return NULL;

    char *temp;

    // SQLite transaction modes -> PostgreSQL BEGIN
    temp = str_replace_nocase(current, "BEGIN IMMEDIATE", "BEGIN");
    free(current);
    current = temp;

    temp = str_replace_nocase(current, "BEGIN DEFERRED", "BEGIN");
    free(current);
    current = temp;

    temp = str_replace_nocase(current, "BEGIN EXCLUSIVE", "BEGIN");
    free(current);
    current = temp;

    // REPLACE INTO -> INSERT INTO ... ON CONFLICT DO UPDATE
    // This is complex - for now just convert to INSERT
    temp = str_replace_nocase(current, "REPLACE INTO", "INSERT INTO");
    free(current);
    current = temp;

    // INSERT OR IGNORE -> INSERT ... ON CONFLICT DO NOTHING
    temp = str_replace_nocase(current, "INSERT OR IGNORE INTO", "INSERT INTO");
    free(current);
    current = temp;

    // INSERT OR REPLACE -> INSERT ... ON CONFLICT DO UPDATE
    temp = str_replace_nocase(current, "INSERT OR REPLACE INTO", "INSERT INTO");
    free(current);
    current = temp;

    // GLOB -> LIKE (patterns need conversion too, but basic replacement)
    temp = str_replace_nocase(current, " GLOB ", " LIKE ");
    free(current);
    current = temp;

    // "index" is a reserved word in PostgreSQL - but "index" (quoted) is valid
    // Only need to quote unquoted uses, but our backtick translation already
    // handles this. Remove the incorrect index_field translation.

    // AS 'alias' -> AS "alias" (SQLite allows single quotes, PostgreSQL requires double)
    temp = translate_alias_quotes(current);
    free(current);
    current = temp;

    // table.'column' -> table."column" (SQLite allows single quotes for column names)
    temp = translate_column_quotes(current);
    free(current);
    current = temp;

    // `column` -> "column" (backticks to double quotes)
    temp = translate_backticks(current);
    free(current);
    current = temp;

    // Remove COLLATE icu_root (SQLite ICU collation not available in PostgreSQL)
    temp = str_replace_nocase(current, " collate icu_root", "");
    free(current);
    current = temp;

    // Fix empty IN clause: IN () -> IN (NULL) (SQLite accepts empty, PostgreSQL doesn't)
    temp = str_replace(current, " in ()", " IN (NULL)");
    free(current);
    current = temp;
    temp = str_replace(current, " IN ()", " IN (NULL)");
    free(current);
    current = temp;

    // Remove INDEXED BY hints (SQLite-specific)
    // Pattern: "indexed by index_name" - need regex-like removal
    // For now, handle common patterns
    temp = current;
    char *indexed_pos;
    while ((indexed_pos = strcasestr(temp, " indexed by ")) != NULL) {
        // Find end of index name (next space or end of clause)
        char *end = indexed_pos + 12;  // skip " indexed by "
        while (*end && !isspace(*end) && *end != ')' && *end != ',') end++;

        // Create new string without this part
        size_t prefix_len = indexed_pos - temp;
        size_t suffix_len = strlen(end);
        char *new_str = malloc(prefix_len + suffix_len + 1);
        if (new_str) {
            memcpy(new_str, temp, prefix_len);
            memcpy(new_str + prefix_len, end, suffix_len);
            new_str[prefix_len + suffix_len] = '\0';
            if (temp != current) free(temp);
            temp = new_str;
        } else {
            break;
        }
    }
    if (temp != current) {
        free(current);
        current = temp;
    }

    return current;
}

// ============================================================================
// Main Translation Function
// ============================================================================

void sql_translator_init(void) {
    // Nothing to initialize for now
}

void sql_translator_cleanup(void) {
    // Nothing to cleanup for now
}

sql_translation_t sql_translate(const char *sqlite_sql) {
    sql_translation_t result = {0};

    if (!sqlite_sql) {
        strcpy(result.error, "NULL input SQL");
        return result;
    }

    // Step 1: Translate placeholders
    char *step1 = sql_translate_placeholders(sqlite_sql, &result.param_names, &result.param_count);
    if (!step1) {
        strcpy(result.error, "Placeholder translation failed");
        return result;
    }

    // Step 2: Translate functions
    char *step2 = sql_translate_functions(step1);
    free(step1);
    if (!step2) {
        strcpy(result.error, "Function translation failed");
        return result;
    }

    // Step 3: Translate types (mainly for DDL)
    char *step3 = sql_translate_types(step2);
    free(step2);
    if (!step3) {
        strcpy(result.error, "Type translation failed");
        return result;
    }

    // Step 4: Translate keywords
    char *step4 = sql_translate_keywords(step3);
    free(step3);
    if (!step4) {
        strcpy(result.error, "Keyword translation failed");
        return result;
    }

    result.sql = step4;
    result.success = 1;

    return result;
}

void sql_translation_free(sql_translation_t *result) {
    if (!result) return;

    if (result->sql) {
        free(result->sql);
        result->sql = NULL;
    }

    if (result->param_names) {
        for (int i = 0; i < result->param_count; i++) {
            if (result->param_names[i]) {
                free(result->param_names[i]);
            }
        }
        free(result->param_names);
        result->param_names = NULL;
    }

    result->param_count = 0;
    result->success = 0;
}

void sql_translator_free(char *sql) {
    free(sql);
}
