/*
 * SQL Translator - Quote/Identifier Translations
 * Converts SQLite identifier quoting to PostgreSQL style
 */

#include "sql_translator_internal.h"

// ============================================================================
// Translate backticks `column` -> "column"
// ============================================================================

char* translate_backticks(const char *sql) {
    if (!sql) return NULL;

    char *result = malloc(strlen(sql) + 1);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;

    while (*p) {
        if (*p == '`') {
            *out++ = '"';
        } else {
            *out++ = *p;
        }
        p++;
    }

    *out = '\0';
    return result;
}

// ============================================================================
// Translate table.'column' -> table."column"
// ============================================================================

char* translate_column_quotes(const char *sql) {
    if (!sql) return NULL;

    char *result = malloc(strlen(sql) * 2 + 1);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;
    int in_string = 0;

    while (*p) {
        // Check for table.'column' pattern
        if (*p == '\'' && p > sql && *(p-1) == '.') {
            *out++ = '"';
            p++;

            while (*p && *p != '\'') {
                *out++ = *p++;
            }

            if (*p == '\'') {
                *out++ = '"';
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

// ============================================================================
// Translate AS 'alias' -> AS "alias"
// ============================================================================

char* translate_alias_quotes(const char *sql) {
    if (!sql) return NULL;

    char *result = malloc(strlen(sql) * 2 + 1);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;
    int in_string = 0;
    char string_char = 0;

    while (*p) {
        if ((*p == '\'' || *p == '"') && !in_string) {
            const char *back = p - 1;
            while (back > sql && isspace(*back)) back--;

            // Check if preceded by AS
            if (back >= sql + 1 &&
                (back[-1] == 'a' || back[-1] == 'A') &&
                (back[0] == 's' || back[0] == 'S') &&
                (back == sql + 1 || !is_ident_char(back[-2]))) {

                if (*p == '\'') {
                    *out++ = '"';
                    p++;

                    while (*p && *p != '\'') {
                        *out++ = *p++;
                    }

                    if (*p == '\'') {
                        *out++ = '"';
                        p++;
                    }
                    continue;
                }
            }

            in_string = 1;
            string_char = *p;
            *out++ = *p++;
            continue;
        }

        if (in_string && *p == string_char) {
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
// Translate DDL single-quoted identifiers -> double-quoted
// ============================================================================

char* translate_ddl_quotes(const char *sql) {
    if (!sql) return NULL;

    const char *s = sql;
    while (*s && isspace(*s)) s++;
    int is_ddl = (strncasecmp(s, "CREATE", 6) == 0 ||
                  strncasecmp(s, "DROP", 4) == 0 ||
                  strncasecmp(s, "ALTER", 5) == 0);

    if (!is_ddl) {
        return strdup(sql);
    }

    char *result = malloc(strlen(sql) * 2 + 1);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;
    int in_parens = 0;

    while (*p) {
        if (*p == '(') in_parens++;
        if (*p == ')') in_parens--;

        if (*p == '\'') {
            const char *back = p - 1;
            while (back > sql && isspace(*back)) back--;

            int is_identifier = 0;

            if (back >= sql) {
                if ((back >= sql + 4 && strncasecmp(back - 4, "TABLE", 5) == 0) ||
                    (back >= sql + 4 && strncasecmp(back - 4, "INDEX", 5) == 0) ||
                    (back >= sql + 1 && strncasecmp(back - 1, "ON", 2) == 0) ||
                    (back >= sql + 5 && strncasecmp(back - 5, "UNIQUE", 6) == 0) ||
                    (back >= sql + 2 && strncasecmp(back - 2, "ADD", 3) == 0) ||
                    (back >= sql + 5 && strncasecmp(back - 5, "COLUMN", 6) == 0) ||
                    (back >= sql + 3 && strncasecmp(back - 3, "DROP", 4) == 0) ||
                    *back == '(' || *back == ',' || *back == '.') {
                    is_identifier = 1;
                }
            }

            if (p > sql) {
                const char *keyword = sql;
                while (*keyword && isspace(*keyword)) keyword++;
                if ((strncasecmp(keyword, "CREATE TABLE ", 13) == 0 ||
                     strncasecmp(keyword, "CREATE INDEX ", 13) == 0 ||
                     strncasecmp(keyword, "CREATE UNIQUE INDEX ", 20) == 0) &&
                    p > keyword) {
                    const char *check = p - 1;
                    while (check > keyword && isspace(*check)) check--;
                    if (check > keyword && (
                        (check >= keyword + 4 && strncasecmp(check - 4, "TABLE", 5) == 0) ||
                        (check >= keyword + 4 && strncasecmp(check - 4, "INDEX", 5) == 0))) {
                        is_identifier = 1;
                    }
                }
            }

            if (in_parens > 0 && back >= sql && (*back == '(' || *back == ',')) {
                is_identifier = 1;
            }

            if (is_identifier) {
                *out++ = '"';
                p++;

                while (*p && *p != '\'') {
                    *out++ = *p++;
                }

                if (*p == '\'') {
                    *out++ = '"';
                    p++;
                }
                continue;
            }
        }

        *out++ = *p++;
    }

    *out = '\0';
    return result;
}

// ============================================================================
// Add IF NOT EXISTS to CREATE TABLE/INDEX
// ============================================================================

char* add_if_not_exists(const char *sql) {
    if (!sql) return NULL;

    const char *s = sql;
    while (*s && isspace(*s)) s++;

    // CREATE TABLE
    if (strncasecmp(s, "CREATE TABLE ", 13) == 0 &&
        strncasecmp(s + 13, "IF NOT EXISTS ", 14) != 0) {
        size_t prefix_len = (s - sql) + 12;
        size_t rest_len = strlen(s + 12);
        char *result = malloc(prefix_len + 15 + rest_len + 1);
        if (!result) return NULL;

        memcpy(result, sql, prefix_len);
        memcpy(result + prefix_len, " IF NOT EXISTS", 14);
        strcpy(result + prefix_len + 14, s + 12);
        return result;
    }

    // CREATE INDEX
    if (strncasecmp(s, "CREATE INDEX ", 13) == 0 &&
        strncasecmp(s + 13, "IF NOT EXISTS ", 14) != 0) {
        size_t prefix_len = (s - sql) + 12;
        size_t rest_len = strlen(s + 12);
        char *result = malloc(prefix_len + 15 + rest_len + 1);
        if (!result) return NULL;

        memcpy(result, sql, prefix_len);
        memcpy(result + prefix_len, " IF NOT EXISTS", 14);
        strcpy(result + prefix_len + 14, s + 12);
        return result;
    }

    // CREATE UNIQUE INDEX
    if (strncasecmp(s, "CREATE UNIQUE INDEX ", 20) == 0 &&
        strncasecmp(s + 20, "IF NOT EXISTS ", 14) != 0) {
        size_t prefix_len = (s - sql) + 19;
        size_t rest_len = strlen(s + 19);
        char *result = malloc(prefix_len + 15 + rest_len + 1);
        if (!result) return NULL;

        memcpy(result, sql, prefix_len);
        memcpy(result + prefix_len, " IF NOT EXISTS", 14);
        strcpy(result + prefix_len + 14, s + 19);
        return result;
    }

    return strdup(sql);
}
