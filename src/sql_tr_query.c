/*
 * SQL Translator - Query Structure Translations
 * Fixes for PostgreSQL query structure requirements
 */

#include "sql_translator_internal.h"

// ============================================================================
// Fix GROUP BY strict mode - add missing columns
// ============================================================================

char* fix_group_by_strict(const char *sql) {
    if (!sql) return NULL;

    // Only process if it has GROUP BY
    if (!strcasestr(sql, "group by")) {
        return strdup(sql);
    }

    // Special case: metadata_item_clusterings query with missing column
    if (strcasestr(sql, "metadata_item_clusterings") &&
        strcasestr(sql, "metadata_item_cluster_id") &&
        strcasestr(sql, "group by") &&
        strcasestr(sql, "metadata_item_id")) {

        char *group_pos = strcasestr(sql, "group by");
        if (group_pos) {
            char *having_pos = strcasestr(group_pos, "having");
            char *end_pos = having_pos ? having_pos : (group_pos + strlen(group_pos));

            size_t group_clause_len = end_pos - group_pos;
            char *group_clause = malloc(group_clause_len + 1);
            if (group_clause) {
                memcpy(group_clause, group_pos, group_clause_len);
                group_clause[group_clause_len] = '\0';

                if (!strcasestr(group_clause, "metadata_item_cluster_id")) {
                    char *result = str_replace_nocase(sql,
                        "group by metadata_item_clusterings.metadata_item_id",
                        "group by metadata_item_clusterings.metadata_item_id,metadata_item_clusterings.metadata_item_cluster_id");
                    free(group_clause);
                    return result ? result : strdup(sql);
                }
                free(group_clause);
            }
        }
    }

    return strdup(sql);
}

// ============================================================================
// Add alias to subqueries in FROM clause
// PostgreSQL requires: FROM (SELECT ...) AS alias
// ============================================================================

char* add_subquery_alias(const char *sql) {
    if (!sql) return NULL;

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
                    // Check if already has alias
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
            }
        } else {
            *out++ = *p++;
        }
    }

    *out = '\0';
    return result;
}

// ============================================================================
// Translate CASE with mixed boolean/integer types
// ============================================================================

char* translate_case_booleans(const char *sql) {
    if (!sql) return NULL;

    char *current = strdup(sql);
    if (!current) return NULL;

    char *temp;

    temp = str_replace_nocase(current, " else 1 end)", " else true end)");
    free(current);
    if (!temp) return NULL;
    current = temp;

    temp = str_replace_nocase(current, " else 0 end)", " else false end)");
    free(current);
    if (!temp) return NULL;
    current = temp;

    temp = str_replace_nocase(current, "then 0 else true end)", "then false else true end)");
    free(current);
    if (!temp) return NULL;
    current = temp;

    temp = str_replace_nocase(current, "then 1 else false end)", "then true else false end)");
    free(current);
    if (!temp) return NULL;
    current = temp;

    return current;
}

// ============================================================================
// max(a, b) -> GREATEST(a, b) when used with 2+ arguments
// ============================================================================

char* translate_max_to_greatest(const char *sql) {
    if (!sql) return NULL;

    char *result = malloc(strlen(sql) * 2 + 1);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;

    while (*p) {
        if ((p == sql || !is_ident_char(*(p-1))) &&
            strncasecmp(p, "max(", 4) == 0) {

            const char *start = p;
            p += 4;

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
                memcpy(out, "GREATEST(", 9);
                out += 9;
                size_t content_len = p - content_start;
                memcpy(out, content_start, content_len);
                out += content_len;
                *out++ = ')';
                p++;
            } else {
                size_t len = (p + 1) - start;
                memcpy(out, start, len);
                out += len;
                p++;
            }
        } else {
            *out++ = *p++;
        }
    }

    *out = '\0';
    return result;
}

// ============================================================================
// min(a, b) -> LEAST(a, b) when used with 2+ arguments
// ============================================================================

char* translate_min_to_least(const char *sql) {
    if (!sql) return NULL;

    char *result = malloc(strlen(sql) * 2 + 1);
    if (!result) return NULL;

    char *out = result;
    const char *p = sql;

    while (*p) {
        if ((p == sql || !is_ident_char(*(p-1))) &&
            strncasecmp(p, "min(", 4) == 0) {

            const char *start = p;
            p += 4;

            int depth = 1;
            const char *content_start = p;
            while (*p && depth > 0) {
                if (*p == '(') depth++;
                else if (*p == ')') depth--;
                if (depth > 0) p++;
            }

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
                memcpy(out, "LEAST(", 6);
                out += 6;
                size_t content_len = p - content_start;
                memcpy(out, content_start, content_len);
                out += content_len;
                *out++ = ')';
                p++;
            } else {
                size_t len = (p + 1) - start;
                memcpy(out, start, len);
                out += len;
                p++;
            }
        } else {
            *out++ = *p++;
        }
    }

    *out = '\0';
    return result;
}

// ============================================================================
// Translate SQLite FTS4 queries to PostgreSQL ILIKE queries
// ============================================================================

char* translate_fts(const char *sql) {
    if (!sql) return NULL;

    if (!strcasestr(sql, "fts4_")) {
        return strdup(sql);
    }

    char *result = malloc(strlen(sql) * 2 + 100);
    if (!result) return NULL;

    strcpy(result, sql);

    // Remove JOIN with fts4_metadata_titles_icu
    char *join_start = strcasestr(result, "join fts4_metadata_titles_icu");
    if (join_start) {
        char *join_end = join_start;
        while (*join_end) {
            if (strncasecmp(join_end, " where ", 7) == 0 ||
                strncasecmp(join_end, " join ", 6) == 0 ||
                strncasecmp(join_end, " left ", 6) == 0 ||
                strncasecmp(join_end, " group ", 7) == 0 ||
                strncasecmp(join_end, " order ", 7) == 0) {
                break;
            }
            join_end++;
        }
        memmove(join_start, join_end, strlen(join_end) + 1);
    }

    // Remove JOIN with fts4_tag_titles_icu
    join_start = strcasestr(result, "join fts4_tag_titles_icu");
    if (join_start) {
        char *join_end = join_start;
        while (*join_end) {
            if (strncasecmp(join_end, " where ", 7) == 0 ||
                strncasecmp(join_end, " join ", 6) == 0 ||
                strncasecmp(join_end, " left ", 6) == 0 ||
                strncasecmp(join_end, " group ", 7) == 0 ||
                strncasecmp(join_end, " order ", 7) == 0) {
                break;
            }
            join_end++;
        }
        memmove(join_start, join_end, strlen(join_end) + 1);
    }

    // Translate MATCH clauses
    char *match_pos;
    while ((match_pos = strcasestr(result, "fts4_metadata_titles_icu.title match ")) != NULL) {
        char *quote_start = strchr(match_pos, '\'');
        if (!quote_start) break;
        quote_start++;
        char *quote_end = strchr(quote_start, '\'');
        if (!quote_end) break;

        char search_term[256] = {0};
        size_t term_len = quote_end - quote_start;
        if (term_len > 254) term_len = 254;
        strncpy(search_term, quote_start, term_len);

        if (term_len > 0 && search_term[term_len-1] == '*') {
            search_term[term_len-1] = '\0';
        }

        char replacement[512];
        snprintf(replacement, sizeof(replacement), "metadata_items.title ILIKE '%%%s%%'", search_term);

        size_t old_len = (quote_end + 1) - match_pos;
        size_t new_len = strlen(replacement);

        if (new_len > old_len) {
            memmove(match_pos + new_len, quote_end + 1, strlen(quote_end + 1) + 1);
        } else {
            memmove(match_pos + new_len, quote_end + 1, strlen(quote_end + 1) + 1);
        }
        memcpy(match_pos, replacement, new_len);
    }

    // Handle title_sort match
    while ((match_pos = strcasestr(result, "fts4_metadata_titles_icu.title_sort match ")) != NULL) {
        char *quote_start = strchr(match_pos, '\'');
        if (!quote_start) break;
        quote_start++;
        char *quote_end = strchr(quote_start, '\'');
        if (!quote_end) break;

        char search_term[256] = {0};
        size_t term_len = quote_end - quote_start;
        if (term_len > 254) term_len = 254;
        strncpy(search_term, quote_start, term_len);
        if (term_len > 0 && search_term[term_len-1] == '*') {
            search_term[term_len-1] = '\0';
        }

        char replacement[512];
        snprintf(replacement, sizeof(replacement), "metadata_items.title_sort ILIKE '%%%s%%'", search_term);

        size_t old_len = (quote_end + 1) - match_pos;
        size_t new_len = strlen(replacement);

        memmove(match_pos + new_len, quote_end + 1, strlen(quote_end + 1) + 1);
        memcpy(match_pos, replacement, new_len);
    }

    // Handle tag titles
    while (1) {
        match_pos = strcasestr(result, "fts4_tag_titles_icu.title match ");
        if (!match_pos) {
            match_pos = strcasestr(result, "fts4_tag_titles_icu.tag match ");
        }
        if (!match_pos) break;
        char *quote_start = strchr(match_pos, '\'');
        if (!quote_start) break;
        quote_start++;
        char *quote_end = strchr(quote_start, '\'');
        if (!quote_end) break;

        char search_term[256] = {0};
        size_t term_len = quote_end - quote_start;
        if (term_len > 254) term_len = 254;
        strncpy(search_term, quote_start, term_len);
        if (term_len > 0 && search_term[term_len-1] == '*') {
            search_term[term_len-1] = '\0';
        }

        char replacement[512];
        snprintf(replacement, sizeof(replacement), "tags.tag ILIKE '%%%s%%'", search_term);

        size_t old_len = (quote_end + 1) - match_pos;
        size_t new_len = strlen(replacement);

        memmove(match_pos + new_len, quote_end + 1, strlen(quote_end + 1) + 1);
        memcpy(match_pos, replacement, new_len);
    }

    return result;
}

// ============================================================================
// Fix forward references in self-joins
// ============================================================================

char* fix_forward_reference_joins(const char *sql) {
    if (!sql) return NULL;

    const char *first_alias_join = strcasestr(sql, "join metadata_items as ");
    if (!first_alias_join) return strdup(sql);

    const char *unaliased_join = strcasestr(sql, " join metadata_items on ");
    if (!unaliased_join) return strdup(sql);

    if (unaliased_join < first_alias_join) return strdup(sql);

    // Check for forward reference
    const char *check = first_alias_join;
    int has_forward_ref = 0;
    while (check < unaliased_join) {
        if (strncasecmp(check, "metadata_items.", 15) == 0) {
            has_forward_ref = 1;
            break;
        }
        check++;
    }

    if (!has_forward_ref) return strdup(sql);

    // Move the unaliased join before the aliased joins
    const char *move_start = unaliased_join + 1;

    const char *move_end = move_start;
    int paren_depth = 0;
    while (*move_end) {
        if (*move_end == '(') paren_depth++;
        else if (*move_end == ')') paren_depth--;
        else if (paren_depth == 0) {
            if (strncasecmp(move_end, " join ", 6) == 0 ||
                strncasecmp(move_end, " left ", 6) == 0 ||
                strncasecmp(move_end, " where ", 7) == 0 ||
                strncasecmp(move_end, " group ", 7) == 0 ||
                strncasecmp(move_end, " order ", 7) == 0 ||
                strncasecmp(move_end, " limit ", 7) == 0) {
                break;
            }
        }
        move_end++;
    }

    size_t prefix_len = first_alias_join - sql;
    size_t move_len = move_end - move_start;
    size_t middle_len = move_start - first_alias_join - 1;
    size_t suffix_len = strlen(move_end);

    size_t result_len = prefix_len + move_len + 1 + middle_len + suffix_len + 1;
    char *result = malloc(result_len);
    if (!result) return strdup(sql);

    char *out = result;

    memcpy(out, sql, prefix_len);
    out += prefix_len;

    memcpy(out, move_start, move_len);
    out += move_len;

    *out++ = ' ';

    memcpy(out, first_alias_join, middle_len);
    out += middle_len;

    memcpy(out, move_end, suffix_len);
    out += suffix_len;

    *out = '\0';

    return result;
}

// ============================================================================
// Remove DISTINCT when ORDER BY is present
// ============================================================================

char* translate_distinct_orderby(const char *sql) {
    if (!sql) return NULL;

    if (!strcasestr(sql, "distinct")) {
        return strdup(sql);
    }

    if (strcasestr(sql, "group by") || strcasestr(sql, "order by")) {
        char *result = str_replace_nocase(sql, "select distinct", "select");
        return result ? result : strdup(sql);
    }

    return strdup(sql);
}
