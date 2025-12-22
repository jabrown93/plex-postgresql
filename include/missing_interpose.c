
// ============================================================================
// Missing Interpositions for libpython compatibility
// ============================================================================

static int my_sqlite3_enable_shared_cache(int enable) {
    static int (*orig)(int) = NULL;
    if (!orig) orig = (int(*)(int)) resolve_sqlite_symbol("sqlite3_enable_shared_cache");
    return orig ? orig(enable) : SQLITE_ERROR;
}

static void my_sqlite3_interrupt(sqlite3 *db) {
    static void (*orig)(sqlite3*) = NULL;
    if (!orig) orig = (void(*)(sqlite3*)) resolve_sqlite_symbol("sqlite3_interrupt");
    if (orig) orig(db);
}

static int my_sqlite3_busy_timeout(sqlite3 *db, int ms) {
    static int (*orig)(sqlite3*, int) = NULL;
    if (!orig) orig = (int(*)(sqlite3*, int)) resolve_sqlite_symbol("sqlite3_busy_timeout");
    return orig ? orig(db, ms) : SQLITE_ERROR;
}

static int my_sqlite3_complete(const char *sql) {
    static int (*orig)(const char*) = NULL;
    if (!orig) orig = (int(*)(const char*)) resolve_sqlite_symbol("sqlite3_complete");
    return orig ? orig(sql) : SQLITE_NOMEM;
}

static int my_sqlite3_create_collation(sqlite3* db, const char* zName, int eTextRep, void* pArg, int(*xCompare)(void*,int,const void*,int,const void*)) {
    static int (*orig)(sqlite3*, const char*, int, void*, int(*)(void*,int,const void*,int,const void*)) = NULL;
    if (!orig) orig = (int(*)(sqlite3*, const char*, int, void*, int(*)(void*,int,const void*,int,const void*))) resolve_sqlite_symbol("sqlite3_create_collation");
    return orig ? orig(db, zName, eTextRep, pArg, xCompare) : SQLITE_ERROR;
}

static int my_sqlite3_create_function(sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep, void *pApp, void (*xFunc)(sqlite3_context*,int,sqlite3_value**), void (*xStep)(sqlite3_context*,int,sqlite3_value**), void (*xFinal)(sqlite3_context*)) {
    static int (*orig)(sqlite3*, const char*, int, int, void*, void*, void*, void*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*, const char*, int, int, void*, void*, void*, void*)) resolve_sqlite_symbol("sqlite3_create_function");
    return orig ? orig(db, zFunctionName, nArg, eTextRep, pApp, xFunc, xStep, xFinal) : SQLITE_ERROR;
}

static int my_sqlite3_errcode(sqlite3 *db) {
    static int (*orig)(sqlite3*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*)) resolve_sqlite_symbol("sqlite3_errcode");
    return orig ? orig(db) : SQLITE_OK;
}

static const char *my_sqlite3_errmsg(sqlite3 *db) {
    static const char* (*orig)(sqlite3*) = NULL;
    if (!orig) orig = (const char*(*)(sqlite3*)) resolve_sqlite_symbol("sqlite3_errmsg");
    return orig ? orig(db) : "Shim Error";
}

static int my_sqlite3_get_autocommit(sqlite3 *db) {
    static int (*orig)(sqlite3*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*)) resolve_sqlite_symbol("sqlite3_get_autocommit");
    return orig ? orig(db) : 0;
}

static const char *my_sqlite3_libversion(void) {
    static const char* (*orig)(void) = NULL;
    if (!orig) orig = (const char*(*)(void)) resolve_sqlite_symbol("sqlite3_libversion");
    return orig ? orig() : "Unknown";
}

static void my_sqlite3_progress_handler(sqlite3 *db, int nOps, int (*xProgress)(void*), void *pArg) {
    static void (*orig)(sqlite3*, int, int (*)(void*), void*) = NULL;
    if (!orig) orig = (void(*)(sqlite3*, int, int (*)(void*), void*)) resolve_sqlite_symbol("sqlite3_progress_handler");
    if (orig) orig(db, nOps, xProgress, pArg);
}

static int my_sqlite3_set_authorizer(sqlite3 *db, int (*xAuth)(void*,int,const char*,const char*,const char*,const char*), void *pUserData) {
    static int (*orig)(sqlite3*, int (*)(void*,int,const char*,const char*,const char*,const char*), void*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*, int (*)(void*,int,const char*,const char*,const char*,const char*), void*)) resolve_sqlite_symbol("sqlite3_set_authorizer");
    return orig ? orig(db, xAuth, pUserData) : SQLITE_ERROR;
}

static int my_sqlite3_total_changes(sqlite3 *db) {
    static int (*orig)(sqlite3*) = NULL;
    if (!orig) orig = (int(*)(sqlite3*)) resolve_sqlite_symbol("sqlite3_total_changes");
    return orig ? orig(db) : 0;
}

static void *my_sqlite3_user_data(sqlite3_context *ctx) {
    static void* (*orig)(sqlite3_context*) = NULL;
    if (!orig) orig = (void*(*)(sqlite3_context*)) resolve_sqlite_symbol("sqlite3_user_data");
    return orig ? orig(ctx) : NULL;
}

static void *my_sqlite3_aggregate_context(sqlite3_context *ctx, int nBytes) {
    static void* (*orig)(sqlite3_context*, int) = NULL;
    if (!orig) orig = (void*(*)(sqlite3_context*, int)) resolve_sqlite_symbol("sqlite3_aggregate_context");
    return orig ? orig(ctx, nBytes) : NULL;
}

static void my_sqlite3_result_blob(sqlite3_context *ctx, const void *data, int len, void(*destructor)(void*)) {
    static void (*orig)(sqlite3_context*, const void*, int, void(*)(void*)) = NULL;
    if (!orig) orig = (void(*)(sqlite3_context*, const void*, int, void(*)(void*))) resolve_sqlite_symbol("sqlite3_result_blob");
    if (orig) orig(ctx, data, len, destructor);
}

static void my_sqlite3_result_double(sqlite3_context *ctx, double val) {
    static void (*orig)(sqlite3_context*, double) = NULL;
    if (!orig) orig = (void(*)(sqlite3_context*, double)) resolve_sqlite_symbol("sqlite3_result_double");
    if (orig) orig(ctx, val);
}

static void my_sqlite3_result_error(sqlite3_context *ctx, const char *msg, int len) {
    static void (*orig)(sqlite3_context*, const char*, int) = NULL;
    if (!orig) orig = (void(*)(sqlite3_context*, const char*, int)) resolve_sqlite_symbol("sqlite3_result_error");
    if (orig) orig(ctx, msg, len);
}

static void my_sqlite3_result_int64(sqlite3_context *ctx, sqlite3_int64 val) {
    static void (*orig)(sqlite3_context*, sqlite3_int64) = NULL;
    if (!orig) orig = (void(*)(sqlite3_context*, sqlite3_int64)) resolve_sqlite_symbol("sqlite3_result_int64");
    if (orig) orig(ctx, val);
}

static void my_sqlite3_result_null(sqlite3_context *ctx) {
    static void (*orig)(sqlite3_context*) = NULL;
    if (!orig) orig = (void(*)(sqlite3_context*)) resolve_sqlite_symbol("sqlite3_result_null");
    if (orig) orig(ctx);
}

static void my_sqlite3_result_text(sqlite3_context *ctx, const char *str, int len, void(*destructor)(void*)) {
    static void (*orig)(sqlite3_context*, const char*, int, void(*)(void*)) = NULL;
    if (!orig) orig = (void(*)(sqlite3_context*, const char*, int, void(*)(void*))) resolve_sqlite_symbol("sqlite3_result_text");
    if (orig) orig(ctx, str, len, destructor);
}

static const void *my_sqlite3_value_blob(sqlite3_value *val) {
    static const void* (*orig)(sqlite3_value*) = NULL;
    if (!orig) orig = (const void*(*)(sqlite3_value*)) resolve_sqlite_symbol("sqlite3_value_blob");
    return orig ? orig(val) : NULL;
}

static int my_sqlite3_value_bytes(sqlite3_value *val) {
    static int (*orig)(sqlite3_value*) = NULL;
    if (!orig) orig = (int(*)(sqlite3_value*)) resolve_sqlite_symbol("sqlite3_value_bytes");
    return orig ? orig(val) : 0;
}

static double my_sqlite3_value_double(sqlite3_value *val) {
    static double (*orig)(sqlite3_value*) = NULL;
    if (!orig) orig = (double(*)(sqlite3_value*)) resolve_sqlite_symbol("sqlite3_value_double");
    return orig ? orig(val) : 0.0;
}

static sqlite3_int64 my_sqlite3_value_int64(sqlite3_value *val) {
    static sqlite3_int64 (*orig)(sqlite3_value*) = NULL;
    if (!orig) orig = (sqlite3_int64(*)(sqlite3_value*)) resolve_sqlite_symbol("sqlite3_value_int64");
    return orig ? orig(val) : 0;
}

static const unsigned char *my_sqlite3_value_text(sqlite3_value *val) {
    static const unsigned char* (*orig)(sqlite3_value*) = NULL;
    if (!orig) orig = (const unsigned char*(*)(sqlite3_value*)) resolve_sqlite_symbol("sqlite3_value_text");
    return orig ? orig(val) : NULL;
}

static int my_sqlite3_value_type(sqlite3_value *val) {
    static int (*orig)(sqlite3_value*) = NULL;
    if (!orig) orig = (int(*)(sqlite3_value*)) resolve_sqlite_symbol("sqlite3_value_type");
    return orig ? orig(val) : SQLITE_NULL;
}

static int my_sqlite3_bind_parameter_count(sqlite3_stmt *pStmt) {
    static int (*orig)(sqlite3_stmt*) = NULL;
    if (!orig) orig = (int(*)(sqlite3_stmt*)) resolve_sqlite_symbol("sqlite3_bind_parameter_count");
    return orig ? orig(pStmt) : 0;
}

static const char *my_sqlite3_bind_parameter_name(sqlite3_stmt *pStmt, int idx) {
    static const char* (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = (const char*(*)(sqlite3_stmt*, int)) resolve_sqlite_symbol("sqlite3_bind_parameter_name");
    return orig ? orig(pStmt, idx) : NULL;
}

static const char *my_sqlite3_column_decltype(sqlite3_stmt *pStmt, int idx) {
    static const char* (*orig)(sqlite3_stmt*, int) = NULL;
    if (!orig) orig = (const char*(*)(sqlite3_stmt*, int)) resolve_sqlite_symbol("sqlite3_column_decltype");
    return orig ? orig(pStmt, idx) : NULL;
}

// Interpose Definitions
DYLD_INTERPOSE(my_sqlite3_enable_shared_cache, sqlite3_enable_shared_cache)
DYLD_INTERPOSE(my_sqlite3_interrupt, sqlite3_interrupt)
DYLD_INTERPOSE(my_sqlite3_busy_timeout, sqlite3_busy_timeout)
DYLD_INTERPOSE(my_sqlite3_complete, sqlite3_complete)
DYLD_INTERPOSE(my_sqlite3_create_collation, sqlite3_create_collation)
DYLD_INTERPOSE(my_sqlite3_create_function, sqlite3_create_function)
DYLD_INTERPOSE(my_sqlite3_errcode, sqlite3_errcode)
DYLD_INTERPOSE(my_sqlite3_errmsg, sqlite3_errmsg)
DYLD_INTERPOSE(my_sqlite3_get_autocommit, sqlite3_get_autocommit)
DYLD_INTERPOSE(my_sqlite3_libversion, sqlite3_libversion)
DYLD_INTERPOSE(my_sqlite3_progress_handler, sqlite3_progress_handler)
DYLD_INTERPOSE(my_sqlite3_set_authorizer, sqlite3_set_authorizer)
DYLD_INTERPOSE(my_sqlite3_total_changes, sqlite3_total_changes)
DYLD_INTERPOSE(my_sqlite3_user_data, sqlite3_user_data)
DYLD_INTERPOSE(my_sqlite3_aggregate_context, sqlite3_aggregate_context)
DYLD_INTERPOSE(my_sqlite3_result_blob, sqlite3_result_blob)
DYLD_INTERPOSE(my_sqlite3_result_double, sqlite3_result_double)
DYLD_INTERPOSE(my_sqlite3_result_error, sqlite3_result_error)
DYLD_INTERPOSE(my_sqlite3_result_int64, sqlite3_result_int64)
DYLD_INTERPOSE(my_sqlite3_result_null, sqlite3_result_null)
DYLD_INTERPOSE(my_sqlite3_result_text, sqlite3_result_text)
DYLD_INTERPOSE(my_sqlite3_value_blob, sqlite3_value_blob)
DYLD_INTERPOSE(my_sqlite3_value_bytes, sqlite3_value_bytes)
DYLD_INTERPOSE(my_sqlite3_value_double, sqlite3_value_double)
DYLD_INTERPOSE(my_sqlite3_value_int64, sqlite3_value_int64)
DYLD_INTERPOSE(my_sqlite3_value_text, sqlite3_value_text)
DYLD_INTERPOSE(my_sqlite3_value_type, sqlite3_value_type)
DYLD_INTERPOSE(my_sqlite3_bind_parameter_count, sqlite3_bind_parameter_count)
DYLD_INTERPOSE(my_sqlite3_bind_parameter_name, sqlite3_bind_parameter_name)
DYLD_INTERPOSE(my_sqlite3_column_decltype, sqlite3_column_decltype)
