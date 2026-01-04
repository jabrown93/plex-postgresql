# Dockerfile for plex-postgresql
# Build with glibc (same as linuxserver/plex base) for compatibility

FROM linuxserver/plex:latest AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libsqlite3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY src/ src/
COPY include/ include/

# Build shim with glibc, statically link libpq
RUN gcc -shared -fPIC -o db_interpose_pg.so \
    src/db_interpose_pg_linux.c \
    src/sql_translator.c src/sql_tr_helpers.c src/sql_tr_placeholders.c \
    src/sql_tr_functions.c src/sql_tr_query.c src/sql_tr_groupby.c \
    src/sql_tr_types.c src/sql_tr_quotes.c src/sql_tr_keywords.c \
    src/sql_tr_upsert.c src/pg_config.c src/pg_logging.c \
    src/pg_client.c src/pg_statement.c \
    -I/usr/include/postgresql -Iinclude -Isrc \
    -lpq -lsqlite3 -lpthread -Wl,-rpath,/usr/lib

FROM linuxserver/plex:latest

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy shim
RUN mkdir -p /usr/local/lib/plex-postgresql
COPY --from=builder /build/db_interpose_pg.so /usr/local/lib/plex-postgresql/

# Copy schema file
COPY schema/plex_schema.sql /usr/local/lib/plex-postgresql/

# Copy and setup custom entrypoint
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Use custom entrypoint
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
