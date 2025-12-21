# plex-postgresql Makefile

# PostgreSQL paths (Homebrew on Apple Silicon)
PG_INCLUDE = /opt/homebrew/opt/postgresql@15/include
PG_LIB = /opt/homebrew/opt/postgresql@15/lib

# Compiler settings
CC = clang
CFLAGS = -Wall -Wextra -O2 -I$(PG_INCLUDE) -Iinclude
LDFLAGS = -L$(PG_LIB) -lpq -lsqlite3

# Output
DYLIB = db_interpose_pg.dylib
OBJECTS = src/sql_translator.o

.PHONY: all clean install test

all: $(DYLIB)

# Build the shim library
$(DYLIB): src/db_interpose_pg.c $(OBJECTS)
	$(CC) -dynamiclib -o $@ $< $(OBJECTS) $(CFLAGS) $(LDFLAGS) -flat_namespace

# Build SQL translator object
src/sql_translator.o: src/sql_translator.c include/sql_translator.h
	$(CC) -c -o $@ $< $(CFLAGS)

# Clean build artifacts
clean:
	rm -f $(DYLIB) $(OBJECTS)

# Install to system location
install: $(DYLIB)
	@mkdir -p /usr/local/lib/plex-postgresql
	cp $(DYLIB) /usr/local/lib/plex-postgresql/
	@echo "Installed to /usr/local/lib/plex-postgresql/"

# Test the shim (requires Plex to be stopped)
test: $(DYLIB)
	@echo "Testing shim library..."
	@DYLD_INSERT_LIBRARIES=./$(DYLIB) \
		PLEX_PG_HOST=localhost \
		PLEX_PG_DATABASE=plex_test \
		PLEX_PG_USER=plex \
		/bin/echo "Shim loaded successfully"

# Development: rebuild and test
dev: clean all test
