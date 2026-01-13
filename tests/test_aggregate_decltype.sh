#!/bin/bash
#
# Integration test for aggregate function decltype workaround
# 
# Tests the fix for std::bad_cast bug in Plex's SOCI version
# where BIGINT aggregate functions cause crashes in TV shows endpoint
#
# Related: SOCI Issue #1190, v0.8.12 fix
#

set -e

echo "========================================"
echo "Aggregate Decltype Workaround Test"
echo "========================================"
echo ""

# Configuration
PLEX_TOKEN="${PLEX_TOKEN:-***REMOVED***}"
PLEX_URL="http://127.0.0.1:32400"
LOG_FILE="/tmp/plex_redirect_pg.log"
PLEX_LOG="$HOME/Library/Logs/Plex Media Server/Plex Media Server.log"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0

test_endpoint() {
    local name="$1"
    local section="$2"
    local type="$3"
    
    echo -n "Testing $name endpoint... "
    
    HTTP_CODE=$(curl -s -w "%{http_code}" -o /dev/null \
        "$PLEX_URL/library/sections/$section/all?type=$type&X-Plex-Token=$PLEX_TOKEN")
    
    if [ "$HTTP_CODE" = "200" ]; then
        echo -e "${GREEN}✓ PASS${NC} (HTTP $HTTP_CODE)"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗ FAIL${NC} (HTTP $HTTP_CODE)"
        ((TESTS_FAILED++))
        return 1
    fi
}

check_no_exceptions() {
    echo -n "Checking for std::bad_cast exceptions... "
    
    if [ ! -f "$PLEX_LOG" ]; then
        echo -e "${YELLOW}⚠ SKIP${NC} (Plex log not found)"
        return 0
    fi
    
    # Check last 100 lines for bad_cast (recent activity)
    EXCEPTIONS=$(tail -100 "$PLEX_LOG" 2>/dev/null | grep -c "std::bad_cast" || true)
    
    if [ "$EXCEPTIONS" -eq 0 ]; then
        echo -e "${GREEN}✓ PASS${NC} (no exceptions found)"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗ FAIL${NC} ($EXCEPTIONS exceptions found)"
        echo "  Recent exceptions:"
        tail -100 "$PLEX_LOG" | grep "std::bad_cast" | tail -3 | sed 's/^/    /'
        ((TESTS_FAILED++))
        return 1
    fi
}

check_aggregate_workaround() {
    echo -n "Checking aggregate TEXT workaround is active... "
    
    if [ ! -f "$LOG_FILE" ]; then
        echo -e "${YELLOW}⚠ SKIP${NC} (shim log not found)"
        return 0
    fi
    
    # Trigger a fresh request to generate log entries
    curl -s "$PLEX_URL/library/sections/6/all?type=2&X-Plex-Token=$PLEX_TOKEN" > /dev/null 2>&1
    sleep 1
    
    # Check if aggregate workaround is being used
    WORKAROUND=$(grep -c "DECLTYPE_AGGREGATE.*TEXT" "$LOG_FILE" 2>/dev/null || true)
    
    if [ "$WORKAROUND" -gt 0 ]; then
        echo -e "${GREEN}✓ PASS${NC} (found $WORKAROUND occurrences)"
        echo "  Sample log:"
        grep "DECLTYPE_AGGREGATE.*TEXT" "$LOG_FILE" | tail -1 | sed 's/^/    /'
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗ FAIL${NC} (workaround not detected)"
        ((TESTS_FAILED++))
        return 1
    fi
}

# Check if Plex is running
echo "Checking Plex Media Server status..."
if ! pgrep -q "Plex Media Server"; then
    echo -e "${RED}ERROR: Plex Media Server is not running${NC}"
    echo "Please start Plex with the shim loaded:"
    echo "  DYLD_INSERT_LIBRARIES=\"./db_interpose_pg.dylib\" \\"
    echo "  \"/Applications/Plex Media Server.app/Contents/MacOS/Plex Media Server.original\" &"
    exit 1
fi
echo -e "${GREEN}✓${NC} Plex is running"
echo ""

# Run tests
echo "Running endpoint tests:"
echo "----------------------"
test_endpoint "Movies" "1" "1"
test_endpoint "TV Shows" "6" "2"
echo ""

echo "Running diagnostic tests:"
echo "------------------------"
check_no_exceptions
check_aggregate_workaround
echo ""

# Summary
echo "========================================"
echo "Test Summary"
echo "========================================"
echo -e "Passed: ${GREEN}$TESTS_PASSED${NC}"
echo -e "Failed: ${RED}$TESTS_FAILED${NC}"
echo ""

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed! ✓${NC}"
    echo ""
    echo "The aggregate decltype workaround is working correctly."
    echo "TV shows endpoint no longer throws std::bad_cast exceptions."
    exit 0
else
    echo -e "${RED}Some tests failed ✗${NC}"
    echo ""
    echo "Troubleshooting:"
    echo "  1. Check if shim is loaded: grep 'SHIM_INIT' $LOG_FILE"
    echo "  2. Check Plex version: should be compatible with TEXT decltype workaround"
    echo "  3. Check logs: tail -50 $PLEX_LOG"
    exit 1
fi
