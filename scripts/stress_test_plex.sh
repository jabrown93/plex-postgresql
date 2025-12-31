#!/bin/bash
# Stress test Plex to try to reproduce hang
# Usage: ./stress_test_plex.sh [concurrent_requests] [total_requests]

CONCURRENT=${1:-20}
TOTAL=${2:-200}
PLEX_URL="http://localhost:32400"
TOKEN="${PLEX_TOKEN:-REDACTED_TOKEN}"

echo "=== Plex Stress Test ==="
echo "Concurrent: $CONCURRENT"
echo "Total: $TOTAL"
echo ""

# Various endpoints to hit (as string for subshell compatibility)
ENDPOINTS="/library/sections /library/sections/1/all /library/sections/2/all /library/sections/6/all /library/recentlyAdded /library/onDeck /status/sessions /identity /media/providers"

stress_request() {
    local endpoints_arr=($ENDPOINTS)
    local count=${#endpoints_arr[@]}
    local endpoint="${endpoints_arr[$((RANDOM % count))]}"
    local start=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time*1000')
    local http_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 30 \
        "${PLEX_URL}${endpoint}?X-Plex-Token=${TOKEN}" 2>/dev/null)
    local end=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time*1000')
    local duration=$((end - start))
    echo "$(date +%H:%M:%S) | ${http_code} | ${duration}ms | ${endpoint}"
}

export -f stress_request
export PLEX_URL TOKEN ENDPOINTS

echo "Starting stress test..."
echo "Time       | Code | Duration | Endpoint"
echo "-----------|------|----------|----------"

# Run concurrent requests
seq $TOTAL | xargs -P $CONCURRENT -I {} bash -c 'stress_request'

echo ""
echo "=== Test Complete ==="

# Check if Plex is still responding
echo -n "Plex status: "
if curl -s --max-time 5 "${PLEX_URL}/identity?X-Plex-Token=${TOKEN}" >/dev/null 2>&1; then
    echo "OK"
else
    echo "NOT RESPONDING - Capture with lldb!"
    PLEX_PID=$(pgrep -x "Plex Media Server" || lsof -ti :32400)
    if [[ -n "$PLEX_PID" ]]; then
        echo "Run: lldb -p $PLEX_PID -o 'bt all' -o 'quit' > /tmp/plex_hang_$(date +%Y%m%d_%H%M%S).txt 2>&1"
    fi
fi
