#!/bin/bash
# Update Table of Contents in source files
# Scans for function definitions and section markers

FILE="$1"

if [ -z "$FILE" ]; then
    echo "Usage: $0 <source_file.c>"
    exit 1
fi

if [ ! -f "$FILE" ]; then
    echo "File not found: $FILE"
    exit 1
fi

echo "=== Functions in $FILE ==="
echo ""

# Find all function definitions (static and non-static)
grep -n "^static.*(" "$FILE" | grep -v "^static const\|^static int \*\|^static char \*\*" | head -30
echo ""
grep -n "^char\* \|^int \|^void " "$FILE" | grep "(" | head -20

echo ""
echo "=== Section Markers ==="
grep -n "^// ====" "$FILE" | head -20

echo ""
echo "=== Summary ==="
echo "Total lines: $(wc -l < "$FILE")"
echo "Functions: $(grep -c "^static.*(.*).*{$\|^char\* .*(.*).*{\|^int .*(.*).*{\|^void .*(.*).*{" "$FILE")"
echo "Sections: $(grep -c "^// ====" "$FILE")"
