#!/bin/bash
#
# Plex PostgreSQL Shim - Installation Script
# Version: 0.8.12
#

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Plex PostgreSQL Shim v0.8.12${NC}"
echo -e "${BLUE}Installation Script${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Default installation directory
INSTALL_DIR="${HOME}/.plex-postgresql"
PLEX_APP="/Applications/Plex Media Server.app"
PLEX_BINARY="${PLEX_APP}/Contents/MacOS/Plex Media Server"

# Check if running on macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
    echo -e "${RED}Error: This installer is for macOS only${NC}"
    exit 1
fi

# Check if running on ARM64
ARCH=$(uname -m)
if [[ "$ARCH" != "arm64" ]]; then
    echo -e "${RED}Error: This binary is for Apple Silicon (ARM64) only${NC}"
    echo "Your architecture: $ARCH"
    exit 1
fi

# Check if Plex is installed
if [ ! -d "$PLEX_APP" ]; then
    echo -e "${RED}Error: Plex Media Server not found at $PLEX_APP${NC}"
    exit 1
fi

echo -e "${GREEN}✓${NC} macOS ARM64 detected"
echo -e "${GREEN}✓${NC} Plex Media Server found"
echo ""

# Ask for installation directory
echo -e "Installation directory: ${BLUE}$INSTALL_DIR${NC}"
read -p "Use this directory? [Y/n] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]] && [[ ! -z $REPLY ]]; then
    read -p "Enter installation directory: " INSTALL_DIR
fi

# Create installation directory
echo -e "\n${YELLOW}Creating installation directory...${NC}"
mkdir -p "$INSTALL_DIR"

# Copy files
echo -e "${YELLOW}Installing files...${NC}"
cp db_interpose_pg.dylib "$INSTALL_DIR/"
cp VERSION "$INSTALL_DIR/"
cp RELEASE_NOTES.md "$INSTALL_DIR/" 2>/dev/null || true
cp test_aggregate_decltype.sh "$INSTALL_DIR/" 2>/dev/null || true
chmod +x "$INSTALL_DIR/db_interpose_pg.dylib"
chmod +x "$INSTALL_DIR/test_aggregate_decltype.sh" 2>/dev/null || true

echo -e "${GREEN}✓${NC} Files installed to $INSTALL_DIR"

# Backup original Plex binary if needed
if [ ! -f "${PLEX_BINARY}.original" ]; then
    echo -e "\n${YELLOW}Backing up Plex Media Server binary...${NC}"
    sudo cp "${PLEX_BINARY}" "${PLEX_BINARY}.original"
    echo -e "${GREEN}✓${NC} Backup created: ${PLEX_BINARY}.original"
else
    echo -e "${GREEN}✓${NC} Backup already exists"
fi

# Create wrapper script
echo -e "\n${YELLOW}Creating launcher script...${NC}"
cat > "$INSTALL_DIR/start_plex.sh" << EOF
#!/bin/bash
#
# Plex Media Server with PostgreSQL Shim
#

# Stop existing Plex
pkill "Plex Media Server" 2>/dev/null || true
sleep 2

# Start Plex with shim
DYLD_INSERT_LIBRARIES="$INSTALL_DIR/db_interpose_pg.dylib" \\
  "${PLEX_BINARY}.original" &

echo "Plex Media Server started with PostgreSQL shim"
echo "Log: /tmp/plex_redirect_pg.log"
EOF

chmod +x "$INSTALL_DIR/start_plex.sh"
echo -e "${GREEN}✓${NC} Launcher created: $INSTALL_DIR/start_plex.sh"

# Create stop script
cat > "$INSTALL_DIR/stop_plex.sh" << EOF
#!/bin/bash
pkill "Plex Media Server"
echo "Plex Media Server stopped"
EOF

chmod +x "$INSTALL_DIR/stop_plex.sh"
echo -e "${GREEN}✓${NC} Stop script created: $INSTALL_DIR/stop_plex.sh"

# Create uninstall script
cat > "$INSTALL_DIR/uninstall.sh" << 'UNINSTALL_EOF'
#!/bin/bash
set -e

echo "Plex PostgreSQL Shim - Uninstaller"
echo "==================================="
echo ""

read -p "This will remove the shim and restore original Plex. Continue? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Uninstall cancelled"
    exit 0
fi

# Stop Plex
echo "Stopping Plex Media Server..."
pkill "Plex Media Server" 2>/dev/null || true
sleep 2

# Restore original binary
PLEX_BINARY="/Applications/Plex Media Server.app/Contents/MacOS/Plex Media Server"
if [ -f "${PLEX_BINARY}.original" ]; then
    echo "Restoring original Plex binary..."
    sudo cp "${PLEX_BINARY}.original" "${PLEX_BINARY}"
    echo "✓ Original binary restored"
fi

# Remove installation directory
INSTALL_DIR="$(dirname "$0")"
echo "Removing installation directory: $INSTALL_DIR"
cd ~
rm -rf "$INSTALL_DIR"

echo ""
echo "✓ Uninstall complete"
echo ""
echo "Note: Your Plex database remains in PostgreSQL."
echo "To migrate back to SQLite, use the migration tool."
UNINSTALL_EOF

chmod +x "$INSTALL_DIR/uninstall.sh"
echo -e "${GREEN}✓${NC} Uninstall script created: $INSTALL_DIR/uninstall.sh"

# Installation summary
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Installation Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Installation directory: $INSTALL_DIR"
echo ""
echo "Next steps:"
echo ""
echo "1. Set your Plex token (required for testing):"
echo -e "   ${BLUE}export PLEX_TOKEN=\"your_token_here\"${NC}"
echo ""
echo "2. Start Plex with the shim:"
echo -e "   ${BLUE}$INSTALL_DIR/start_plex.sh${NC}"
echo ""
echo "3. Test the installation:"
echo -e "   ${BLUE}$INSTALL_DIR/test_aggregate_decltype.sh${NC}"
echo ""
echo "4. To stop Plex:"
echo -e "   ${BLUE}$INSTALL_DIR/stop_plex.sh${NC}"
echo ""
echo "5. To uninstall:"
echo -e "   ${BLUE}$INSTALL_DIR/uninstall.sh${NC}"
echo ""
echo "Logs:"
echo "  Shim: /tmp/plex_redirect_pg.log"
echo "  Plex: ~/Library/Logs/Plex Media Server/Plex Media Server.log"
echo ""
echo -e "${YELLOW}Important:${NC} Your Plex database must already be migrated to PostgreSQL!"
echo ""
