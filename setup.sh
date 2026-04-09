#!/bin/bash

###############################################################################
# Crypto-Lens Application Setup Script
# This script configures the environment for the crypto-lens application
###############################################################################

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${APP_DIR}/venv"
SERVICE_USER="crypto-lens"
LOG_PATH="/var/log/crypto-lens/"
OUTPUT_PATH="/var/run/crypto-lens/"

###############################################################################
# Helper Functions
###############################################################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run as root for system-level operations."
        exit 1
    fi
}

###############################################################################
# Step 1: Verify Python and pip availability
###############################################################################

step_verify_python() {
    log_info "Step 1: Verifying Python installation..."
    
    if ! command -v python3 &> /dev/null; then
        log_error "Python3 is not installed. Please install Python3 first."
        exit 1
    fi
    
    PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
    log_success "Python3 found: version $PYTHON_VERSION"
}

###############################################################################
# Step 2: Setup Virtual Environment
###############################################################################

setup_venv() {
    log_info "Step 2: Setting up virtual environment..."
    
    if [[ -d "$VENV_DIR" ]]; then
        log_warning "Virtual environment already exists at $VENV_DIR"
    else
        python3 -m venv "$VENV_DIR"
        log_success "Virtual environment created at $VENV_DIR"
    fi
}

###############################################################################
# Step 3: Install Python Dependencies
###############################################################################

install_dependencies() {
    log_info "Step 3: Installing Python dependencies..."
    
    if [[ ! -f "${APP_DIR}/requirements.txt" ]]; then
        log_error "requirements.txt not found in $APP_DIR"
        exit 1
    fi
    
    source "${VENV_DIR}/bin/activate"
    
    # Upgrade pip first
    log_info "Upgrading pip, setuptools, and wheel..."
    python3 -m pip install --upgrade pip setuptools wheel
    
    if [[ $? -ne 0 ]]; then
        log_error "Failed to upgrade pip/setuptools/wheel"
        deactivate
        exit 1
    fi
    log_success "pip, setuptools, and wheel upgraded"
    
    # Install from requirements.txt with verbose output
    log_info "Installing dependencies from requirements.txt (this may take a few minutes)..."
    pip install -r "${APP_DIR}/requirements.txt" --no-cache-dir
    
    if [[ $? -ne 0 ]]; then
        log_error "Failed to install dependencies from requirements.txt"
        deactivate
        exit 1
    fi
    
    deactivate
    log_success "All dependencies installed successfully"
}

###############################################################################
# Step 4: Create Service User
###############################################################################

create_service_user() {
    log_info "Step 4: Creating service user '$SERVICE_USER'..."
    
    if id "$SERVICE_USER" &>/dev/null; then
        log_warning "User '$SERVICE_USER' already exists"
    else
        useradd --system --no-create-home --shell /bin/false "$SERVICE_USER"
        log_success "Service user '$SERVICE_USER' created"
    fi
}

###############################################################################
# Step 5: Setup Directories and Permissions
###############################################################################

setup_directories() {
    log_info "Step 5: Setting up directories and permissions..."
    
    # Create log directory
    if [[ ! -d "$LOG_PATH" ]]; then
        mkdir -p "$LOG_PATH"
        log_success "Created log directory: $LOG_PATH"
    else
        log_warning "Log directory already exists: $LOG_PATH"
    fi
    
    # Create output directory
    if [[ ! -d "$OUTPUT_PATH" ]]; then
        mkdir -p "$OUTPUT_PATH"
        log_success "Created output directory: $OUTPUT_PATH"
    else
        log_warning "Output directory already exists: $OUTPUT_PATH"
    fi
    
    # Set ownership and permissions for app directory
    # App directory: crypto-lens user owns it, readable by group
    chown -R "$SERVICE_USER:$SERVICE_USER" "$APP_DIR"
    chmod 750 "$APP_DIR"
    log_success "Set ownership of app directory to $SERVICE_USER"
    
    # Log directory: crypto-lens user owns it, accessible only by owner
    chown -R "$SERVICE_USER:$SERVICE_USER" "$LOG_PATH"
    chmod 755 "$LOG_PATH"
    chmod 644 "$LOG_PATH"* 2>/dev/null || true
    log_success "Configured log directory permissions"
    
    # Output directory: crypto-lens user owns it, world readable (for Grafana)
    # This follows best practice: service writes, other services (Grafana) read
    chown -R "$SERVICE_USER:$SERVICE_USER" "$OUTPUT_PATH"
    chmod 755 "$OUTPUT_PATH"
    chmod 644 "$OUTPUT_PATH"* 2>/dev/null || true
    log_success "Configured output directory permissions (readable by Grafana)"
}

###############################################################################
# Step 6: Verify Setup
###############################################################################

verify_setup() {
    log_info "Step 6: Verifying setup..."
    
    echo ""
    log_info "Setup Verification Summary:"
    echo "───────────────────────────────────────────────────────────────"
    
    # Check venv
    if [[ -d "$VENV_DIR" ]]; then
        log_success "✓ Virtual environment exists"
    else
        log_error "✗ Virtual environment not found"
        return 1
    fi
    
    # Check dependencies
    if [[ -d "${VENV_DIR}/lib" ]]; then
        log_success "✓ Dependencies installed"
    else
        log_error "✗ Dependencies not installed"
        return 1
    fi
    
    # Check service user
    if id "$SERVICE_USER" &>/dev/null; then
        log_success "✓ Service user '$SERVICE_USER' exists"
    else
        log_error "✗ Service user '$SERVICE_USER' not found"
        return 1
    fi
    
    # Check directories
    if [[ -d "$LOG_PATH" ]]; then
        LOG_OWNER=$(stat -c '%U:%G' "$LOG_PATH")
        log_success "✓ Log directory exists (Owner: $LOG_OWNER)"
    else
        log_error "✗ Log directory not found"
        return 1
    fi
    
    if [[ -d "$OUTPUT_PATH" ]]; then
        OUTPUT_OWNER=$(stat -c '%U:%G' "$OUTPUT_PATH")
        log_success "✓ Output directory exists (Owner: $OUTPUT_OWNER)"
    else
        log_error "✗ Output directory not found"
        return 1
    fi
    
    # Check app ownership
    APP_OWNER=$(stat -c '%U:%G' "$APP_DIR")
    log_success "✓ App directory owned by: $APP_OWNER"
    
    echo "───────────────────────────────────────────────────────────────"
    echo ""
}

###############################################################################
# Main Execution
###############################################################################

main() {
    echo ""
    echo "╔═════════════════════════════════════════════════════════════╗"
    echo "║   Crypto-Lens Application Setup                             ║"
    echo "╚═════════════════════════════════════════════════════════════╝"
    echo ""
    
    check_root
    
    step_verify_python
    setup_venv
    install_dependencies
    create_service_user
    setup_directories
    verify_setup
    
    echo ""
    log_success "Setup completed successfully!"
    echo ""
    log_info "Next steps:"
    echo "  1. Review the configuration in: $APP_DIR/config.conf"
    echo "  2. Create a systemd service file for automated execution"
    echo "  3. Test the application with: source $VENV_DIR/bin/activate && python3 $APP_DIR/main.py"
    echo ""
}

# Run main function
main "$@"
