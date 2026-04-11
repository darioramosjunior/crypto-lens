#!/bin/bash

###############################################################################
# Crypto-Lens Application Setup Script
# This script configures the environment for the crypto-lens application
# 
# It automatically detects the application directory and uses absolute paths
# throughout, allowing it to work regardless of where the app is deployed.
###############################################################################

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

###############################################################################
# Detect Application Directory
###############################################################################
# This script detects its own location and uses that as the app directory.
# This works regardless of:
# - Where the script is run from (current working directory)
# - Where the application is deployed (any filesystem path)
# - Whether it's run with relative or absolute path

# Get the absolute path to this script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$SCRIPT_DIR"

# Validate that we found the app directory
if [[ ! -d "$APP_DIR" ]]; then
    echo -e "${RED}[ERROR]${NC} Failed to detect application directory"
    exit 1
fi

###############################################################################
# Configuration
###############################################################################

VENV_DIR="${APP_DIR}/venv"
SERVICE_USER="crypto-lens"
LOG_PATH="/var/log/crypto-lens/"
OUTPUT_PATH="/var/run/crypto-lens/"

# Store application directory info for reference
echo -e "${BLUE}[INFO]${NC} Application Directory: $APP_DIR"
echo -e "${BLUE}[INFO]${NC} Virtual Environment: $VENV_DIR"
echo ""

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

validate_app_directory() {
    log_info "Validating application directory..."
    
    # Check if critical files exist in the app directory
    local required_files=(
        "config.conf"
        "requirements.txt"
        "main.py"
    )
    
    for file in "${required_files[@]}"; do
        if [[ ! -f "${APP_DIR}/${file}" ]]; then
            log_error "Required file not found: ${APP_DIR}/${file}"
            log_error "This does not appear to be a valid crypto-lens application directory"
            exit 1
        fi
    done
    
    log_success "Application directory validated: $APP_DIR"
}

step_verify_python() {
    log_info "Step 1: Verifying Python installation..."
    
    validate_app_directory
    
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
    
    # Use explicit path to venv python and pip executables
    VENV_PYTHON="${VENV_DIR}/bin/python3"
    VENV_PIP="${VENV_DIR}/bin/pip"
    
    # Verify virtual environment python exists
    if [[ ! -f "$VENV_PYTHON" ]]; then
        log_error "Virtual environment python executable not found at $VENV_PYTHON"
        exit 1
    fi
    
    # Upgrade pip first
    log_info "Upgrading pip, setuptools, and wheel..."
    "$VENV_PYTHON" -m pip install --upgrade pip setuptools wheel
    
    if [[ $? -ne 0 ]]; then
        log_error "Failed to upgrade pip/setuptools/wheel"
        exit 1
    fi
    log_success "pip, setuptools, and wheel upgraded"
    
    # Install from requirements.txt with verbose output
    log_info "Installing dependencies from requirements.txt (this may take a few minutes)..."
    "$VENV_PIP" install -r "${APP_DIR}/requirements.txt" --no-cache-dir
    
    if [[ $? -ne 0 ]]; then
        log_error "Failed to install dependencies from requirements.txt"
        exit 1
    fi
    
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

extract_config_value() {
    local section="$1"
    local key="$2"
    local config_file="${APP_DIR}/config.conf"
    
    # Parse INI file: look for [section] then extract key=value
    local in_section=0
    while IFS='=' read -r line; do
        # Skip empty lines and comments
        [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
        
        # Check if we found the target section
        if [[ "$line" =~ ^\[${section}\] ]]; then
            in_section=1
            continue
        fi
        
        # If we find another section header, we've left our section
        if [[ "$line" =~ ^\[[^]]+\] ]]; then
            in_section=0
            continue
        fi
        
        # If we're in the right section, look for our key
        if [[ $in_section -eq 1 ]]; then
            if [[ "$line" =~ ^${key}[[:space:]]*= ]]; then
                # Extract value after the = sign
                echo "$line" | cut -d'=' -f2- | xargs
                return 0
            fi
        fi
    done < "$config_file"
    
    return 1
}

create_cron_job() {
    local job_name="$1"
    local cron_schedule="$2"
    local script_path="$3"
    local description="$4"
    local cron_file="/etc/cron.d/$job_name"
    local log_file="${LOG_PATH}${job_name}.log"
    
    # Verify script exists before creating cron job
    if [[ ! -f "$script_path" ]]; then
        log_error "Script not found: $script_path"
        return 1
    fi
    
    # Get absolute path to python and script (defensive programming)
    local python_exec="$(cd "$(dirname "${VENV_DIR}")" && pwd)/venv/bin/python3"
    local script_abs="$(cd "$(dirname "$script_path")" && pwd)/$(basename "$script_path")"
    
    # Validate absolute paths
    if [[ ! -f "$python_exec" ]]; then
        log_error "Python executable not found: $python_exec"
        return 1
    fi
    
    # Create cron file with proper headers and environment variables
    # Using absolute paths ensures the cron job works regardless of deployment location
    cat > "$cron_file" << EOF
# $description
# Generated by crypto-lens setup.sh
# DO NOT EDIT MANUALLY - changes will be overwritten on next setup run
# 
# This cron job uses absolute paths and thus will work regardless of deployment location

SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
PYTHONUNBUFFERED=1
PYTHON_ENV=${VENV_DIR}
APP_DIR=${APP_DIR}

$cron_schedule $SERVICE_USER ${python_exec} ${script_abs} >> $log_file 2>&1
EOF
    
    # Set proper permissions (root owner, world readable)
    chmod 644 "$cron_file"
    chown root:root "$cron_file"
    
    log_success "Created cron job: $job_name ($cron_schedule)"
}

setup_cron_jobs() {
    log_info "Step 5 (continued): Setting up cron jobs..."
    
    # Parse config.conf to extract cron schedules from INI format
    MAIN_CRON=$(extract_config_value "schedules" "main_cron_sched")
    CLEANER_CRON=$(extract_config_value "schedules" "logs_cleaner_cron_sched")
    
    # Validate extracted values
    if [[ -z "$MAIN_CRON" ]] || [[ -z "$CLEANER_CRON" ]]; then
        log_error "Failed to parse cron schedules from config.conf"
        log_error "Ensure config.conf contains:"
        log_error "  [schedules]"
        log_error "  main_cron_sched=*/5 * * * *"
        log_error "  logs_cleaner_cron_sched=0 15 * * *"
        exit 1
    fi
    
    log_info "Parsed cron schedules from config.conf:"
    log_info "  - main.py schedule: $MAIN_CRON"
    log_info "  - logs_cleaner.py schedule: $CLEANER_CRON"
    log_info "  (Note: coin_data_collector.py is executed within main.py)"
    
    # Create cron jobs using absolute paths
    # APP_DIR is already an absolute path from our initialization
    create_cron_job "crypto-lens-main" "$MAIN_CRON" "${APP_DIR}/main.py" "Main crypto-lens pipeline"
    create_cron_job "crypto-lens-cleaner" "$CLEANER_CRON" "${APP_DIR}/logs_cleaner.py" "Log files cleanup"
    
    log_success "Cron jobs configured successfully"
}

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
    # Owner can read/write/execute, group and others can read/execute (view but not modify)
    chown -R "$SERVICE_USER:$SERVICE_USER" "$APP_DIR"
    chmod 755 "$APP_DIR"
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
# Step 5 (continued): Setup systemd tmpfiles.d for persistent /var/run
###############################################################################

setup_tmpfiles_d() {
    log_info "Step 5 (continued): Configuring systemd tmpfiles.d for /var/run persistence..."
    
    local tmpfiles_config="/etc/tmpfiles.d/crypto-lens.conf"
    
    # Create systemd tmpfiles.d configuration
    # This ensures /var/run/crypto-lens/ is recreated with proper permissions on every boot
    # Format: type path mode user group
    cat > "$tmpfiles_config" << 'EOF'
# crypto-lens runtime directory configuration
# Generated by crypto-lens setup.sh
# 
# This configuration ensures the runtime directory is properly created
# with correct permissions after system reboots (when /var/run is cleared)
# DO NOT EDIT MANUALLY - changes will be overwritten on next setup run

# Create /var/run/crypto-lens directory
d /var/run/crypto-lens 0755 crypto-lens crypto-lens -
EOF
    
    chmod 644 "$tmpfiles_config"
    chown root:root "$tmpfiles_config"
    
    log_success "Created systemd tmpfiles.d configuration: $tmpfiles_config"
}

###############################################################################
# Step 5 (continued): Setup systemd service for post-boot permission fix
###############################################################################

setup_systemd_service() {
    log_info "Step 5 (continued): Configuring systemd service for permission persistence..."
    
    local service_file="/etc/systemd/system/crypto-lens-init.service"
    local service_script="/usr/local/bin/crypto-lens-init.sh"
    
    # Create the initialization script that will run on boot
    cat > "$service_script" << 'SCRIPT_EOF'
#!/bin/bash
# crypto-lens post-boot initialization script
# Ensures directories and permissions are correct after system reboots

LOG_PATH="/var/log/crypto-lens"
OUTPUT_PATH="/var/run/crypto-lens"
SERVICE_USER="crypto-lens"

# Ensure log directory exists and has correct permissions
if [[ ! -d "$LOG_PATH" ]]; then
    mkdir -p "$LOG_PATH"
fi
chown -R "$SERVICE_USER:$SERVICE_USER" "$LOG_PATH"
chmod 755 "$LOG_PATH"

# Ensure output directory exists (should be created by tmpfiles.d, but double-check)
if [[ ! -d "$OUTPUT_PATH" ]]; then
    mkdir -p "$OUTPUT_PATH"
fi
chown -R "$SERVICE_USER:$SERVICE_USER" "$OUTPUT_PATH"
chmod 755 "$OUTPUT_PATH"

# Fix any existing log files' permissions
find "$LOG_PATH" -type f -exec chmod 644 {} \; 2>/dev/null || true
find "$OUTPUT_PATH" -type f -exec chmod 644 {} \; 2>/dev/null || true
SCRIPT_EOF
    
    chmod 755 "$service_script"
    chown root:root "$service_script"
    
    # Create the systemd service file
    cat > "$service_file" << 'SERVICE_EOF'
# crypto-lens initialization service
# Generated by crypto-lens setup.sh
# DO NOT EDIT MANUALLY - changes will be overwritten on next setup run
# 
# This service ensures that after system reboots, the log and runtime
# directories exist with correct ownership and permissions

[Unit]
Description=Crypto-Lens Post-Boot Initialization
After=network.target
Before=cron.service

[Service]
Type=oneshot
ExecStart=/usr/local/bin/crypto-lens-init.sh
RemainAfterExit=yes
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
SERVICE_EOF
    
    chmod 644 "$service_file"
    chown root:root "$service_file"
    
    # Reload systemd daemon and enable the service
    systemctl daemon-reload
    systemctl enable crypto-lens-init.service
    
    log_success "Created systemd service for post-boot initialization"
    log_info "  Service file: $service_file"
    log_info "  Init script: $service_script"
}

###############################################################################
# Step 6: Verify Setup
###############################################################################

verify_setup() {
    log_info "Step 6: Verifying setup..."
    
    echo ""
    log_info "Setup Verification Summary:"
    echo "───────────────────────────────────────────────────────────────"
    
    # Display deployment paths
    log_info "Deployment Paths:"
    echo "  App Directory: $APP_DIR"
    echo "  Venv Directory: $VENV_DIR"
    echo "  Log Directory: $LOG_PATH"
    echo "  Output Directory: $OUTPUT_PATH"
    echo ""
    
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
    
    # Check systemd tmpfiles.d configuration
    if [[ -f "/etc/tmpfiles.d/crypto-lens.conf" ]]; then
        log_success "✓ systemd tmpfiles.d configuration exists"
    else
        log_error "✗ systemd tmpfiles.d configuration not found"
        return 1
    fi
    
    # Check systemd service
    if [[ -f "/etc/systemd/system/crypto-lens-init.service" ]]; then
        if systemctl is-enabled crypto-lens-init.service &>/dev/null; then
            log_success "✓ crypto-lens-init service enabled"
        else
            log_error "✗ crypto-lens-init service not enabled"
            return 1
        fi
    else
        log_error "✗ crypto-lens-init service file not found"
        return 1
    fi
    
    # Check cron jobs
    local cron_count=0
    for cron_file in /etc/cron.d/crypto-lens-*; do
        if [[ -f "$cron_file" ]]; then
            ((cron_count++))
        fi
    done
    
    if [[ $cron_count -eq 2 ]]; then
        log_success "✓ All 2 cron jobs configured"
        echo ""
        log_info "Cron Job Paths:"
        for cron_file in /etc/cron.d/crypto-lens-*; do
            if [[ -f "$cron_file" ]]; then
                local job_name=$(basename "$cron_file")
                local python_path=$(grep -m1 'bin/python3' "$cron_file" | sed -E 's/.*([^ ]+\/bin\/python3).*/\1/')
                local script_path=$(grep -m1 '>>' "$cron_file" | sed -E 's/.*(\/[^ ]+\.py) >>.*/\1/')
                echo "  $job_name"
                echo "    Python: $python_path"
                echo "    Script: $script_path"
            fi
        done
    else
        log_error "✗ Expected 2 cron jobs, found $cron_count"
        return 1
    fi
    
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
    setup_tmpfiles_d
    setup_systemd_service
    setup_cron_jobs
    verify_setup
    
    echo ""
    log_success "Setup completed successfully!"
    echo ""
    log_info "Deployment Information:"
    echo "  Application deployed at: $APP_DIR"
    echo "  Using Python from: ${VENV_DIR}/bin/python3"
    echo "  All cron jobs use absolute paths and will work regardless of deployment location"
    echo ""
    log_info "Persistence Configuration:"
    echo "  - systemd tmpfiles.d: /etc/tmpfiles.d/crypto-lens.conf"
    echo "    Recreates /var/run/crypto-lens/ on every boot with correct permissions"
    echo "  - systemd service: crypto-lens-init (enabled and runs on boot)"
    echo "    Ensures log and runtime directories have correct ownership after reboot"
    echo ""
    log_info "Next steps:"
    echo "  1. Review the configuration in: ${APP_DIR}/config.conf"
    echo "  2. Verify cron jobs: ls -la /etc/cron.d/crypto-lens-*"
    echo "  3. Verify systemd service: systemctl status crypto-lens-init"
    echo "  4. View tmpfiles.d config: cat /etc/tmpfiles.d/crypto-lens.conf"
    echo "  5. Monitor logs: tail -f /var/log/crypto-lens/*.log"
    echo "  6. Test the application: source ${VENV_DIR}/bin/activate && python3 ${APP_DIR}/main.py"
    echo ""
}

# Run main function
main "$@"
