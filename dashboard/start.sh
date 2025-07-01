#!/bin/bash

# Sales Dashboard Startup Script
# This script handles installation and startup of the Node.js dashboard

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check Node.js version
check_node_version() {
    if command_exists node; then
        NODE_VERSION=$(node --version | cut -d'v' -f2)
        MAJOR_VERSION=$(echo $NODE_VERSION | cut -d'.' -f1)
        if [ "$MAJOR_VERSION" -ge 14 ]; then
            print_success "Node.js v$NODE_VERSION detected"
            return 0
        else
            print_error "Node.js version $NODE_VERSION is too old. Please install v14 or higher."
            return 1
        fi
    else
        print_error "Node.js is not installed. Please install Node.js v14 or higher."
        return 1
    fi
}

# Function to check if .env exists
check_env_file() {
    if [ -f "../.env" ]; then
        print_success "Environment file found"
        return 0
    else
        print_error "No .env file found in parent directory"
        print_warning "Please create a .env file with database configuration"
        return 1
    fi
}

# Function to install dependencies
install_dependencies() {
    if [ ! -d "node_modules" ]; then
        print_status "Installing Node.js dependencies..."
        npm install
        print_success "Dependencies installed successfully"
    else
        print_status "Dependencies already installed"
    fi
}

# Function to test database connection
test_database_connection() {
    print_status "Testing database connection..."
    node -e "
    require('dotenv').config({ path: '../.env' });
    const mysql = require('mysql2/promise');
    
    const dbConfig = {
        host: process.env.DB_HOST,
        port: process.env.DB_PORT,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: process.env.DB_NAME
    };
    
    mysql.createConnection(dbConfig)
        .then(connection => {
            console.log('✅ Database connection successful');
            connection.end();
            process.exit(0);
        })
        .catch(error => {
            console.error('❌ Database connection failed:', error.message);
            process.exit(1);
        });
    " 2>/dev/null
    
    if [ $? -eq 0 ]; then
        print_success "Database connection test passed"
        return 0
    else
        print_error "Database connection test failed"
        print_warning "Please check your database configuration in .env file"
        return 1
    fi
}

# Function to start the dashboard
start_dashboard() {
    local mode=${1:-"production"}
    
    print_status "Starting Sales Dashboard in $mode mode..."
    
    if [ "$mode" = "development" ]; then
        if command_exists nodemon; then
            npm run dev
        else
            print_warning "nodemon not found, installing..."
            npm install -g nodemon
            npm run dev
        fi
    else
        npm start
    fi
}

# Function to show usage
show_usage() {
    echo "Sales Dashboard Startup Script"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -d, --dev          Start in development mode with auto-reload"
    echo "  -i, --install      Install dependencies only"
    echo "  -t, --test         Test database connection only"
    echo "  -h, --help         Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                 Start dashboard in production mode"
    echo "  $0 --dev           Start dashboard in development mode"
    echo "  $0 --install       Install dependencies only"
    echo "  $0 --test          Test database connection"
}

# Function to display system info
show_system_info() {
    print_status "System Information:"
    echo "  OS: $(uname -s)"
    echo "  Architecture: $(uname -m)"
    if command_exists node; then
        echo "  Node.js: $(node --version)"
    fi
    if command_exists npm; then
        echo "  npm: $(npm --version)"
    fi
    echo ""
}

# Main function
main() {
    echo "🚀 Sales Dashboard Startup Script"
    echo "=================================="
    echo ""
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -d|--dev)
                DEV_MODE=true
                shift
                ;;
            -i|--install)
                INSTALL_ONLY=true
                shift
                ;;
            -t|--test)
                TEST_ONLY=true
                shift
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Show system info
    show_system_info
    
    # Check prerequisites
    print_status "Checking prerequisites..."
    
    if ! check_node_version; then
        exit 1
    fi
    
    if ! check_env_file; then
        exit 1
    fi
    
    # Install dependencies
    install_dependencies
    
    # Test database connection if requested or by default
    if [[ "$TEST_ONLY" == true || "$INSTALL_ONLY" != true ]]; then
        if ! test_database_connection; then
            if [[ "$TEST_ONLY" == true ]]; then
                exit 1
            else
                print_warning "Database connection failed, but continuing with startup..."
            fi
        fi
    fi
    
    # Exit if only testing or installing
    if [[ "$TEST_ONLY" == true || "$INSTALL_ONLY" == true ]]; then
        print_success "Operation completed successfully"
        exit 0
    fi
    
    # Start the dashboard
    echo ""
    print_status "All checks passed! Starting dashboard..."
    echo ""
    
    if [[ "$DEV_MODE" == true ]]; then
        start_dashboard "development"
    else
        start_dashboard "production"
    fi
}

# Trap Ctrl+C and cleanup
trap 'echo -e "\n${YELLOW}[INFO]${NC} Dashboard stopped by user"; exit 0' INT

# Run main function with all arguments
main "$@"