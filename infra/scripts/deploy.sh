#!/bin/bash
# ============================================================
# Blockchain Analytics Platform - Deployment Script
# ============================================================
# Usage:
#   ./infra/scripts/deploy.sh [environment] [action]
#   
# Examples:
#   ./infra/scripts/deploy.sh dev up        # Start development
#   ./infra/scripts/deploy.sh prod up       # Start production
#   ./infra/scripts/deploy.sh dev down      # Stop development
#   ./infra/scripts/deploy.sh dev logs      # View logs
#   ./infra/scripts/deploy.sh dev build     # Rebuild containers
# ============================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Default values
ENVIRONMENT="${1:-dev}"
ACTION="${2:-up}"

# Print banner
echo -e "${BLUE}"
echo "============================================================"
echo " Blockchain Analytics Platform - Deployment"
echo "============================================================"
echo -e "${NC}"

# Validate environment
validate_environment() {
    if [[ ! "$ENVIRONMENT" =~ ^(dev|prod|staging)$ ]]; then
        echo -e "${RED}Error: Invalid environment '$ENVIRONMENT'${NC}"
        echo "Valid environments: dev, prod, staging"
        exit 1
    fi
    echo -e "${GREEN}Environment: ${ENVIRONMENT}${NC}"
}

# Load environment variables
load_env() {
    local env_file="$PROJECT_ROOT/.env"
    
    if [[ -f "$env_file" ]]; then
        echo -e "${BLUE}Loading environment from $env_file${NC}"
        export $(grep -v '^#' "$env_file" | xargs)
    else
        echo -e "${YELLOW}Warning: .env file not found${NC}"
        echo "Copy infra/env/.env.example to .env and configure it"
    fi
}

# Check prerequisites
check_prerequisites() {
    echo -e "${BLUE}Checking prerequisites...${NC}"
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}Error: Docker is not installed${NC}"
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        echo -e "${RED}Error: Docker Compose is not installed${NC}"
        exit 1
    fi
    
    # Check for credentials
    if [[ ! -d "$PROJECT_ROOT/credentials" ]]; then
        echo -e "${YELLOW}Warning: credentials directory not found${NC}"
        echo "Create credentials/ directory and add service-account.json"
    fi
    
    echo -e "${GREEN}Prerequisites OK${NC}"
}

# Get docker compose command
get_compose_cmd() {
    if docker compose version &> /dev/null; then
        echo "docker compose"
    else
        echo "docker-compose"
    fi
}

# Build containers
build() {
    local compose_cmd=$(get_compose_cmd)
    echo -e "${BLUE}Building containers...${NC}"
    
    cd "$PROJECT_ROOT"
    
    if [[ "$ENVIRONMENT" == "dev" ]]; then
        $compose_cmd -f docker-compose.yml -f docker-compose.dev.yml build
    elif [[ "$ENVIRONMENT" == "prod" ]]; then
        $compose_cmd -f docker-compose.yml -f docker-compose.prod.yml build
    else
        $compose_cmd build
    fi
    
    echo -e "${GREEN}Build complete${NC}"
}

# Start services
start() {
    local compose_cmd=$(get_compose_cmd)
    echo -e "${BLUE}Starting services...${NC}"
    
    cd "$PROJECT_ROOT"
    
    if [[ "$ENVIRONMENT" == "dev" ]]; then
        $compose_cmd -f docker-compose.yml -f docker-compose.dev.yml up -d
    elif [[ "$ENVIRONMENT" == "prod" ]]; then
        $compose_cmd -f docker-compose.yml -f docker-compose.prod.yml up -d
    else
        $compose_cmd up -d
    fi
    
    echo -e "${GREEN}Services started${NC}"
    echo ""
    echo "Access the application:"
    echo "  Frontend: http://localhost:${FRONTEND_PORT:-3000}"
    echo "  Backend:  http://localhost:${BACKEND_PORT:-8080}"
    echo "  API Docs: http://localhost:${BACKEND_PORT:-8080}/docs"
}

# Stop services
stop() {
    local compose_cmd=$(get_compose_cmd)
    echo -e "${BLUE}Stopping services...${NC}"
    
    cd "$PROJECT_ROOT"
    
    if [[ "$ENVIRONMENT" == "dev" ]]; then
        $compose_cmd -f docker-compose.yml -f docker-compose.dev.yml down
    elif [[ "$ENVIRONMENT" == "prod" ]]; then
        $compose_cmd -f docker-compose.yml -f docker-compose.prod.yml down
    else
        $compose_cmd down
    fi
    
    echo -e "${GREEN}Services stopped${NC}"
}

# View logs
logs() {
    local compose_cmd=$(get_compose_cmd)
    local service="${3:-}"
    
    cd "$PROJECT_ROOT"
    
    if [[ -n "$service" ]]; then
        $compose_cmd logs -f "$service"
    else
        $compose_cmd logs -f
    fi
}

# Show status
status() {
    local compose_cmd=$(get_compose_cmd)
    
    cd "$PROJECT_ROOT"
    $compose_cmd ps
}

# Clean up
clean() {
    local compose_cmd=$(get_compose_cmd)
    echo -e "${YELLOW}Cleaning up...${NC}"
    
    cd "$PROJECT_ROOT"
    $compose_cmd down -v --rmi local
    
    echo -e "${GREEN}Cleanup complete${NC}"
}

# Main execution
main() {
    validate_environment
    load_env
    check_prerequisites
    
    case "$ACTION" in
        up|start)
            start
            ;;
        down|stop)
            stop
            ;;
        restart)
            stop
            start
            ;;
        build)
            build
            ;;
        rebuild)
            build
            start
            ;;
        logs)
            logs "$@"
            ;;
        status|ps)
            status
            ;;
        clean)
            clean
            ;;
        *)
            echo -e "${RED}Unknown action: $ACTION${NC}"
            echo "Valid actions: up, down, restart, build, rebuild, logs, status, clean"
            exit 1
            ;;
    esac
}

# Run
main "$@"

