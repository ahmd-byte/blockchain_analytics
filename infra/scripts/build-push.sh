#!/bin/bash
# ============================================================
# Blockchain Analytics Platform - Build and Push Images
# ============================================================
# Usage:
#   ./infra/scripts/build-push.sh [registry] [tag]
#
# Examples:
#   ./infra/scripts/build-push.sh gcr.io/my-project latest
#   ./infra/scripts/build-push.sh docker.io/myuser v1.0.0
# ============================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Configuration
REGISTRY="${1:-${DOCKER_REGISTRY:-gcr.io/blockchain-analytics}}"
TAG="${2:-${IMAGE_TAG:-latest}}"
BACKEND_IMAGE="${REGISTRY}/blockchain-backend:${TAG}"
FRONTEND_IMAGE="${REGISTRY}/blockchain-frontend:${TAG}"

echo -e "${BLUE}"
echo "============================================================"
echo " Building and Pushing Docker Images"
echo "============================================================"
echo " Registry: ${REGISTRY}"
echo " Tag: ${TAG}"
echo "============================================================"
echo -e "${NC}"

# Build backend
echo -e "${BLUE}Building backend image...${NC}"
docker build \
    -f "$PROJECT_ROOT/infra/docker/backend.Dockerfile" \
    -t "$BACKEND_IMAGE" \
    --target production \
    "$PROJECT_ROOT"
echo -e "${GREEN}Backend image built: $BACKEND_IMAGE${NC}"

# Build frontend
echo -e "${BLUE}Building frontend image...${NC}"
docker build \
    -f "$PROJECT_ROOT/infra/docker/frontend.Dockerfile" \
    -t "$FRONTEND_IMAGE" \
    --target production \
    --build-arg VITE_API_URL="${VITE_API_URL:-/api}" \
    "$PROJECT_ROOT"
echo -e "${GREEN}Frontend image built: $FRONTEND_IMAGE${NC}"

# Push images
echo -e "${BLUE}Pushing images to registry...${NC}"

# Authenticate with GCR if using Google Container Registry
if [[ "$REGISTRY" == gcr.io/* ]]; then
    echo "Authenticating with Google Container Registry..."
    gcloud auth configure-docker gcr.io --quiet
fi

docker push "$BACKEND_IMAGE"
echo -e "${GREEN}Pushed: $BACKEND_IMAGE${NC}"

docker push "$FRONTEND_IMAGE"
echo -e "${GREEN}Pushed: $FRONTEND_IMAGE${NC}"

echo -e "${GREEN}"
echo "============================================================"
echo " Build and Push Complete!"
echo "============================================================"
echo " Backend:  $BACKEND_IMAGE"
echo " Frontend: $FRONTEND_IMAGE"
echo "============================================================"
echo -e "${NC}"


