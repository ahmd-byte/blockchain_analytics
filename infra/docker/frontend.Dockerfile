# ============================================================
# Blockchain Analytics Platform - Frontend Dockerfile
# React + Vite Application with Nginx
# ============================================================

# Build stage
FROM node:20-alpine as builder

# Set working directory
WORKDIR /build

# Copy package files
COPY frontend/package*.json ./

# Install dependencies
RUN npm ci --silent

# Copy source code
COPY frontend/ .

# Build arguments for environment configuration
ARG VITE_API_URL=http://localhost:8080
ARG VITE_APP_NAME="Blockchain Analytics"

# Set environment variables for build
ENV VITE_API_URL=${VITE_API_URL} \
    VITE_APP_NAME=${VITE_APP_NAME}

# Build the application
RUN npm run build

# ============================================================
# Production stage with Nginx
# ============================================================
FROM nginx:alpine as production

# Labels
LABEL maintainer="Blockchain Analytics Team" \
      version="1.0" \
      description="React frontend for Blockchain Analytics Platform"

# Remove default nginx config
RUN rm -rf /etc/nginx/conf.d/default.conf

# Copy custom nginx configuration
COPY infra/docker/nginx.conf /etc/nginx/conf.d/default.conf

# Copy built assets from builder stage
COPY --from=builder /build/dist /usr/share/nginx/html

# Create non-root user
RUN addgroup -g 1000 appgroup && \
    adduser -u 1000 -G appgroup -s /bin/sh -D appuser && \
    chown -R appuser:appgroup /usr/share/nginx/html && \
    chown -R appuser:appgroup /var/cache/nginx && \
    chown -R appuser:appgroup /var/log/nginx && \
    touch /var/run/nginx.pid && \
    chown -R appuser:appgroup /var/run/nginx.pid

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:80/health || exit 1

# Expose port
EXPOSE 80

# Start nginx
CMD ["nginx", "-g", "daemon off;"]

