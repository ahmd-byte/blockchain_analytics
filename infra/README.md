# 🚀 Infrastructure & Deployment

This directory contains all infrastructure and deployment configurations for the Blockchain Analytics Platform.

## 📁 Directory Structure

```
infra/
├── docker/                    # Docker configurations
│   ├── backend.Dockerfile     # FastAPI backend container
│   ├── frontend.Dockerfile    # React frontend container
│   └── nginx.conf             # Nginx configuration for frontend
├── env/                       # Environment templates
│   ├── env.example            # Development environment template
│   └── env.production         # Production environment template
├── k8s/                       # Kubernetes manifests
│   ├── namespace.yaml         # Namespace definition
│   ├── backend-deployment.yaml    # Backend deployment + service + HPA
│   ├── frontend-deployment.yaml   # Frontend deployment + service + HPA
│   ├── configmap.yaml         # Configuration values
│   ├── ingress.yaml           # Ingress routing rules
│   └── service-account.yaml   # RBAC & Workload Identity
├── scripts/                   # Deployment scripts
│   ├── deploy.sh              # Main deployment script
│   └── build-push.sh          # Build and push Docker images
└── README.md                  # This file
```

## 🐳 Docker

### Prerequisites

- Docker Engine 20.10+
- Docker Compose v2.0+

### Quick Start (Development)

```bash
# 1. Copy environment template
cp infra/env/env.example .env

# 2. Configure your GCP project
# Edit .env and set GOOGLE_CLOUD_PROJECT

# 3. Add GCP credentials
mkdir -p credentials
# Copy your service-account.json to credentials/

# 4. Start services
./infra/scripts/deploy.sh dev up

# 5. Access the application
# Frontend: http://localhost:3000
# Backend:  http://localhost:8080
# API Docs: http://localhost:8080/docs
```

### Docker Commands

```bash
# Start development environment
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# Start production environment
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# View logs
docker compose logs -f backend

# Rebuild containers
docker compose up -d --build

# Stop all services
docker compose down

# Clean up (removes volumes)
docker compose down -v --rmi local
```

### Building Images

```bash
# Build backend
docker build -f infra/docker/backend.Dockerfile -t blockchain-backend .

# Build frontend
docker build -f infra/docker/frontend.Dockerfile -t blockchain-frontend \
  --build-arg VITE_API_URL=https://api.example.com .

# Build and push to registry
./infra/scripts/build-push.sh gcr.io/your-project v1.0.0
```

## ☸️ Kubernetes

### Prerequisites

- kubectl configured with cluster access
- GKE cluster (recommended) or any Kubernetes 1.25+
- Workload Identity enabled (for GKE)

### Deployment

```bash
# 1. Create namespace
kubectl apply -f infra/k8s/namespace.yaml

# 2. Configure settings
# Edit infra/k8s/configmap.yaml with your values
kubectl apply -f infra/k8s/configmap.yaml

# 3. Create service account
# Update PROJECT_ID in service-account.yaml
kubectl apply -f infra/k8s/service-account.yaml

# 4. Deploy backend
kubectl apply -f infra/k8s/backend-deployment.yaml

# 5. Deploy frontend
kubectl apply -f infra/k8s/frontend-deployment.yaml

# 6. Configure ingress
# Update hostnames in ingress.yaml
kubectl apply -f infra/k8s/ingress.yaml
```

### Workload Identity Setup (GKE)

```bash
# Create GCP service account
gcloud iam service-accounts create blockchain-backend \
  --display-name="Blockchain Backend"

# Grant BigQuery access
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:blockchain-backend@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:blockchain-backend@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Link KSA to GSA
gcloud iam service-accounts add-iam-policy-binding \
  blockchain-backend@PROJECT_ID.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:PROJECT_ID.svc.id.goog[blockchain-analytics/blockchain-backend-sa]"
```

## 🔄 CI/CD

The platform uses GitHub Actions for CI/CD:

### Workflows

1. **CI/CD Pipeline** (`.github/workflows/ci-cd.yml`)
   - Triggers: Push to `main`/`develop`, manual dispatch
   - Steps: Lint → Test → Build → Deploy

2. **PR Checks** (`.github/workflows/pr-checks.yml`)
   - Triggers: Pull requests
   - Steps: Quick lint, Docker build check, Security scan

### Required Secrets

Configure these in GitHub repository settings:

| Secret | Description |
|--------|-------------|
| `GCP_PROJECT_ID` | Google Cloud project ID |
| `GCP_SA_KEY` | Service account JSON key (base64 encoded) |
| `VITE_API_URL` | Production API URL |

### Manual Deployment

```bash
# Trigger production deployment manually
gh workflow run ci-cd.yml -f environment=production
```

## ☁️ Cloud Run Deployment (GCP)

For serverless deployment on GCP Cloud Run:

```bash
# Deploy backend
gcloud run deploy blockchain-backend \
  --image=gcr.io/PROJECT_ID/blockchain-backend:latest \
  --platform=managed \
  --region=us-central1 \
  --allow-unauthenticated \
  --set-env-vars="GOOGLE_CLOUD_PROJECT=PROJECT_ID"

# Deploy frontend
gcloud run deploy blockchain-frontend \
  --image=gcr.io/PROJECT_ID/blockchain-frontend:latest \
  --platform=managed \
  --region=us-central1 \
  --allow-unauthenticated
```

## 🔐 Security Best Practices

1. **Never commit secrets** - Use environment variables or secret managers
2. **Use Workload Identity** - Avoid service account keys in production
3. **Enable HTTPS** - Configure SSL/TLS for all production traffic
4. **Least privilege** - Grant minimal IAM permissions
5. **Container security** - Run as non-root, read-only filesystem
6. **Network policies** - Restrict pod-to-pod communication

## 📊 Monitoring

### Health Checks

- Backend: `GET /api/v1/health`
- Frontend: `GET /health`

### Recommended Tools

- **Google Cloud Monitoring** - Metrics and alerting
- **Cloud Logging** - Centralized logging
- **Cloud Trace** - Distributed tracing
- **Prometheus + Grafana** - For Kubernetes deployments

## 🔧 Troubleshooting

### Common Issues

1. **BigQuery authentication fails**
   ```bash
   # Check service account
   gcloud auth activate-service-account --key-file=credentials/service-account.json
   
   # Verify permissions
   bq ls --project_id=PROJECT_ID
   ```

2. **Docker build fails**
   ```bash
   # Clean Docker cache
   docker builder prune -f
   
   # Build with no cache
   docker build --no-cache -f infra/docker/backend.Dockerfile .
   ```

3. **Kubernetes pods not starting**
   ```bash
   # Check pod status
   kubectl get pods -n blockchain-analytics
   
   # View logs
   kubectl logs -f deployment/blockchain-backend -n blockchain-analytics
   
   # Describe pod for events
   kubectl describe pod <pod-name> -n blockchain-analytics
   ```

## 📝 Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `GOOGLE_CLOUD_PROJECT` | GCP project ID | Required |
| `GOOGLE_CREDENTIALS_PATH` | Path to service account JSON | `./credentials` |
| `BACKEND_PORT` | Backend API port | `8080` |
| `FRONTEND_PORT` | Frontend port | `3000` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `CORS_ORIGINS` | Allowed CORS origins | `http://localhost:3000` |
| `VITE_API_URL` | API URL for frontend | `http://localhost:8080` |

