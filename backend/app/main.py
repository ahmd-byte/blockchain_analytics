"""
Blockchain Analytics API - Main Application.

This is the main entry point for the FastAPI application.
It configures the application, sets up CORS, and registers
all API routes.

Usage:
    uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi
import logging
import time

from app.core.config import get_settings
from app.api.routes import dashboard, wallet, fraud, health


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    
    Handles startup and shutdown events for the application.
    """
    # Startup
    logger.info("Starting Blockchain Analytics API...")
    settings = get_settings()
    logger.info(f"App: {settings.app_name} v{settings.app_version}")
    logger.info(f"Debug mode: {settings.debug}")
    logger.info(f"BigQuery Project: {settings.google_cloud_project}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Blockchain Analytics API...")


# Initialize FastAPI application
settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    description="""
## Blockchain Transaction Analytics & Fraud Detection API

This API provides endpoints for:

- **Dashboard**: Summary statistics and metrics for blockchain analytics
- **Wallet**: Detailed wallet information and transaction history  
- **Fraud Detection**: Fraud scores and suspicious wallet data

### Authentication
Currently, the API does not require authentication. 
For production, implement API key or OAuth2 authentication.

### Rate Limiting
No rate limiting is currently implemented.
For production, consider adding rate limiting middleware.
    """,
    version=settings.app_version,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)


# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """
    Add processing time header to all responses.
    
    This middleware measures and includes the request processing
    time in the X-Process-Time header.
    """
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(round(process_time, 4))
    return response


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Global exception handler for unhandled exceptions.
    
    Logs the error and returns a standardized error response.
    """
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "INTERNAL_SERVER_ERROR",
            "message": "An unexpected error occurred",
            "detail": str(exc) if settings.debug else None
        }
    )


# Register routers
app.include_router(health.router)
app.include_router(
    dashboard.router, 
    prefix=settings.api_prefix
)
app.include_router(
    wallet.router, 
    prefix=settings.api_prefix
)
app.include_router(
    fraud.router, 
    prefix=settings.api_prefix
)


# Custom OpenAPI schema
def custom_openapi():
    """
    Generate custom OpenAPI schema with additional metadata.
    """
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title=settings.app_name,
        version=settings.app_version,
        description=app.description,
        routes=app.routes,
    )
    
    # Add custom info
    openapi_schema["info"]["x-logo"] = {
        "url": "https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png"
    }
    
    # Add contact info
    openapi_schema["info"]["contact"] = {
        "name": "API Support",
        "email": "support@blockchain-analytics.com"
    }
    
    # Add license
    openapi_schema["info"]["license"] = {
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT"
    }
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )


