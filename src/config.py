"""
Configuration for COO module.
"""
import os
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database
    database_url: str = os.getenv("DATABASE_URL", "postgresql://localhost:5432/dearborn")

    # Redis for event bus
    redis_url: str = os.getenv("REDIS_URL", "")

    # Module identification
    module_name: str = "coo"

    # Shopify API (for inventory sync)
    shopify_store: str = os.getenv("SHOPIFY_STORE", "dearborndenim.myshopify.com")
    shopify_client_id: str = os.getenv("SHOPIFY_CLIENT_ID", "")
    shopify_client_secret: str = os.getenv("SHOPIFY_CLIENT_SECRET", "")
    shopify_access_token: str = os.getenv("SHOPIFY_ACCESS_TOKEN", "")  # Stored after OAuth

    # Other module URLs for HTTP fallback
    ceo_api_url: str = os.getenv("CEO_API_URL", "")
    cfo_api_url: str = os.getenv("CFO_API_URL", "")
    cdo_api_url: str = os.getenv("CDO_API_URL", "")
    cmo_api_url: str = os.getenv("CMO_API_URL", "")

    # Inventory thresholds
    default_reorder_point: int = int(os.getenv("DEFAULT_REORDER_POINT", "10"))
    low_stock_threshold: int = int(os.getenv("LOW_STOCK_THRESHOLD", "5"))

    class Config:
        env_file = ".env"
        extra = "allow"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
