from pydantic import BaseModel, Field
from typing import Optional
from pydantic_settings import BaseSettings
import os

class ConnectionSettings(BaseSettings):
    """Pydantic settings for TCP connection and caching behavior."""
    
    # Cache settings
    CACHE_ENABLED: bool = Field(True, description="Enable data caching to preserve progress between runs")
    CACHE_DIRECTORY: str = Field("cache", description="Directory to store cache data")
    
    # Retry settings
    BATCH_RETRY_ATTEMPTS: int = Field(5, description="Number of retry attempts for batch operations")
    MIN_RETRY_DELAY: float = Field(1.0, description="Minimum delay in seconds between retries")
    MAX_RETRY_DELAY: float = Field(60.0, description="Maximum delay in seconds between retries")
    BATCH_SUCCESS_DELAY: float = Field(3.0, description="Delay after successful batch operations")
    
    # Rate limit handling
    RATE_LIMIT_WAIT_TIME: float = Field(60.0, description="How long to wait on rate limit errors (seconds)")

    # TCP Connection optimization settings
    TCP_TIMEOUT: int = Field(300, description="TCP socket timeout in seconds")
    TCP_KEEPALIVE_ENABLED: bool = Field(True, description="Enable TCP keepalive")
    TCP_KEEPALIVE_IDLE: int = Field(60, description="Seconds before sending keepalive probes")
    TCP_KEEPALIVE_INTERVAL: int = Field(10, description="Interval between keepalive probes")
    TCP_KEEPALIVE_COUNT: int = Field(6, description="Number of keepalive probes before dropping connection")
    
    # Buffer settings
    TCP_BUFFER_SIZE: int = Field(262144, description="TCP socket buffer size (256KB)")
    
    class Config:
        # You can load from environment variables with this prefix
        env_prefix = "BALTIMORE_"
        
        # Optionally load from .env file
        env_file = os.path.join(os.path.dirname(__file__), ".env")
        env_file_encoding = "utf-8"