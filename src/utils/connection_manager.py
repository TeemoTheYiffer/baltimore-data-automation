import socket
import platform
import logging
import httplib2
import time
import random
import ssl
from typing import Optional
from googleapiclient.errors import HttpError
from utils.connection_settings import ConnectionSettings

logger = logging.getLogger("baltimore_tcp")

class TCPConnectionManager:
    """
    Optimizes TCP connection settings to prevent WinError 10053 when sending large batches.
    """
    
    def __init__(self, settings: Optional[ConnectionSettings] = None):
        """
        Initialize and apply TCP connection optimizations.
        
        Args:
            settings: Connection settings (Pydantic model)
        """
        self.settings = settings or ConnectionSettings()
        self.is_windows = platform.system().lower() == 'windows'
        self.apply_optimizations()
        
    def apply_optimizations(self):
        """Apply TCP connection optimizations based on Pydantic settings."""
        # Set socket timeout from settings
        original_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(self.settings.TCP_TIMEOUT)
        logger.info(f"Increased socket timeout from {original_timeout} to {self.settings.TCP_TIMEOUT} seconds")
        
        if self.is_windows:
            try:
                # Windows-specific TCP optimizations
                logger.info("Applying Windows-specific TCP optimizations")
                
                # Import Windows-specific modules if needed
                try:
                    import win_inet_pton
                    logger.info("win_inet_pton imported successfully")
                except ImportError:
                    logger.warning("win_inet_pton not available - some IPv6 optimizations won't be applied")
                
                # Configure httplib2 for better reliability
                httplib2.RETRIES = self.settings.BATCH_RETRY_ATTEMPTS
                
            except Exception as e:
                logger.error(f"Failed to apply some Windows-specific TCP optimizations: {e}")
                
        # Optimize SSL settings
        try:
            # Set longer SSL handshake timeout
            original_ssl_connect = ssl.SSLSocket.connect
            
            def patched_ssl_connect(self, addr=None):
                """Patched SSL connect with longer timeout."""
                old_timeout = self.gettimeout()
                self.settimeout(20.0)  # 20 second timeout for SSL handshake
                try:
                    return original_ssl_connect(self, addr)
                finally:
                    self.settimeout(old_timeout)
            
            ssl.SSLSocket.connect = patched_ssl_connect
            logger.info("Applied SSL optimizations")
        except Exception as e:
            logger.error(f"Failed to apply SSL optimizations: {e}")
    
    def execute_batch_with_retry(self, service, batch_func, *args, **kwargs):
        """
        Execute a batch update with robust retry logic specifically for connection errors.
        
        Args:
            service: Google Sheets service
            batch_func: The batch update function to call
            *args, **kwargs: Arguments to pass to the batch function
            
        Returns:
            Result of the batch update
            
        Raises:
            Exception: If the batch update fails after all retries
        """
        max_retries = self.settings.BATCH_RETRY_ATTEMPTS
        
        for retry in range(max_retries):
            try:
                # If not the first attempt, add a delay with exponential backoff
                if retry > 0:
                    # Exponential backoff with jitter based on settings
                    base_delay = min(self.settings.MAX_RETRY_DELAY, 
                                     self.settings.MIN_RETRY_DELAY * (2 ** retry))
                    jitter = random.random() * min(1, retry)
                    wait_time = base_delay + jitter
                    
                    logger.info(f"Retry {retry}/{max_retries} for batch update. Waiting {wait_time:.2f} seconds")
                    time.sleep(wait_time)
                
                # Execute the batch update
                logger.info(f"Executing batch update (attempt {retry+1}/{max_retries})")
                result = batch_func(*args, **kwargs)
                
                # Add delay after successful batch if configured
                if self.settings.BATCH_SUCCESS_DELAY > 0:
                    time.sleep(self.settings.BATCH_SUCCESS_DELAY)
                    
                return result
                
            except (ConnectionAbortedError, ConnectionResetError, ConnectionError) as e:
                logger.warning(f"Connection error during batch update (retry {retry+1}/{max_retries}): {e}")
                
                # Reset connection on error
                if hasattr(service, '_http'):
                    service._http.connections.clear()
                    logger.info("Cleared connection pool")
                
                # On last retry, raise the exception
                if retry == max_retries - 1:
                    logger.error(f"Failed to execute batch update after {max_retries} retries")
                    raise
                    
            except HttpError as e:
                # Handle rate limit errors
                if e.resp.status == 429:
                    wait_time = self.settings.RATE_LIMIT_WAIT_TIME
                    logger.warning(f"Rate limit exceeded (retry {retry+1}/{max_retries}). Waiting {wait_time} seconds")
                    time.sleep(wait_time)
                    
                    # On last retry, raise the exception
                    if retry == max_retries - 1:
                        logger.error(f"Failed to execute batch update after {max_retries} retries due to rate limiting")
                        raise
                else:
                    # For other HTTP errors, just raise immediately
                    logger.error(f"HTTP error during batch update: {e}")
                    raise
                    
            except Exception as e:
                # For any other exception, log and raise
                logger.error(f"Unexpected error during batch update: {e}")
                raise