import json
import logging
import time
import hashlib
from typing import Dict, Any, List, Optional

logger = logging.getLogger("redis_cache")


class RedisCacheManager:
    """Redis-based cache manager for fast concurrent access."""
    
    def __init__(self, redis_client):
        """Initialize with Redis client."""
        self.redis = redis_client
        
    def _get_cache_key(self, identifier: str, data_type: str) -> str:
        """Generate a cache key for an identifier."""
        hash_obj = hashlib.md5(identifier.encode("utf-8"))
        return f"cache:{data_type}:{hash_obj.hexdigest()}"
    
    def save_to_cache(self, identifier: str, data: Dict[str, Any], data_type: str) -> bool:
        """Save data to Redis cache."""
        try:
            cache_key = self._get_cache_key(identifier, data_type)
            
            # Add timestamp and metadata to cache entry
            cache_entry = {
                "timestamp": time.time(),
                "identifier": identifier, 
                "data": data,
            }
            
            # Store in Redis with 24 hour expiration
            self.redis.setex(
                cache_key, 
                86400,  # 24 hours TTL
                json.dumps(cache_entry)
            )
            
            logger.debug(f"Saved {data_type} data for {identifier} to Redis cache")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to Redis cache: {e}")
            return False
    
    def get_from_cache(self, identifier: str, data_type: str) -> Optional[Dict[str, Any]]:
        """Get data from Redis cache."""
        try:
            cache_key = self._get_cache_key(identifier, data_type)
            cached_data = self.redis.get(cache_key)
            
            if cached_data:
                cache_entry = json.loads(cached_data)
                logger.debug(f"Retrieved {data_type} data for {identifier} from Redis cache")
                return cache_entry.get("data")
                
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving from Redis cache: {e}")
            return None
    
    def remove_from_cache(self, identifier: str, data_type: str) -> bool:
        """Remove data from Redis cache."""
        try:
            cache_key = self._get_cache_key(identifier, data_type)
            result = self.redis.delete(cache_key)
            logger.debug(f"Removed {data_type} data for {identifier} from Redis cache")
            return result > 0
            
        except Exception as e:
            logger.error(f"Error removing from Redis cache: {e}")
            return False
    
    def get_pending_updates(self, data_type: str) -> List:
        """Get pending updates (Redis doesn't need this for our use case)."""
        # For Redis, we don't maintain pending updates list
        # This method exists for compatibility with MinimalCacheManager interface
        return []
    
    def clear_cache(self, data_type: str) -> bool:
        """Clear all cache entries for a data type."""
        try:
            pattern = f"cache:{data_type}:*"
            keys = self.redis.keys(pattern)
            if keys:
                self.redis.delete(*keys)
                logger.info(f"Cleared {len(keys)} cache entries for {data_type}")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing Redis cache: {e}")
            return False