import os
import json
import logging
import time
import hashlib
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger("cache")


class CacheManager:
    """Manages caching of scraped data to allow resuming after errors."""

    def __init__(self, cache_dir: str = "cache"):
        """Initialize the cache manager."""
        self.cache_dir = cache_dir
        self._ensure_cache_dir()

    def _ensure_cache_dir(self):
        """Ensure the cache directory exists."""
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_cache_key(self, identifier: str, data_type: str) -> str:
        """Generate a cache key for an identifier."""
        # Create a hash to use as the filename
        hash_obj = hashlib.md5(identifier.encode("utf-8"))
        return f"{data_type}_{hash_obj.hexdigest()}.json"

    def save_to_cache(
        self, identifier: str, data: Dict[str, Any], data_type: str
    ) -> bool:
        """
        Save data to the cache.

        Args:
            identifier: Unique identifier (like address or account number)
            data: Data to cache
            data_type: Type of data (e.g., 'water_bill', 'property')

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            cache_key = self._get_cache_key(identifier, data_type)
            cache_path = os.path.join(self.cache_dir, cache_key)

            # Add timestamp to cache entry
            cache_entry = {
                "timestamp": time.time(),
                "identifier": identifier,
                "data": data,
            }

            with open(cache_path, "w") as f:
                json.dump(cache_entry, f)

            logger.info(f"Saved {data_type} data for {identifier} to cache")
            return True

        except Exception as e:
            logger.error(f"Error saving to cache: {e}")
            return False

    def get_from_cache(
        self, identifier: str, data_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get data from the cache.

        Args:
            identifier: Unique identifier (like address or account number)
            data_type: Type of data (e.g., 'water_bill', 'property')

        Returns:
            Dict or None: The cached data if found, None otherwise
        """
        try:
            cache_key = self._get_cache_key(identifier, data_type)
            cache_path = os.path.join(self.cache_dir, cache_key)

            if not os.path.exists(cache_path):
                return None

            with open(cache_path, "r") as f:
                cache_entry = json.load(f)

            logger.info(f"Retrieved {data_type} data for {identifier} from cache")
            return cache_entry["data"]

        except Exception as e:
            logger.error(f"Error reading from cache: {e}")
            return None

    def remove_from_cache(self, identifier: str, data_type: str) -> bool:
        """
        Remove data from the cache.

        Args:
            identifier: Unique identifier (like address or account number)
            data_type: Type of data (e.g., 'water_bill', 'property')

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            cache_key = self._get_cache_key(identifier, data_type)
            cache_path = os.path.join(self.cache_dir, cache_key)

            if os.path.exists(cache_path):
                os.remove(cache_path)
                logger.info(f"Removed {data_type} data for {identifier} from cache")

            return True

        except Exception as e:
            logger.error(f"Error removing from cache: {e}")
            return False

    def get_all_cached_data(self, data_type: str) -> Dict[str, Dict[str, Any]]:
        """
        Get all cached data of a specific type.

        Args:
            data_type: Type of data (e.g., 'water_bill', 'property')

        Returns:
            Dict: Mapping of identifiers to cached data
        """
        cached_data = {}

        try:
            # Find all cache files for this data type
            for filename in os.listdir(self.cache_dir):
                if not filename.startswith(f"{data_type}_") or not filename.endswith(
                    ".json"
                ):
                    continue

                cache_path = os.path.join(self.cache_dir, filename)

                try:
                    with open(cache_path, "r") as f:
                        cache_entry = json.load(f)

                    identifier = cache_entry.get("identifier")
                    data = cache_entry.get("data")

                    if identifier and data:
                        cached_data[identifier] = data

                except Exception as inner_e:
                    logger.error(f"Error reading cache file {filename}: {inner_e}")
                    continue

            logger.info(f"Retrieved {len(cached_data)} cached {data_type} entries")
            return cached_data

        except Exception as e:
            logger.error(f"Error getting all cached data: {e}")
            return {}

    def clear_cache(self, data_type: Optional[str] = None) -> bool:
        """
        Clear the cache, optionally for a specific data type.

        Args:
            data_type: Type of data (e.g., 'water_bill', 'property'), or None for all

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            for filename in os.listdir(self.cache_dir):
                if data_type and not filename.startswith(f"{data_type}_"):
                    continue

                if filename.endswith(".json"):
                    cache_path = os.path.join(self.cache_dir, filename)
                    os.remove(cache_path)

            logger.info(f"Cleared cache{' for ' + data_type if data_type else ''}")
            return True

        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
            return False

    def save_batch_to_cache(self, batch_data: List[tuple], data_type: str) -> bool:
        """
        Save a batch of data to the cache.

        Args:
            batch_data: List of (row_index, data) tuples
            data_type: Type of data (e.g., 'water_bill', 'property')

        Returns:
            bool: True if successful, False otherwise
        """
        success = True

        for row_index, data in batch_data:
            # Get identifier from data (address or account number)
            identifier = None

            if data_type == "water_bill":
                if data.get("success", False) and "data" in data:
                    identifier = data["data"].get("account_number")
                    if not identifier and "address" in data:
                        identifier = data["address"]
                elif "account_number" in data:
                    identifier = data["account_number"]
            elif data_type == "property":
                if "address" in data:
                    identifier = data["address"]

            if not identifier:
                # Use row index as fallback identifier
                identifier = f"row_{row_index}"

            # Save this entry to cache with row index included
            cache_data = {"row_index": row_index, "data": data}

            if not self.save_to_cache(identifier, cache_data, data_type):
                success = False

        return success

    def get_pending_updates(self, data_type: str) -> List[Tuple[int, Dict[str, Any]]]:
        """
        Get all pending updates from the cache.

        Args:
            data_type: Type of data (e.g., 'water_bill', 'property')

        Returns:
            List[tuple]: List of (row_index, data) tuples
        """
        pending_updates = []

        all_cached = self.get_all_cached_data(data_type)

        for _, cache_data in all_cached.items():
            row_index = cache_data.get("row_index")
            data = cache_data.get("data")

            if row_index is not None and data:
                pending_updates.append((row_index, data))

        # Sort by row index
        pending_updates.sort(key=lambda x: x[0])

        return pending_updates
