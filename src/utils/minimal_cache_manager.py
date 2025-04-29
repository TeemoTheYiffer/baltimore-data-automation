import os
import json
import logging
import time
import hashlib
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger("minimal_cache")

class MinimalCacheManager:
    """
    Simple cache manager to preserve data between runs without changing batch behavior.
    Focuses on preserving scraped data only, not modifying batch sizes or strategies.
    """
    
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
        hash_obj = hashlib.md5(identifier.encode('utf-8'))
        return f"{data_type}_{hash_obj.hexdigest()}.json"
    
    def save_to_cache(self, identifier: str, data: Dict[str, Any], data_type: str) -> bool:
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
            
            # Add timestamp and metadata to cache entry
            cache_entry = {
                'timestamp': time.time(),
                'identifier': identifier,
                'data': data
            }
            
            with open(cache_path, 'w') as f:
                json.dump(cache_entry, f)
                
            logger.debug(f"Saved {data_type} data for {identifier} to cache")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to cache: {e}")
            return False
    
    def get_from_cache(self, identifier: str, data_type: str) -> Optional[Dict[str, Any]]:
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
                
            with open(cache_path, 'r') as f:
                cache_entry = json.load(f)
                
            logger.debug(f"Retrieved {data_type} data for {identifier} from cache")
            return cache_entry['data']
            
        except Exception as e:
            logger.error(f"Error reading from cache: {e}")
            return None
    
    def get_pending_updates(self, data_type: str) -> List[Tuple[int, Dict[str, Any]]]:
        """
        Get all pending updates from the cache.
        
        Args:
            data_type: Type of data (e.g., 'water_bill', 'property')
            
        Returns:
            List[tuple]: List of (row_index, data) tuples
        """
        pending_updates = []
        
        try:
            # Find all cache files for this data type
            for filename in os.listdir(self.cache_dir):
                if not filename.startswith(f"{data_type}_") or not filename.endswith(".json"):
                    continue
                    
                cache_path = os.path.join(self.cache_dir, filename)
                
                try:
                    with open(cache_path, 'r') as f:
                        cache_entry = json.load(f)
                        
                    data = cache_entry.get('data', {})
                    row_index = data.get('row_index')
                    
                    if row_index is not None and 'data' in data:
                        pending_updates.append((row_index, data['data']))
                        
                except Exception as inner_e:
                    logger.error(f"Error reading cache file {filename}: {inner_e}")
                    continue
                    
            # Sort by row index
            pending_updates.sort(key=lambda x: x[0])
            
            logger.info(f"Retrieved {len(pending_updates)} pending {data_type} updates from cache")
            return pending_updates
            
        except Exception as e:
            logger.error(f"Error getting pending updates: {e}")
            return []
    
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
                logger.debug(f"Removed {data_type} data for {identifier} from cache")
                
            return True
            
        except Exception as e:
            logger.error(f"Error removing from cache: {e}")
            return False
    
    def mark_batch_complete(self, batch_id: str, batch: List[tuple]) -> bool:
        """
        Mark a batch as successfully completed.
        
        Args:
            batch_id: Unique identifier for the batch (e.g. "water_bill_0-499")
            batch: List of (row_index, data) tuples that were processed
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create a batch completion record
            batch_path = os.path.join(self.cache_dir, f"batch_{batch_id}.json")
            
            batch_data = {
                'timestamp': time.time(),
                'batch_id': batch_id,
                'row_indices': [idx for idx, _ in batch]
            }
            
            with open(batch_path, 'w') as f:
                json.dump(batch_data, f)
                
            logger.info(f"Marked batch {batch_id} as complete")
            return True
            
        except Exception as e:
            logger.error(f"Error marking batch as complete: {e}")
            return False
    
    def is_batch_complete(self, batch_id: str) -> bool:
        """
        Check if a batch has been successfully completed.
        
        Args:
            batch_id: Unique identifier for the batch
            
        Returns:
            bool: True if the batch is marked as complete, False otherwise
        """
        batch_path = os.path.join(self.cache_dir, f"batch_{batch_id}.json")
        return os.path.exists(batch_path)
    
    def get_all_completed_batches(self) -> List[str]:
        """
        Get all batch IDs that have been marked as complete.
        
        Returns:
            List[str]: List of completed batch IDs
        """
        completed_batches = []
        
        try:
            for filename in os.listdir(self.cache_dir):
                if filename.startswith('batch_') and filename.endswith('.json'):
                    batch_id = filename[6:-5]  # Remove 'batch_' prefix and '.json' suffix
                    completed_batches.append(batch_id)
                    
            return completed_batches
            
        except Exception as e:
            logger.error(f"Error getting completed batches: {e}")
            return []
    
    def cache_batch_results(self, batch: List[tuple], data_type: str) -> bool:
        """
        Cache results for an entire batch.
        
        Args:
            batch: List of (row_index, data) tuples
            data_type: Type of data (e.g., 'water_bill', 'property')
            
        Returns:
            bool: True if all items were cached successfully, False otherwise
        """
        success = True
        
        for row_index, data in batch:
            # Get identifier from data
            identifier = None
            
            if data_type == 'water_bill':
                if data.get('success', False) and 'data' in data:
                    identifier = data['data'].get('account_number', f"row_{row_index}")
                elif 'account_number' in data:
                    identifier = data['account_number']
                else:
                    identifier = f"row_{row_index}"
            elif data_type == 'property':
                if data.get('success', False) and 'address' in data:
                    identifier = data['address']
                else:
                    identifier = f"row_{row_index}"
            else:
                identifier = f"row_{row_index}"
                
            cache_data = {
                'row_index': row_index,
                'data': data
            }
            
            if not self.save_to_cache(identifier, cache_data, data_type):
                success = False
                
        return success