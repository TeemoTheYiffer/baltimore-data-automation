import time
import logging
from typing import Dict, Any, Optional
import uuid

logger = logging.getLogger("job_store")

class JobStore:
    """Manages job status storage, with Redis or in-memory fallback."""
    
    def __init__(self, use_redis=False, redis_client=None):
        self.use_redis = use_redis
        self.redis = redis_client
        if use_redis and not redis_client:
            self.use_redis = False
            logger.warning("Redis client not provided, falling back to in-memory storage")
        
        if not self.use_redis:
            self.jobs = {}
            logger.info("Initialized in-memory job store")
    
    def create_job(self) -> str:
        """Create a new job and return its ID."""
        job_id = str(uuid.uuid4())
        self._initialize_job(job_id)
        return job_id

    def _initialize_job(self, job_id: str) -> None:
        """Initialize a job in the store."""
        if self.use_redis:
            key = f"job:{job_id}"
            job_data = {
                "progress": "0",
                "status": "queued",
                "message": "Job has been queued",
                "created_at": str(time.time())
            }
            self.redis.hmset(key, job_data)
            self.redis.expire(key, 86400)  # 24 hour expiration
        else:
            self.jobs[job_id] = {
                "progress": 0,
                "status": "queued",
                "message": "Job has been queued",
                "created_at": time.time()
            }
    
    def update_job_progress(self, job_id: str, progress: int, status: str = "running", message: Optional[str] = None):
        """Update job progress in Redis or memory."""
        if self.use_redis:
            key = f"job:{job_id}"
            self.redis.hset(key, "progress", progress)
            self.redis.hset(key, "status", status)
            if message:
                self.redis.hset(key, "message", message)
            self.redis.expire(key, 86400)  # 24 hour expiration
        else:
            # In-memory fallback
            if job_id not in self.jobs:
                self.jobs[job_id] = {}
            self.jobs[job_id]["progress"] = progress
            self.jobs[job_id]["status"] = status
            if message:
                self.jobs[job_id]["message"] = message
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status from Redis or memory."""
        try:
            if self.use_redis:
                key = f"job:{job_id}"
                if not self.redis.exists(key):
                    logger.warning(f"Job not found in Redis: {job_id}")
                    return None

                try:
                    progress = int(self.redis.hget(key, "progress") or 0)
                    status = self.redis.hget(key, "status").decode("utf-8")
                    message_bytes = self.redis.hget(key, "message")
                    message = (
                        message_bytes.decode("utf-8")
                        if message_bytes
                        else f"Job is {status}. Progress: {progress}%"
                    )

                    return {
                        "job_id": job_id,
                        "progress": progress,
                        "status": status,
                        "message": message,
                    }
                except Exception as e:
                    logger.error(f"Error reading from Redis: {e}")
                    return None
            else:
                # In-memory fallback
                if job_id not in self.jobs:
                    logger.warning(f"Job not found in memory: {job_id}")
                    return None

                job_data = self.jobs[job_id]
                progress = job_data.get("progress", 0)
                status = job_data.get("status", "unknown")
                message = job_data.get("message", f"Job is {status}. Progress: {progress}%")

                return {
                    "job_id": job_id,
                    "progress": progress,
                    "status": status,
                    "message": message,
                }
        except Exception as e:
            logger.exception(f"Unexpected error in get_job_status: {e}")
            return None