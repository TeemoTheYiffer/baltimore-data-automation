from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import logging
import asyncio
from config import AppConfig, ProcessingMode
from sheets import SheetsManager
from utils.connection_manager import TCPConnectionManager
from utils.minimal_cache_manager import MinimalCacheManager
from utils.connection_settings import ConnectionSettings
from property_api import PropertyDataAPI
from scraper import WaterBillScraper
import redis

# Initialize FastAPI app
app = FastAPI(
    title="Maryland Property API",
    description="API for retrieving Maryland property and water bill data",
    version="1.0.0"
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('maryland_api.log')
    ]
)
logger = logging.getLogger("maryland_api")

# Initialize global config
config = AppConfig()
conn_settings = ConnectionSettings(
    TCP_TIMEOUT=config.TCP_TIMEOUT,
    BATCH_RETRY_ATTEMPTS=config.BATCH_RETRY_ATTEMPTS
)

# Initialize shared services
tcp_manager = TCPConnectionManager(settings=conn_settings)
sheets_manager = SheetsManager(
    config=config,
    tcp_manager=tcp_manager
)

# Try to connect to Redis, fall back to in-memory if unavailable
try:
    import redis
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    # Test connection
    redis_client.ping()
    use_redis = True
    logger.info("Redis connection successful - using Redis for job tracking")
except (ImportError, redis.exceptions.ConnectionError):
    use_redis = False
    job_store = {}
    logger.warning("Redis unavailable - using in-memory job tracking")

cache_manager = MinimalCacheManager(cache_dir=config.CACHE_DIRECTORY) if config.CACHE_ENABLED else None

# Request/response models
class PropertyRequestModel(BaseModel):
    address: Optional[str] = None
    parcel_id: Optional[str] = None
    county: str = "baltimore"

class WaterBillRequestModel(BaseModel):
    address: Optional[str] = None
    account_number: Optional[str] = None

class ProcessBatchRequestModel(BaseModel):
    counties: List[str] = Field(default=["baltimore"])
    mode: ProcessingMode = Field(default=ProcessingMode.PROPERTY)
    spreadsheet_id: Optional[str] = Field(default=None)
    sheet_name: Optional[str] = Field(default="LIENS")
    start_row: Optional[int] = Field(default=None)
    stop_row: Optional[int] = Field(default=None)
    max_rows: Optional[int] = Field(default=None)

class StatusResponse(BaseModel):
    job_id: str
    status: str
    message: str

# In-memory job tracking
active_jobs = {}

# Dependencies
def get_config():
    """Dependency to get application configuration."""
    return config

def get_sheets_manager():
    """Dependency to get sheets manager."""
    return sheets_manager

def get_cache_manager():
    """Dependency to get cache manager."""
    return cache_manager

def update_job_progress(job_id, progress, status="running", message=None):
    """Update job progress in Redis or memory."""
    if use_redis:
        key = f"job:{job_id}"
        redis_client.hset(key, "progress", progress)
        redis_client.hset(key, "status", status)
        if message:
            redis_client.hset(key, "message", message)
        redis_client.expire(key, 86400)  # 24 hour expiration
    else:
        # In-memory fallback
        if job_id not in job_store:
            job_store[job_id] = {}
        job_store[job_id]["progress"] = progress
        job_store[job_id]["status"] = status
        if message:
            job_store[job_id]["message"] = message
    
def get_job_status(job_id):
    """Get job status from Redis or memory."""
    try:
        if use_redis:
            key = f"job:{job_id}"
            if not redis_client.exists(key):
                logger.warning(f"Job not found in Redis: {job_id}")
                return None
            
            try:
                progress = int(redis_client.hget(key, "progress") or 0)
                status = redis_client.hget(key, "status").decode('utf-8')
                message_bytes = redis_client.hget(key, "message")
                message = message_bytes.decode('utf-8') if message_bytes else f"Job is {status}. Progress: {progress}%"
                
                return {
                    "job_id": job_id,
                    "progress": progress,
                    "status": status,
                    "message": message
                }
            except Exception as e:
                logger.error(f"Error reading from Redis: {e}")
                return None
        else:
            # In-memory fallback
            if job_id not in job_store:
                logger.warning(f"Job not found in memory: {job_id}")
                return None
            
            job_data = job_store[job_id]
            progress = job_data.get("progress", 0)
            status = job_data.get("status", "unknown")
            message = job_data.get("message", f"Job is {status}. Progress: {progress}%")
            
            return {
                "job_id": job_id,
                "progress": progress,
                "status": status,
                "message": message
            }
    except Exception as e:
        logger.exception(f"Unexpected error in get_job_status: {e}")
        return None

# Routes
@app.get("/", response_class=JSONResponse)
async def root():
    """Root endpoint."""
    return {"message": "Maryland Property API is running"}

@app.get("/sheets", response_class=JSONResponse)
async def get_sheets(sheets_manager: SheetsManager = Depends(get_sheets_manager)):
    """Get list of available sheets."""
    try:
        sheet_names = sheets_manager.get_all_sheet_names()
        return {"sheets": sheet_names}
    except Exception as e:
        logger.error(f"Error listing sheets: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/property", response_class=JSONResponse)
async def get_property_data(
    request: PropertyRequestModel,
    config: AppConfig = Depends(get_config)
):
    """Get property data for a single address or parcel ID."""
    try:
        # Validate request
        if not request.address and not request.parcel_id:
            raise HTTPException(status_code=400, detail="Either address or parcel_id is required")
        
        # Determine which field to use based on county
        county_config = config.get_county_config(request.county)
        if county_config.identifier_type == "parcel_id" and not request.parcel_id:
            raise HTTPException(
                status_code=400, 
                detail=f"{request.county} county requires a parcel_id"
            )
        
        # Get the identifier value
        identifier = request.parcel_id if county_config.identifier_type == "parcel_id" else request.address
        
        # Initialize API client and get data
        api = PropertyDataAPI(config, county=request.county)
        result = api.get_property_data(identifier)
        
        if not result.get("success", False):
            return JSONResponse(
                status_code=404,
                content={
                    "success": False,
                    "message": result.get("message", "Property data not found")
                }
            )
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting property data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/waterbill", response_class=JSONResponse)
async def get_water_bill_data(
    request: WaterBillRequestModel,
    config: AppConfig = Depends(get_config)
):
    """Get water bill data for a single address or account number."""
    try:
        # Validate request
        if not request.address and not request.account_number:
            raise HTTPException(status_code=400, detail="Either address or account_number is required")
        
        # Initialize scraper
        scraper = WaterBillScraper(config)
        
        # Get data based on what was provided
        if request.account_number:
            result = scraper.get_bill_details_by_account_number(request.account_number)
        else:
            result = scraper.get_water_bill_details(request.address)
        
        if not result.get("success", False):
            return JSONResponse(
                status_code=404,
                content={
                    "success": False,
                    "message": result.get("message", "Water bill data not found")
                }
            )
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting water bill data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/batch", response_model=StatusResponse)
async def process_batch(
    request: ProcessBatchRequestModel,
    background_tasks: BackgroundTasks,
    config: AppConfig = Depends(get_config),
    sheets_manager: SheetsManager = Depends(get_sheets_manager),
    cache_manager: Optional[MinimalCacheManager] = Depends(get_cache_manager)
):
    """Start a batch processing job."""
    try:
        # Generate a job ID
        import uuid
        job_id = str(uuid.uuid4())
        
        # Configure the job based on request
        job_config = AppConfig()  # Create a copy of the base config
        job_config.PROCESSING_MODE = request.mode
        job_config.COUNTIES = request.counties

        # Use the spreadsheet_id from request if provided, otherwise use county-specific defaults
        if request.spreadsheet_id:
            # Override the spreadsheet ID for all counties in this batch
            for county in request.counties:
                county_config = job_config.get_county_config(county)
                county_config.spreadsheet_id = request.spreadsheet_id
                
        logger.info(f"Request start_row: {request.start_row}, stop_row: {request.stop_row}, max_rows: {request.max_rows}")
        
        job_config.SHEET_NAME = request.sheet_name
        job_config.START_ROW = request.start_row if request.start_row is not None else config.START_ROW
        job_config.STOP_ROW = request.stop_row if request.stop_row is not None else config.STOP_ROW
        job_config.MAX_ROWS = request.max_rows if request.max_rows is not None else config.MAX_ROWS

        logger.info(f"Job configuration: mode={job_config.PROCESSING_MODE}, counties={job_config.COUNTIES}")
        logger.info(f"Job rows: start={job_config.START_ROW}, stop={job_config.STOP_ROW}, max={job_config.MAX_ROWS}")

        update_job_progress(
            job_id=job_id,
            progress=0,
            status="queued",
            message="Batch processing job has been queued"
        )

        if not use_redis:
            logger.info(f"Current job store contents: {list(job_store.keys())}")

        # Add background task
        background_tasks.add_task(
            run_batch_processing_job,
            job_id,
            job_config,
            sheets_manager,
            cache_manager
        )
        
        return {
            "job_id": job_id,
            "status": "queued",
            "message": "Batch processing job has been queued"
        }
        
    except Exception as e:
        logger.error(f"Error starting batch job: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/batch/{job_id}", response_model=StatusResponse)
async def get_batch_status(job_id: str):
    """Get the status of a batch processing job."""
    try:
        # Log that we're accessing this endpoint
        logger.info(f"Checking status for job: {job_id}")
        
        # Get job status
        job_status = get_job_status(job_id)
        
        # Debug log the retrieved status
        logger.info(f"Retrieved status: {job_status}")
        
        if not job_status:
            logger.warning(f"Job not found: {job_id}")
            raise HTTPException(status_code=404, detail="Job not found")
        
        return {
            "job_id": job_id,
            "status": job_status["status"],
            "message": job_status["message"]
        }
    except Exception as e:
        # Log the full exception with traceback
        logger.exception(f"Error checking job status for {job_id}: {str(e)}")
        # Return a 500 with some information
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

# Background processing function
async def run_batch_processing_job(
    job_id: str,
    config: AppConfig,
    sheets_manager: SheetsManager,
    cache_manager: Optional[MinimalCacheManager]
):
    """Run a batch processing job in the background."""
    try:
        logger.info(f"Job {job_id} starting with config: MAX_ROWS={config.MAX_ROWS}, START_ROW={config.START_ROW}")
        # Initialize progress
        update_job_progress(
            job_id=job_id, 
            progress=0, 
            status="running",
            message="Batch processing started"
        )

        # Process based on mode
        if config.PROCESSING_MODE in [ProcessingMode.WATER, ProcessingMode.BOTH, ProcessingMode.ALL]:
            # Process water bills
            water_sheet_name = config.SHEET_NAME or config.WATER_BILL_SHEET_NAME
            if sheets_manager.sheet_exists(water_sheet_name):
                # Import here to avoid circular imports
                from routes import process_addresses_for_bill_details
                
                # Update job progress
                update_job_progress(
                    job_id=job_id,
                    progress=10,
                    message="Water bill in-progress..."
                )
                
                await asyncio.to_thread(
                    process_addresses_for_bill_details,
                    config=config,
                    sheets_manager=sheets_manager,
                    sheet_name=water_sheet_name,
                    cache_manager=cache_manager
                )
                
                # Update job progress
                update_job_progress(
                    job_id=job_id,
                    progress=40,
                    message="Water bill processing completed"
                )
        
        if config.PROCESSING_MODE in [ProcessingMode.PROPERTY, ProcessingMode.BOTH, ProcessingMode.ALL]:
            # Process property data
            property_sheet_name = config.SHEET_NAME or config.PROPERTY_SHEET_NAME
            
            if sheets_manager.sheet_exists(property_sheet_name):
                # Import here to avoid circular imports
                from routes import process_county_property_data
                
                counties = config.get_counties_to_process()
                logger.info(f"Processing {len(counties)} counties: {', '.join(counties)}")
                
                # Calculate progress for property processing
                counties = config.get_counties_to_process()
                progress_per_county = 60 / max(len(counties), 1)  # Avoid division by zero
                
                for i, county in enumerate(counties):
                    # Update progress before processing this county
                    current_progress = 40 + i * progress_per_county
                    update_job_progress(
                        job_id=job_id,
                        progress=int(current_progress),
                        message=f"Processing {county} county ({i+1}/{len(counties)})"
                    )

                    # Process county
                    await asyncio.to_thread(
                        process_county_property_data,
                        county_name=county,
                        config=config,
                        sheets_manager=sheets_manager,
                        sheet_name=config.SHEET_NAME or config.PROPERTY_SHEET_NAME,
                        cache_manager=cache_manager
                    )
                    
                    # Update progress after processing this county
                    current_progress = 40 + (i + 1) * progress_per_county
                    update_job_progress(
                        job_id=job_id,
                        progress=int(current_progress),
                        message=f"Completed {county} county ({i+1}/{len(counties)})"
                    )
        
        
        # Mark as complete
        update_job_progress(
            job_id=job_id,
            progress=100,
            status="completed",
            message="Batch processing completed successfully"
        )
        
        # Keep job info for a while, then clean up
        asyncio.create_task(cleanup_job(job_id))
        
    except Exception as e:
        # Handle errors
        update_job_progress(
            job_id=job_id,
            progress=0,
            status="failed",
            message=f"Error: {str(e)}"
        )

async def cleanup_job(job_id: str):
    """Clean up job data after a delay."""
    await asyncio.sleep(3600)  # Keep job data for 1 hour
    if job_id in active_jobs:
        del active_jobs[job_id]