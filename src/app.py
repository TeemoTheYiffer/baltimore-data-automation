from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from config import AppConfig, ProcessingMode, CountyEnum
from fastapi.responses import JSONResponse
from typing import Optional
from functools import lru_cache
import logging
import asyncio
from sheets import SheetsManager
from utils.connection_manager import TCPConnectionManager
from utils.minimal_cache_manager import MinimalCacheManager
from utils.connection_settings import ConnectionSettings
from web_utils.api_routes import process_property_data, process_water_bills
from web_utils.models import ProcessBatchRequestModel, WaterBillRequestModel, StatusResponse, PropertyRequestModel
from web_utils.job_store import JobStore
from property_api import PropertyDataAPI
from scraper import WaterBillScraper
import redis

# Initialize FastAPI app
app = FastAPI(
    title="Maryland Property API",
    description="API for retrieving Maryland property and water bill data",
    version="1.0.0",
    docs_url="/"
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("maryland_api.log")],
)
logger = logging.getLogger("maryland_api")

# Initialize global config
config = AppConfig()
conn_settings = ConnectionSettings(
    TCP_TIMEOUT=config.TCP_TIMEOUT, BATCH_RETRY_ATTEMPTS=config.BATCH_RETRY_ATTEMPTS
)

# Initialize shared services
tcp_manager = TCPConnectionManager(settings=conn_settings)
sheets_manager = SheetsManager(config=config, tcp_manager=tcp_manager)

# Try to connect to Redis, fall back to in-memory if unavailable
try:
    import redis
    redis_client = redis.Redis(host="localhost", port=6379, db=0) # Test connection
    redis_client.ping()
    job_store = JobStore(use_redis=True, redis_client=redis_client)
    logger.info("Redis connection successful - using Redis for job tracking")
except (ImportError, redis.exceptions.ConnectionError):
    job_store = JobStore(use_redis=False)
    logger.warning("Redis unavailable - using in-memory job tracking")

cache_manager = (
    MinimalCacheManager(cache_dir=config.CACHE_DIRECTORY)
    if config.CACHE_ENABLED
    else None
)

# In-memory job tracking
active_jobs = {}


# Dependencies
@lru_cache()
def get_config():
    """Get cached application configuration."""
    return AppConfig()


def get_sheets_manager():
    """Dependency to get sheets manager."""
    return sheets_manager


def get_cache_manager():
    """Dependency to get cache manager."""
    return cache_manager


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
    request: PropertyRequestModel, config: AppConfig = Depends(get_config)
):
    """Get property data for a single address or parcel ID."""
    try:
        # Validate request
        if not request.address and not request.parcel_id:
            raise HTTPException(
                status_code=400, detail="Either address or parcel_id is required"
            )

        # Determine which field to use based on county
        county_config = config.get_county_config(request.county)
        if county_config.identifier_type == "parcel_id" and not request.parcel_id:
            raise HTTPException(
                status_code=400, detail=f"{request.county} county requires a parcel_id"
            )

        # Get the identifier value
        identifier = (
            request.parcel_id
            if county_config.identifier_type == "parcel_id"
            else request.address
        )

        # Initialize API client and get data
        api = PropertyDataAPI(config, county=request.county)
        result = api.get_property_data(identifier)

        if not result.get("success", False):
            return JSONResponse(
                status_code=404,
                content={
                    "success": False,
                    "message": result.get("message", "Property data not found"),
                },
            )

        return result

    except Exception as e:
        logger.error(f"Error getting property data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/waterbill", response_class=JSONResponse)
async def get_water_bill_data(
    request: WaterBillRequestModel, config: AppConfig = Depends(get_config)
):
    """Get water bill data for a single address or account number."""
    try:
        # Validate request
        if not request.address and not request.account_number:
            raise HTTPException(
                status_code=400, detail="Either address or account_number is required"
            )

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
                    "message": result.get("message", "Water bill data not found"),
                },
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
    cache_manager: Optional[MinimalCacheManager] = Depends(get_cache_manager),
):
    """
    Start a batch processing job for property or water bill data.
    
    - **counties**: List of counties to process ('baltimore', 'pg', 'frederick', or 'all')
    - **mode**: Processing mode ('water', 'property', 'both', or 'all')
    - **spreadsheet_id**: Optional custom spreadsheet ID to use
    - **sheet_name**: Name of the sheet to process
    - **start_row**: First row to process (2 is the first data row)
    - **stop_row**: Last row to process (0 means use MAX_ROWS from start_row)
    - **max_rows**: Maximum number of rows to process
    
    Returns a job ID that can be used to check progress with the /batch/{job_id} endpoint.
    """
    try:
        # Validate that counties were provided
        if not request.counties:
            raise HTTPException(
                status_code=400, 
                detail="At least one county must be specified in the request"
            )
        
        # Validate modes
        if request.mode not in [m.value for m in ProcessingMode]:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid processing mode. Valid modes are: {', '.join([m.value for m in ProcessingMode])}"
            )

        # Validate counties exist in our system
        valid_counties = CountyEnum.get_all()
        for county in request.counties:
            if county.lower() not in valid_counties and county.lower() != "all":
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid county '{county}'. Valid counties are: {', '.join(valid_counties)} or 'all'"
                )

        # Generate a job ID
        job_id = job_store.create_job()

        # Configure the job based on request
        job_config = AppConfig()  # Create a copy of the base config
        job_config.PROCESSING_MODE = request.mode
        job_config.COUNTIES = request.counties

        # Set the current county based on the first county in the request
        current_county = request.counties[0]
        job_config.set_current_county(current_county)

        # Apply spreadsheet_id override if provided
        if request.spreadsheet_id:
            county_config = job_config.get_county_config(current_county)
            county_config.spreadsheet_id = request.spreadsheet_id

        # Initialize sheets manager
        sheets_manager.county = current_county
        sheets_manager.spreadsheet_id = job_config.get_county_config(current_county).spreadsheet_id

        logger.info(
            f"Request start_row: {request.start_row}, stop_row: {request.stop_row}, max_rows: {request.max_rows}"
        )

        job_config.SHEET_NAME = request.sheet_name
        job_config.START_ROW = (
            request.start_row if request.start_row is not None else config.START_ROW
        )
        job_config.STOP_ROW = (
            request.stop_row if request.stop_row is not None else config.STOP_ROW
        )
        job_config.MAX_ROWS = (
            request.max_rows if request.max_rows is not None else config.MAX_ROWS
        )

        logger.info(
            f"Job configuration: mode={job_config.PROCESSING_MODE}, counties={job_config.COUNTIES}"
        )
        logger.info(
            f"Job rows: start={job_config.START_ROW}, stop={job_config.STOP_ROW}, max={job_config.MAX_ROWS}"
        )

        job_store.update_job_progress(
            job_id=job_id,
            progress=0,
            status="queued",
            message="Batch processing job has been queued",
        )

        if not job_store.use_redis:
            logger.info(f"Current job store contents: {list(job_store.keys())}")

        # Add background task
        background_tasks.add_task(
            run_batch_processing_job, job_id, job_config, sheets_manager, cache_manager
        )

        return {
            "job_id": job_id,
            "status": "queued",
            "message": "Batch processing job has been queued",
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
        job_status = job_store.get_job_status(job_id)

        # Debug log the retrieved status
        logger.info(f"Retrieved status: {job_status}")

        if not job_status:
            logger.warning(f"Job not found: {job_id}")
            raise HTTPException(status_code=404, detail="Job not found")

        return {
            "job_id": job_id,
            "status": job_status["status"],
            "message": job_status["message"],
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
    cache_manager: Optional[MinimalCacheManager],
):
    """Run a batch processing job in the background."""
    try:
        logger.info(f"Job {job_id} starting with config: MAX_ROWS={config.MAX_ROWS}, START_ROW={config.START_ROW}")

        # Initialize progress
        job_store.update_job_progress(
            job_id=job_id,
            progress=0,
            status="running",
            message="Batch processing started",
        )

        # Process based on mode
        if config.PROCESSING_MODE in [ProcessingMode.WATER, ProcessingMode.BOTH, ProcessingMode.ALL]:
            # Process water bills
            await process_water_bills(job_id, config, sheets_manager, cache_manager, job_store)

        if config.PROCESSING_MODE in [ProcessingMode.PROPERTY, ProcessingMode.BOTH, ProcessingMode.ALL]:
            # Process property data
            await process_property_data(job_id, config, sheets_manager, cache_manager, job_store)

        # Mark as complete
        job_store.update_job_progress(
            job_id=job_id,
            progress=100,
            status="completed",
            message="Batch processing completed successfully",
        )

    except Exception as e:
        # Handle errors
        logger.exception(f"Error in batch job {job_id}: {e}")
        job_store.update_job_progress(
            job_id=job_id, 
            progress=0, 
            status="failed", 
            message=f"Error: {str(e)}"
        )

    except Exception as e:
        # Handle errors
        job_store.update_job_progress(
            job_id=job_id, progress=0, status="failed", message=f"Error: {str(e)}"
        )


async def cleanup_job(job_id: str):
    """Clean up job data after a delay."""
    await asyncio.sleep(3600)  # Keep job data for 1 hour
    if job_id in active_jobs:
        del active_jobs[job_id]
