from fastapi import FastAPI, HTTPException, BackgroundTasks
from config import AppConfig, CountyEnum
from web_utils.models import ProcessingMode
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
from web_utils.models import (
    ProcessBatchRequestModel, WaterBillRequestModel, StatusResponse,
    PropertyRequestModel, PropertySampleRequestModel, SheetsRequestModel,
    NJPropertyRequestModel, NJPropertySampleRequestModel, NJBatchRequestModel
)
from web_utils.job_store import JobStore
from utils.redis_cache_manager import RedisCacheManager
from property_api import PropertyDataAPI
from scraper import WaterBillScraper
from nj_property_api import NJPropertyAPI, NJ_MUNICIPALITIES, NJ_FIELD_MAPPING
from web_utils.nj_routes import process_nj_property_data


# Initialize FastAPI app
app = FastAPI(
    title="Maryland Property API",
    description="API for retrieving Maryland property and water bill data",
    version="1.0.0",
    docs_url="/"
)

# Get config first
config = AppConfig()

# Convert string level to logging level
log_level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)

# Configure logging
logging.basicConfig(
    level=log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("maryland_api.log")],
)
logger = logging.getLogger("maryland_api")

# Initialize global config
conn_settings = ConnectionSettings(
    TCP_TIMEOUT=config.TCP_TIMEOUT, BATCH_RETRY_ATTEMPTS=config.BATCH_RETRY_ATTEMPTS
)

# Initialize shared services
tcp_manager = TCPConnectionManager(settings=conn_settings)
sheets_manager = SheetsManager(config=config, tcp_manager=tcp_manager)

# Try to connect to Redis, fall back to in-memory if unavailable
redis_client = None  # Initialize to None for use in endpoints
try:
    import redis
    # Create a connection pool for better concurrency
    redis_pool = redis.ConnectionPool(
        host="localhost",
        port=6379,
        db=0,
        socket_connect_timeout=5,
        socket_timeout=10,
        retry_on_timeout=True,
        max_connections=20  # Support 10 threads + overhead
    )

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.ping()
    job_store = JobStore(use_redis=True, redis_client=redis_client)
    
    # Use Redis for cache too!
    cache_manager = (
        RedisCacheManager(redis_client=redis_client)
        if config.CACHE_ENABLED
        else None
    )
    
    logger.info("Redis connection successful - using Redis for job tracking AND caching")
except Exception:
    job_store = JobStore(use_redis=False)
    cache_manager = (
        MinimalCacheManager(cache_dir=config.CACHE_DIRECTORY)
        if config.CACHE_ENABLED
        else None
    )
    logger.warning("Redis unavailable - using in-memory job tracking and file caching")

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


@app.post("/sheets", response_class=JSONResponse)
async def get_sheets(request: SheetsRequestModel):
    """Get list of available sheets for a specific spreadsheet."""
    try:
        # Create a temporary SheetsManager for this specific spreadsheet
        temp_config = AppConfig()
        temp_sheets_manager = SheetsManager(config=temp_config)
        temp_sheets_manager.spreadsheet_id = request.spreadsheet_id
        
        sheet_names = temp_sheets_manager.get_all_sheet_names()
        return {"sheets": sheet_names, "spreadsheet_id": request.spreadsheet_id}
    except Exception as e:
        logger.error(f"Error listing sheets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/property", response_class=JSONResponse)
async def get_property_data(request: PropertyRequestModel):
    """
    Get property data for a single address or parcel ID.
    
    - **address**: Property address to search for (required if parcel_id not provided)
    - **parcel_id**: Parcel ID to search for (required if address not provided)  
    - **county**: County to search in (default: baltimore)
    - **optional_params**: Additional search parameters (optional)
        - Keys should match FIELD_MAPPING names (e.g., "District")
        - Values are the actual search values to use in queries
        - Example: {"District": "01"}
        - Use /field-mappings endpoint to see available keys
    """
    try:
        # Create job-specific config instead of dependency injection
        job_config = AppConfig()
        job_config.set_current_county(request.county)
        
        # Validate request
        if not request.address and not request.parcel_id:
            raise HTTPException(
                status_code=400, detail="Either address or parcel_id is required"
            )

        # Determine which field to use based on county
        county_config = job_config.get_county_config(request.county)
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
        api = PropertyDataAPI(request.county, job_config)
        result = api.get_property_data(identifier, optional_params=request.optional_params)

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
async def get_water_bill_data(request: WaterBillRequestModel):
    """Get water bill data for a single address or account number."""
    try:
        # Create job-specific config instead of dependency injection
        job_config = AppConfig()
        
        # Validate request
        if not request.address and not request.account_number:
            raise HTTPException(
                status_code=400, detail="Either address or account_number is required"
            )

        # Initialize scraper
        scraper = WaterBillScraper(job_config)

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

@app.get("/field-mappings", response_class=JSONResponse)
async def get_field_mappings():
    """
    Get available field mappings for optional parameters.
    
    Returns a dictionary where:
    - Keys are friendly parameter names you can use in optional_params
    - Values are the actual API field names they map to
    
    Example response:
    {
        "ParcelID": "record_key_account_number_sdat_field_3",
        "District": "record_key_district_ward_sdat_field_2", 
        "ADDRESS": "mdp_street_address_mdp_field_address",
        ...
    }
    """
    try:
        # Get field mappings from config
        temp_config = AppConfig()
        filtered_mappings = {
            key: value for key, value in temp_config.FIELD_MAPPING.items() 
            if value and not key.startswith('_')
        }
        
        return {
            "field_mappings": filtered_mappings,
            "applies_to_counties": CountyEnum.get_all(),
            "usage_example": {
                "optional_params": {
                    "NEW_DISTRICT": "District",
                    "CUSTOM_GRADE": "GRADE"
                }
            },
            "explanation": {
                "column_name": "Create columns in your spreadsheet with these names",
                "field_mapping_key": "Maps to these FIELD_MAPPING keys for API queries",
                "note": "Each row's value from the column will be used in that row's API query"
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting field mappings: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/batch", response_model=StatusResponse)
async def process_batch(
    request: ProcessBatchRequestModel,
    background_tasks: BackgroundTasks
):
    """
    Start a batch processing job for property or water bill data.
    
    - **county**: County to process (required)
    - **spreadsheet_id**: Spreadsheet ID to write to (required)
    - **start_row**: First row to process (default: 2)
    - **max_rows**: Maximum number of rows to process (default: 100)
    - **stop_row**: Explicit stop row (optional, 0 = use max_rows logic)
    - **sheet_name**: Name of the sheet to process (default: "LIENS")
    - **identifier_type**: Type of identifier used ("parcel_id" or "address")
    - **identifier_column**: The column header name for the identifier ("ParcelID" or "ADDRESS")
    - **force_reprocess**: Set to True if want to process even if status is "Success"
    - **parcel_digits**: Amount of digits in the parcel ID
    - **optional_params**: Column mappings for additional search parameters (optional)
        - Keys are column header names in your spreadsheet (case-sensitive)
        - Values are FIELD_MAPPING names that the columns map to
        - Example: {"NEW_DISTRICT": "District", "GRADE_COL": "GRADE"}
        - System will read values from these columns for each row
        - Use /field-mappings endpoint to see available FIELD_MAPPING names
    
    Returns a job ID that can be used to check progress with the /batch/{job_id} endpoint.
    """
    try:
        # Validate that county were provided
        if not request.county:
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
        if request.county.lower() not in valid_counties:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid county '{request.county}'. Valid counties are: {', '.join(valid_counties)}"
            )

        # Generate a job ID
        job_id = job_store.create_job()

        # Create job-specific config and managers
        job_config = AppConfig.create_job_config_from_request(request)
        job_sheets_manager = SheetsManager(config=job_config)
        if redis_client:
            try:
                redis_client.ping()
                job_cache_manager = RedisCacheManager(redis_client=redis_client)
                logger.info(f"Job {job_id}: Using Redis cache manager")
            except Exception:
                job_cache_manager = MinimalCacheManager(cache_dir=job_config.CACHE_DIRECTORY)
                logger.info(f"Job {job_id}: Redis unavailable, using file cache manager")
        else:
            job_cache_manager = MinimalCacheManager(cache_dir=job_config.CACHE_DIRECTORY)
            logger.info(f"Job {job_id}: Using file cache manager")
                
        # Log job configuration
        logger.info(f"Job {job_id}: mode={job_config.PROCESSING_MODE}, county={job_config._current_county}")
        logger.info(f"Job {job_id}: rows={job_config.START_ROW}-{job_config.STOP_ROW}, max={job_config.MAX_ROWS}")

        # Initialize job status
        job_store.update_job_progress(
            job_id=job_id,
            progress=0,
            status="queued",
            message="Batch processing job has been queued",
        )

        # Start background task with job-specific instances
        background_tasks.add_task(
            run_batch_processing_job, 
            job_id, 
            job_config, 
            job_sheets_manager, 
            job_cache_manager
        )

        return {
            "job_id": job_id,
            "status": "queued",
            "message": "Batch processing job has been queued",
        }

    except Exception as e:
        logger.error(f"Error starting batch job: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    health_status = {"status": "healthy", "redis": "unavailable"}
    
    try:
        if redis_client:
            redis_client.ping()
            health_status["redis"] = "connected"
    except Exception:
        pass
        
    return health_status

@app.get("/batch/{job_id}", response_model=StatusResponse)
async def get_batch_status(job_id: str):
    """Get detailed status of a batch processing job."""
    try:
        logger.info(f"Checking status for job: {job_id}")
        job_status = job_store.get_job_status(job_id)
        
        if not job_status:
            logger.warning(f"Job not found: {job_id}")
            raise HTTPException(status_code=404, detail="Job not found")

        return StatusResponse(
            job_id=job_id,
            status=job_status["status"],
            message=job_status["message"],
            progress=job_status.get("progress", 0),
            errors=job_status.get("errors", []),
            error_count=job_status.get("error_count", 0),
            success_count=job_status.get("success_count", 0),
            total_processed=job_status.get("total_processed", 0)
        )
    except Exception as e:
        logger.exception(f"Error checking job status for {job_id}: {str(e)}")
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

        # Mark as complete. Preserve the detailed message set by the inner processors
        # (which includes stats and any learned-config hints) by passing message=None.
        job_store.update_job_progress(
            job_id=job_id,
            progress=100,
            status="completed",
            message=None,
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

async def cleanup_job(job_id: str):
    """Clean up job data after a delay."""
    await asyncio.sleep(3600)  # Keep job data for 1 hour
    if job_id in active_jobs:
        del active_jobs[job_id]

@app.post("/property/sample", response_class=JSONResponse)
async def get_sample_property_data(request: PropertySampleRequestModel):
    """
    Get a sample property from the specified county's API to demonstrate data structure.
    
    This endpoint returns raw API data from a random property to help users understand
    what fields are available for each county. No address or parcel ID is required.
    
    - **county**: County to sample from (default: baltimore)
    """
    try:
        # Validate county exists in our system
        valid_counties = CountyEnum.get_all()
        if request.county.lower() not in valid_counties:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid county '{request.county}'. Valid counties are: {', '.join(valid_counties)}"
            )

        # Create job-specific config
        job_config = AppConfig()
        job_config.set_current_county(request.county)
        
        # Initialize API client and get sample data
        api = PropertyDataAPI(request.county, job_config)
        result = api.get_sample_property()

        if not result.get("success", False):
            return JSONResponse(
                status_code=404,
                content={
                    "success": False,
                    "message": result.get("message", "Sample property data not found"),
                },
            )

        return result

    except Exception as e:
        logger.error(f"Error getting sample property data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============== NJ Property Endpoints ==============

@app.post("/nj/property", response_class=JSONResponse)
async def get_nj_property_data(request: NJPropertyRequestModel):
    """
    Get property data for a single NJ property by Block/Lot.

    - **block**: Block number (required)
    - **lot**: Lot number (required)
    - **qual**: Qualifier (optional)
    - **county**: NJ county name (default: ocean)
    - **municipality**: Township name (default: stafford)
    """
    try:
        # Initialize NJ API
        nj_api = NJPropertyAPI(county=request.county, municipality=request.municipality)

        # Get property data
        result = nj_api.get_property_data(
            block=request.block,
            lot=request.lot,
            qual=request.qual
        )

        if not result.get("success", False):
            return JSONResponse(
                status_code=404,
                content={
                    "success": False,
                    "message": result.get("message", "Property not found"),
                }
            )

        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting NJ property data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/nj/property/sample", response_class=JSONResponse)
async def get_nj_sample_property(request: NJPropertySampleRequestModel):
    """
    Get a sample property from an NJ municipality to see available data fields.

    - **county**: NJ county name (default: ocean)
    - **municipality**: Township name (default: stafford)
    """
    try:
        nj_api = NJPropertyAPI(county=request.county, municipality=request.municipality)
        result = nj_api.get_sample_property()

        if not result.get("success", False):
            return JSONResponse(
                status_code=404,
                content=result
            )

        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting NJ sample property: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/nj/municipalities", response_class=JSONResponse)
async def get_nj_municipalities(county: Optional[str] = None):
    """
    Get list of supported NJ municipalities.

    - **county**: Optional filter by county name
    """
    try:
        if county:
            county_lower = county.lower()
            if county_lower in NJ_MUNICIPALITIES:
                return {
                    "county": county_lower,
                    "municipalities": NJ_MUNICIPALITIES[county_lower]
                }
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"County '{county}' not supported. Available: {list(NJ_MUNICIPALITIES.keys())}"
                )
        return {"municipalities": NJ_MUNICIPALITIES}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting NJ municipalities: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/nj/field-mappings", response_class=JSONResponse)
async def get_nj_field_mappings():
    """
    Get available NJ field mappings showing what data can be retrieved.

    Returns mapping of spreadsheet column names to ArcGIS API field names.
    """
    return {
        "field_mappings": NJ_FIELD_MAPPING,
        "description": "Maps spreadsheet column names to NJ ArcGIS API fields",
        "note": "Create columns in your spreadsheet with these names (case-insensitive)"
    }


@app.post("/nj/batch", response_model=StatusResponse)
async def process_nj_batch(
    request: NJBatchRequestModel,
    background_tasks: BackgroundTasks
):
    """
    Start a batch processing job for NJ property data.

    Reads Block/Lot/Qual from Google Sheet, queries NJOGIS ArcGIS API,
    and writes results (address, assessed values, year built, etc.) back to sheet.

    - **county**: NJ county (default: ocean)
    - **municipality**: Township name (required, e.g., 'stafford')
    - **spreadsheet_id**: Google Spreadsheet ID (required)
    - **sheet_name**: Sheet name (default: LIENS)
    - **block_column**: Column header for Block (default: Block)
    - **lot_column**: Column header for Lot (default: Lot)
    - **qual_column**: Column header for Qualifier (default: Qual)
    - **start_row**: First row to process (default: 2)
    - **max_rows**: Maximum rows to process (default: 100)
    - **force_reprocess**: Reprocess rows with Status='Success' (default: false)

    Returns a job_id to check status with GET /nj/batch/{job_id}
    """
    try:
        # Validate municipality
        county_lower = request.county.lower()
        mun_lower = request.municipality.lower().replace(" ", "_").replace("-", "_")

        if county_lower not in NJ_MUNICIPALITIES:
            raise HTTPException(
                status_code=400,
                detail=f"County '{request.county}' not supported. Available: {list(NJ_MUNICIPALITIES.keys())}"
            )

        if mun_lower not in NJ_MUNICIPALITIES[county_lower]:
            raise HTTPException(
                status_code=400,
                detail=f"Municipality '{request.municipality}' not found in {request.county} county. "
                       f"Available: {list(NJ_MUNICIPALITIES[county_lower].keys())}"
            )

        # Generate job ID
        job_id = job_store.create_job()

        # Create sheets manager for this job
        job_config = AppConfig()
        job_sheets_manager = SheetsManager(config=job_config)
        job_sheets_manager.spreadsheet_id = request.spreadsheet_id

        # Create cache manager
        if redis_client:
            try:
                redis_client.ping()
                job_cache_manager = RedisCacheManager(redis_client=redis_client)
            except Exception:
                job_cache_manager = MinimalCacheManager(cache_dir=job_config.CACHE_DIRECTORY)
        else:
            job_cache_manager = MinimalCacheManager(cache_dir=job_config.CACHE_DIRECTORY)

        # Initialize job
        job_store.update_job_progress(
            job_id=job_id,
            progress=0,
            status="queued",
            message=f"NJ batch job queued for {request.municipality}, {request.county} county"
        )

        logger.info(f"Starting NJ batch job {job_id} for {request.municipality} ({request.max_rows} rows)")

        # Start background task
        background_tasks.add_task(
            run_nj_batch_processing_job,
            job_id,
            request,
            job_sheets_manager,
            job_cache_manager
        )

        return StatusResponse(
            job_id=job_id,
            status="queued",
            message=f"NJ batch job queued for {request.municipality}, {request.county} county"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting NJ batch job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/nj/batch/{job_id}", response_model=StatusResponse)
async def get_nj_batch_status(job_id: str):
    """Get status of an NJ batch processing job."""
    try:
        job_status = job_store.get_job_status(job_id)

        if not job_status:
            raise HTTPException(status_code=404, detail="Job not found")

        return StatusResponse(
            job_id=job_id,
            status=job_status["status"],
            message=job_status["message"],
            progress=job_status.get("progress", 0),
            errors=job_status.get("errors", []),
            error_count=job_status.get("error_count", 0),
            success_count=job_status.get("success_count", 0),
            total_processed=job_status.get("total_processed", 0)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking NJ job status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# NJ background processing function
async def run_nj_batch_processing_job(
    job_id: str,
    request: NJBatchRequestModel,
    sheets_manager: SheetsManager,
    cache_manager: Optional[MinimalCacheManager]
):
    """Run NJ batch processing job in the background."""
    try:
        logger.info(f"NJ Job {job_id} starting")

        job_store.update_job_progress(
            job_id=job_id,
            progress=0,
            status="running",
            message="NJ batch processing started"
        )

        # Process NJ property data
        await process_nj_property_data(
            job_id=job_id,
            request=request,
            sheets_manager=sheets_manager,
            cache_manager=cache_manager,
            job_store=job_store
        )

        # Mark complete
        job_store.update_job_progress(
            job_id=job_id,
            progress=100,
            status="completed",
            message="NJ batch processing completed"
        )

    except Exception as e:
        logger.exception(f"Error in NJ batch job {job_id}: {e}")
        job_store.update_job_progress(
            job_id=job_id,
            progress=0,
            status="failed",
            message=f"Error: {str(e)}"
        )