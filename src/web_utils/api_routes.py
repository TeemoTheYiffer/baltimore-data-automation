from typing import Optional
import logging
import asyncio
from sheets import SheetsManager
from utils.minimal_cache_manager import MinimalCacheManager
from web_utils.job_store import JobStore
from config import AppConfig
from routes import process_addresses_for_bill_details

logger = logging.getLogger("web_utils.api_routes")


async def process_water_bills(
    job_id: str,
    config: AppConfig,
    sheets_manager: SheetsManager,
    cache_manager: Optional[MinimalCacheManager],
    job_store: JobStore,
) -> None:
    """Process water bills with status updates."""
    try:
        # Get appropriate sheet name
        water_sheet_name = config.SHEET_NAME or config.WATER_BILL_SHEET_NAME

        # Update progress
        job_store.update_job_progress(
            job_id=job_id, progress=10, message="Water bill processing starting..."
        )

        # Process water bills, passing job_store to the function
        results_data = await asyncio.to_thread(
            process_addresses_for_bill_details,
            job_id=job_id,
            config=config,
            sheets_manager=sheets_manager,
            sheet_name=water_sheet_name,
            cache_manager=cache_manager,
            job_store=job_store,
        )

        # Access the different parts of the return value
        processed_results = results_data["results"]  # The list of (idx, result) tuples
        stats = results_data["stats"]  # The statistics dictionary

        # Use the processed_results for batch updates with the SAME sheet name
        sheets_manager.batch_update_bill_details(processed_results, water_sheet_name)

        # Update progress based on results
        job_store.update_job_progress(
            job_id=job_id,
            progress=40,
            message=f"Water bill processing completed: {stats['success']} successful, {stats['failed']} failed",
        )
    except Exception as e:
        logger.exception(f"Error processing water bills: {e}")
        job_store.update_job_progress(
            job_id=job_id, progress=0, status="failed", message=f"Water bill error: {str(e)}"
        )


async def process_property_data(
    job_id: str,
    config: AppConfig,
    sheets_manager: SheetsManager,
    cache_manager: Optional[MinimalCacheManager],
    job_store: JobStore,
) -> None:
    """Process property data with status updates."""
    try:
        # Get appropriate sheet name
        property_sheet_name = config.SHEET_NAME or config.PROPERTY_SHEET_NAME

        if not sheets_manager.sheet_exists(property_sheet_name):
            job_store.update_job_progress(
                job_id=job_id,
                progress=40,
                message=f"Property sheet '{property_sheet_name}' not found",
            )
            return

        # Import here to avoid circular imports
        from routes import process_county_property_data

        # Get the single county from the current config
        county = config._current_county
        logger.info(f"Processing single county: {county}")

        job_store.update_job_progress(
            job_id=job_id,
            progress=50,
            message=f"Processing {county} county property data",
        )

        # Process the single county
        results_data = await asyncio.to_thread(
            process_county_property_data,
            county_name=county,
            config=config,
            sheets_manager=sheets_manager,
            sheet_name=property_sheet_name,
            cache_manager=cache_manager,
            job_id=job_id,
            job_store=job_store,
        )

        stats = (results_data or {}).get("stats", {}) if isinstance(results_data, dict) else {}
        completion_message = f"Completed {county} county property data: {stats.get('success', 0)} successful, {stats.get('error', 0)} failed"

        learned = stats.get("learned_parcel_digits")
        if learned is not None:
            requested = stats.get("requested_parcel_digits")
            completion_message += (
                f". NOTE: parcel_digits auto-corrected from {requested} to {learned} mid-batch — "
                f"update your request to parcel_digits={learned} to avoid discovery overhead on future runs."
            )

        job_store.update_job_progress(
            job_id=job_id,
            progress=80,
            message=completion_message,
            success_count=stats.get("success", 0),
            error_count=stats.get("error", 0),
            total_processed=stats.get("total", 0),
        )
    except Exception as e:
        logger.exception(f"Error processing property data: {e}")
        job_store.update_job_progress(
            job_id=job_id,
            progress=0,
            status="failed", 
            message=f"Property data error: {str(e)}",
        )
