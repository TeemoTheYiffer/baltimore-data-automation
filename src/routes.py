import logging
import time
import os
from typing import Optional
import concurrent.futures
from googleapiclient.errors import HttpError
from config import AppConfig
from sheets import SheetsManager
from property_api import PropertyDataAPI
from scraper import WaterBillScraper
from utils.cache_manager import CacheManager
from utils.minimal_cache_manager import MinimalCacheManager
import math
from collections import defaultdict

logger = logging.getLogger("routes")


async def process_addresses_for_bill_details(
    job_id: str,  # Add job_id for status updates
    config: AppConfig,
    sheets_manager: SheetsManager,
    sheet_name: str,
    cache_manager: Optional[MinimalCacheManager],
    job_store=None  # Add job_store parameter
) -> dict:  # Return a result dictionary for API
    """Process addresses for water bill details."""

    # Stats dictionary for tracking counts
    stats = {"processed": 0, "success": 0, "failed": 0}
    
    # Update job status if tracking enabled
    if job_store and job_id:
        job_store.update_job_progress(
            job_id=job_id, 
            progress=15, 
            message="Processing water bill addresses"
        )

    config = config or AppConfig()
    cache_manager = cache_manager or CacheManager()

    logger.info(f"Starting water bill processing for sheet: {sheet_name}")
    if hasattr(config, '_stop_row_was_set') and config._stop_row_was_set:
        actual_stop_row = config.STOP_ROW
    else:
        actual_stop_row = config.START_ROW + config.MAX_ROWS - 1

    logger.info(f"Processing rows from {config.START_ROW} to {actual_stop_row}")

    # Use the provided sheets_manager or create a new one
    sheets = sheets_manager or SheetsManager(config)
    try:
        sheets.setup_headers(sheet_name)
        addresses = sheets.get_addresses(sheet_name)
    except Exception as e:
        logger.error(f"Failed to set up sheet {sheet_name}: {e}")
        return {"results": [], "stats": stats}  # Return empty results with stats

    if not addresses:
        logger.warning(f"No addresses found in sheet: {sheet_name}")
        return {"results": [], "stats": stats}  # Return empty results with stats

    logger.info(
        f"Processing {len(addresses)} addresses using {config.MAX_WORKERS} threads with batch updates of {config.BATCH_SIZE} rows"
    )

    # Check for pending cached results from previous runs
    pending_updates = cache_manager.get_pending_updates("water_bill")
    if pending_updates:
        logger.info(f"Found {len(pending_updates)} cached results from previous run")

        # Update the sheet with cached results first
        for i in range(0, len(pending_updates), config.BATCH_SIZE):
            batch = pending_updates[i : i + config.BATCH_SIZE]
            try:
                sheets.batch_update_bill_details(batch, sheet_name)
                logger.info(
                    f"Updated batch {i // config.BATCH_SIZE + 1} of {(len(pending_updates) + config.BATCH_SIZE - 1) // config.BATCH_SIZE} from cache"
                )

                # Remove successfully updated entries from cache
                for idx, result in batch:
                    # Get identifier from result
                    identifier = None
                    if result.get("success", False) and "data" in result:
                        identifier = result["data"].get("account_number")
                    elif "account_number" in result:
                        identifier = result["account_number"]

                    if identifier:
                        cache_manager.remove_from_cache(identifier, "water_bill")
                    else:
                        # Use row index as fallback identifier
                        cache_manager.remove_from_cache(f"row_{idx}", "water_bill")

                # Add delay between batches to avoid hitting API limits
                if i + config.BATCH_SIZE < len(
                    pending_updates
                ):  # Don't delay after the last batch
                    logger.info(
                        f"Sleeping for {config.DELAY_BETWEEN_BATCHES} seconds before next batch"
                    )
                    time.sleep(config.DELAY_BETWEEN_BATCHES)
            except Exception as e:
                logger.error(f"Failed to update results batch from cache: {e}")
                # Don't remove cache entries if update failed
    
    # Initialize results variable before any reference to it
    results = []

    # Filter out addresses that were already processed from cache
    addresses_to_process = []
    processed_indices = {idx for idx, _ in pending_updates}

    for idx, addr in addresses:
        if idx not in processed_indices:
            addresses_to_process.append((idx, addr))

    logger.info(
        f"Processing {len(addresses_to_process)} addresses after filtering out cached results"
    )

    # Scraper worker function
    def scrape_address(args):
        idx, address = args
        if not address:
            return idx, None

        # Check if result is already in cache
        cached_result = None
        if cache_manager:
            cached_result = cache_manager.get_from_cache(address, "water_bill")
            if cached_result and "data" in cached_result:
                logger.info(f"Found cached result for address: {address}")
                return idx, cached_result["data"]

        scraper = WaterBillScraper(config)  # Create a new scraper for each thread
        try:
            result = scraper.get_water_bill_details(address)

            # Cache the result
            if cache_manager and result:
                cache_data = {"row_index": idx, "data": result}
                cache_manager.save_to_cache(address, cache_data, "water_bill")

                # If we got an account number, also cache by that for future lookups
                if (
                    result.get("success", False)
                    and "data" in result
                    and "account_number" in result["data"]
                ):
                    account_number = result["data"]["account_number"]
                    cache_manager.save_to_cache(
                        account_number, cache_data, "water_bill"
                    )

            return idx, result
        except Exception as e:
            logger.error(f"Error processing address {address}: {e}")
            error_result = {
                "success": False,
                "message": f"Error: {str(e)}",
                "data": {"Status": "Error"},
            }

            # Cache the error result too
            if cache_manager:
                cache_data = {"row_index": idx, "data": error_result}
                cache_manager.save_to_cache(address, cache_data, "water_bill")

            return idx, error_result

    # Start timing
    start_time = time.time()

    # Process addresses in parallel
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=config.MAX_WORKERS
    ) as executor:
        # Submit all jobs
        future_to_idx = {
            executor.submit(scrape_address, (idx, addr)): idx
            for idx, addr in addresses_to_process
        }

        # Calculate progress intervals for both logging and job updates
        completed = 0
        total = len(future_to_idx)
        progress_interval = min(100, max(1, total // 10))  # Report at 10% intervals or every 100 items
        job_update_interval = max(1, total // 20)  # Update job status more frequently (5% intervals)

        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_idx):
            try:
                idx, result = future.result()
                if result:  # Skip None results (empty addresses)
                    results.append((idx, result))
                    stats["processed"] += 1
                    
                    # Track success/failure
                    if result.get("success", False):
                        stats["success"] += 1
                    else:
                        stats["failed"] += 1

                completed += 1
                
                # Log progress at regular intervals
                if completed % progress_interval == 0 or completed == total:
                    logger.info(
                        f"Progress: {completed}/{total} addresses processed ({completed / total * 100:.1f}%)"
                    )
                
                # Update job status more frequently for real-time UI updates
                if job_store and job_id and (completed % job_update_interval == 0 or completed == total):
                    # Calculate percentage completed (scale between start_progress and end_progress)
                    start_progress = 15  # Starting progress percentage for this phase
                    end_progress = 35    # Ending progress percentage for this phase
                    current_progress = start_progress + ((completed / total) * (end_progress - start_progress))
                    
                    job_store.update_job_progress(
                        job_id=job_id,
                        progress=int(current_progress),
                        message=f"Processing: {completed}/{total} addresses ({completed / total * 100:.1f}%) - {stats['success']} successful, {stats['failed']} failed"
                    )

            except Exception as e:
                logger.error(f"Worker thread error: {e}")
                completed += 1  # Still count errors towards progress
                stats["processed"] += 1
                stats["failed"] += 1

        # Final job update after completion
        if job_store and job_id:
            job_store.update_job_progress(
                job_id=job_id,
                progress=35,  # End progress for this phase
                message=f"Completed processing {stats['processed']} addresses: {stats['success']} successful, {stats['failed']} failed"
            )

    # Sort results by index to maintain order
    results.sort(key=lambda x: x[0])

    # Update the sheet in batches
    batch_count = (len(results) + config.BATCH_SIZE - 1) // config.BATCH_SIZE
    logger.info(f"Starting updates for {len(results)} records in {batch_count} batches")
    for i in range(0, len(results), config.BATCH_SIZE):
        batch = results[i : i + config.BATCH_SIZE]
        batch_num = i // config.BATCH_SIZE + 1
        logger.info(
            f"Processing batch {batch_num}/{batch_count} ({len(batch)} records)"
        )
        try:
            sheets.batch_update_bill_details(batch, sheet_name)
            logger.info(
                f"Updated batch {i // config.BATCH_SIZE + 1} of {(len(results) + config.BATCH_SIZE - 1) // config.BATCH_SIZE}"
            )
            logger.info(f"Completed batch {batch_num}/{batch_count} successfully")

            # Remove successfully updated entries from cache
            if cache_manager:
                for idx, result in batch:
                    # Get identifier from result
                    identifier = None
                    if result.get("success", False) and "data" in result:
                        identifier = result["data"].get("account_number")
                    elif "account_number" in result:
                        identifier = result["account_number"]

                    if identifier:
                        cache_manager.remove_from_cache(identifier, "water_bill")
                    else:
                        # Use row index as fallback identifier
                        cache_manager.remove_from_cache(f"row_{idx}", "water_bill")

            # Add delay between batches to avoid hitting API limits
            if i + config.BATCH_SIZE < len(results):  # Don't delay after the last batch
                logger.info(
                    f"Sleeping for {config.DELAY_BETWEEN_BATCHES} seconds before next batch"
                )
                time.sleep(config.DELAY_BETWEEN_BATCHES)
        except HttpError as e:
            if e.resp.status == 429:  # Rate limit error
                logger.warning(
                    "Rate limit exceeded on batch. Waiting 60 seconds for quota reset..."
                )
                time.sleep(60)  # Wait a full minute for quota reset
                try:
                    sheets.batch_update_bill_details(batch, sheet_name)

                    # Remove from cache on success
                    if cache_manager:
                        for idx, result in batch:
                            identifier = (
                                result["data"].get("account_number")
                                if result.get("success", False) and "data" in result
                                else None
                            )
                            if identifier:
                                cache_manager.remove_from_cache(
                                    identifier, "water_bill"
                                )
                            else:
                                cache_manager.remove_from_cache(
                                    f"row_{idx}", "water_bill"
                                )
                except Exception as retry_e:
                    logger.error(f"Retry after quota reset failed: {retry_e}")
                    # Cache entries remain for next run
            else:
                logger.error(f"Error in batch {batch_num}: {e}")
                # Attempt individual updates as fallback for non-rate-limit errors
                for idx, result in batch:
                    try:
                        time.sleep(1)  # Add delay between individual requests
                        sheets.update_row_with_bill_details(idx, result, sheet_name)

                        # Remove from cache on success
                        if cache_manager:
                            identifier = (
                                result["data"].get("account_number")
                                if result.get("success", False) and "data" in result
                                else None
                            )
                            if identifier:
                                cache_manager.remove_from_cache(
                                    identifier, "water_bill"
                                )
                            else:
                                cache_manager.remove_from_cache(
                                    f"row_{idx}", "water_bill"
                                )
                    except Exception as inner_e:
                        logger.error(f"Failed to update row {idx + 2}: {inner_e}")
                        # Cache entry remains for next run

    # Calculate and log timing information
    total_time = time.time() - start_time
    logger.info(f"Water bill processing completed for sheet: {sheet_name}")
    logger.info(
        f"Total processing time: {total_time:.2f} seconds for {len(addresses)} addresses"
    )
    logger.info(f"Average time per address: {total_time / len(addresses):.2f} seconds")

    # Clear all water bill cache files after successful processing
    if cache_manager:
        logger.info("Processing complete - clearing water bill cache")
        try:
            # Check if it's Redis cache manager or file cache manager
            if hasattr(cache_manager, 'cache_dir'):
                # File-based cache (MinimalCacheManager)
                file_count = 0
                for filename in os.listdir(cache_manager.cache_dir):
                    if filename.startswith("water_bill_") and filename.endswith(".json"):
                        try:
                            os.remove(os.path.join(cache_manager.cache_dir, filename))
                            file_count += 1
                        except Exception as e:
                            logger.error(f"Error removing cache file {filename}: {e}")
                logger.info(f"Removed {file_count} water bill cache files")
            else:
                # Redis cache (RedisCacheManager)
                cache_manager.clear_cache("water_bill")
                logger.info("Cleared water bill cache from Redis")
        except Exception as e:
            logger.error(f"Error clearing water bill cache: {e}")

    return {
        "results": results,
        "stats": stats
    }

def process_county_property_data(
    county_name: str,
    config: AppConfig,
    sheets_manager: SheetsManager,
    sheet_name: str,
    cache_manager: Optional[MinimalCacheManager] = None,
    job_id: Optional[str] = None,
    job_store=None,
    delay_seconds: Optional[float] = None
) -> dict:  # Return a result dictionary
    """Process property data for a specific county."""
    
    import time
    import threading
    current_time = time.time()
    thread_id = threading.get_ident()
    logger.error(f"*** FUNCTION_ENTRY: process_county_property_data called at {current_time}")
    logger.error(f"*** THREAD_ID: {thread_id}")
    logger.error(f"*** JOB_ID: {job_id}")
    logger.error(f"*** COUNTY: {county_name}")

    total_start_time = time.time()
    logger.info(f"=== TIMING: Starting {county_name} processing for {config.MAX_ROWS} rows ===")
    logger.info(f"process_county_property_data called with county_name={county_name}")

    # Initialize stats dictionary
    #stats = {"total": 0, "processed": 0, "success": 0, "error": 0, "cache_hits": 0, "api_calls": 0}
    stats_lock = threading.Lock()
    stats = defaultdict(int)
    
    # Set the current county
    config.set_current_county(county_name)
    county_config = config.get_county_config(county_name)

    # config = config or AppConfig()
    cache_manager = cache_manager or CacheManager()

    # Initialize the sheets manager
    sheets = sheets_manager
    
    # Set cache key based on county
    cache_key = f"{county_config.county_name}_property"

    # If job tracking is enabled, update initial status
    if job_id and job_store:
        job_store.update_job_progress(
            job_id=job_id,
            progress=45,  # Starting progress for property processing
            message=f"Starting {county_name} property data processing"
        )

    try:
        # Get identifiers with their row indices using unified method
        sheet_read_start = time.time()
        identifier_data = sheets.get_property_identifiers(
            config, sheet_name, county_config
        )
        sheet_read_time = time.time() - sheet_read_start
        logger.info(f"=== TIMING: Sheet read took {sheet_read_time:.2f} seconds for {len(identifier_data) if identifier_data else 0} rows ===")
    except Exception as e:
        logger.error(f"Failed to get {county_config.identifier_type} from sheet {sheet_name}: {e}")
        if job_id and job_store:
            job_store.update_job_progress(
                job_id=job_id,
                progress=0,
                status="failed",
                message=f"Sheet error: {str(e)}"
            )
        return {"results": [], "stats": {"error": 1, "total": 0}}
    if not identifier_data:
        logger.warning(
            f"No {county_config.identifier_type} found in sheet: {sheet_name}"
        )
        return {"results": [], "stats": {"total": 0, "success": 0, "error": 0}}
    
    stats["total"] = len(identifier_data)
    logger.info(f"Processing with MAX_ROWS={config.MAX_ROWS}")

    # Determine sheet name
    if not sheet_name:
        sheet_name = config.PROPERTY_SHEET_NAME

    # Set cache key based on county
    cache_key = f"{county_config.county_name}_property"

    logger.info(
        f"Starting {county_config.county_name.title()} property data processing for sheet: {sheet_name}"
    )

    # Pass both config objects to SheetsManager
    if hasattr(config, '_stop_row_was_set') and config._stop_row_was_set:
        actual_stop_row = config.STOP_ROW
    else:
        actual_stop_row = config.START_ROW + config.MAX_ROWS - 1

    logger.info(f"Processing rows from {config.START_ROW} to {actual_stop_row}")

    logger.info(
        f"Processing {len(identifier_data)} {county_config.identifier_type}s using {config.MAX_WORKERS} "
        + f"threads with batch updates of {config.BATCH_SIZE} rows"
    )

    # Check for pending cached results from previous runs
    pending_updates = cache_manager.get_pending_updates(cache_key)
    if pending_updates:
        logger.info(f"Found {len(pending_updates)} cached results from previous run")

        # Update the sheet with cached results first
        for i in range(0, len(pending_updates), config.BATCH_SIZE):
            batch = pending_updates[i : i + config.BATCH_SIZE]
            try:
                sheets.batch_update_property_data(batch, sheet_name)
                logger.info(
                    f"Updated batch {i // config.BATCH_SIZE + 1} of "
                    + f"{(len(pending_updates) + config.BATCH_SIZE - 1) // config.BATCH_SIZE} from cache"
                )

                if job_id and job_store:
                    cache_progress = 5  # Allocate 5% of progress to cache processing
                    batch_progress = cache_progress * (i // config.BATCH_SIZE + 1) / math.ceil(len(pending_updates) / config.BATCH_SIZE)
                    current_progress = 45 + batch_progress
                    job_store.update_job_progress(
                        job_id=job_id,
                        progress=int(current_progress),
                        message=f"Processed cached batch {i // config.BATCH_SIZE + 1}/{(len(pending_updates) + config.BATCH_SIZE - 1) // config.BATCH_SIZE} for {county_name}"
                    )

                # Remove successfully updated entries from cache
                for idx, result in batch:
                    # Get identifier from result based on county config
                    if county_config.identifier_type == "parcel_id":
                        identifier = (
                            result.get("parcel_id")
                            if result.get("success", False)
                            else None
                        )
                    else:
                        identifier = (
                            result.get("address")
                            if result.get("success", False)
                            else None
                        )

                    if identifier:
                        cache_manager.remove_from_cache(identifier, cache_key)
                    else:
                        # Use row index as fallback identifier
                        cache_manager.remove_from_cache(f"row_{idx}", cache_key)

                # Add delay between batches to avoid hitting API limits
                if i + config.BATCH_SIZE < len(
                    pending_updates
                ):  # Don't delay after the last batch
                    delay_time = config.DELAY_BETWEEN_BATCHES
                    logger.info(f"Sleeping for {delay_time} seconds before next batch")
                    time.sleep(delay_time)
            except Exception as e:
                logger.error(f"Failed to update results batch from cache: {e}")

    # Initialize the property API client with county configuration
    property_api = PropertyDataAPI(county_config.county_name, config=config)
    results = []

    # Start timing
    start_time = time.time()

    # Filter out identifiers that were already processed from cache
    identifiers_to_process = []
    processed_indices = {idx for idx, _ in pending_updates}

    for idx, identifier, row_optional_params in identifier_data:
        if idx not in processed_indices:
            identifiers_to_process.append((idx, identifier, row_optional_params))

    logger.info(
        f"Processing {len(identifiers_to_process)} {county_config.identifier_type}s after filtering out cached results"
    )

    # Define robust process_identifier function with built-in retries
    def process_identifier(row_idx, identifier, row_optional_params):
        if not identifier:
            return row_idx, None

        # Check cache first
        cached_result = cache_manager.get_from_cache(identifier, cache_key)
        if cached_result and "data" in cached_result:
            stats["cache_hits"] += 1
            return row_idx, cached_result["data"]

        try:
            # Get property data
            with stats_lock:
                stats["api_calls"] += 1
            
            result = property_api.get_property_data(identifier, optional_params=row_optional_params)

            with stats_lock:
                if result.get("success", False):
                    stats["success"] += 1
                else:
                    stats["error"] += 1

            # Cache the result
            if cache_manager and result:
                cache_data = {"row_index": row_idx, "data": result}
                cache_manager.save_to_cache(identifier, cache_data, cache_key)

            # Add delay between requests
            delay = delay_seconds if delay_seconds is not None else config.REQUEST_DELAY
            time.sleep(delay)

            return row_idx, result

        except Exception as e:
            stats["error"] += 1
            logger.exception(f"DETAILED ERROR for row {row_idx + 2}, identifier={identifier}, optional_params={row_optional_params}: {e}")
            logger.error(
                f"Error processing {county_config.identifier_type} (row {row_idx + 2})"
            )

            # Create error result without logging the actual exception
            error_result = {
                "success": False,
                "message": f"Error: {str(e)}",  # This is what shows in Status column
                "data": {"Status": f"Error: {str(e)}"},
            }

            # Cache the error result
            if cache_manager:
                cache_data = {"row_index": row_idx, "data": error_result}
                cache_manager.save_to_cache(identifier, cache_data, cache_key)

            return row_idx, error_result

    # Process identifiers in parallel with streaming writes to Google Sheets
    api_calls_start = time.time()
    FLUSH_SIZE = 50  # Write to sheet every N completed results
    pending_batch = []  # Buffer for results awaiting sheet write
    total_written = 0
    sheets_update_duration = 0

    def flush_to_sheet(batch_to_write):
        """Write a batch of results to Google Sheets immediately."""
        nonlocal total_written, sheets_update_duration
        if not batch_to_write:
            return
        batch_to_write.sort(key=lambda x: x[0])  # Sort by row index
        flush_start = time.time()
        try:
            sheets.batch_update_property_data(batch_to_write, sheet_name)
            total_written += len(batch_to_write)
            logger.info(f"Flushed {len(batch_to_write)} rows to sheet (total written: {total_written})")

            # Remove from cache
            if cache_manager:
                for idx, result in batch_to_write:
                    identifier = result.get("identifier") if result.get("success", False) else None
                    if identifier:
                        cache_manager.remove_from_cache(identifier, cache_key)
                    else:
                        cache_manager.remove_from_cache(f"row_{idx}", cache_key)
        except HttpError as e:
            if e.resp.status == 429:
                logger.warning("Rate limit on sheet write. Waiting 65s for quota reset...")
                time.sleep(65)
                try:
                    sheets.batch_update_property_data(batch_to_write, sheet_name)
                    total_written += len(batch_to_write)
                except Exception as retry_e:
                    logger.error(f"Retry after quota reset failed: {retry_e}")
            else:
                logger.error(f"Failed to flush batch to sheet: {e}")
        except Exception as e:
            logger.error(f"Error flushing batch to sheet: {e}")
        sheets_update_duration += time.time() - flush_start

    logger.info(f"=== TIMING: Starting API calls for {len(identifiers_to_process)} identifiers with {config.MAX_WORKERS} workers (streaming writes every {FLUSH_SIZE} results) ===")
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=config.MAX_WORKERS
    ) as executor:
        # Submit all jobs
        future_to_idx = {
            executor.submit(process_identifier, idx, identifier, row_optional_params): idx
            for idx, identifier, row_optional_params in identifiers_to_process
            if identifier
        }
        completed = 0
        total = len(future_to_idx)
        progress_interval = min(
            100, max(1, total // 10)
        )

        for future in concurrent.futures.as_completed(future_to_idx):
            try:
                idx, result = future.result()
                if result:
                    results.append((idx, result))
                    pending_batch.append((idx, result))

                completed += 1

                # Flush to sheet when buffer is full
                if len(pending_batch) >= FLUSH_SIZE:
                    flush_to_sheet(pending_batch)
                    pending_batch = []

                if completed % progress_interval == 0 or completed == total:
                    logger.info(
                        f"Progress: {completed}/{total} {county_config.identifier_type}s processed ({completed / total * 100:.1f}%), {total_written} written to sheet"
                    )
                    if job_id and job_store:
                        processing_progress = 45 * (completed / total)
                        current_progress = 50 + processing_progress
                        job_store.update_job_progress(
                            job_id=job_id,
                            progress=int(current_progress),
                            message=f"Processing: {completed}/{total} ({completed / total * 100:.1f}%) - {total_written} written"
                        )

            except Exception as e:
                logger.error(f"Worker thread error: {e}")
                completed += 1

    # Flush any remaining results
    if pending_batch:
        flush_to_sheet(pending_batch)
        pending_batch = []

    api_calls_end = time.time()
    api_calls_duration = api_calls_end - api_calls_start
    logger.info(f"=== TIMING: API calls completed in {api_calls_duration:.2f} seconds ===")
    if identifiers_to_process:
        logger.info(f"=== TIMING: Average time per row: {api_calls_duration / len(identifiers_to_process):.2f} seconds ===")

    # Log summary
    logger.info(f"Processing summary for {county_name} county:")
    logger.info(f"Total records: {stats['total']}")
    logger.info(
        f"Successful: {stats['success']} ({stats['success'] / stats['total'] * 100:.1f}%)"
    )
    logger.info(
        f"Failed: {stats['error']} ({stats['error'] / stats['total'] * 100:.1f}%)"
    )
    logger.info(
        f"Cache hits: {stats['cache_hits']} ({stats['cache_hits'] / stats['total'] * 100:.1f}%)"
    )
    logger.info(f"API calls made: {stats['api_calls']}")
    logger.info(f"Total rows written to sheet: {total_written}")

    if job_id and job_store:
        job_store.update_job_progress(
            job_id=job_id,
            progress=95,
            message=f"Completed: {stats['success']} successful, {stats['error']} failed. {total_written} rows written."
        )

    total_duration = time.time() - total_start_time

    logger.info(f"=== TIMING: Google Sheets updates completed in {sheets_update_duration:.2f} seconds ===")
    logger.info(f"=== TIMING: TOTAL PROCESSING TIME: {total_duration:.2f} seconds for {len(results)} rows ===")
    logger.info(f"=== TIMING: Breakdown - Sheets read: {sheet_read_time:.2f}s, API calls: {api_calls_duration:.2f}s, Sheets update: {sheets_update_duration:.2f}s ===")

    # Calculate and log timing information
    total_time = time.time() - start_time
    logger.info(
        f"{county_config.county_name.title()} property data processing completed for sheet: {sheet_name}"
    )
    logger.info(
        f"Total processing time: {total_time:.2f} seconds for {len(identifier_data)} {county_config.identifier_type}s"
    )
    logger.info(
        f"Average time per {county_config.identifier_type}: {total_time / len(identifier_data):.2f} seconds"
    )

    # Clear all property cache files after successful processing
    if cache_manager:
        logger.info(
            f"Processing complete - clearing {county_config.county_name} property cache"
        )
        try:
            # Check if it's Redis cache manager or file cache manager
            if hasattr(cache_manager, 'cache_dir'):
                # File-based cache (MinimalCacheManager)
                file_count = 0
                for filename in os.listdir(cache_manager.cache_dir):
                    if filename.startswith(f"{cache_key}_") and filename.endswith(".json"):
                        try:
                            os.remove(os.path.join(cache_manager.cache_dir, filename))
                            file_count += 1
                        except Exception as e:
                            logger.error(f"Error removing cache file {filename}: {e}")
                logger.info(
                    f"Removed {file_count} {county_config.county_name} property cache files"
                )
            else:
                # Redis cache (RedisCacheManager)
                cache_manager.clear_cache(cache_key)
                logger.info(
                    f"Cleared {county_config.county_name} property cache from Redis"
                )
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")

    if job_id and job_store:
        job_store.update_job_progress(
            job_id=job_id,
            progress=95,
            message=f"Completed {county_name} property processing: {stats['success']} successful, {stats['error']} failed"
        )

    # Surface any parcel width learned mid-batch so the API caller can update their request
    if property_api._learned_parcel_digits is not None:
        stats["learned_parcel_digits"] = property_api._learned_parcel_digits
        stats["requested_parcel_digits"] = county_config.parcel_digits

    return {
        "results": results,
        "stats": stats
    }