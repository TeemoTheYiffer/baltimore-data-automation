"""
NJ-specific API route handlers for batch processing.
"""

import logging
import asyncio
import time
import threading
import os
import concurrent.futures
from typing import Optional
from collections import defaultdict

from sheets import SheetsManager
from utils.minimal_cache_manager import MinimalCacheManager
from web_utils.job_store import JobStore
from nj_property_api import NJPropertyAPI, NJ_FIELD_MAPPING

logger = logging.getLogger("web_utils.nj_routes")


async def process_nj_property_data(
    job_id: str,
    request,  # NJBatchRequestModel
    sheets_manager: SheetsManager,
    cache_manager: Optional[MinimalCacheManager],
    job_store: JobStore,
) -> None:
    """Process NJ property data with status updates."""
    try:
        job_store.update_job_progress(
            job_id=job_id,
            progress=5,
            status="running",
            message=f"Starting NJ property data processing for {request.municipality}"
        )

        # Run the blocking function in a thread
        await asyncio.to_thread(
            process_nj_county_property_data,
            request=request,
            sheets_manager=sheets_manager,
            cache_manager=cache_manager,
            job_id=job_id,
            job_store=job_store,
        )

        job_store.update_job_progress(
            job_id=job_id,
            progress=100,
            status="completed",
            message=f"Completed NJ property processing for {request.municipality}"
        )

    except Exception as e:
        logger.exception(f"Error processing NJ property data: {e}")
        job_store.update_job_progress(
            job_id=job_id,
            progress=0,
            status="failed",
            message=f"NJ property error: {str(e)}"
        )


def process_nj_county_property_data(
    request,  # NJBatchRequestModel
    sheets_manager: SheetsManager,
    cache_manager: Optional[MinimalCacheManager],
    job_id: Optional[str] = None,
    job_store=None,
) -> dict:
    """
    Process NJ property data for a specific municipality.

    Reads Block/Lot/Qual from Google Sheet, queries NJOGIS ArcGIS API,
    and writes results back to the sheet.
    """
    total_start_time = time.time()
    logger.info(f"=== Starting NJ property processing for {request.municipality}, {request.county} county ===")

    # Initialize stats
    stats_lock = threading.Lock()
    stats = defaultdict(int)

    # Set cache key
    cache_key = f"nj_{request.county}_{request.municipality}_property"

    try:
        # Initialize the NJ property API
        nj_api = NJPropertyAPI(county=request.county, municipality=request.municipality)
    except ValueError as e:
        logger.error(f"Failed to initialize NJ API: {e}")
        if job_store and job_id:
            job_store.update_job_progress(
                job_id=job_id,
                progress=0,
                status="failed",
                message=str(e)
            )
        return {"results": [], "stats": {"error": 1, "total": 0}}

    # Update job status
    if job_store and job_id:
        job_store.update_job_progress(
            job_id=job_id,
            progress=10,
            message=f"Reading data from sheet '{request.sheet_name}'"
        )

    try:
        # Get identifiers from sheet
        sheet_read_start = time.time()
        # Calculate stop_row: if stop_row > 0, use it; else if max_rows > 0, calculate; else unlimited
        if request.stop_row > 0:
            effective_stop_row = request.stop_row
        elif request.max_rows > 0:
            effective_stop_row = request.start_row + request.max_rows - 1
        else:
            # max_rows=0 means unlimited - use a large number, will be clipped by sheet dimensions
            effective_stop_row = 1000000

        identifier_data = sheets_manager.get_nj_property_identifiers(
            sheet_name=request.sheet_name,
            block_column=request.block_column,
            lot_column=request.lot_column,
            qual_column=request.qual_column,
            start_row=request.start_row,
            stop_row=effective_stop_row,
            force_reprocess=request.force_reprocess,
        )
        sheet_read_time = time.time() - sheet_read_start
        logger.info(f"Sheet read took {sheet_read_time:.2f}s, found {len(identifier_data)} rows to process")

    except Exception as e:
        logger.error(f"Failed to read sheet: {e}")
        if job_store and job_id:
            job_store.update_job_progress(
                job_id=job_id,
                progress=0,
                status="failed",
                message=f"Sheet read error: {str(e)}"
            )
        return {"results": [], "stats": {"error": 1, "total": 0}}

    if not identifier_data:
        logger.warning("No rows to process")
        if job_store and job_id:
            job_store.update_job_progress(
                job_id=job_id,
                progress=100,
                status="completed",
                message="No rows found to process"
            )
        return {"results": [], "stats": {"total": 0}}

    stats["total"] = len(identifier_data)

    if job_store and job_id:
        job_store.update_job_progress(
            job_id=job_id,
            progress=15,
            message=f"Processing {len(identifier_data)} properties"
        )

    # Check for cached results
    results = []
    pending_updates = cache_manager.get_pending_updates(cache_key) if cache_manager else []

    if pending_updates:
        logger.info(f"Found {len(pending_updates)} cached results from previous run")
        # Process cached results first
        for i in range(0, len(pending_updates), request.batch_size):
            batch = pending_updates[i:i + request.batch_size]
            try:
                sheets_manager.batch_update_nj_property_data(batch, request.sheet_name)
                # Remove from cache
                if cache_manager:
                    for idx, result in batch:
                        identifier = result.get("identifier", f"row_{idx}")
                        cache_manager.remove_from_cache(identifier, cache_key)
            except Exception as e:
                logger.error(f"Failed to update cached batch: {e}")

    # Filter out already processed rows
    processed_indices = {idx for idx, _ in pending_updates}
    identifiers_to_process = [
        (idx, block, lot, qual)
        for idx, block, lot, qual in identifier_data
        if idx not in processed_indices
    ]

    logger.info(f"Processing {len(identifiers_to_process)} rows after filtering cached results")

    # Define worker function
    def process_property(row_idx: int, block: str, lot: str, qual: Optional[str]):
        if not block or not lot:
            return row_idx, None

        identifier = f"{block}/{lot}" + (f"/{qual}" if qual else "")

        # Check cache
        if cache_manager:
            cached = cache_manager.get_from_cache(identifier, cache_key)
            if cached and "data" in cached:
                with stats_lock:
                    stats["cache_hits"] += 1
                return row_idx, cached["data"]

        try:
            with stats_lock:
                stats["api_calls"] += 1

            result = nj_api.get_property_data(block, lot, qual)

            with stats_lock:
                if result.get("success", False):
                    stats["success"] += 1
                else:
                    stats["error"] += 1

            # Cache the result
            if cache_manager and result:
                cache_data = {"row_index": row_idx, "data": result}
                cache_manager.save_to_cache(identifier, cache_data, cache_key)

            # Small delay between API calls
            time.sleep(0.1)

            return row_idx, result

        except Exception as e:
            with stats_lock:
                stats["error"] += 1
            logger.error(f"Error processing row {row_idx + 2}: {e}")

            error_result = {
                "success": False,
                "message": f"Error: {str(e)}",
                "identifier": identifier,
                "data": {"Status": f"Error: {str(e)}"}
            }

            if cache_manager:
                cache_data = {"row_index": row_idx, "data": error_result}
                cache_manager.save_to_cache(identifier, cache_data, cache_key)

            return row_idx, error_result

    # Process in parallel with thread pool
    api_start = time.time()
    max_workers = 10  # Reasonable concurrency for ArcGIS

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(process_property, idx, block, lot, qual): idx
            for idx, block, lot, qual in identifiers_to_process
            if block and lot
        }

        completed = 0
        total = len(future_to_idx)
        progress_interval = max(1, total // 20)

        for future in concurrent.futures.as_completed(future_to_idx):
            try:
                idx, result = future.result()
                if result:
                    results.append((idx, result))

                completed += 1

                if completed % progress_interval == 0 or completed == total:
                    logger.info(f"Progress: {completed}/{total} ({completed/total*100:.1f}%)")

                    if job_store and job_id:
                        # Scale progress from 15% to 70%
                        current_progress = 15 + (55 * completed / total)
                        job_store.update_job_progress(
                            job_id=job_id,
                            progress=int(current_progress),
                            message=f"Processing: {completed}/{total} properties ({stats['success']} successful, {stats['error']} failed)"
                        )

            except Exception as e:
                logger.error(f"Worker error: {e}")
                completed += 1

    api_duration = time.time() - api_start
    logger.info(f"API calls completed in {api_duration:.2f}s")

    # Sort results by row index
    results.sort(key=lambda x: x[0])

    # Update job progress
    if job_store and job_id:
        job_store.update_job_progress(
            job_id=job_id,
            progress=75,
            message=f"Writing {len(results)} results to sheet"
        )

    # Batch update the sheet
    sheets_update_start = time.time()
    batch_count = (len(results) + request.batch_size - 1) // request.batch_size

    for i in range(0, len(results), request.batch_size):
        batch = results[i:i + request.batch_size]
        batch_num = i // request.batch_size + 1

        try:
            sheets_manager.batch_update_nj_property_data(batch, request.sheet_name)
            logger.info(f"Updated batch {batch_num}/{batch_count}")

            # Remove from cache
            if cache_manager:
                for idx, result in batch:
                    identifier = result.get("identifier", f"row_{idx}")
                    cache_manager.remove_from_cache(identifier, cache_key)

            # Update progress
            if job_store and job_id:
                batch_progress = 20 * (batch_num / batch_count)
                current_progress = 75 + batch_progress
                job_store.update_job_progress(
                    job_id=job_id,
                    progress=int(current_progress),
                    message=f"Writing batch {batch_num}/{batch_count}"
                )

            # Delay between batches
            if i + request.batch_size < len(results):
                time.sleep(1.0)

        except Exception as e:
            logger.error(f"Batch update error: {e}")
            # Fall back to individual updates
            for idx, result in batch:
                try:
                    sheets_manager.update_row_with_nj_property_data(idx, result, request.sheet_name)
                    if cache_manager:
                        identifier = result.get("identifier", f"row_{idx}")
                        cache_manager.remove_from_cache(identifier, cache_key)
                except Exception as inner_e:
                    logger.error(f"Failed to update row {idx + 2}: {inner_e}")

    sheets_duration = time.time() - sheets_update_start
    total_duration = time.time() - total_start_time

    # Log summary
    logger.info(f"=== NJ Processing Summary ===")
    logger.info(f"Total: {stats['total']}, Success: {stats['success']}, Errors: {stats['error']}")
    logger.info(f"Cache hits: {stats['cache_hits']}, API calls: {stats['api_calls']}")
    logger.info(f"Total time: {total_duration:.2f}s (API: {api_duration:.2f}s, Sheets: {sheets_duration:.2f}s)")

    # Clear cache
    if cache_manager:
        try:
            if hasattr(cache_manager, 'cache_dir'):
                file_count = 0
                for filename in os.listdir(cache_manager.cache_dir):
                    if filename.startswith(f"{cache_key}_") and filename.endswith(".json"):
                        try:
                            os.remove(os.path.join(cache_manager.cache_dir, filename))
                            file_count += 1
                        except Exception:
                            pass
                logger.info(f"Removed {file_count} cache files")
            else:
                cache_manager.clear_cache(cache_key)
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")

    if job_store and job_id:
        job_store.update_job_progress(
            job_id=job_id,
            progress=95,
            success_count=stats['success'],
            error_count=stats['error'],
            total_processed=stats['total'],
            message=f"Completed: {stats['success']} successful, {stats['error']} failed"
        )

    return {
        "results": results,
        "stats": dict(stats)
    }
