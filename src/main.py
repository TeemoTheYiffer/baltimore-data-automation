import logging
import time
import argparse
from typing import Optional
import random
import ssl
import requests
from config import Settings
from scraper import WaterBillScraper
from sheets import SheetsManager
import concurrent.futures
from functools import partial
import time
from config import MarylandPropertySettings
from property_api import PropertyDataAPI
from googleapiclient.errors import HttpError
import random

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('baltimore.log')
    ]
)

logger = logging.getLogger("baltimore")

def process_addresses_for_bill_details(
    settings: Optional[Settings] = None,
    sheets_manager: Optional[SheetsManager] = None,
    sheet_name: str = "Water Bill",
    delay_seconds: float = None,
) -> None:
    """
    Process addresses using parallel scraping and batched sheet updates.
    
    Args:
        settings: Application settings
        sheet_name: Name of the sheet to use
        delay_seconds: Delay between requests to override settings
    """
    settings = settings or Settings()
    
    logger.info(f"Starting water bill processing for sheet: {sheet_name}")
    logger.info(f"Processing rows from {settings.START_ROW} to " + 
                (f"{settings.STOP_ROW}" if settings.STOP_ROW > 0 else 
                f"{settings.START_ROW + settings.MAX_ROWS - 1}"))
    
    if settings.SKIP_ROW_RANGE:
        logger.info(f"Skipping rows: {settings.SKIP_ROW_RANGE}")
    
    # Use the provided sheets_manager or create a new one
    sheets = sheets_manager or SheetsManager(settings)
    try:
        sheets.setup_headers(sheet_name)
        addresses = sheets.get_addresses(sheet_name)
    except Exception as e:
        logger.error(f"Failed to set up sheet {sheet_name}: {e}")
        return
    
    if not addresses:
        logger.warning(f"No addresses found in sheet: {sheet_name}")
        return
    
    logger.info(f"Processing {len(addresses)} addresses using {settings.MAX_WORKERS} threads with batch updates of {settings.BATCH_SIZE} rows")
    
    # Initialize results variable before any reference to it
    results = []
    
    # Scraper worker function
    def scrape_address(args):
        idx, address = args
        if not address:
            return idx, None
        
        scraper = WaterBillScraper(settings)  # Create a new scraper for each thread
        try:
            result = scraper.get_water_bill_details(address)
            return idx, result
        except Exception as e:
            logger.error(f"Error processing address {address}: {e}")
            return idx, {"success": False, "message": f"Error: {str(e)}"}
    
    # Start timing
    start_time = time.time()
    
    # Process addresses in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
        # Submit all jobs - note that addresses now contains tuples of (index, address)
        future_to_idx = {
            executor.submit(scrape_address, (idx, addr)): idx 
            for idx, addr in addresses
        }
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_idx):
            try:
                idx, result = future.result()
                if result:  # Skip None results (empty addresses)
                    results.append((idx, result))
            except Exception as e:
                logger.error(f"Worker thread error: {e}")
    
    # Sort results by index to maintain order
    results.sort(key=lambda x: x[0])
    
    # Update the sheet in batches
    for i in range(0, len(results), settings.BATCH_SIZE):
        batch = results[i:i+settings.BATCH_SIZE]
        try:
            sheets.batch_update_bill_details(batch, sheet_name)
            logger.info(f"Updated batch {i//settings.BATCH_SIZE + 1} of {(len(results) + settings.BATCH_SIZE - 1)//settings.BATCH_SIZE}")
            
            # Add delay between batches to avoid hitting API limits
            if i + settings.BATCH_SIZE < len(results):  # Don't delay after the last batch
                logger.info(f"Sleeping for {settings.DELAY_BETWEEN_BATCHES} seconds before next batch")
                time.sleep(settings.DELAY_BETWEEN_BATCHES)
        except HttpError as e:
            if e.resp.status == 429:  # Rate limit error
                logger.warning("Rate limit exceeded on batch. Waiting 60 seconds for quota reset...")
                time.sleep(60)  # Wait a full minute for quota reset
                try:
                    sheets.batch_update_bill_details(batch, sheet_name)
                except Exception as retry_e:
                    logger.error(f"Retry after quota reset failed: {retry_e}")
                    # Only fall back to individual updates as last resort
                    for idx, result in batch:
                        try:
                            time.sleep(1)  # Add delay between individual requests
                            sheets.update_row_with_bill_details(idx, result, sheet_name)
                        except Exception as inner_e:
                            logger.error(f"Failed to update row {idx+2}: {inner_e}")
            else:
                logger.error(f"Failed to update results batch: {e}")
                # Attempt individual updates as fallback for non-rate-limit errors
                for idx, result in batch:
                    try:
                        time.sleep(1)  # Add delay between individual requests
                        sheets.update_row_with_bill_details(idx, result, sheet_name)
                    except Exception as inner_e:
                        logger.error(f"Failed to update row {idx+2}: {inner_e}")
    
    # Calculate and log timing information
    total_time = time.time() - start_time
    logger.info(f"Water bill processing completed for sheet: {sheet_name}")
    logger.info(f"Total processing time: {total_time:.2f} seconds for {len(addresses)} addresses")
    logger.info(f"Average time per address: {total_time/len(addresses):.2f} seconds")

def list_sheets():
    """List all sheets in the spreadsheet."""
    settings = Settings()
    sheets_manager = SheetsManager(settings)
    
    try:
        sheet_names = sheets_manager.get_all_sheet_names()
        print("\nAvailable sheets in the spreadsheet:")
        for i, name in enumerate(sheet_names, 1):
            print(f"{i}. {name}")
        print()
    except Exception as e:
        logger.error(f"Failed to list sheets: {e}")
        print(f"Error: {e}")

def process_addresses_for_property_data(
    settings: Optional[Settings] = None,
    property_settings: Optional[MarylandPropertySettings] = None,
    sheets_manager: Optional[SheetsManager] = None,
    sheet_name: str = "LIENS",
    delay_seconds: float = None,
) -> None:
    """
    Process addresses to get Maryland property data.
    
    Args:
        settings: Application settings
        property_settings: Maryland Property API settings
        sheets_manager: Existing SheetsManager instance to reuse (optional)
        sheet_name: Name of the sheet to use
        delay_seconds: Delay between requests to override settings
    """
    settings = settings or Settings()
    property_settings = property_settings or MarylandPropertySettings()
    
    logger.info(f"Starting property data processing for sheet: {sheet_name}")
    
    # Pass BOTH settings objects to SheetsManager
    sheets = sheets_manager or SheetsManager(settings, property_settings)
    logger.info(f"Processing rows from {settings.START_ROW} to " + 
                (f"{settings.STOP_ROW}" if settings.STOP_ROW > 0 else 
                f"{settings.START_ROW + settings.MAX_ROWS - 1}"))
    
    if settings.SKIP_ROW_RANGE:
        logger.info(f"Skipping rows: {settings.SKIP_ROW_RANGE}")
    
    try:
        # Get addresses with their row indices
        address_data = sheets.get_property_addresses(sheet_name)
    except Exception as e:
        logger.error(f"Failed to get addresses from sheet {sheet_name}: {e}")
        return
    
    if not address_data:
        logger.warning(f"No addresses found in sheet: {sheet_name}")
        return
    
    logger.info(f"Processing {len(address_data)} addresses using {settings.MAX_WORKERS} threads with batch updates of {settings.BATCH_SIZE} rows")
    
    # Initialize the property API client
    property_api = PropertyDataAPI(settings, property_settings)
    results = []
    
    # Start timing
    start_time = time.time()
    
    # Define robust process_address function with built-in retries
    def process_address(row_idx, address):
        if not address:
            return row_idx, None
        
        try:
            # Get property data
            result = property_api.get_property_data(address)
            
            # Add delay between requests
            delay = delay_seconds if delay_seconds is not None else property_settings.REQUEST_DELAY
            time.sleep(delay)
            
            return row_idx, result
            
        except Exception as e:
            # Try to log error without including the exception details
            logger.error(f"Error processing address (row {row_idx+2})")
            
            # Create error result without logging the actual exception
            error_result = {"success": False, "message": "Error processing address"}
                
            return row_idx, error_result
    
    # Process addresses in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
        # Submit all jobs
        future_to_idx = {
            executor.submit(process_address, idx, addr): idx 
            for idx, addr in address_data if addr
        }
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_idx):
            try:
                idx, result = future.result()
                if result:  # Skip None results (empty addresses)
                    results.append((idx, result))
            except Exception as e:
                logger.error(f"Worker thread error: {e}")
    
    # Sort results by index to maintain order
    results.sort(key=lambda x: x[0])
    
    # Update the sheet in batches
    for i in range(0, len(results), property_settings.BATCH_SIZE):
        batch = results[i:i+property_settings.BATCH_SIZE]
        try:
            sheets.batch_update_property_data(batch, sheet_name)
            logger.info(f"Updated batch {i//property_settings.BATCH_SIZE + 1} of {(len(results) + property_settings.BATCH_SIZE - 1)//property_settings.BATCH_SIZE}")
            
            # Add substantial delay between batches to avoid hitting API limits
            if i + property_settings.BATCH_SIZE < len(results):  # Don't delay after the last batch
                delay_time = property_settings.DELAY_BETWEEN_BATCHES
                logger.info(f"Sleeping for {delay_time} seconds before next batch")
                time.sleep(delay_time)
        except HttpError as e:
            if e.resp.status == 429:  # Rate limit error
                logger.warning("Rate limit exceeded on batch. Waiting 60 seconds for quota reset...")
                time.sleep(60)  # Wait a full minute for quota reset
                try:
                    sheets.batch_update_property_data(batch, sheet_name)
                except Exception as retry_e:
                    logger.error(f"Retry failed: {retry_e}")
            else:
                logger.error(f"Failed to update results batch: {e}")
                # Only attempt individual updates for non-rate-limit errors
                if "RATE_LIMIT_EXCEEDED" not in str(e):
                    logger.warning("Falling back to individual updates")
                    for idx, result in batch:
                        try:
                            time.sleep(1)  # Add delay between individual requests
                            sheets.update_row_with_property_data(idx, result, sheet_name)
                        except Exception as inner_e:
                            logger.error(f"Failed to update row {idx+2}: {inner_e}")
    
    # Calculate and log timing information
    total_time = time.time() - start_time
    logger.info(f"Property data processing completed for sheet: {sheet_name}")
    logger.info(f"Total processing time: {total_time:.2f} seconds for {len(address_data)} addresses")
    logger.info(f"Average time per address: {total_time/len(address_data):.2f} seconds")

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Baltimore Data Processing")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["water", "property", "both"],
        default="water",
        help="Processing mode: water bill, property data, or both"
    )
    parser.add_argument(
        "--sheet", 
        type=str, 
        help="Name of the sheet to use (defaults to value in config)"
    )
    parser.add_argument(
        "--list-sheets",
        action="store_true",
        help="List all available sheets in the spreadsheet"
    )
    parser.add_argument(
        "--delay", 
        type=float, 
        help="Delay between requests in seconds (overrides config)"
    )
    parser.add_argument(
        "--start-row",
        type=int,
        help="Starting row for processing (1-indexed, overrides config)"
    )
    parser.add_argument(
        "--stop-row",
        type=int,
        help="Ending row for processing (1-indexed, overrides config)"
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        help="Maximum number of rows to process (overrides config)"
    )
    parser.add_argument(
        "--skip-rows",
        type=str,
        help="Comma-separated list of rows or ranges to skip (e.g. '5,8,10-15') (overrides config)"
    )
    parser.add_argument(
        "--address", 
        type=str,
        help="Process a single address (for testing)"
    )
    parser.add_argument(
        "--account", 
        type=str,
        help="Process a single account number (for testing)"
    )
    
    args = parser.parse_args()
    
    settings = Settings()
    property_settings = MarylandPropertySettings()
    
    # Apply command line overrides to settings
    if args.start_row:
        settings.START_ROW = args.start_row
        property_settings.START_ROW = args.start_row
    if args.stop_row:
        settings.STOP_ROW = args.stop_row
        property_settings.STOP_ROW = args.stop_row  
    if args.max_rows:
        settings.MAX_ROWS = args.max_rows
        property_settings.MAX_ROWS = args.max_rows
    if args.skip_rows:
        settings.SKIP_ROW_RANGE = args.skip_rows
        property_settings.SKIP_ROW_RANGE = args.skip_rows
    
    if args.list_sheets:
        list_sheets()
        return
        
    if args.address:
        # Test a single address
        logger.info(f"Testing with a single address: {args.address}")
        scraper = WaterBillScraper(settings)
        result = scraper.get_water_bill_details(args.address)
        print(result)
        return
        
    elif args.account:
        # Test a single account number
        logger.info(f"Testing with a single account number: {args.account}")
        scraper = WaterBillScraper(settings)
        result = scraper.get_bill_details_by_account_number(args.account)
        print(result)
        return
    
    sheets_manager = SheetsManager(settings, property_settings)

    if args.mode in ["water", "both"]:
        water_sheet_name = args.sheet if args.sheet else settings.SHEET_NAME
        if sheets_manager.sheet_exists(water_sheet_name):
            process_addresses_for_bill_details(
                settings=settings,
                sheets_manager=sheets_manager,
                sheet_name=water_sheet_name,
                delay_seconds=args.delay
            )
    
    if args.mode in ["property", "both"]:
        property_sheet_name = args.sheet if args.sheet else property_settings.PROPERTY_SHEET_NAME
        if sheets_manager.sheet_exists(property_sheet_name):
            process_addresses_for_property_data(
                settings=settings,
                property_settings=property_settings,
                sheets_manager=sheets_manager,
                sheet_name=property_sheet_name,
                delay_seconds=args.delay
            )

if __name__ == "__main__":
    main()