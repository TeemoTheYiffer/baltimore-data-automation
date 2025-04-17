import logging
import time
import argparse
from typing import Optional
import random
import ssl
from config import Settings
from scraper import WaterBillScraper
from sheets import SheetsManager
import concurrent.futures
from functools import partial
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('baltimore_water.log')
    ]
)

logger = logging.getLogger("baltimore_water")

def process_addresses_for_bill_details(
    settings: Optional[Settings] = None,
    sheet_name: str = "Sheet1",
    delay_seconds: float = None,
    batch_size: int = 100,
    max_workers: int = 5 
) -> None:
    """
    Process addresses using parallel scraping and batched sheet updates.
    
    Args:
        settings: Application settings
        sheet_name: Name of the sheet to use
        delay_seconds: Delay between requests to override settings
        batch_size: Number of addresses to process in each batch
        max_workers: Maximum number of worker threads
    """
    settings = settings or Settings()
    
    logger.info(f"Starting water bill processing for sheet: {sheet_name}")
    
    # Set up sheets manager and headers
    sheets = SheetsManager(settings)
    try:
        sheets.setup_headers(sheet_name)
        addresses = sheets.get_addresses(sheet_name)
    except Exception as e:
        logger.error(f"Failed to set up sheet {sheet_name}: {e}")
        return
    
    if not addresses:
        logger.warning(f"No addresses found in sheet: {sheet_name}")
        return
    
    logger.info(f"Processing {len(addresses)} addresses using {max_workers} threads with batch updates of {batch_size}")
    
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
    
    # Update all rows to "Processing..."
    status_updates = [(i, {"success": False, "message": "Processing..."}) 
                      for i, addr in enumerate(addresses) if addr]
    
    # Update statuses in batches
    for i in range(0, len(status_updates), batch_size):
        batch = status_updates[i:i+batch_size]
        try:
            sheets.batch_update_bill_details(batch, sheet_name)
        except Exception as e:
            logger.error(f"Failed to update processing status batch: {e}")
    
    # Process addresses in parallel
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all jobs
        future_to_idx = {
            executor.submit(scrape_address, (i, addr)): i 
            for i, addr in enumerate(addresses) if addr
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
    for i in range(0, len(results), batch_size):
        batch = results[i:i+batch_size]
        try:
            sheets.batch_update_bill_details(batch, sheet_name)
            logger.info(f"Updated batch {i//batch_size + 1} of {(len(results) + batch_size - 1)//batch_size}")
        except Exception as e:
            logger.error(f"Failed to update results batch: {e}")
            # Attempt individual updates as fallback
            for idx, result in batch:
                try:
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

def process_address_worker(args):
    """Standalone worker function that creates its own connections"""
    address, index, sheet_name, spreadsheet_id = args
    
    # Setup independent logging for this process
    process_logger = logging.getLogger(f"baltimore_water.worker.{index}")
    
    if not address:
        return
    
    process_logger.info(f"Processing address {index+1}: {address}")
    
    # Create fresh instances for each worker process
    settings = Settings()
    scraper = WaterBillScraper(settings)
    sheets = SheetsManager(settings)
    
    try:
        # Update status
        sheets.update_status(index, "Processing...", sheet_name)
        
        # Get water bill details
        result = scraper.get_water_bill_details(address)
        
        # Update the spreadsheet
        sheets.update_row_with_bill_details(index, result, sheet_name)
        
        return True
    except Exception as e:
        process_logger.error(f"Error processing address {address}: {e}")
        
        # Try updating status, but don't fail if it doesn't work
        try:
            sheets.update_status(index, f"Error: {str(e)[:50]}", sheet_name)
        except:
            pass
        
        return False

def process_single_address(address, index, scraper, sheets, sheet_name):
    """Process a single address with the scraper and update sheets."""
    if not address:
        return
        
    logger.info(f"Processing address {index+1}: {address}")
    
    # Update status to "Processing"
    for retry in range(3):  # Add retries for SSL errors
        try:
            sheets.update_status(index, "Processing...", sheet_name)
            break
        except ssl.SSLError as e:
            logger.warning(f"SSL error updating status (retry {retry+1}/3): {e}")
            time.sleep(1 + random.random())  # Add delay with jitter
            if retry == 2:  # Last retry
                logger.error(f"Failed to update status for {address} after 3 retries")
    
    try:
        # Get water bill details
        result = scraper.get_water_bill_details(address)
        
        # Update with retries for SSL errors
        for retry in range(3):
            try:
                sheets.update_row_with_bill_details(index, result, sheet_name)
                break
            except ssl.SSLError as e:
                logger.warning(f"SSL error updating row (retry {retry+1}/3): {e}")
                time.sleep(1 + random.random())
                if retry == 2:
                    raise
                
    except Exception as e:
        logger.error(f"Error processing address {address}: {e}")
        try:
            sheets.update_status(index, f"Error: {str(e)}", sheet_name)
        except:
            logger.error(f"Failed to update error status for {address}")

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Baltimore Water Bill Scraper")
    parser.add_argument(
        "--sheet", 
        type=str, 
        help="Name of the sheet to use (omit to process all sheets)"
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
    
    if args.list_sheets:
        # List all available sheets
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
    
    # Initialize sheets manager
    sheets_manager = SheetsManager(settings)
    
    # Get sheets to process
    sheets_to_process = []
    
    if args.sheet:
        # Process a specific sheet - check if it exists
        if sheets_manager.sheet_exists(args.sheet):
            sheets_to_process = [args.sheet]
        else:
            logger.error(f"Sheet '{args.sheet}' not found in the spreadsheet")
            print(f"Error: Sheet '{args.sheet}' not found. Use --list-sheets to see available sheets.")
            return
    else:
        # Process all sheets that have addresses
        try:
            sheets_to_process = sheets_manager.get_all_sheet_names()
            logger.info(f"Found {len(sheets_to_process)} sheets to process")
        except Exception as e:
            logger.error(f"Failed to get sheet names: {e}")
            return
    
    if not sheets_to_process:
        logger.error("No sheets to process")
        return
    
    # Process each sheet
    for sheet_name in sheets_to_process:
        logger.info(f"Processing sheet: {sheet_name}")
        
        try:
            # Process all addresses in the spreadsheet
            process_addresses_for_bill_details(
                settings=settings,
                sheet_name=sheet_name,
                delay_seconds=args.delay
            )
        except Exception as e:
            logger.error(f"Error processing sheet {sheet_name}: {e}")
            continue

if __name__ == "__main__":
    main()