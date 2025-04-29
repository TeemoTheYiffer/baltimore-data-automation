import logging
import argparse
import os
from config import AppConfig, ProcessingMode, CountyEnum
from scraper import WaterBillScraper
from sheets import SheetsManager
from routes import process_addresses_for_bill_details, process_county_property_data, list_sheets
from utils.connection_manager import TCPConnectionManager
from utils.minimal_cache_manager import MinimalCacheManager
from utils.connection_settings import ConnectionSettings
from property_api import PropertyDataAPI

def setup_logging(config: AppConfig):
    """Set up logging based on configuration."""
    log_level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('maryland.log')
        ]
    )
    
    return logging.getLogger("maryland_main")

def parse_cli_args():
    """Parse command line arguments as overrides to the config."""
    parser = argparse.ArgumentParser(description="Maryland Property and Water Bill Data Processing")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["water", "property", "both", "all"],
        help="Processing mode (overrides config)"
    )
    parser.add_argument(
        "--county",
        type=str,
        help="County to process (comma-separated for multiple, 'all' for all counties)"
    )
    parser.add_argument(
        "--sheet", 
        type=str, 
        help="Name of the sheet to use (overrides config)"
    )
    parser.add_argument(
        "--config", 
        type=str, 
        help="Path to config file (defaults to .env)"
    )
    parser.add_argument(
        "--list-sheets",
        action="store_true",
        help="List all available sheets in the spreadsheet"
    )
    
    return parser.parse_args()

def load_config(cli_args=None):
    """
    Load configuration from files, environment, and CLI overrides.
    
    Args:
        cli_args: Command line arguments (optional)
        
    Returns:
        AppConfig instance
    """
    # First load from .env or specified config file
    config_file = getattr(cli_args, "config", None) if cli_args else None
    
    # Create config instance
    config = AppConfig(_env_file=config_file) if config_file else AppConfig()
    
    # Apply CLI overrides if provided
    if cli_args:
        # Mode override
        if cli_args.mode:
            config.PROCESSING_MODE = ProcessingMode(cli_args.mode)
            
        # County override
        if cli_args.county:
            if cli_args.county.lower() == "all":
                config.COUNTIES = ["all"]
            else:
                config.COUNTIES = [county.strip().lower() for county in cli_args.county.split(",")]
                
        # Sheet override
        if cli_args.sheet:
            config.SHEET_NAME = cli_args.sheet
    
    return config

def main():
    """Main entry point."""
    # Parse CLI args (optional, only used as overrides)
    cli_args = parse_cli_args()
    
    # Load config (from files, environment, and CLI overrides)
    config = load_config(cli_args)
    
    # Set up logging
    logger = setup_logging(config)
    logger.info(f"Starting Maryland data processing with mode: {config.PROCESSING_MODE}")
    
    # Initialize TCP connection manager
    conn_settings = ConnectionSettings(
        TCP_TIMEOUT=config.TCP_TIMEOUT,
        BATCH_RETRY_ATTEMPTS=config.BATCH_RETRY_ATTEMPTS
    )
    tcp_manager = TCPConnectionManager(settings=conn_settings)
    logger.info("TCP connection manager initialized with optimized settings")
    
    # Initialize sheets manager (passing necessary config)
    sheets_manager = SheetsManager(
        config=config,
        tcp_manager=tcp_manager
    )

    # Initialize cache if enabled
    cache_manager = None
    if config.CACHE_ENABLED:
        cache_manager = MinimalCacheManager(cache_dir=config.CACHE_DIRECTORY)
        logger.info(f"Cache initialized in directory: {config.CACHE_DIRECTORY}")
    else:
        logger.info("Caching disabled")

    # Handle list-sheets command
    if cli_args and cli_args.list_sheets:
        list_sheets(sheets_manager)
        return

    # Handle single item testing (for debugging)
    if config.DEBUG_SINGLE_ADDRESS:
        logger.info(f"Testing with a single address: {config.DEBUG_SINGLE_ADDRESS}")
        if config.PROCESSING_MODE == ProcessingMode.WATER:
            scraper = WaterBillScraper(config)
            result = scraper.get_water_bill_details(config.DEBUG_SINGLE_ADDRESS)
            print(result)
        else:
            # Test property data for the address
            county = config.COUNTIES[0] if config.COUNTIES else "baltimore"
            api = PropertyDataAPI(config=config, county=county)
            result = api.get_property_data(config.DEBUG_SINGLE_ADDRESS)
            print(result)
        return
        
    elif config.DEBUG_SINGLE_ACCOUNT:
        # Test a single account number
        logger.info(f"Testing with a single account number: {config.DEBUG_SINGLE_ACCOUNT}")
        scraper = WaterBillScraper(config=config)
        result = scraper.get_bill_details_by_account_number(config.DEBUG_SINGLE_ACCOUNT)
        print(result)
        return
    
    elif config.DEBUG_SINGLE_PARCEL_ID:
        # Test a single parcel ID
        logger.info(f"Testing with a single parcel ID: {config.DEBUG_SINGLE_PARCEL_ID}")
        api = PropertyDataAPI(config=config, county="pg")
        result = api.get_property_data(config.DEBUG_SINGLE_PARCEL_ID)
        print(result)
        return

    # Process water bill data if requested
    if config.PROCESSING_MODE in [ProcessingMode.WATER, ProcessingMode.BOTH, ProcessingMode.ALL]:
        water_sheet_name = config.SHEET_NAME or config.WATER_BILL_SHEET_NAME
        if sheets_manager.sheet_exists(water_sheet_name):
            process_addresses_for_bill_details(
                config=config,
                sheets_manager=sheets_manager,
                sheet_name=water_sheet_name,
                cache_manager=cache_manager
            )
    
    # Process property data if requested
    if config.PROCESSING_MODE in [ProcessingMode.PROPERTY, ProcessingMode.BOTH, ProcessingMode.ALL]:
        property_sheet_name = config.SHEET_NAME or config.PROPERTY_SHEET_NAME
        
        if sheets_manager.sheet_exists(property_sheet_name):
            counties = config.get_counties_to_process()
            logger.info(f"Processing {len(counties)} counties: {', '.join(counties)}")
            
            for county in counties:
                process_county_property_data(
                    county_name=county,
                    config=config,
                    sheets_manager=sheets_manager,
                    sheet_name=property_sheet_name,
                    cache_manager=cache_manager
                )

if __name__ == "__main__":
    main()