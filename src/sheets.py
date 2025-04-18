import logging
import time
from typing import List, Dict, Any, Optional
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
import random
from property_api import MarylandPropertySettings
import ssl
import requests
logger = logging.getLogger("baltimore")
from config import Settings

# Import service account file path from google_credentials
try:
    # First, try to import directly from the secrets directory
    from google_credentials import SERVICE_ACCOUNT_FILE, IMPERSONATED_USER
    logger.info(f"Successfully imported credentials from google_credentials.py")
except ImportError:
    try:
        # If that fails, try importing from secrets.google_credentials
        from secrets.google_credentials import SERVICE_ACCOUNT_FILE, IMPERSONATED_USER
        logger.info(f"Successfully imported credentials from secrets.google_credentials")
    except ImportError:
        raise ImportError(
            "Failed to import Google credentials. Please ensure the file exists and is correctly referenced."
        )


class SheetsManager:
    """Manager for Google Sheets operations."""
    
    def __init__(self, 
                 settings: Optional[Settings] = None,
                 property_settings: Optional[MarylandPropertySettings] = None):
        """Initialize the sheets manager with settings."""
        self.settings = settings or Settings()
        self.property_settings = property_settings or MarylandPropertySettings()
        
        # Set service account file from imported constant if not in settings
        if not self.settings.SERVICE_ACCOUNT_FILE and SERVICE_ACCOUNT_FILE:
            self.settings.SERVICE_ACCOUNT_FILE = SERVICE_ACCOUNT_FILE
            
        # Set impersonated user from imported constant if not in settings
        if not self.settings.IMPERSONATED_USER and IMPERSONATED_USER:
            self.settings.IMPERSONATED_USER = IMPERSONATED_USER
            
        self.service = self._get_sheets_service()

        # Cache for sheet headers
        self._headers_cache = {}

    def _get_sheet_headers(self, sheet_name: str) -> List[str]:
        """Get headers from sheet with caching to avoid quota issues."""
        # Check if headers are already cached
        if sheet_name in self._headers_cache:
            return self._headers_cache[sheet_name]
        
        # Fetch headers with exponential backoff for rate limits
        for retry in range(5):
            try:
                header_range = f"{sheet_name}!1:1"
                header_response = self.service.spreadsheets().values().get(
                    spreadsheetId=self.settings.SPREADSHEET_ID,
                    range=header_range
                ).execute()
                
                headers = header_response.get('values', [[]])[0]
                logger.info(f"Fetched and cached {len(headers)} headers for sheet: {sheet_name}")
                
                # Cache the headers
                self._headers_cache[sheet_name] = headers
                return headers
                
            except HttpError as e:
                if e.resp.status == 429:  # Rate limit error
                    wait_time = (2 ** retry) + random.random()
                    logger.warning(f"Rate limit on header fetch. Waiting {wait_time:.2f}s before retry {retry+1}/5")
                    time.sleep(wait_time)
                    if retry == 4:  # Last retry failed
                        raise
                else:
                    raise
        
        raise RuntimeError("Failed to fetch sheet headers after multiple retries")

    def batch_update_bill_details(self, updates, sheet_name="Water Bill"):
        """
        Perform a batch update of multiple rows with water bill details.
        
        Args:
            updates: List of tuples (row_index, bill_data)
            sheet_name: Name of the sheet to update
        """
        try:
            # Prepare batch request data
            batch_data = {
                "valueInputOption": "RAW",
                "data": []
            }
            
            # Process each update
            for row_index, bill_data in updates:
                # Adjust row index (sheets API is 1-based and we need to account for header row)
                sheet_row_index = row_index + 2  # +1 for 1-based, +1 for header row
                
                # Prepare values to update
                if bill_data.get("success", False) and "data" in bill_data:
                    data = bill_data["data"]
                    values = [
                        data.get("account_number", ""),
                        data.get("bill_date", ""),
                        data.get("current_bill_amount", ""),
                        data.get("current_balance", ""),
                        data.get("penalty_date", ""),
                        data.get("last_payment_date", ""),
                        data.get("last_payment_amount", ""),
                        "Success"
                    ]
                else:
                    # Something went wrong, update the status column
                    values = ["" for _ in range(7)]  # Empty cells for columns B-H
                    values.append(bill_data.get("message", "Error"))
                    
                    # If we have an account number despite the error, include it
                    if bill_data.get("account_number"):
                        values[0] = bill_data["account_number"]
                
                # Add this update to the batch
                range_name = f"{sheet_name}!B{sheet_row_index}:I{sheet_row_index}"
                batch_data["data"].append({
                    "range": range_name,
                    "values": [values]
                })
            
            # Execute the batch update with exponential backoff for rate limits
            if batch_data["data"]:
                logger.info(f"Executing batch update for {len(batch_data['data'])} rows")
                
                for retry in range(5):  # Maximum 5 retries
                    try:
                        self.service.spreadsheets().values().batchUpdate(
                            spreadsheetId=self.settings.SPREADSHEET_ID,
                            body=batch_data
                        ).execute()
                        logger.info(f"Batch update successful for {len(batch_data['data'])} rows")
                        time.sleep(2)  # Delay between batches
                        return
                    except (ConnectionAbortedError, ConnectionResetError) as e:
                        logger.warning(f"Connection aborted during batch update: {e}. Reducing batch size.")
                        # Split the batch and try with smaller chunks
                        if len(batch_data["data"]) > 10:
                            midpoint = len(batch_data["data"]) // 2
                            first_half = {"valueInputOption": "RAW", "data": batch_data["data"][:midpoint]}
                            second_half = {"valueInputOption": "RAW", "data": batch_data["data"][midpoint:]}
                            
                            logger.info(f"Retrying with split batch (1/{2})")
                            self.service.spreadsheets().values().batchUpdate(
                                spreadsheetId=self.settings.SPREADSHEET_ID,
                                body=first_half
                            ).execute()
                            
                            logger.info(f"Retrying with split batch (2/{2})")
                            self.service.spreadsheets().values().batchUpdate(
                                spreadsheetId=self.settings.SPREADSHEET_ID,
                                body=second_half
                            ).execute()
                    except HttpError as e:
                        if e.resp.status == 429:  # Rate limit error
                            wait_time = (2 ** retry) + random.random()  # Exponential backoff with jitter
                            logger.warning(f"Rate limit exceeded. Waiting {wait_time:.2f} seconds before retry {retry+1}/5")
                            time.sleep(wait_time)
                            if retry == 4:  # Last retry failed
                                raise
                        else:
                            raise
                
        except Exception as e:
            logger.error(f"Error performing batch update: {e}")
            raise

    def get_all_sheet_names(self) -> List[str]:
        """
        Get all sheet names from the spreadsheet.
        
        Returns:
            List of sheet names
        """
        try:
            # Get the spreadsheet
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=self.settings.SPREADSHEET_ID
            ).execute()
            
            # Extract sheet names
            sheets = spreadsheet.get('sheets', [])
            sheet_names = [sheet.get('properties', {}).get('title', '') for sheet in sheets]
            
            logger.info(f"Retrieved {len(sheet_names)} sheet names")
            return sheet_names
            
        except Exception as e:
            logger.error(f"Error getting sheet names: {e}")
            raise
            
    def sheet_exists(self, sheet_name: str) -> bool:
        """
        Check if a sheet exists in the spreadsheet.
        
        Args:
            sheet_name: Name of the sheet to check
            
        Returns:
            True if the sheet exists, False otherwise
        """
        try:
            sheet_names = self.get_all_sheet_names()
            return sheet_name in sheet_names
        except Exception as e:
            logger.error(f"Error checking if sheet {sheet_name} exists: {e}")
            return False
    
    def _get_credentials(self) -> Credentials:
        """
        Get Google API credentials from service account file.
        
        Returns:
            Google API credentials
        """
        if not self.settings.SERVICE_ACCOUNT_FILE:
            raise ValueError("No service account file specified")
            
        if not self.settings.IMPERSONATED_USER:
            raise ValueError("No impersonated user specified")
        
        logger.info(f"Impersonating user: {self.settings.IMPERSONATED_USER}")
        logger.info(f"Using service account file: {self.settings.SERVICE_ACCOUNT_FILE}")
        
        # Define scopes needed for the application
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # Get Google API credentials with domain-wide delegation
        creds = Credentials.from_service_account_file(
            self.settings.SERVICE_ACCOUNT_FILE,
            scopes=scopes
        )
        
        # Create delegated credentials
        delegated_creds = creds.with_subject(self.settings.IMPERSONATED_USER)
        return delegated_creds
    
    def _get_sheets_service(self):
        """
        Get Google Sheets API service with retry logic.
        
        Returns:
            Google Sheets API service
        """
        retry_count = 0
        
        while retry_count < self.settings.MAX_RETRIES:
            try:
                # Get credentials
                creds = self._get_credentials()
                
                # Create service
                logger.info("Building Sheets API service")
                service = build(
                    'sheets', 
                    'v4', 
                    credentials=creds,
                    cache_discovery=False
                )
                
                logger.info("Successfully built Sheets API service")
                return service
                
            except Exception as e:
                retry_count += 1
                wait_time = 2 ** retry_count  # Exponential backoff
                
                logger.error(f"Error building Sheets service (attempt {retry_count}/{self.settings.MAX_RETRIES}): {e}")
                
                if retry_count < self.settings.MAX_RETRIES:
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.critical(f"Failed to build Sheets service after {self.settings.MAX_RETRIES} attempts")
                    raise
        
        # Should never reach here due to raise in the loop, but just in case
        raise RuntimeError("Failed to build Sheets service")
        
    def setup_headers(self, sheet_name: str = "Water Bill") -> None:
        """
        Set up headers in the spreadsheet.
        
        Args:
            sheet_name: Name of the sheet to update
        """
        try:
            # Choose headers based on sheet name
            if sheet_name == self.property_settings.PROPERTY_SHEET_NAME:
                # Use property headers
                headers = list(self.property_settings.FIELD_MAPPING.keys())
                logger.info(f"Using property headers for {sheet_name}")
            else:
                # Use water bill headers
                headers = self.settings.SHEET_HEADERS
                logger.info(f"Using water bill headers for {sheet_name}")
            
            # Convert column index to letter safely (handles > 26 columns)
            def col_num_to_letter(n):
                result = ""
                while n >= 0:
                    remainder = n % 26
                    result = chr(65 + remainder) + result
                    n = n // 26 - 1
                return result
                
            # Calculate the ending column letter correctly
            end_col = col_num_to_letter(len(headers) - 1)
            
            # Prepare the update request
            range_name = f"{sheet_name}!A1:{end_col}1"
            
            body = {
                "values": [headers]
            }
            
            # Update the sheet
            self.service.spreadsheets().values().update(
                spreadsheetId=self.settings.SPREADSHEET_ID,
                range=range_name,
                valueInputOption="RAW",
                body=body
            ).execute()
            
            logger.info(f"Headers set up successfully in {sheet_name}")
            
        except Exception as e:
            logger.error(f"Error setting up headers: {e}")
            raise
        
    def get_addresses(self, sheet_name: str = "Water Bill") -> List[str]:
        """
        Get addresses from the spreadsheet with row control, skipping rows
        that have already been processed successfully.
        
        Args:
            sheet_name: Name of the sheet to read from
            
        Returns:
            List of addresses and their indices
        """
        try:
            # Determine the row range to fetch
            start_row = self.settings.START_ROW  # Already 1-indexed in config
            
            # Calculate the end row based on settings
            if self.settings.STOP_ROW > 0:
                end_row = self.settings.STOP_ROW
            else:
                end_row = start_row + self.settings.MAX_ROWS - 1
            
            logger.info(f"Fetching addresses from row {start_row} to {end_row}")
            
            # Prepare the range - get both addresses and statuses
            range_name = f"{sheet_name}!A{start_row}:I{end_row}"
            
            # Get the values
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.settings.SPREADSHEET_ID,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            
            # Check for rows to skip
            rows_to_skip = set()
            if self.settings.SKIP_ROW_RANGE:
                # Parse the skip range string (format: "5,8,10-15,20-25")
                for part in self.settings.SKIP_ROW_RANGE.split(','):
                    if '-' in part:
                        # Handle range (e.g., "10-15")
                        start, end = map(int, part.split('-'))
                        for row in range(start, end + 1):
                            rows_to_skip.add(row)
                    else:
                        # Handle single row (e.g., "5")
                        try:
                            rows_to_skip.add(int(part))
                        except ValueError:
                            # Skip invalid entries
                            continue
            
            # Extract addresses, skipping specified rows and already processed rows
            addresses = []
            for i, row in enumerate(values):
                # Calculate actual row number (1-indexed for user reference)
                actual_row = start_row + i
                
                # Skip rows in the skip list
                if actual_row in rows_to_skip:
                    logger.info(f"Skipping row {actual_row} as specified in SKIP_ROW_RANGE")
                    continue
                
                # Check if this row has an address and hasn't been processed successfully yet
                has_address = row and len(row) > 0 and row[0].strip()

                # Check status column (column I, index 8)
                skip_row = False
                if len(row) > 8 and row[8].strip():
                    status = row[8].strip()
                    if status == "Success":
                        skip_row = True
                        logger.info(f"Skipping row {actual_row} with status: {status}")
                    if "Could not find account number for address" in status and not self.settings.RETRY_FAILED_ROWS:
                        skip_row = True    
                        logger.info(f"Skipping row {actual_row} with status: {status}")

                if has_address and not skip_row:
                    addresses.append((i, row[0].strip()))
            
            # Log how many addresses we're processing
            logger.info(f"Retrieved {len(addresses)} addresses to process from {sheet_name}")
            return addresses
                
        except Exception as e:
            logger.error(f"Error getting addresses: {e}")
            raise
        
    def update_row_with_bill_details(
        self, 
        row_index: int, 
        bill_data: Dict[str, Any],
        sheet_name: str = "Water Bill"
    ) -> None:
        """
        Update a row with water bill details.
        
        Args:
            row_index: 0-based index of the row to update
            bill_data: Dictionary with bill details
            sheet_name: Name of the sheet to update
        """
        try:
            # Adjust row index (sheets API is 1-based and we need to account for header row)
            sheet_row_index = row_index + 2  # +1 for 1-based, +1 for header row
            
            # Prepare the update
            if bill_data.get("success", False) and "data" in bill_data:
                data = bill_data["data"]
                
                values = [
                    data.get("account_number", ""),
                    data.get("bill_date", ""),
                    data.get("current_bill_amount", ""),
                    data.get("current_balance", ""),
                    data.get("penalty_date", ""),
                    data.get("last_payment_date", ""),
                    data.get("last_payment_amount", ""),
                    "Success"
                ]
            else:
                # Something went wrong, update the status column
                values = ["" for _ in range(7)]  # Empty cells for columns A-H
                values.append(bill_data.get("message", "Error"))
                
                # If we have an account number despite the error, include it
                if bill_data.get("account_number"):
                    values[0] = bill_data["account_number"]
            
            # Define the range to update (all columns for this row)
            range_name = f"{sheet_name}!B{sheet_row_index}:I{sheet_row_index}"
            
            body = {
                "values": [values]
            }
            
            # Update the sheet
            self.service.spreadsheets().values().update(
                spreadsheetId=self.settings.SPREADSHEET_ID,
                range=range_name,
                valueInputOption="RAW",
                body=body
            ).execute()
            
            logger.info(f"Updated row {sheet_row_index} with bill details")
            
        except Exception as e:
            logger.error(f"Error updating row with bill details: {e}")
            # We don't want to raise here to allow processing to continue for other rows
            return

    def update_row_with_property_data(self, 
                                    row_index: int, 
                                    property_data: Dict[str, Any],
                                    sheet_name: str = "LIENS") -> None:
        """
        Update a single row with property data.
        
        Args:
            row_index: 0-based index of the row to update
            property_data: Dictionary with property data result
            sheet_name: Name of the sheet to update
        """
        try:
            # Helper function to convert column index to column letter(s)
            def col_num_to_letter(n):
                """Convert 0-based column index to Excel-style column letter(s)."""
                result = ""
                while n >= 0:
                    remainder = n % 26
                    result = chr(65 + remainder) + result
                    n = n // 26 - 1
                return result
                
            # Ensure we're never updating the header row
            sheet_row_index = row_index + 2  # +1 for 1-based, +1 for header row
            if sheet_row_index <= 1:
                logger.warning(f"Skipping update for row {sheet_row_index} to protect headers")
                return
                
            # Get sheet headers to map column names to indices
            try:
                headers = self._get_sheet_headers(sheet_name)
            except Exception as e:
                logger.error(f"Error fetching headers: {e}")
                # Wait and try again with longer delay if rate limited
                if "RATE_LIMIT_EXCEEDED" in str(e):
                    logger.warning("Rate limit exceeded, waiting 60 seconds before retry")
                    time.sleep(60)
                    headers = self._get_sheet_headers(sheet_name)
                else:
                    raise
            
            # If property data is successful, update fields
            if property_data.get("success", False) and "data" in property_data:
                data = property_data.get("data", {})
                
                # Group updates to minimize API calls
                columns_to_update = {}
                
                # Find fields that exist in headers
                for field, value in data.items():
                    try:
                        col_index = headers.index(field)
                        columns_to_update[col_index] = value
                    except ValueError:
                        logger.warning(f"Field '{field}' not found in headers, skipping")
                
                if not columns_to_update:
                    logger.warning(f"No valid fields to update for row {sheet_row_index}")
                    return
                    
                # Update in batches of related columns to minimize API calls
                # Sort columns by index
                col_indices = sorted(columns_to_update.keys())
                
                # Find consecutive runs of columns to update together
                current_run = []
                all_runs = []
                
                for i, idx in enumerate(col_indices):
                    if i == 0 or idx != col_indices[i-1] + 1:
                        # Start a new run
                        if current_run:
                            all_runs.append(current_run)
                        current_run = [idx]
                    else:
                        # Continue current run
                        current_run.append(idx)
                
                # Add the last run
                if current_run:
                    all_runs.append(current_run)
                
                # Update each run
                for run in all_runs:
                    start_col = run[0]
                    end_col = run[-1]
                    
                    # Convert to column letters
                    start_col_letter = col_num_to_letter(start_col)
                    end_col_letter = col_num_to_letter(end_col)
                    
                    # Prepare range and values
                    if start_col == end_col:
                        range_name = f"{sheet_name}!{start_col_letter}{sheet_row_index}"
                        values = [[columns_to_update[start_col]]]
                    else:
                        range_name = f"{sheet_name}!{start_col_letter}{sheet_row_index}:{end_col_letter}{sheet_row_index}"
                        values = [[columns_to_update[i] for i in range(start_col, end_col + 1)]]
                    
                    logger.info(f"Updating range: {range_name}")
                    
                    # Try the update with retry logic
                    for retry in range(5):
                        try:
                            self.service.spreadsheets().values().update(
                                spreadsheetId=self.settings.SPREADSHEET_ID,
                                range=range_name,
                                valueInputOption="RAW",
                                body={"values": values}
                            ).execute()
                            break
                        except HttpError as api_error:
                            if api_error.resp.status == 429:  # Rate limit error
                                wait_time = (2 ** retry) + random.random()
                                logger.warning(f"Rate limit, waiting {wait_time:.2f}s before retry {retry+1}/5")
                                time.sleep(wait_time)
                                if retry == 4:
                                    raise
                            else:
                                raise
                
                logger.info(f"Successfully updated row {sheet_row_index} with property data")
            else:
                logger.warning(f"No successful property data for row {sheet_row_index}")
                    
        except Exception as e:
            logger.error(f"Error updating row {row_index+2} with property data: {e}")
        
    def get_property_addresses(self, sheet_name: str = "LIENS") -> List[tuple]:
        """Get addresses with row indices from the property sheet."""
        try:
            property_settings = self.property_settings

            # Log the value of RETRY_FAILED_ROWS for debugging
            logger.info(f"RETRY_FAILED_ROWS setting: {property_settings.RETRY_FAILED_ROWS}")

            # Fetch headers to find ADDRESS and Status columns
            header_range = f"{sheet_name}!1:1"
            header_response = self.service.spreadsheets().values().get(
                spreadsheetId=self.settings.SPREADSHEET_ID,
                range=header_range
            ).execute()
            
            if not header_response.get('values'):
                logger.error(f"No headers found in sheet: {sheet_name}")
                return []
                
            headers = header_response.get('values', [[]])[0]
            logger.info(f"Found headers: {headers}")
            
            # Find ADDRESS column
            address_col_index = -1
            status_col_index = -1
            for i, header in enumerate(headers):
                if header == 'ADDRESS':
                    address_col_index = i
                if header == 'Status':
                    status_col_index = i
                    
            if address_col_index == -1:
                logger.error("ADDRESS column not found in the sheet")
                return []
            
            # Determine range based on PROPERTY settings
            start_row = property_settings.START_ROW
            if property_settings.STOP_ROW > 0:
                end_row = property_settings.STOP_ROW
            else:
                end_row = start_row + property_settings.MAX_ROWS - 1
            
            # Fix for character encoding issues - use simple range format
            range_name = f"{sheet_name}!{start_row}:{end_row}"
            
            full_sheet_response = self.service.spreadsheets().values().get(
                spreadsheetId=self.settings.SPREADSHEET_ID,
                range=range_name
            ).execute()
            
            all_values = full_sheet_response.get('values', [])
            
            # Parse skip ranges from PROPERTY settings
            rows_to_skip = set()
            if property_settings.SKIP_ROW_RANGE:
                for part in property_settings.SKIP_ROW_RANGE.split(','):
                    if '-' in part:
                        start, end = map(int, part.split('-'))
                        for row in range(start, end + 1):
                            rows_to_skip.add(row)
                    else:
                        try:
                            rows_to_skip.add(int(part.strip()))
                        except ValueError:
                            continue
            
            # Return address data with row indices
            address_data = []
            for i, row in enumerate(all_values):
                actual_row = start_row + i
                if actual_row in rows_to_skip:
                    logger.info(f"Skipping row {actual_row} as specified in SKIP_ROW_RANGE")
                    continue
                
                # Check if row has enough columns for the address
                if len(row) <= address_col_index:
                    continue
                    
                # Check status if available
                skip_row = False
                if status_col_index != -1 and len(row) > status_col_index:
                    status = row[status_col_index].strip() if row[status_col_index] else ""
                    
                    # Skip rows with "Success" status
                    if status == "Success":
                        skip_row = True
                        logger.info(f"Skipping row {actual_row} with status: {status}")
                    
                    # Skip rows with failed lookups if not retrying failed rows
                    # The key issue - checking for the exact error message pattern
                    if "No data found for address" in status and not property_settings.RETRY_FAILED_ROWS:
                        skip_row = True
                        logger.info(f"Skipping row {actual_row} with status containing 'No data found for address' (RETRY_FAILED_ROWS is False)")
                
                address = row[address_col_index].strip() if row[address_col_index] else ""
                if address and not skip_row:
                    # Return absolute row index adjusted to be 0-indexed
                    address_data.append((actual_row - 2, address))
                        
            logger.info(f"Retrieved {len(address_data)} addresses from {sheet_name}")
            return address_data
                
        except Exception as e:
            logger.error(f"Error getting property addresses: {e}")
            raise

    def batch_update_property_data(self, updates, sheet_name="LIENS"):
        """Perform a batch update of multiple rows with property data."""
        try:
            # Helper function to convert column index to column letter(s)
            def col_num_to_letter(n):
                """Convert 0-based column index to Excel-style column letter(s)."""
                result = ""
                while n >= 0:
                    remainder = n % 26
                    result = chr(65 + remainder) + result
                    n = n // 26 - 1
                return result
            
            # Get headers using the caching method
            headers = self._get_sheet_headers(sheet_name)
            
            # Find Status column index
            status_col_index = -1
            for i, header in enumerate(headers):
                if header == 'Status':
                    status_col_index = i
                    break
            
            # If Status column wasn't found, use a default
            status_col_letter = self.property_settings.STATUS_COLUMN
            if status_col_index != -1:
                status_col_letter = col_num_to_letter(status_col_index)
            
            # Prepare batch updates
            batch_data = {"valueInputOption": "RAW", "data": []}
            
            # Group updates by row for efficiency
            for row_index, property_data in updates:
                # Ensure we're never updating row 1 (headers)
                sheet_row_index = row_index + 2  # +1 for 1-based indexing, +1 to skip header row
                if sheet_row_index <= 1:
                    logger.warning(f"Skipping update for row {sheet_row_index} to protect headers")
                    continue
                    
                if property_data.get("success", False) and "data" in property_data:
                    # Successful data fetch - update data fields
                    data = property_data.get("data", {})
                    
                    # Find columns to update - map field names to column indices
                    columns_to_update = {}
                    for field, value in data.items():
                        try:
                            col_index = headers.index(field)
                            columns_to_update[col_index] = value
                        except ValueError:
                            logger.warning(f"Field '{field}' not found in headers, skipping")
                    
                    # Always update status column for successful fetches
                    # Use status_col_letter which is either from the found column or default 'S'
                    status_range = f"{sheet_name}!{status_col_letter}{sheet_row_index}"
                    batch_data["data"].append({
                        "range": status_range,
                        "values": [["Success"]]
                    })
                    
                    # Only add data updates if we have data to update
                    if columns_to_update:
                        # Group columns for more efficient updates
                        col_indices = sorted(columns_to_update.keys())
                        
                        # Create batched updates by column groups
                        start_col = col_indices[0]
                        current_group = [start_col]
                        last_col = start_col
                        
                        for idx in col_indices[1:]:
                            if idx == last_col + 1:
                                # Continue group
                                current_group.append(idx)
                            else:
                                # End current group and start a new one
                                start_col_letter = col_num_to_letter(current_group[0])
                                end_col_letter = col_num_to_letter(current_group[-1])
                                
                                # Create range and values
                                values = [columns_to_update[i] if i in columns_to_update else "" 
                                        for i in range(current_group[0], current_group[-1] + 1)]
                                
                                range_name = f"{sheet_name}!{start_col_letter}{sheet_row_index}:{end_col_letter}{sheet_row_index}"
                                
                                batch_data["data"].append({
                                    "range": range_name,
                                    "values": [values]
                                })
                                
                                # Start new group
                                current_group = [idx]
                            
                            last_col = idx
                        
                        # Add the last group
                        if current_group:
                            start_col_letter = col_num_to_letter(current_group[0])
                            end_col_letter = col_num_to_letter(current_group[-1])
                            
                            values = [columns_to_update[i] if i in columns_to_update else "" 
                                    for i in range(current_group[0], current_group[-1] + 1)]
                            
                            range_name = f"{sheet_name}!{start_col_letter}{sheet_row_index}:{end_col_letter}{sheet_row_index}"
                            
                            batch_data["data"].append({
                                "range": range_name,
                                "values": [values]
                            })
                else:
                    # Failed to get data - just update status column with error message
                    error_message = property_data.get("message", "Error")
                    status_range = f"{sheet_name}!{status_col_letter}{sheet_row_index}"
                    batch_data["data"].append({
                        "range": status_range,
                        "values": [[error_message]]
                    })
                
            # Execute the batch update if there's data
            if batch_data["data"]:
                logger.info(f"Executing batch update for {len(batch_data['data'])} ranges")
                
                # Use exponential backoff for rate limits
                for retry in range(5):
                    try:
                        self.service.spreadsheets().values().batchUpdate(
                            spreadsheetId=self.settings.SPREADSHEET_ID,
                            body=batch_data
                        ).execute()
                        logger.info(f"Batch update successful")
                        time.sleep(2)  # Delay between batches
                        return
                    except (ConnectionAbortedError, ConnectionResetError) as e:
                        logger.warning(f"Connection aborted during batch update: {e}. Reducing batch size.")
                        # Split the batch and try with smaller chunks
                        if len(batch_data["data"]) > 10:
                            midpoint = len(batch_data["data"]) // 2
                            first_half = {"valueInputOption": "RAW", "data": batch_data["data"][:midpoint]}
                            second_half = {"valueInputOption": "RAW", "data": batch_data["data"][midpoint:]}
                            
                            logger.info(f"Retrying with split batch (1/{2})")
                            self.service.spreadsheets().values().batchUpdate(
                                spreadsheetId=self.settings.SPREADSHEET_ID,
                                body=first_half
                            ).execute()
                            
                            logger.info(f"Retrying with split batch (2/{2})")
                            self.service.spreadsheets().values().batchUpdate(
                                spreadsheetId=self.settings.SPREADSHEET_ID,
                                body=second_half
                            ).execute()
                    except HttpError as e:
                        if e.resp.status == 429:  # Rate limit error
                            wait_time = (2 ** retry) + random.random()
                            logger.warning(f"Rate limit exceeded. Waiting {wait_time:.2f}s before retry {retry+1}/5")
                            time.sleep(wait_time)
                            if retry == 4:  # Last retry failed
                                raise
                        else:
                            raise
            
        except Exception as e:
            logger.error(f"Error performing batch update for property data: {e}")
            raise