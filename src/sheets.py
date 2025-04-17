import logging
import time
from typing import List, Dict, Any, Optional
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
logger = logging.getLogger("baltimore_water")
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

# Setup logging
logger = logging.getLogger("baltimore_water")

class SheetsManager:
    """Manager for Google Sheets operations."""
    
    def __init__(self, settings: Optional[Settings] = None):
        """Initialize the sheets manager with settings."""
        self.settings = settings or Settings()
        
        # Set service account file from imported constant if not in settings
        if not self.settings.SERVICE_ACCOUNT_FILE and SERVICE_ACCOUNT_FILE:
            self.settings.SERVICE_ACCOUNT_FILE = SERVICE_ACCOUNT_FILE
            
        # Set impersonated user from imported constant if not in settings
        if not self.settings.IMPERSONATED_USER and IMPERSONATED_USER:
            self.settings.IMPERSONATED_USER = IMPERSONATED_USER
            
        self.service = self._get_sheets_service()

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
            
            # Execute the batch update
            if batch_data["data"]:
                logger.info(f"Executing batch update for {len(batch_data['data'])} rows")
                self.service.spreadsheets().values().batchUpdate(
                    spreadsheetId=self.settings.SPREADSHEET_ID,
                    body=batch_data
                ).execute()
            
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
            # Prepare the update request
            range_name = f"{sheet_name}!A1:{chr(65 + len(self.settings.SHEET_HEADERS) - 1)}1"
            
            body = {
                "values": [self.settings.SHEET_HEADERS]
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
        Get addresses from the spreadsheet.
        
        Args:
            sheet_name: Name of the sheet to read from
            
        Returns:
            List of addresses
        """
        try:
            # Prepare the range
            range_name = f"{sheet_name}!A2:A"
            
            # Get the values
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.settings.SPREADSHEET_ID,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            
            # Extract addresses
            addresses = []
            for row in values:
                if row and row[0].strip():
                    addresses.append(row[0].strip())
            
            logger.info(f"Retrieved {len(addresses)} addresses from {sheet_name}")
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
    
    def update_status(
        self, 
        row_index: int, 
        status: str,
        sheet_name: str = "Water Bill"
    ) -> None:
        """
        Update the status column for a row.
        
        Args:
            row_index: 0-based index of the row to update
            status: Status message
            sheet_name: Name of the sheet to update
        """
        try:
            # Adjust row index (sheets API is 1-based and we need to account for header row)
            sheet_row_index = row_index + 2  # +1 for 1-based, +1 for header row
            
            # Define the range to update (status column only)
            range_name = f"{sheet_name}!I{sheet_row_index}"
            
            body = {
                "values": [[status]]
            }
            
            # Update the sheet
            self.service.spreadsheets().values().update(
                spreadsheetId=self.settings.SPREADSHEET_ID,
                range=range_name,
                valueInputOption="RAW",
                body=body
            ).execute()
            
            logger.info(f"Updated status for row {sheet_row_index} to: {status}")
            
        except Exception as e:
            logger.error(f"Error updating status: {e}")
            # We don't want to raise here to allow processing to continue for other rows
            return