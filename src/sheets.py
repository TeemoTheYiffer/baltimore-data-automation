import logging
import time
from typing import List, Dict, Any, Optional
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import random
import os
from config import CountyConfig, AppConfig

logger = logging.getLogger("sheets")

try:
    # If that fails, try importing from secrets.google_credentials
    from app_secrets.google_credentials import SERVICE_ACCOUNT_FILE, IMPERSONATED_USER

    logger.info("Successfully imported credentials from secrets.google_credentials")
except ImportError:
    raise ImportError(
        "Failed to import Google credentials. Please ensure the file exists and is correctly referenced."
    )


class SheetsManager:
    """Manager for Google Sheets operations."""

    def __init__(self, config: Optional[AppConfig] = None, tcp_manager=None, county_name: Optional[str] = None):
        """Initialize the sheets manager with settings."""

        self.config = config or AppConfig()
        self.tcp_manager = tcp_manager

        # Determine the appropriate spreadsheet ID based on county or current configuration
        self.county = county_name or self.config._current_county
        county_config = self.config.get_county_config(self.county)
        
        # Store the spreadsheet_id as an instance variable
        self.spreadsheet_id = county_config.spreadsheet_id
        
        logger.info(f"SheetsManager initialized for county '{self.county}' with spreadsheet ID: {self.spreadsheet_id}")

        # Set service account file from imported constant if not in settings
        if not self.config.SERVICE_ACCOUNT_FILE and SERVICE_ACCOUNT_FILE:
            self.config.SERVICE_ACCOUNT_FILE = SERVICE_ACCOUNT_FILE

        # Set impersonated user from imported constant if not in settings
        if not self.config.IMPERSONATED_USER and IMPERSONATED_USER:
            self.config.IMPERSONATED_USER = IMPERSONATED_USER

        self.service = self._get_sheets_service()

        # Cache for sheet headers
        self._headers_cache = {}

    def _find_column_index(self, headers, target_column):
        """Find column index with case-insensitive matching."""
        target_lower = target_column.lower()
        for i, header in enumerate(headers):
            if header.lower() == target_lower:
                return i
        return -1

    def _get_header_map(self, headers):
        """Create a case-insensitive map of header names to column indices."""
        header_map = {}
        for i, header in enumerate(headers):
            if header:  # Skip empty headers
                header_map[header.lower()] = i
        return header_map

    def _get_sheet_headers(
        self, sheet_name: str, county_config: Optional[CountyConfig] = None
    ) -> List[str]:
        """Get headers from sheet with caching to avoid quota issues."""
        # Create a cache key that includes both sheet name and spreadsheet ID
        spreadsheet_id = (
            county_config.spreadsheet_id
            if county_config
            else self.config.SPREADSHEET_ID
        )
        cache_key = f"{spreadsheet_id}:{sheet_name}"

        # Check if headers are already cached
        if cache_key in self._headers_cache:
            return self._headers_cache[cache_key]

        # Fetch headers with exponential backoff for rate limits
        for retry in range(5):
            try:
                header_range = f"{sheet_name}!1:1"
                header_response = (
                    self.service.spreadsheets()
                    .values()
                    .get(spreadsheetId=self.spreadsheet_id, range=header_range)
                    .execute()
                )

                headers = header_response.get("values", [[]])[0]
                logger.info(
                    f"Fetched and cached {len(headers)} headers for sheet: {sheet_name} in spreadsheet: {spreadsheet_id}"
                )

                # Cache the headers
                self._headers_cache[cache_key] = headers
                return headers

            except HttpError as e:
                if e.resp.status == 429:  # Rate limit error
                    wait_time = (2**retry) + random.random()
                    logger.warning(
                        f"Rate limit on header fetch. Waiting {wait_time:.2f}s before retry {retry + 1}/5"
                    )
                    time.sleep(wait_time)
                    if retry == 4:  # Last retry failed
                        raise
                else:
                    raise

        raise RuntimeError("Failed to fetch sheet headers after multiple retries")

    def update_county(self, county_name: str, config: Optional[AppConfig] = None):
        """Update the county and spreadsheet ID."""
        config_to_use = config or self.config
        self.county = county_name
        county_config = config_to_use.get_county_config(county_name)
        self.spreadsheet_id = county_config.spreadsheet_id
        logger.info(f"SheetsManager updated to use county '{self.county}' with spreadsheet ID: {self.spreadsheet_id}")

    def get_property_identifiers(
        self, config, sheet_name: str, county_config: CountyConfig
    ) -> List[tuple]:
        """
        Get property identifiers (address or parcel ID) based on county configuration.

        Args:
            sheet_name: Name of the sheet to read from
            county_config: County-specific configuration

        Returns:
            List of (row_index, identifier) tuples
        """
        try:
            # Fetch headers to find identifier and Status columns
            headers = self._get_sheet_headers(sheet_name, county_config)
            logger.info(f"Found headers: {headers}")

            # Create case-insensitive header map
            header_map = self._get_header_map(headers)

            # Find identifier and Status columns using case-insensitive matching
            identifier_col_index = header_map.get(
                county_config.identifier_column.lower(), -1
            )
            status_col_index = header_map.get("status", -1)

            # If identifier column not found, raise an exception
            if identifier_col_index == -1:
                raise ValueError(
                    f"{county_config.identifier_column} column not found in headers"
                )

            # If status column not found, raise an exception
            if status_col_index == -1:
                raise ValueError("Status column not found in headers")

            # Determine range based on PROPERTY settings
            start_row = config.START_ROW
            if config.STOP_ROW > 0:
                end_row = config.STOP_ROW
            else:
                end_row = start_row + config.MAX_ROWS - 1

            logger.info(f"Fetching property data from row {start_row} to {end_row}")

            # Get the status values
            status_col_letter = self.col_num_to_letter(status_col_index)
            status_range = f"{sheet_name}!{status_col_letter}{start_row}:{status_col_letter}{end_row}"
            status_result = (
                self.service.spreadsheets()
                .values()
                .get(spreadsheetId=config.SPREADSHEET_ID, range=status_range)
                .execute()
            )

            status_values = status_result.get("values", [])
            logger.info(f"Retrieved {len(status_values)} status values")

            # Parse skip ranges from PROPERTY settings
            rows_to_skip = set()
            if config.SKIP_ROW_RANGE:
                for part in config.SKIP_ROW_RANGE.split(","):
                    if "-" in part:
                        start_skip, end_skip = map(int, part.split("-"))
                        for row in range(start_skip, end_skip + 1):
                            rows_to_skip.add(row)
                    else:
                        try:
                            rows_to_skip.add(int(part.strip()))
                        except ValueError:
                            continue

            # If status_values is empty, it means all cells in that range are empty
            # Therefore, all rows need processing
            if not status_values:
                logger.info(
                    "No status values found in range - treating all rows as needing processing"
                )
                rows_to_process = [
                    row
                    for row in range(start_row, end_row + 1)
                    if row not in rows_to_skip
                ]
            else:
                # Find which rows need processing
                rows_to_process = []
                for i, row_status in enumerate(status_values):
                    actual_row = start_row + i

                    # Skip rows in the skip list
                    if actual_row in rows_to_skip:
                        logger.info(
                            f"Skipping row {actual_row} as specified in SKIP_ROW_RANGE"
                        )
                        continue

                    status_raw = (
                        row_status[0] if row_status and len(row_status) > 0 else ""
                    )
                    status = status_raw.strip() if status_raw else ""

                    # Check status if available
                    skip_row = False

                    # Skip rows with "Success" status
                    if status.lower() in ["success", "skipped"]:
                        skip_row = True
                        logger.info(f"Skipping row {actual_row} with status: {status}")

                    # Skip rows with failed lookups if not retrying failed rows
                    no_data_msg = f"no data found for {county_config.identifier_type}"
                    if no_data_msg in status.lower() and not config.RETRY_FAILED_ROWS:
                        skip_row = True
                        logger.info(
                            f"Skipping row {actual_row} with status containing '{no_data_msg}' (RETRY_FAILED_ROWS is False)"
                        )

                    if not skip_row:
                        rows_to_process.append(actual_row)

            logger.info(f"Found {len(rows_to_process)} rows that need processing")

            # Now only fetch the identifiers for rows that need processing
            identifier_col_letter = self.col_num_to_letter(identifier_col_index)
            identifier_data = []

            # Process in smaller batches to avoid large API calls
            batch_size = 100
            for i in range(0, len(rows_to_process), batch_size):
                batch = rows_to_process[i : i + batch_size]
                batch_ranges = []
                for row in batch:
                    batch_ranges.append(f"{sheet_name}!{identifier_col_letter}{row}")

                if not batch_ranges:
                    continue

                # Get identifiers for this batch
                result = (
                    self.service.spreadsheets()
                    .values()
                    .batchGet(spreadsheetId=config.SPREADSHEET_ID, ranges=batch_ranges)
                    .execute()
                )

                value_ranges = result.get("valueRanges", [])
                for j, value_range in enumerate(value_ranges):
                    row_values = value_range.get("values", [[]])[0]
                    if not row_values:
                        continue

                    identifier = row_values[0].strip() if row_values[0] else ""
                    if identifier:
                        # Return absolute row index adjusted to be 0-indexed
                        row_index = (
                            rows_to_process[i + j] - 2
                        )  # -2 to convert from 1-indexed sheet to 0-indexed program
                        identifier_data.append((row_index, identifier))

            logger.info(
                f"Retrieved {len(identifier_data)} identifiers from {sheet_name}"
            )
            return identifier_data

        except Exception as e:
            logger.error(f"Error getting property identifiers: {e}")
            raise

    def batch_update_bill_details(self, updates, sheet_name="Water Bill"):
        """
        Perform a batch update of multiple rows with water bill details.

        Args:
            updates: List of tuples (row_index, bill_data)
            sheet_name: Name of the sheet to update
        """
        try:
            # Use batch size from config, or default if not available
            batch_size = (
                self.config.BATCH_SIZE if hasattr(self.config, "BATCH_SIZE") else 100
            )

            # If updates list is too large, recursively split it
            if len(updates) > batch_size:
                logger.info(
                    f"Batch size {len(updates)} exceeds max size {batch_size}, splitting into smaller batches"
                )
                midpoint = len(updates) // 2
                first_half = updates[:midpoint]
                second_half = updates[midpoint:]

                # Process first half
                logger.info(f"Processing first half batch ({len(first_half)} items)")
                try:
                    self.batch_update_bill_details(first_half, sheet_name)
                except Exception as e:
                    logger.error(f"Error processing first half batch: {e}")
                    # If the batch is still large, reduce further
                    if len(first_half) > 10:
                        logger.info("Further reducing first half batch size")
                        for i in range(0, len(first_half), 10):
                            mini_batch = first_half[i : i + 10]
                            try:
                                logger.info(
                                    f"Processing mini-batch of {len(mini_batch)} items"
                                )
                                self.batch_update_bill_details(mini_batch, sheet_name)
                                time.sleep(2)
                            except Exception as mini_e:
                                logger.error(f"Error in mini-batch: {mini_e}")
                    else:
                        # If batch is already small, re-raise exception
                        raise

                # Add delay between halves
                time.sleep(5)

                # Process second half
                logger.info(f"Processing second half batch ({len(second_half)} items)")
                try:
                    self.batch_update_bill_details(second_half, sheet_name)
                except Exception as e:
                    logger.error(f"Error processing second half batch: {e}")
                    # If the batch is still large, reduce further
                    if len(second_half) > 10:
                        logger.info("Further reducing second half batch size")
                        for i in range(0, len(second_half), 10):
                            mini_batch = second_half[i : i + 10]
                            try:
                                logger.info(
                                    f"Processing mini-batch of {len(mini_batch)} items"
                                )
                                self.batch_update_bill_details(mini_batch, sheet_name)
                                time.sleep(2)
                            except Exception as mini_e:
                                logger.error(f"Error in mini-batch: {mini_e}")
                    else:
                        # If batch is already small, re-raise exception
                        raise

                return

            # Prepare batch request data
            batch_data = {"valueInputOption": "RAW", "data": []}

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
                        data.get("previous_balance", ""),
                        data.get("current_balance", ""),
                        data.get("penalty_date", ""),
                        data.get("last_payment_date", ""),
                        data.get("last_payment_amount", ""),
                        "Success",
                    ]
                else:
                    # Something went wrong, update the status column
                    values = ["" for _ in range(8)]  # Empty cells for columns B-H
                    values.append(bill_data.get("message", "Error"))

                    # If we have an account number despite the error, include it
                    if bill_data.get("account_number"):
                        values[0] = bill_data["account_number"]

                # Add this update to the batch
                range_name = f"{sheet_name}!B{sheet_row_index}:J{sheet_row_index}"
                batch_data["data"].append({"range": range_name, "values": [values]})

            # Execute the batch update with exponential backoff for rate limits and connection issues
            if batch_data["data"]:
                logger.info(
                    f"Executing batch update for {len(batch_data['data'])} rows"
                )

                max_retries = 5  # Maximum retry attempts

                for retry in range(max_retries):
                    try:
                        # Use exponential backoff with jitter
                        if retry > 0:
                            wait_time = min(60, (2**retry) + random.random())
                            logger.info(
                                f"Retry {retry + 1}/{max_retries}: Waiting {wait_time:.2f} seconds before retry"
                            )
                            time.sleep(wait_time)

                        if self.tcp_manager:
                            try:
                                # Define the batch update function
                                def execute_batch():
                                    return (
                                        self.service.spreadsheets()
                                        .values()
                                        .batchUpdate(
                                            spreadsheetId=self.spreadsheet_id,
                                            body=batch_data,
                                        )
                                        .execute()
                                    )

                                # Execute with optimized retry logic
                                return self.tcp_manager.execute_batch_with_retry(
                                    self.service, execute_batch
                                )
                            except Exception as e:
                                logger.error(
                                    f"Failed to execute batch update after multiple retries: {e}"
                                )
                                raise
                        else:
                            self.service.spreadsheets().values().batchUpdate(
                                spreadsheetId=self.spreadsheet_id,
                                body=batch_data,
                            ).execute()

                        logger.info(
                            f"Batch update successful for {len(batch_data['data'])} rows"
                        )
                        time.sleep(2)  # Delay after successful batch
                        return
                    except (
                        ConnectionAbortedError,
                        ConnectionResetError,
                        ConnectionError,
                    ) as e:
                        logger.warning(
                            f"Connection error during batch update (retry {retry + 1}/{max_retries}): {e}"
                        )

                        if retry == max_retries - 1:  # Last retry failed
                            logger.error(
                                "Maximum retries reached for batch update. Raising exception."
                            )
                            raise

                        # Reduce request size on connection errors by slicing the batch data
                        if len(batch_data["data"]) > 10:
                            logger.info(
                                f"Reducing batch size from {len(batch_data['data'])} to 10 for next retry"
                            )
                            # Only keep the first items for the next retry
                            batch_data["data"] = batch_data["data"][:10]
                    except HttpError as e:
                        if e.resp.status == 429:  # Rate limit error
                            wait_time = min(
                                60, (2**retry) + random.random()
                            )  # Exponential backoff with jitter
                            logger.warning(
                                f"Rate limit exceeded. Waiting {wait_time:.2f} seconds before retry {retry + 1}/{max_retries}"
                            )
                            time.sleep(wait_time)
                            if retry == max_retries - 1:  # Last retry failed
                                logger.error(
                                    "Maximum retries reached for batch update. Raising exception."
                                )
                                raise
                        else:
                            logger.error(f"HTTP error during batch update: {e}")
                            raise
                    except Exception as e:
                        logger.error(f"Unexpected error during batch update: {e}")
                        raise

        except Exception as e:
            logger.error(f"Error performing batch update: {e}")
            raise  # Re-raise the exception for caller to handle

    def get_all_sheet_names(self) -> List[str]:
        """
        Get all sheet names from the spreadsheet.

        Returns:
            List of sheet names
        """
        try:
            # Get the spreadsheet
            spreadsheet = (
                self.service.spreadsheets()
                .get(spreadsheetId=self.spreadsheet_id)
                .execute()
            )

            # Extract sheet names
            sheets = spreadsheet.get("sheets", [])
            sheet_names = [
                sheet.get("properties", {}).get("title", "") for sheet in sheets
            ]

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
        if not self.config.SERVICE_ACCOUNT_FILE:
            raise ValueError("No service account file specified")

        if not self.config.IMPERSONATED_USER:
            raise ValueError("No impersonated user specified")

        logger.info(f"Impersonating user: {self.config.IMPERSONATED_USER}")
        logger.info(f"Using service account file: {self.config.SERVICE_ACCOUNT_FILE}")

        # Define scopes needed for the application
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        script_dir = os.path.dirname(os.path.abspath(__file__))
        secrets_path = os.path.normpath(
            os.path.join(script_dir, self.config.SERVICE_ACCOUNT_FILE)
        )
        # Get Google API credentials with domain-wide delegation
        creds = Credentials.from_service_account_file(secrets_path, scopes=scopes)

        # Create delegated credentials
        delegated_creds = creds.with_subject(self.config.IMPERSONATED_USER)
        return delegated_creds

    def _get_sheets_service(self):
        """
        Get Google Sheets API service with retry logic.

        Returns:
            Google Sheets API service
        """
        retry_count = 0

        while retry_count < self.config.MAX_RETRIES:
            try:
                # Get credentials
                creds = self._get_credentials()

                # Create service
                logger.info("Building Sheets API service")
                service = build(
                    "sheets", "v4", credentials=creds, cache_discovery=False
                )

                logger.info("Successfully built Sheets API service")
                return service

            except Exception as e:
                retry_count += 1
                wait_time = 2**retry_count  # Exponential backoff

                logger.error(
                    f"Error building Sheets service (attempt {retry_count}/{self.config.MAX_RETRIES}): {e}"
                )

                if retry_count < self.config.MAX_RETRIES:
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.critical(
                        f"Failed to build Sheets service after {self.config.MAX_RETRIES} attempts"
                    )
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
            if sheet_name == self.config.PROPERTY_SHEET_NAME:
                # Use property headers
                headers = list(self.config.FIELD_MAPPING.keys())
                logger.info(f"Using property headers for {sheet_name}")
            else:
                # Use water bill headers
                headers = self.config.SHEET_HEADERS
                logger.info(f"Using water bill headers for {sheet_name}")

            # Calculate the ending column letter correctly
            end_col = self.col_num_to_letter(len(headers) - 1)

            # Prepare the update request
            range_name = f"{sheet_name}!A1:{end_col}1"

            body = {"values": [headers]}

            # Update the sheet
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                body=body,
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
            start_row = self.config.START_ROW  # Already 1-indexed in config

            # Safety check: if start_row is 1, adjust it to 2 to protect headers
            if start_row == 1:
                logger.info("Adjusting START_ROW from 1 to 2 to protect headers")
                start_row = 2

            # Calculate the end row based on settings
            if self.config.STOP_ROW > 0:
                end_row = self.config.STOP_ROW
            else:
                end_row = start_row + self.config.MAX_ROWS - 1

            logger.info(f"Fetching addresses from row {start_row} to {end_row}")

            # Get progress information first to avoid fetching all rows
            status_range = f"{sheet_name}!I{start_row}:I{end_row}"
            status_result = (
                self.service.spreadsheets()
                .values()
                .get(spreadsheetId=self.spreadsheet_id, range=status_range)
                .execute()
            )

            status_values = status_result.get("values", [])
            logger.info(f"Retrieved {len(status_values)} status values")

            # If status_values is empty, it means all cells in that range are empty
            # Therefore, all rows need processing
            if not status_values:
                logger.info(
                    "No status values found in range - treating all rows as needing processing"
                )
                rows_to_process = [row for row in range(start_row, end_row + 1)]
            else:
                # Here's the key change - we need to process ALL rows in the range
                rows_to_process = []

                # Create a map of row numbers to status values
                status_map = {}
                for i, row in enumerate(status_values):
                    row_num = start_row + i
                    status = row[0].strip() if row and len(row) > 0 else ""
                    status_map[row_num] = status

                # Now check ALL rows in the range
                for row_num in range(start_row, end_row + 1):
                    status = status_map.get(
                        row_num, ""
                    )  # Default to empty string if no status
                    if status != "Success" and status != "Skipped":
                        rows_to_process.append(row_num)

            logger.info(f"Found {len(rows_to_process)} rows that need processing")

            # Now only fetch the address data for rows that need processing
            addresses = []

            # Process in smaller batches to avoid large API calls
            batch_size = 100
            for i in range(0, len(rows_to_process), batch_size):
                batch = rows_to_process[i : i + batch_size]
                batch_ranges = []
                for row in batch:
                    batch_ranges.append(f"{sheet_name}!A{row}")

                if not batch_ranges:
                    continue

                # Get addresses for this batch
                address_result = (
                    self.service.spreadsheets()
                    .values()
                    .batchGet(
                        spreadsheetId=self.spreadsheet_id, ranges=batch_ranges
                    )
                    .execute()
                )

                value_ranges = address_result.get("valueRanges", [])
                for j, value_range in enumerate(value_ranges):
                    row_values = value_range.get("values", [[]])[0]
                    row_index = (
                        rows_to_process[i + j] - 2
                    )  # Adjust to be 0-based with correct offset for update

                    if row_values and row_values[0].strip():
                        addresses.append((row_index, row_values[0].strip()))

            # Check for rows to skip
            rows_to_skip = set()
            if self.config.SKIP_ROW_RANGE:
                # Parse the skip range string (format: "5,8,10-15,20-25")
                for part in self.config.SKIP_ROW_RANGE.split(","):
                    if "-" in part:
                        # Handle range (e.g., "10-15")
                        start, end = map(int, part.split("-"))
                        for row in range(start, end + 1):
                            rows_to_skip.add(row)
                    else:
                        # Handle single row (e.g., "5")
                        try:
                            rows_to_skip.add(int(part))
                        except ValueError:
                            # Skip invalid entries
                            continue

            # Filter out rows that should be skipped
            addresses = [
                (idx, addr)
                for idx, addr in addresses
                if (idx + start_row) not in rows_to_skip
            ]

            # Log how many addresses we're processing
            logger.info(
                f"Retrieved {len(addresses)} addresses to process from {sheet_name}"
            )
            return addresses

        except Exception as e:
            logger.error(f"Error getting addresses: {e}")
            raise

    def update_row_with_bill_details(
        self, row_index: int, bill_data: Dict[str, Any], sheet_name: str = "Water Bill"
    ) -> None:
        """
        Update a row with water bill details using dynamic header mapping.

        Args:
            row_index: 0-based index of the row to update
            bill_data: Dictionary with bill details
            sheet_name: Name of the sheet to update
        """
        try:
            # Adjust row index (sheets API is 1-based and we need to account for header row)
            sheet_row_index = row_index + 2  # +1 for 1-based, +1 for header row

            # Get headers to map column names to indices
            headers = self._get_sheet_headers(sheet_name)

            # Define field mapping between data fields and sheet headers
            field_mapping = {
                "account_number": "Account Number",
                "bill_date": "Bill Date",
                "current_bill_amount": "Bill Amount",
                "previous_balance": "Previous Balance",
                "current_balance": "Current Balance",
                "penalty_date": "Penalty Date",
                "last_payment_date": "Last Payment Date",
                "last_payment_amount": "Last Payment Amount",
            }

            # Prepare values based on mapping
            if bill_data.get("success", False) and "data" in bill_data:
                data = bill_data["data"]

                # Map data to appropriate columns
                columns_to_update = {}

                # Add data fields
                for field, header in field_mapping.items():
                    try:
                        col_index = headers.index(header)
                        columns_to_update[col_index] = data.get(field, "")
                    except ValueError:
                        logger.warning(
                            f"Field '{header}' not found in headers, skipping"
                        )

                # Add status field
                try:
                    status_col_index = headers.index("Status")
                    columns_to_update[status_col_index] = "Success"
                except ValueError:
                    logger.warning("Status column not found in headers")

                # Build the range and values for update
                col_indices = sorted(columns_to_update.keys())
                if not col_indices:
                    logger.warning(
                        f"No valid columns to update for row {sheet_row_index}"
                    )
                    return

                start_col = col_indices[0]
                end_col = col_indices[-1]

                # Convert to column letters
                start_col_letter = self.col_num_to_letter(start_col)
                end_col_letter = self.col_num_to_letter(end_col)

                # Build values list with correct ordering
                values = ["" for _ in range(end_col - start_col + 1)]
                for idx in col_indices:
                    values[idx - start_col] = columns_to_update[idx]

                # Define the range to update
                range_name = f"{sheet_name}!{start_col_letter}{sheet_row_index}:{end_col_letter}{sheet_row_index}"

                body = {"values": [values]}

                # Update the sheet
                self.service.spreadsheets().values().update(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name,
                    valueInputOption="RAW",
                    body=body,
                ).execute()

                logger.info(f"Updated row {sheet_row_index} with bill details")
            else:
                # Something went wrong, update only the status column
                try:
                    status_col_index = headers.index("Status")
                    status_col_letter = self.col_num_to_letter(status_col_index)

                    range_name = f"{sheet_name}!{status_col_letter}{sheet_row_index}"

                    body = {"values": [[bill_data.get("message", "Error")]]}

                    self.service.spreadsheets().values().update(
                        spreadsheetId=self.spreadsheet_id,
                        range=range_name,
                        valueInputOption="RAW",
                        body=body,
                    ).execute()

                    logger.info(f"Updated status for row {sheet_row_index}")
                except ValueError:
                    logger.warning("Status column not found in headers")

        except Exception as e:
            logger.error(f"Error updating row with bill details: {e}")
            return

    def update_row_with_property_data(
        self, row_index: int, property_data: Dict[str, Any], sheet_name: str = "LIENS"
    ) -> None:
        """
        Update a single row with property data.

        Args:
            row_index: 0-based index of the row to update
            property_data: Dictionary with property data result
            sheet_name: Name of the sheet to update
        """
        try:
            # Ensure we're never updating the header row
            sheet_row_index = row_index + 2  # +1 for 1-based, +1 for header row
            if sheet_row_index <= 1:
                logger.warning(
                    f"Skipping update for row {sheet_row_index} to protect headers"
                )
                return

            # Get sheet headers to map column names to indices
            try:
                headers = self._get_sheet_headers(sheet_name)
            except Exception as e:
                logger.error(f"Error fetching headers: {e}")
                # Wait and try again with longer delay if rate limited
                if "RATE_LIMIT_EXCEEDED" in str(e):
                    logger.warning(
                        "Rate limit exceeded, waiting 60 seconds before retry"
                    )
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
                        logger.warning(
                            f"Field '{field}' not found in headers, skipping"
                        )

                if not columns_to_update:
                    logger.warning(
                        f"No valid fields to update for row {sheet_row_index}"
                    )
                    return

                # Update in batches of related columns to minimize API calls
                # Sort columns by index
                col_indices = sorted(columns_to_update.keys())

                # Find consecutive runs of columns to update together
                current_run = []
                all_runs = []

                for i, idx in enumerate(col_indices):
                    if i == 0 or idx != col_indices[i - 1] + 1:
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
                    start_col_letter = self.col_num_to_letter(start_col)
                    end_col_letter = self.col_num_to_letter(end_col)

                    # Prepare range and values
                    if start_col == end_col:
                        range_name = f"{sheet_name}!{start_col_letter}{sheet_row_index}"
                        values = [[columns_to_update[start_col]]]
                    else:
                        range_name = self.format_range(
                            sheet_name,
                            f"{start_col_letter}:{end_col_letter}",
                            sheet_row_index,
                        )
                        values = [
                            [
                                columns_to_update[i]
                                for i in range(start_col, end_col + 1)
                            ]
                        ]

                    logger.info(f"Updating range: {range_name}")

                    # Try the update with retry logic
                    for retry in range(5):
                        try:
                            self.service.spreadsheets().values().update(
                                spreadsheetId=self.spreadsheet_id,
                                range=range_name,
                                valueInputOption="RAW",
                                body={"values": values},
                            ).execute()
                            break
                        except HttpError as api_error:
                            if api_error.resp.status == 429:  # Rate limit error
                                wait_time = (2**retry) + random.random()
                                logger.warning(
                                    f"Rate limit, waiting {wait_time:.2f}s before retry {retry + 1}/5"
                                )
                                time.sleep(wait_time)
                                if retry == 4:
                                    raise
                            else:
                                raise

                logger.info(
                    f"Successfully updated row {sheet_row_index} with property data"
                )
            else:
                logger.warning(f"No successful property data for row {sheet_row_index}")

        except Exception as e:
            logger.error(f"Error updating row {row_index + 2} with property data: {e}")

    def col_num_to_letter(self, n):
        """Convert 0-based column index to Excel-style column letter(s)."""
        result = ""
        while n >= 0:
            remainder = n % 26
            result = chr(65 + remainder) + result
            n = n // 26 - 1
        return result

    def format_range(self, sheet_name, column_ref, row_index):
        """Format a range string, ensuring no duplicate sheet names."""
        # If column_ref already contains sheet name, extract just the column part
        if "!" in column_ref:
            column_ref = column_ref.split("!", 1)[1]

        return f"{sheet_name}!{column_ref}{row_index}"

    def batch_update_property_data(self, updates, sheet_name="LIENS"):
        """Perform a batch update of multiple rows with property data."""
        try:
            # Use batch size from property settings
            batch_size = self.config.BATCH_SIZE

            updates = [
                (idx, data) for idx, data in updates if idx + 2 > 1
            ]  # +2 accounts for 0-indexed to 1-indexed and header row
            if len(updates) == 0:
                logger.warning(
                    "All updates filtered out to protect headers, nothing to do"
                )
                return

            # If updates list is too large, recursively split it
            if len(updates) > batch_size:
                logger.info(
                    f"Property batch size {len(updates)} exceeds configured size {batch_size}, splitting into smaller batches"
                )
                midpoint = len(updates) // 2
                first_half = updates[:midpoint]
                second_half = updates[midpoint:]

                # Process first half
                logger.info(
                    f"Processing first half property batch ({len(first_half)} items)"
                )
                try:
                    self.batch_update_property_data(first_half, sheet_name)
                except Exception as e:
                    logger.error(f"Error processing first half property batch: {e}")
                    # If the batch is still large, reduce further
                    if len(first_half) > 10:
                        logger.info("Further reducing first half property batch size")
                        for i in range(0, len(first_half), 10):
                            mini_batch = first_half[i : i + 10]
                            try:
                                logger.info(
                                    f"Processing property mini-batch of {len(mini_batch)} items"
                                )
                                self.batch_update_property_data(mini_batch, sheet_name)
                                time.sleep(3)  # Add delay between mini-batches
                            except Exception as mini_e:
                                logger.error(f"Error in property mini-batch: {mini_e}")
                    else:
                        # If batch is already small, re-raise exception
                        raise

                # Add delay between halves
                time.sleep(10)  # Longer delay for property data

                # Process second half
                logger.info(
                    f"Processing second half property batch ({len(second_half)} items)"
                )
                try:
                    self.batch_update_property_data(second_half, sheet_name)
                except Exception as e:
                    logger.error(f"Error processing second half property batch: {e}")
                    # If the batch is still large, reduce further
                    if len(second_half) > 10:
                        logger.info("Further reducing second half property batch size")
                        for i in range(0, len(second_half), 10):
                            mini_batch = second_half[i : i + 10]
                            try:
                                logger.info(
                                    f"Processing property mini-batch of {len(mini_batch)} items"
                                )
                                self.batch_update_property_data(mini_batch, sheet_name)
                                time.sleep(3)  # Add delay between mini-batches
                            except Exception as mini_e:
                                logger.error(f"Error in property mini-batch: {mini_e}")
                    else:
                        # If batch is already small, re-raise exception
                        raise
                return

            # Get headers using the caching method
            headers = self._get_sheet_headers(sheet_name)

            # Create case-insensitive header map
            header_map = self._get_header_map(headers)

            # Find Status column index
            status_col_index = header_map.get("status", -1)

            # If no status column found, raise exception
            if status_col_index == -1:
                raise ValueError("Status column not found in headers")

            status_col_letter = self.col_num_to_letter(status_col_index)

            # Prepare batch updates
            batch_data = {"valueInputOption": "RAW", "data": []}

            # Process each row update (this should already be in the loop)
            for row_index, property_data in updates:
                # Ensure we're never updating row 1 (headers)
                sheet_row_index = (
                    row_index + 2
                )  # +1 for 1-based indexing, +1 to skip header row

                # Now use sheet_row_index to create the status range for this specific row
                status_range = self.format_range(
                    sheet_name, status_col_letter, sheet_row_index
                )

                # Add the update to the batch
                batch_data["data"].append(
                    {"range": status_range, "values": [["Success"]]}
                )

                if property_data.get("success", False) and "data" in property_data:
                    # Successful data fetch - update data fields
                    data = property_data.get("data", {})

                    # Find columns to update - map field names to column indices
                    columns_to_update = {}

                    for field, value in data.items():
                        # Find column using case-insensitive match
                        col_index = header_map.get(field.lower(), -1)
                        if col_index >= 0:
                            columns_to_update[col_index] = value
                        else:
                            logger.warning(
                                f"Field '{field}' not found in headers, skipping"
                            )

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
                                start_col_letter = self.col_num_to_letter(
                                    current_group[0]
                                )
                                end_col_letter = self.col_num_to_letter(
                                    current_group[-1]
                                )

                                # Create range and values
                                values = [
                                    columns_to_update[i]
                                    if i in columns_to_update
                                    else ""
                                    for i in range(
                                        current_group[0], current_group[-1] + 1
                                    )
                                ]

                                range_name = self.format_range(
                                    sheet_name,
                                    f"{start_col_letter}:{end_col_letter}",
                                    sheet_row_index,
                                )

                                batch_data["data"].append(
                                    {"range": range_name, "values": [values]}
                                )

                                # Start new group
                                current_group = [idx]

                            last_col = idx

                        # Add the last group
                        if current_group:
                            start_col_letter = self.col_num_to_letter(current_group[0])
                            end_col_letter = self.col_num_to_letter(current_group[-1])

                            values = [
                                columns_to_update[i] if i in columns_to_update else ""
                                for i in range(current_group[0], current_group[-1] + 1)
                            ]

                            range_name = self.format_range(
                                sheet_name,
                                f"{start_col_letter}:{end_col_letter}",
                                sheet_row_index,
                            )

                            batch_data["data"].append(
                                {"range": range_name, "values": [values]}
                            )
                else:
                    # Failed to get data - create a data dictionary with "Status" field
                    error_message = property_data.get("message", "Error")
                    batch_data["data"].append(
                        {"range": status_range, "values": [[error_message]]}
                    )

            # Execute the batch update if there's data
            if batch_data["data"]:
                logger.info(
                    f"Executing property batch update for {len(batch_data['data'])} ranges"
                )

                max_retries = 5  # Maximum retry attempts

                # Use exponential backoff for rate limits and connection issues
                for retry in range(max_retries):
                    try:
                        # Use exponential backoff with jitter
                        if retry > 0:
                            wait_time = min(60, (2**retry) + random.random())
                            logger.info(
                                f"Property retry {retry + 1}/{max_retries}: Waiting {wait_time:.2f} seconds before retry"
                            )
                            time.sleep(wait_time)

                        if self.tcp_manager:
                            try:
                                # Define the batch update function
                                def execute_batch():
                                    return (
                                        self.service.spreadsheets()
                                        .values()
                                        .batchUpdate(
                                            spreadsheetId=self.spreadsheet_id,
                                            body=batch_data,
                                        )
                                        .execute()
                                    )

                                # Execute with optimized retry logic
                                return self.tcp_manager.execute_batch_with_retry(
                                    self.service, execute_batch
                                )
                            except Exception as e:
                                logger.error(
                                    f"Failed to execute batch update after multiple retries: {e}"
                                )
                                raise
                        else:
                            self.service.spreadsheets().values().batchUpdate(
                                spreadsheetId=self.spreadsheet_id,
                                body=batch_data,
                            ).execute()

                        logger.info("Property batch update successful")
                        time.sleep(3)  # Delay after successful batch
                        return
                    except (
                        ConnectionAbortedError,
                        ConnectionResetError,
                        ConnectionError,
                    ) as e:
                        logger.warning(
                            f"Connection error during property batch update (retry {retry + 1}/{max_retries}): {e}"
                        )

                        if retry == max_retries - 1:  # Last retry failed
                            logger.error(
                                "Maximum retries reached for property batch update."
                            )
                            raise

                        # Reduce request size on connection errors by slicing the batch data
                        if len(batch_data["data"]) > 10:
                            logger.info(
                                f"Reducing property batch size from {len(batch_data['data'])} to 10 for next retry"
                            )
                            # Only keep the first items for the next retry
                            batch_data["data"] = batch_data["data"][:10]
                    except HttpError as e:
                        if e.resp.status == 429:  # Rate limit error
                            wait_time = min(60, (2**retry) + random.random())
                            logger.warning(
                                f"Rate limit exceeded. Waiting {wait_time:.2f}s before property retry {retry + 1}/{max_retries}"
                            )
                            time.sleep(wait_time)
                            if retry == max_retries - 1:  # Last retry failed
                                logger.error(
                                    "Maximum retries reached for property batch update."
                                )
                                raise
                        else:
                            logger.error(
                                f"HTTP error during property batch update: {e}"
                            )
                            raise
                    except Exception as e:
                        logger.error(
                            f"Unexpected error during property batch update: {e}"
                        )
                        raise

        except Exception as e:
            logger.error(f"Error performing batch update for property data: {e}")
            raise
