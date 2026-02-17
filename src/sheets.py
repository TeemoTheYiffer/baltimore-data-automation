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
            else self.spreadsheet_id
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
    
    def _execute_batch_get_with_retry(self, batch_ranges: List[str], batch_name: str) -> Optional[Dict]:
        """Execute batchGet with retry for rate limits and connection errors."""
        max_retries = 4

        for attempt in range(max_retries):
            try:
                result = (
                    self.service.spreadsheets()
                    .values()
                    .batchGet(spreadsheetId=self.spreadsheet_id, ranges=batch_ranges)
                    .execute()
                )

                if attempt > 0:
                    logger.info(f"Successfully retrieved {batch_name} after {attempt + 1} attempts")

                return result

            except HttpError as e:
                if e.resp.status == 429:  # Rate limit error
                    if attempt == max_retries - 1:
                        raise
                    # Per-minute quota: wait 60+ seconds for the window to reset
                    wait_time = 65 + random.uniform(0, 10.0)
                    logger.warning(f"Rate limit hit on {batch_name}, waiting {wait_time:.0f}s for quota reset (retry {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    logger.error(f"HTTP error in {batch_name}: {e}")
                    raise
            except Exception as e:
                logger.error(f"Error in {batch_name}: {e}")
                raise

        return None

    def _get_sheet_dimensions(self, sheet_name: str) -> tuple[int, int]:
        """Get the actual dimensions of a sheet."""
        try:
            sheet_metadata = self.service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id,
                fields="sheets.properties"
            ).execute()
            
            for sheet in sheet_metadata.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    grid_props = sheet['properties']['gridProperties']
                    row_count = grid_props['rowCount']
                    col_count = grid_props['columnCount']
                    logger.info(f"Sheet '{sheet_name}' dimensions: {row_count} rows x {col_count} columns")
                    return row_count, col_count
                    
            logger.warning(f"Sheet '{sheet_name}' not found in spreadsheet")
            return 1000, 26  # Default fallback
            
        except Exception as e:
            logger.warning(f"Could not get sheet dimensions: {e}")
            return 1000, 26  # Default fallback

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
            List of (row_index, identifier, row_optional_params) tuples
            where row_optional_params is a dict like {"District": "01"}
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

            # Find optional parameter columns if specified
            optional_param_columns = {}  # Maps column_name -> column_index
            optional_param_mapping = {}  # Maps column_name -> field_mapping_key

            if county_config.optional_params:
                for column_name, field_mapping_key in county_config.optional_params.items():
                    col_index = header_map.get(column_name.lower(), -1)
                    if col_index == -1:
                        raise ValueError(f"Optional parameter column '{column_name}' not found in headers")
                    
                    optional_param_columns[column_name] = col_index
                    optional_param_mapping[column_name] = field_mapping_key
                    logger.info(f"Found optional parameter column '{column_name}' -> '{field_mapping_key}' at index {col_index}")

            logger.info(f"Optional parameter columns: {list(optional_param_columns.keys())}")

            # Determine range based on PROPERTY settings
            start_row = config.START_ROW
            if config.STOP_ROW > 0:
                end_row = config.STOP_ROW
            else:
                end_row = start_row + config.MAX_ROWS - 1

            max_sheet_rows, max_sheet_cols = self._get_sheet_dimensions(sheet_name)

            if end_row > max_sheet_rows:
                logger.warning(f"Requested end_row {end_row} exceeds sheet size {max_sheet_rows}. Adjusting to {max_sheet_rows}")
                end_row = max_sheet_rows

            logger.info(f"Fetching property data from row {start_row} to {end_row} (sheet has {max_sheet_rows} rows)")

            # Get the status values (with retry and chunking for large ranges)
            status_col_letter = self.col_num_to_letter(status_col_index)
            status_range = [f"{sheet_name}!{status_col_letter}{start_row}:{status_col_letter}{end_row}"]
            status_result = self._execute_batch_get_with_retry(status_range, "status column fetch")

            if status_result and status_result.get("valueRanges"):
                status_values = status_result["valueRanges"][0].get("values", [])
            else:
                status_values = []
            logger.info(f"Retrieved {len(status_values)} status values")

            # Parse skip ranges from PROPERTY settings
            rows_to_skip = set()

            # Create a status lookup dictionary for rows that have status values
            status_lookup = {}
            for i, row_status in enumerate(status_values):
                actual_row = start_row + i
                status_raw = row_status[0] if row_status and len(row_status) > 0 else ""
                status_lookup[actual_row] = status_raw.strip() if status_raw else ""

            logger.info(f"Status values found for {len(status_lookup)} rows")

            # Now check ALL rows in the requested range, not just ones with status
            rows_to_process = []
            for row in range(start_row, end_row + 1):
                # Skip rows in the skip list
                if row in rows_to_skip:
                    logger.info(f"Skipping row {row} as specified in SKIP_ROW_RANGE")
                    continue

                # Get status for this row (empty string if no status)
                status = status_lookup.get(row, "").strip()
                
                skip_row = False

                # Only skip if row has a status and force_reprocess is False
                if status and not config.FORCE_REPROCESS:
                    if status.lower() in ["success", "skipped"]:
                        skip_row = True
                        logger.info(f"Skipping row {row} with status: {status}")
                    
                    # Skip rows with failed lookups if not retrying failed rows
                    no_data_msg = f"no data found for {county_config.identifier_type}"
                    if no_data_msg in status.lower() and not config.RETRY_FAILED_ROWS:
                        skip_row = True
                        logger.info(f"Skipping row {row} with status containing '{no_data_msg}' (RETRY_FAILED_ROWS is False)")

                if not skip_row:
                    rows_to_process.append(row)
                    if not status:  # Log when processing rows without status
                        logger.debug(f"Processing row {row} (no previous status)")

            logger.info(f"Found {len(rows_to_process)} rows that need processing")

            # Fetch all needed columns in bulk (1 API call instead of N/100)
            identifier_col_letter = self.col_num_to_letter(identifier_col_index)
            identifier_data = []

            # Build ranges for entire columns at once
            bulk_ranges = [f"{sheet_name}!{identifier_col_letter}{start_row}:{identifier_col_letter}{end_row}"]
            optional_param_order = []  # Track order for parsing
            for column_name, col_index in optional_param_columns.items():
                col_letter = self.col_num_to_letter(col_index)
                bulk_ranges.append(f"{sheet_name}!{col_letter}{start_row}:{col_letter}{end_row}")
                optional_param_order.append(column_name)

            total_rows = end_row - start_row + 1
            CHUNK_SIZE = 5000  # Max rows per API call to avoid timeouts

            if total_rows <= CHUNK_SIZE:
                # Small enough to fetch in one call
                logger.info(f"Fetching {len(bulk_ranges)} column(s) in single API call for rows {start_row}-{end_row}")
                result = self._execute_batch_get_with_retry(bulk_ranges, "bulk identifier fetch")

                if not result:
                    logger.warning("Failed to fetch identifiers in bulk")
                    return identifier_data

                value_ranges = result.get("valueRanges", [])
                identifier_values = value_ranges[0].get("values", []) if value_ranges else []

                optional_values = {}
                for k, column_name in enumerate(optional_param_order):
                    if k + 1 < len(value_ranges):
                        optional_values[column_name] = value_ranges[k + 1].get("values", [])
                    else:
                        optional_values[column_name] = []
            else:
                # Chunk large reads to avoid timeouts
                num_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
                logger.info(f"Large range ({total_rows} rows) - splitting into {num_chunks} chunks of {CHUNK_SIZE}")

                identifier_values = []
                optional_values = {col_name: [] for col_name in optional_param_order}

                for chunk_idx in range(num_chunks):
                    chunk_start = start_row + (chunk_idx * CHUNK_SIZE)
                    chunk_end = min(chunk_start + CHUNK_SIZE - 1, end_row)

                    chunk_ranges = [f"{sheet_name}!{identifier_col_letter}{chunk_start}:{identifier_col_letter}{chunk_end}"]
                    for column_name, col_index in optional_param_columns.items():
                        col_letter = self.col_num_to_letter(col_index)
                        chunk_ranges.append(f"{sheet_name}!{col_letter}{chunk_start}:{col_letter}{chunk_end}")

                    logger.info(f"Fetching chunk {chunk_idx + 1}/{num_chunks}: rows {chunk_start}-{chunk_end}")
                    chunk_result = self._execute_batch_get_with_retry(chunk_ranges, f"bulk identifier fetch chunk {chunk_idx + 1}")

                    if not chunk_result:
                        logger.warning(f"Failed to fetch chunk {chunk_idx + 1}, skipping")
                        # Pad with empty values so row indices stay aligned
                        rows_in_chunk = chunk_end - chunk_start + 1
                        identifier_values.extend([[] for _ in range(rows_in_chunk)])
                        for col_name in optional_param_order:
                            optional_values[col_name].extend([[] for _ in range(rows_in_chunk)])
                        continue

                    chunk_value_ranges = chunk_result.get("valueRanges", [])
                    chunk_identifiers = chunk_value_ranges[0].get("values", []) if chunk_value_ranges else []

                    # Pad to full chunk size if API returned fewer rows (trailing empty cells)
                    rows_in_chunk = chunk_end - chunk_start + 1
                    while len(chunk_identifiers) < rows_in_chunk:
                        chunk_identifiers.append([])

                    identifier_values.extend(chunk_identifiers)

                    for k, column_name in enumerate(optional_param_order):
                        if k + 1 < len(chunk_value_ranges):
                            col_vals = chunk_value_ranges[k + 1].get("values", [])
                        else:
                            col_vals = []
                        while len(col_vals) < rows_in_chunk:
                            col_vals.append([])
                        optional_values[column_name].extend(col_vals)

                logger.info(f"Fetched all {num_chunks} chunks, total identifier values: {len(identifier_values)}")

            # Convert rows_to_process to a set for fast lookup
            rows_to_process_set = set(rows_to_process)

            # Iterate through all rows and filter to ones needing processing
            for i in range(end_row - start_row + 1):
                actual_row = start_row + i

                if actual_row not in rows_to_process_set:
                    continue

                # Get identifier value
                identifier = ""
                if i < len(identifier_values) and identifier_values[i]:
                    identifier = identifier_values[i][0].strip() if identifier_values[i][0] else ""

                if not identifier:
                    continue

                # Get optional parameter values for this row
                row_optional_params = {}
                skip_row = False
                for column_name in optional_param_order:
                    field_mapping_key = optional_param_mapping[column_name]
                    col_values = optional_values.get(column_name, [])
                    param_value = ""
                    if i < len(col_values) and col_values[i]:
                        param_value = col_values[i][0].strip() if col_values[i][0] else ""

                    if not param_value:
                        logger.warning(f"Row {actual_row}: Missing required optional parameter '{column_name}'")
                        skip_row = True
                        break

                    row_optional_params[field_mapping_key] = param_value

                if skip_row:
                    continue

                if row_optional_params:
                    logger.debug(f"Row {actual_row}: Found optional params: {row_optional_params}")

                row_index = actual_row - 2  # Convert to 0-indexed
                identifier_data.append((row_index, identifier, row_optional_params))

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
            if not self.config.IMPERSONATED_USER:
                raise ValueError("No service account file or impersonated user specified")
            
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

                # Get addresses for this batch (with retry for rate limits)
                address_result = self._execute_batch_get_with_retry(batch_ranges, f"addresses batch {i//batch_size + 1}")

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

                # Determine protected columns (identifier columns that should never be overwritten)
                header_map = self._get_header_map(headers)
                protected_columns = set()
                if hasattr(self.config, '_current_county'):
                    try:
                        county_config = self.config.get_county_config(self.config._current_county)
                        identifier_col_index = header_map.get(county_config.identifier_column.lower(), -1)
                        if identifier_col_index >= 0:
                            protected_columns.add(identifier_col_index)
                    except Exception as e:
                        logger.warning(f"Could not determine protected columns: {e}")
                
                # Also protect common identifier columns as fallback
                for common_identifier in ["parcelid", "address"]:
                    col_index = header_map.get(common_identifier, -1)
                    if col_index >= 0:
                        protected_columns.add(col_index)

                # Group updates to minimize API calls
                columns_to_update = {}

                # Find fields that exist in headers
                for field, value in data.items():
                    try:
                        col_index = headers.index(field)
                        # Skip protected columns (identifier columns)
                        if col_index in protected_columns:
                            logger.debug(f"Skipping protected column '{field}' (index {col_index}) - identifier columns are immutable")
                            continue
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
        """Perform optimized batch update using column-based ranges instead of row-by-row."""
        try:
            # ADD THIS DEBUG LOGGING
            logger.info(f"*** BATCH_UPDATE_ENTRY: {len(updates)} updates, sheet={sheet_name}")
            logger.info(f"*** BATCH_SIZE_CONFIG: {self.config.BATCH_SIZE}")
            
            # Filter out header row updates
            updates = [
                (idx, data) for idx, data in updates if idx + 2 > 1
            ]
            
            if len(updates) == 0:
                logger.warning("All updates filtered out to protect headers, nothing to do")
                return

            # ADD THIS DEBUG LOGGING
            logger.info(f"*** AFTER_FILTERING: {len(updates)} updates remain")

            # For very large batches, split recursively using the same logic as before
            batch_size = self.config.BATCH_SIZE
            if len(updates) > batch_size:
                logger.info(f"*** RECURSIVE_SPLIT: {len(updates)} > {batch_size}, splitting")
                midpoint = len(updates) // 2
                
                # Process first half
                logger.info(f"*** PROCESSING_FIRST_HALF: {len(updates[:midpoint])} updates")
                self.batch_update_property_data(updates[:midpoint], sheet_name)
                time.sleep(2)
                
                # Process second half  
                logger.info(f"*** PROCESSING_SECOND_HALF: {len(updates[midpoint:])} updates")
                self.batch_update_property_data(updates[midpoint:], sheet_name)
                return

            logger.info(f"Optimizing batch update for {len(updates)} rows using column-based approach")

            # Get headers and create header map
            headers = self._get_sheet_headers(sheet_name)
            header_map = self._get_header_map(headers)

            # Find Status column index
            status_col_index = header_map.get("status", -1)
            if status_col_index == -1:
                raise ValueError("Status column not found in headers")
            
            # Determine protected columns (identifier columns that should never be overwritten)
            protected_columns = set()
            if hasattr(self.config, '_current_county'):
                try:
                    county_config = self.config.get_county_config(self.config._current_county)
                    identifier_col_index = header_map.get(county_config.identifier_column.lower(), -1)
                    if identifier_col_index >= 0:
                        protected_columns.add(identifier_col_index)
                        logger.info(f"Protecting identifier column '{county_config.identifier_column}' (index {identifier_col_index}) from being overwritten")
                except Exception as e:
                    logger.warning(f"Could not determine protected columns: {e}")
            
            # Also protect common identifier columns as fallback
            for common_identifier in ["parcelid", "address"]:
                col_index = header_map.get(common_identifier, -1)
                if col_index >= 0:
                    protected_columns.add(col_index)
                    logger.debug(f"Protecting common identifier column '{common_identifier}' (index {col_index})")

            # PHASE 1: Collect all data into organized structure
            row_data_matrix = {}  # row_index -> {col_index: value}
            status_updates = {}   # row_index -> status_value
            
            for row_index, property_data in updates:
                sheet_row_index = row_index + 2  # Convert to sheet row (1-based + header)
                
                # Handle status column
                if property_data.get("success", False):
                    status_updates[sheet_row_index] = "Success"
                    
                    # Handle data columns
                    if "data" in property_data:
                        row_data_matrix[sheet_row_index] = {}
                        data = property_data.get("data", {})
                        
                        for field, value in data.items():
                            col_index = header_map.get(field.lower(), -1)
                            if col_index >= 0:
                                # Skip protected columns (identifier columns)
                                if col_index in protected_columns:
                                    logger.debug(f"Skipping protected column '{field}' (index {col_index}) - identifier columns are immutable")
                                    continue
                                row_data_matrix[sheet_row_index][col_index] = value
                            else:
                                logger.debug(f"Field '{field}' not found in headers, skipping")
                else:
                    # Failed data - just update status with error message
                    error_message = property_data.get("message", "Error")
                    status_updates[sheet_row_index] = error_message

            # PHASE 2: Create optimized ranges
            batch_data = {"valueInputOption": "RAW", "data": []}
            
            # Handle status column updates (single column range for all rows)
            if status_updates:
                status_rows = sorted(status_updates.keys())
                if len(status_rows) > 1 and self._are_consecutive_rows(status_rows):
                    # Create single range for consecutive rows
                    start_row = status_rows[0]
                    end_row = status_rows[-1]
                    status_col_letter = self.col_num_to_letter(status_col_index)
                    
                    range_name = f"{sheet_name}!{status_col_letter}{start_row}:{status_col_letter}{end_row}"
                    values = [[status_updates[row]] for row in status_rows]
                    
                    batch_data["data"].append({
                        "range": range_name,
                        "values": values
                    })
                    logger.debug(f"Created status range: {range_name} ({len(values)} rows)")
                else:
                    # Create individual status updates for non-consecutive rows
                    for row_index, status_value in status_updates.items():
                        status_col_letter = self.col_num_to_letter(status_col_index)
                        range_name = f"{sheet_name}!{status_col_letter}{row_index}"
                        batch_data["data"].append({
                            "range": range_name,
                            "values": [[status_value]]
                        })

            # Handle data column updates (column-based optimization)
            if row_data_matrix:
                column_ranges = self._create_optimized_column_ranges(
                    row_data_matrix, sheet_name, headers
                )
                batch_data["data"].extend(column_ranges)

            # PHASE 3: Execute optimized batch
            if batch_data["data"]:
                logger.info(f"Executing optimized batch: {len(batch_data['data'])} ranges for {len(updates)} rows")
                
                max_retries = 5
                for retry in range(max_retries):
                    try:
                        if retry > 0:
                            wait_time = min(60, (2**retry) + random.random())
                            logger.info(f"Retry {retry + 1}/{max_retries}: Waiting {wait_time:.2f}s")
                            time.sleep(wait_time)

                        # Execute the batch update
                        response = (
                            self.service.spreadsheets()
                            .values()
                            .batchUpdate(
                                spreadsheetId=self.spreadsheet_id,
                                body=batch_data,
                            )
                            .execute()
                        )
                        
                        logger.info("Property batch update successful")
                        return response

                    except Exception as e:
                        logger.error(f"Batch update attempt {retry + 1} failed: {e}")
                        if retry == max_retries - 1:
                            raise
                
            else:
                logger.warning("No data to update in batch")

        except Exception as e:
            logger.error(f"Error performing batch update: {e}")
            raise

    def _are_consecutive_rows(self, row_list):
        """Check if row numbers are consecutive."""
        if len(row_list) <= 1:
            return True
        
        for i in range(1, len(row_list)):
            if row_list[i] != row_list[i-1] + 1:
                return False
        return True

    def _create_optimized_column_ranges(self, row_data_matrix, sheet_name, headers):
        """Create optimized column ranges from row data matrix."""
        ranges = []
        
        # Group data by columns
        column_data = {}  # col_index -> {row_index: value}
        
        for row_index, row_columns in row_data_matrix.items():
            for col_index, value in row_columns.items():
                if col_index not in column_data:
                    column_data[col_index] = {}
                column_data[col_index][row_index] = value
        
        # Create ranges for each column
        for col_index, row_values in column_data.items():
            col_letter = self.col_num_to_letter(col_index)
            sorted_rows = sorted(row_values.keys())
            
            if len(sorted_rows) > 1 and self._are_consecutive_rows(sorted_rows):
                # Consecutive rows - create single range
                start_row = sorted_rows[0]
                end_row = sorted_rows[-1]
                range_name = f"{sheet_name}!{col_letter}{start_row}:{col_letter}{end_row}"
                
                values = [[row_values[row]] for row in sorted_rows]
                ranges.append({
                    "range": range_name,
                    "values": values
                })
                logger.debug(f"Created column range: {range_name} ({len(values)} rows)")
            else:
                # Non-consecutive rows - individual updates
                for row_index, value in row_values.items():
                    range_name = f"{sheet_name}!{col_letter}{row_index}"
                    ranges.append({
                        "range": range_name,
                        "values": [[value]]
                    })

        return ranges

    # ============== NJ-specific methods ==============

    def get_nj_property_identifiers(
        self,
        sheet_name: str,
        block_column: str = "Block",
        lot_column: str = "Lot",
        qual_column: Optional[str] = "Qual",
        start_row: int = 2,
        stop_row: int = 100,
        force_reprocess: bool = False,
    ) -> List[tuple]:
        """
        Get NJ property identifiers (Block/Lot/Qual) from sheet.

        Args:
            sheet_name: Name of the sheet to read from
            block_column: Column header for Block numbers
            lot_column: Column header for Lot numbers
            qual_column: Column header for Qualifiers (optional)
            start_row: First row to process (1-indexed)
            stop_row: Last row to process
            force_reprocess: If True, process even if Status='Success'

        Returns:
            List of (row_index, block, lot, qual) tuples
        """
        try:
            # Get headers
            headers = self._get_sheet_headers(sheet_name)
            header_map = self._get_header_map(headers)

            # Find column indices
            block_col_index = header_map.get(block_column.lower(), -1)
            lot_col_index = header_map.get(lot_column.lower(), -1)
            qual_col_index = header_map.get(qual_column.lower(), -1) if qual_column else -1
            status_col_index = header_map.get("status", -1)

            if block_col_index == -1:
                raise ValueError(f"Block column '{block_column}' not found in headers")
            if lot_col_index == -1:
                raise ValueError(f"Lot column '{lot_column}' not found in headers")

            logger.info(f"Found columns - Block: {block_col_index}, Lot: {lot_col_index}, Qual: {qual_col_index}, Status: {status_col_index}")

            # Check sheet dimensions
            max_rows, _ = self._get_sheet_dimensions(sheet_name)
            if stop_row > max_rows:
                logger.warning(f"Requested stop_row {stop_row} exceeds sheet size {max_rows}, adjusting")
                stop_row = max_rows

            logger.info(f"Reading NJ identifiers from rows {start_row} to {stop_row}")

            # Get status values first to filter out already processed rows
            rows_to_process = []

            if status_col_index >= 0:
                status_col_letter = self.col_num_to_letter(status_col_index)
                status_range = f"{sheet_name}!{status_col_letter}{start_row}:{status_col_letter}{stop_row}"

                status_result = (
                    self.service.spreadsheets()
                    .values()
                    .get(spreadsheetId=self.spreadsheet_id, range=status_range)
                    .execute()
                )

                status_values = status_result.get("values", [])

                # Build list of rows to process
                for i in range(stop_row - start_row + 1):
                    row_num = start_row + i
                    status = ""
                    if i < len(status_values) and status_values[i]:
                        status = str(status_values[i][0]).strip().lower()

                    # Skip if already successful (unless force_reprocess)
                    if status == "success" and not force_reprocess:
                        logger.debug(f"Skipping row {row_num} - already successful")
                        continue

                    rows_to_process.append(row_num)
            else:
                # No status column - process all rows
                rows_to_process = list(range(start_row, stop_row + 1))

            logger.info(f"Found {len(rows_to_process)} rows to process")

            if not rows_to_process:
                return []

            # Fetch Block/Lot/Qual values in batches
            identifier_data = []
            batch_size = 100

            for i in range(0, len(rows_to_process), batch_size):
                batch = rows_to_process[i:i + batch_size]
                batch_ranges = []

                for row in batch:
                    # Add Block column
                    block_letter = self.col_num_to_letter(block_col_index)
                    batch_ranges.append(f"{sheet_name}!{block_letter}{row}")

                    # Add Lot column
                    lot_letter = self.col_num_to_letter(lot_col_index)
                    batch_ranges.append(f"{sheet_name}!{lot_letter}{row}")

                    # Add Qual column if exists
                    if qual_col_index >= 0:
                        qual_letter = self.col_num_to_letter(qual_col_index)
                        batch_ranges.append(f"{sheet_name}!{qual_letter}{row}")

                result = self._execute_batch_get_with_retry(batch_ranges, f"NJ identifiers batch {i // batch_size + 1}")

                if not result:
                    continue

                value_ranges = result.get("valueRanges", [])
                cols_per_row = 3 if qual_col_index >= 0 else 2

                for j in range(len(batch)):
                    base_idx = j * cols_per_row

                    if base_idx >= len(value_ranges):
                        continue

                    # Extract Block
                    block_range = value_ranges[base_idx]
                    block_vals = block_range.get("values", [[]])[0]
                    block = str(block_vals[0]).strip() if block_vals else ""

                    # Extract Lot
                    lot_range = value_ranges[base_idx + 1]
                    lot_vals = lot_range.get("values", [[]])[0]
                    lot = str(lot_vals[0]).strip() if lot_vals else ""

                    # Extract Qual (if available)
                    qual = None
                    if qual_col_index >= 0 and base_idx + 2 < len(value_ranges):
                        qual_range = value_ranges[base_idx + 2]
                        qual_vals = qual_range.get("values", [[]])[0]
                        qual = str(qual_vals[0]).strip() if qual_vals else None

                    # Only add if we have Block and Lot
                    if block and lot:
                        row_index = batch[j] - 2  # Convert to 0-indexed
                        identifier_data.append((row_index, block, lot, qual))

            logger.info(f"Retrieved {len(identifier_data)} NJ identifiers from {sheet_name}")
            return identifier_data

        except Exception as e:
            logger.error(f"Error getting NJ property identifiers: {e}")
            raise

    def batch_update_nj_property_data(self, updates, sheet_name: str = "LIENS"):
        """
        Batch update NJ property data to sheet.

        Args:
            updates: List of (row_index, property_data) tuples
            sheet_name: Name of the sheet to update
        """
        try:
            # Import NJ field mapping
            from nj_property_api import NJ_FIELD_MAPPING

            if not updates:
                logger.warning("No updates to process")
                return

            # Filter out header row
            updates = [(idx, data) for idx, data in updates if idx + 2 > 1]

            if not updates:
                return

            # Handle large batches by splitting
            batch_size = self.config.BATCH_SIZE if hasattr(self.config, 'BATCH_SIZE') else 100
            if len(updates) > batch_size:
                logger.info(f"Splitting {len(updates)} updates into smaller batches")
                midpoint = len(updates) // 2
                self.batch_update_nj_property_data(updates[:midpoint], sheet_name)
                time.sleep(2)
                self.batch_update_nj_property_data(updates[midpoint:], sheet_name)
                return

            # Get headers
            headers = self._get_sheet_headers(sheet_name)
            header_map = self._get_header_map(headers)

            # Determine which NJ fields we need columns for
            # Note: Owner is excluded (always null due to privacy/Daniel's Law)
            # Status is placed last for better readability
            nj_output_fields = [
                "Address", "TotalAssessed", "LandValue", "ImprovementValue",
                "YearBuilt", "Acreage", "PropertyClass", "BuildingDesc",
                "VacantLot", "SalePrice", "DeedDate", "MailingAddress",
                "MailingCityState", "ZipCode", "Municipality", "County",
                "PAMS_PIN", "Status"
            ]

            # Check which fields are missing from the sheet
            missing_fields = []
            for field in nj_output_fields:
                if field.lower() not in header_map:
                    missing_fields.append(field)

            # Auto-create missing columns
            if missing_fields:
                logger.info(f"Sheet '{sheet_name}' is missing {len(missing_fields)} NJ columns: {missing_fields}")
                logger.info("Auto-creating missing columns...")

                # Add new headers starting after existing columns
                new_col_start = len(headers)
                total_cols_needed = new_col_start + len(missing_fields)

                # First, expand the sheet if needed
                try:
                    # Get sheet metadata to find sheet ID and current dimensions
                    spreadsheet_meta = self.service.spreadsheets().get(
                        spreadsheetId=self.spreadsheet_id,
                        fields="sheets(properties(sheetId,title,gridProperties))"
                    ).execute()

                    sheet_id = None
                    current_max_cols = 26  # Default

                    for sheet in spreadsheet_meta.get("sheets", []):
                        if sheet["properties"]["title"] == sheet_name:
                            sheet_id = sheet["properties"]["sheetId"]
                            current_max_cols = sheet["properties"]["gridProperties"].get("columnCount", 26)
                            break

                    if sheet_id is not None and total_cols_needed > current_max_cols:
                        # Expand the sheet columns
                        logger.info(f"Expanding sheet from {current_max_cols} to {total_cols_needed} columns")
                        expand_request = {
                            "requests": [{
                                "updateSheetProperties": {
                                    "properties": {
                                        "sheetId": sheet_id,
                                        "gridProperties": {
                                            "columnCount": total_cols_needed
                                        }
                                    },
                                    "fields": "gridProperties.columnCount"
                                }
                            }]
                        }
                        self.service.spreadsheets().batchUpdate(
                            spreadsheetId=self.spreadsheet_id,
                            body=expand_request
                        ).execute()
                        logger.info(f"Sheet expanded to {total_cols_needed} columns")

                except Exception as e:
                    logger.error(f"Failed to expand sheet columns: {e}")
                    # Continue anyway - may still work if columns exist

                # Now add the new headers
                new_headers_data = []
                for i, field in enumerate(missing_fields):
                    col_letter = self.col_num_to_letter(new_col_start + i)
                    new_headers_data.append({
                        "range": f"{sheet_name}!{col_letter}1",
                        "values": [[field]]
                    })

                # Batch update the new headers
                if new_headers_data:
                    try:
                        self.service.spreadsheets().values().batchUpdate(
                            spreadsheetId=self.spreadsheet_id,
                            body={"valueInputOption": "RAW", "data": new_headers_data}
                        ).execute()
                        logger.info(f"Created {len(missing_fields)} new columns: {missing_fields}")

                        # Clear header cache and refresh
                        cache_key = f"{self.spreadsheet_id}:{sheet_name}"
                        if cache_key in self._headers_cache:
                            del self._headers_cache[cache_key]

                        # Re-fetch headers
                        headers = self._get_sheet_headers(sheet_name)
                        header_map = self._get_header_map(headers)

                    except Exception as e:
                        logger.error(f"Failed to create missing columns: {e}")
                        # Continue anyway - will just skip fields without columns

            # Find Status column (may have just been created)
            status_col_index = header_map.get("status", -1)
            if status_col_index < 0:
                logger.warning("No 'Status' column found in sheet - status updates will be skipped")

            # Protected columns (Block, Lot, Qual should not be overwritten)
            protected_columns = set()
            for col_name in ["block", "lot", "qual"]:
                col_idx = header_map.get(col_name, -1)
                if col_idx >= 0:
                    protected_columns.add(col_idx)

            # Prepare batch data
            batch_data = {"valueInputOption": "RAW", "data": []}

            for row_index, property_data in updates:
                sheet_row_index = row_index + 2

                if property_data.get("success", False) and "data" in property_data:
                    data = property_data.get("data", {})

                    # Map NJ fields to columns
                    for field_name, value in data.items():
                        col_index = header_map.get(field_name.lower(), -1)

                        if col_index < 0:
                            # Try to find by NJ field mapping
                            for output_name, api_field in NJ_FIELD_MAPPING.items():
                                if output_name.lower() == field_name.lower():
                                    col_index = header_map.get(output_name.lower(), -1)
                                    break

                        if col_index >= 0 and col_index not in protected_columns:
                            col_letter = self.col_num_to_letter(col_index)
                            range_name = f"{sheet_name}!{col_letter}{sheet_row_index}"
                            batch_data["data"].append({
                                "range": range_name,
                                "values": [[value if value is not None else ""]]
                            })

                    # Update Status to Success
                    if status_col_index >= 0:
                        status_letter = self.col_num_to_letter(status_col_index)
                        range_name = f"{sheet_name}!{status_letter}{sheet_row_index}"
                        batch_data["data"].append({
                            "range": range_name,
                            "values": [["Success"]]
                        })
                else:
                    # Update Status with error message
                    if status_col_index >= 0:
                        error_msg = property_data.get("message", "Error")
                        status_letter = self.col_num_to_letter(status_col_index)
                        range_name = f"{sheet_name}!{status_letter}{sheet_row_index}"
                        batch_data["data"].append({
                            "range": range_name,
                            "values": [[error_msg]]
                        })

            # Execute batch update
            if batch_data["data"]:
                logger.info(f"Executing NJ batch update: {len(batch_data['data'])} ranges")

                for retry in range(5):
                    try:
                        if retry > 0:
                            wait_time = min(60, (2 ** retry) + random.random())
                            time.sleep(wait_time)

                        self.service.spreadsheets().values().batchUpdate(
                            spreadsheetId=self.spreadsheet_id,
                            body=batch_data,
                        ).execute()

                        logger.info("NJ batch update successful")
                        return

                    except HttpError as e:
                        if e.resp.status == 429:
                            logger.warning(f"Rate limit, retry {retry + 1}/5")
                            if retry == 4:
                                raise
                        else:
                            raise

        except Exception as e:
            logger.error(f"Error in NJ batch update: {e}")
            raise

    def update_row_with_nj_property_data(
        self, row_index: int, property_data: Dict[str, Any], sheet_name: str = "LIENS"
    ) -> None:
        """
        Update a single row with NJ property data (fallback for batch failures).

        Args:
            row_index: 0-based index of the row to update
            property_data: Dictionary with property data result
            sheet_name: Name of the sheet to update
        """
        try:
            # Use batch method with single item
            self.batch_update_nj_property_data([(row_index, property_data)], sheet_name)
        except Exception as e:
            logger.error(f"Error updating NJ row {row_index + 2}: {e}")