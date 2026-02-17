import logging
import requests
import urllib.parse
from typing import Dict, Any, Optional, List
from config import AppConfig
from utils.address_utils import parse_address, get_simplified_address

logger = logging.getLogger("property")


class PropertyDataAPI:
    """Client for Maryland Property Data API."""

    def __init__(self, county: str, config: Optional[AppConfig] = None):
        """Initialize the property data API client."""
        self.config = config or AppConfig()
        self.county_config = self.config.get_county_config(county)
        self.session = requests.Session()

        # Socrata API Key authentication (HTTP Basic Auth)
        api_key_id = self.config.MARYLAND_APP_API_KEY_ID
        api_key_secret = self.config.MARYLAND_APP_API_KEY_SECRET

        # Browser-like headers to avoid Cloudflare bot challenges
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        })

        if api_key_id and api_key_secret:
            self.session.auth = (api_key_id, api_key_secret)
            logger.info("Socrata API: using HTTP Basic Auth (API key) for authenticated rate limits")
        else:
            logger.warning("Socrata API: no API key configured — rate limits will be strict")
        

    def _make_request_with_retry(self, url: str, description: str, timeout: int = None) -> Optional[List[Dict]]:
        """
        Make HTTP request with retry logic.

        Args:
            url: The URL to request
            description: Description for logging (e.g., "primary query", "fallback query")

        Returns:
            JSON response data or None if all retries failed
        """
        import time
        import random

        max_retries = 3
        retryable_codes = {500, 429}  # Only retry server errors and explicit rate limits
        max_403_retries = 1  # Single retry for 403 (likely rate limit)
        num_403_retries = 0

        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=timeout or self.config.REQUEST_TIMEOUT)

                # 403: likely Socrata rate limiting — allow ONE retry with short delay
                if response.status_code == 403:
                    body_preview = response.text[:200] if response.text else "(empty)"
                    if num_403_retries < max_403_retries:
                        num_403_retries += 1
                        wait_time = 3 + random.uniform(1.0, 3.0)
                        logger.warning(f"403 on {description} — retrying once in {wait_time:.1f}s (body: {body_preview})")
                        time.sleep(wait_time)
                        continue
                    logger.info(f"403 persisted on {description} — skipping (body: {body_preview})")
                    return []

                if response.status_code in retryable_codes:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) + random.uniform(0.5, 2.0)
                        logger.warning(f"{response.status_code} error on {description}, retrying in {wait_time:.0f}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"{response.status_code} error persists on {description} after {max_retries} attempts")
                        return None

                response.raise_for_status()
                return response.json()

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if hasattr(e, 'response') and e.response else None
                if status_code == 403:
                    body_preview = e.response.text[:200] if e.response and e.response.text else "(empty)"
                    if num_403_retries < max_403_retries:
                        num_403_retries += 1
                        wait_time = 3 + random.uniform(1.0, 3.0)
                        logger.warning(f"403 on {description} — retrying once in {wait_time:.1f}s (body: {body_preview})")
                        time.sleep(wait_time)
                        continue
                    logger.info(f"403 persisted on {description} — skipping (body: {body_preview})")
                    return []
                if status_code in retryable_codes and attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + random.uniform(0.5, 2.0)
                    logger.warning(f"HTTP {status_code} error on {description}, retrying in {wait_time:.0f}s")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"HTTP error on {description}: {e}")
                    if attempt == max_retries - 1:
                        return None
                    raise
            except Exception as e:
                logger.error(f"Request error on {description}: {e}")
                if attempt == max_retries - 1:
                    return None
                raise

        return None

    def _map_optional_params_to_api_fields(self, optional_params: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Map friendly parameter names to actual API field names using FIELD_MAPPING."""
        mapped_params = {}
        
        if not optional_params:
            return mapped_params
        
        # 🎯 CREATE LOCAL COPIES TO AVOID THREADING ISSUES:
        local_optional_params = dict(optional_params)  # Create copy
        local_field_mapping = dict(self.county_config.field_mapping)  # Create copy
        
        for param_key, param_value in local_optional_params.items():
            api_field = None
            for field_name, api_field_name in local_field_mapping.items():
                if field_name.lower() == param_key.lower() and api_field_name:
                    api_field = api_field_name
                    break
            
            if api_field:
                mapped_params[api_field] = param_value
                logger.debug(f"Mapped {param_key} -> {api_field} = {param_value}")
            else:
                logger.warning(f"Optional parameter '{param_key}' not found in FIELD_MAPPING for {self.county_config.county_name}")
        
        return mapped_params

    def _build_optional_params_clause(self, optional_params: Optional[Dict[str, str]] = None) -> str:
        """Build the optional parameters clause for the API URL."""

        if not optional_params:
            return ""
            
        mapped_params = self._map_optional_params_to_api_fields(optional_params)
        if not mapped_params:
            return ""
            
        clauses = []
        for api_field, value in mapped_params.items():
            # Use LIKE operator as requested by the user
            encoded_value = urllib.parse.quote(str(value))
            clauses.append(f"{api_field} LIKE '{encoded_value}'")
        
        if clauses:
            clause_string = " AND " + " AND ".join(clauses)
            logger.debug(f"Built optional params clause: {clause_string}")
            return clause_string
        
        return ""

    def format_api_url(self, identifier: str, optional_params: Optional[Dict[str, str]] = None) -> str:
        """Format the API URL for query based on county configuration."""
        # Handle different identifier types based on county configuration
        if self.county_config.identifier_type == "parcel_id":
            # PG County - search by Parcel ID
            base_query = f"{self.county_config.base_url}?$where=record_key_account_number_sdat_field_3 LIKE '{urllib.parse.quote(identifier)}'"
        else:
            # Other counties - search by address (default)
            cleaned_address, _, _ = parse_address(identifier)
            base_query = f"{self.county_config.base_url}?$where=mdp_street_address_mdp_field_address LIKE '{urllib.parse.quote(cleaned_address)}%25'"
        
        # Add optional parameters if provided
        optional_clause = self._build_optional_params_clause(optional_params)
        final_url = base_query + optional_clause
        
        logger.debug(f"Generated API URL: {final_url}")
        return final_url

    def format_fallback_api_url(self, identifier: str, optional_params: Optional[Dict[str, str]] = None) -> str:
        """Format the fallback API URL using the short field name to avoid Cloudflare WAF.

        Cloudflare blocks queries with long Socrata field names (50+ chars).
        Fallback uses mdp_street_address_mdp_field_address (works) with the
        street type suffix stripped, e.g. '1534 ABBOTSTON%' instead of
        '1534 ABBOTSTON ST%'.
        """
        if self.county_config.identifier_type == "parcel_id":
            base_query = f"{self.county_config.base_url}?$where=record_key_account_number_sdat_field_3 LIKE '{urllib.parse.quote(identifier)}'"
        else:
            _, address_number, street_name = parse_address(identifier)

            if not address_number:
                base_query = f"{self.county_config.base_url}?$where=mdp_street_address_mdp_field_address LIKE '{urllib.parse.quote(identifier)}%25'"
            else:
                # Strip street type suffix (ST, AVE, RD, etc.) for broader matching
                street_suffixes = {"ST", "AVE", "RD", "DR", "LN", "CT", "PL", "BLVD", "WAY", "CIR", "TER", "PKY", "HWY", "SQ", "ALY"}
                street_name_parts = street_name.rsplit(" ", 1)
                if len(street_name_parts) == 2 and street_name_parts[1].upper() in street_suffixes:
                    bare_street = street_name_parts[0]
                else:
                    bare_street = street_name

                # Use short field name with wildcard: "1534 ABBOTSTON%"
                fallback_address = f"{address_number} {bare_street}"
                base_query = f"{self.county_config.base_url}?$where=mdp_street_address_mdp_field_address LIKE '{urllib.parse.quote(fallback_address)}%25'"
        
        # Add optional parameters if provided
        optional_clause = self._build_optional_params_clause(optional_params)
        final_url = base_query + optional_clause
        
        logger.debug(f"Generated fallback API URL: {final_url}")
        return final_url

    def get_property_data(self, identifier: str, optional_params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Get property data for an identifier with retry and fallback logic."""
        original_identifier = identifier

        try:
            # For parcel_id counties, pad with leading zeros if needed
            if (
                self.county_config.identifier_type == "parcel_id"
                and self.county_config.parcel_digits > 0
            ):
                current_length = len(identifier)
                expected_length = self.county_config.parcel_digits

                if current_length < expected_length:
                    padding = expected_length - current_length
                    identifier = "0" * padding + identifier

            # Try primary API call with retry logic
            api_url = self.format_api_url(identifier, optional_params)
            logger.info(f"Primary query for {self.county_config.identifier_type}: {identifier} -> {api_url}")
            data = self._make_request_with_retry(api_url, f"primary query for {self.county_config.identifier_type} '{identifier}'")
            
            if data is None:
                # Request failed after retries
                return {"success": False, "message": f"API request failed for {original_identifier}"}

            # If no results, try fallback approaches
            if len(data) == 0:
                # Fallback 1: same short field, street suffix stripped (e.g. "1534 ABBOTSTON%")
                fallback_url = self.format_fallback_api_url(identifier, optional_params)
                logger.info(f"No results with primary query for {self.county_config.identifier_type}: {identifier}. Trying fallback: {fallback_url}")

                data = self._make_request_with_retry(fallback_url, f"fallback query for '{identifier}'")

                if data is None:
                    return {"success": False, "message": f"Fallback API request failed for {original_identifier}"}
                
                # Fallback 2: simplified address
                if len(data) == 0 and self.county_config.identifier_type == "address":
                    simplified = get_simplified_address(identifier)
                    if simplified != identifier:
                        logger.debug(f"Trying with simplified address: {simplified}")
                        return self.get_property_data(simplified)
                
                # Fallback 3: $q full-text search (for records with empty combined address field)
                # Database stores address numbers as 5 digits (e.g. 121 → 00121, 1534 → 01534)
                # $q is slow (full-text scan) — try padded-to-5 first, then original
                if len(data) == 0 and self.county_config.identifier_type == "address":
                    _, address_number, street_name = parse_address(identifier)
                    if address_number:
                        street_suffixes = {"ST", "AVE", "RD", "DR", "LN", "CT", "PL", "BLVD", "WAY", "CIR", "TER", "PKY", "HWY", "SQ", "ALY"}
                        directionals = {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}

                        # Strip street suffix (ST, AVE, etc.)
                        parts = street_name.rsplit(" ", 1)
                        bare_street = parts[0] if len(parts) == 2 and parts[1].upper() in street_suffixes else street_name

                        # Strip directional prefix (N, S, E, W, etc.) to get core street name
                        # e.g. "S ADDISON" → "ADDISON", "NW BALTIMORE" → "BALTIMORE"
                        core_parts = bare_street.split(" ", 1)
                        core_street = core_parts[1] if len(core_parts) == 2 and core_parts[0].upper() in directionals else bare_street

                        padded_5 = address_number.zfill(5)
                        candidates = [padded_5, address_number] if padded_5 != address_number else [address_number]

                        for num in candidates:
                            q_term = f"{num} {core_street}"
                            q_url = f"{self.county_config.base_url}?$q={urllib.parse.quote(q_term)}&$limit=5"
                            logger.info(f"Trying $q full-text search: '{q_term}'")

                            try:
                                q_data = self._make_request_with_retry(q_url, f"$q search '{q_term}'", timeout=60)
                                if q_data and len(q_data) > 0:
                                    # Validate results: check address number and street name match
                                    num_field = "premise_address_number_mdp_field_premsnum_sdat_field_20"
                                    name_field = "premise_address_name_mdp_field_premsnam_sdat_field_23"
                                    validated = [
                                        r for r in q_data
                                        if r.get(num_field, "").lstrip("0") == address_number.lstrip("0")
                                        and core_street.upper() in r.get(name_field, "").upper()
                                    ]
                                    if validated:
                                        data = validated
                                        logger.info(f"$q search found {len(validated)} validated results for '{q_term}'")
                                        break
                                    else:
                                        logger.info(f"$q search returned {len(q_data)} results for '{q_term}' but none matched address")
                            except Exception as q_err:
                                logger.warning(f"$q search failed for '{q_term}': {q_err}")
                                break  # don't try more $q if it's timing out
                
                # For parcel_id counties - try alternative padding lengths
                if len(data) == 0 and self.county_config.identifier_type == "parcel_id" and self.county_config.parcel_digits > 0:
                    original_digits = self.county_config.parcel_digits
                    
                    # Try +1 and -1 digit variations
                    padding_attempts = [original_digits + 1, original_digits - 1]
                    
                    for attempt_digits in padding_attempts:
                        if attempt_digits <= 0:  # Skip invalid padding lengths
                            continue
                            
                        # Create padded identifier with alternative length
                        current_length = len(original_identifier)
                        if current_length < attempt_digits:
                            padding = attempt_digits - current_length
                            padded_identifier = "0" * padding + original_identifier
                        else:
                            padded_identifier = original_identifier  # No padding needed
                        
                        logger.info(f"Trying alternative padding: {original_identifier} -> {padded_identifier} ({attempt_digits} digits)")
                        
                        # Try primary query with alternative padding
                        alt_url = self.format_api_url(padded_identifier, optional_params)
                        alt_data = self._make_request_with_retry(alt_url, f"alternative padding query ({attempt_digits} digits) for '{padded_identifier}'")
                        
                        if alt_data is not None and len(alt_data) > 0:
                            logger.info(f"Success with alternative padding: {attempt_digits} digits for {original_identifier}")
                            data = alt_data
                            break
                    
                    # If alternative padding found results, update the identifier for processing
                    if len(data) > 0:
                        # Don't change identifier - use original for logging but data for processing
                        pass

            if len(data) == 0:
                logger.warning(
                    f"No data found for {self.county_config.identifier_type}: {original_identifier} (tried primary, fallback, and alternative padding)"
                )
                return {
                    "success": False,
                    "message": f"No data found for {self.county_config.identifier_type}: {original_identifier}",
                }

            # Process the API response
            return self._process_api_response(data[0], original_identifier)

        except Exception as e:
            logger.error(f"Error fetching data for identifier {original_identifier}: {e}")
            return {"success": False, "message": f"Error: {str(e)}"}

    def _process_api_response(
        self, api_response: Dict[str, Any], identifier: str
    ) -> Dict[str, Any]:
        """Process the API response to extract and transform fields."""
        # Map API response to columns using the county-specific field mapping
        value_map = {}

        # Get field mapping from county configuration
        field_mapping = self.county_config.field_mapping

        # First, directly map API fields
        for field, api_field in field_mapping.items():
            if api_field and api_field in api_response:
                value = api_response[api_field]

                # Apply transformations from CONFIG.COLUMN_TRANSFORMS
                if field.upper() in ["BLOCK", "LOT"]:
                    value = value.strip() if value else ""
                elif field.lower() in ["sale1", "sale2", "sale3", "sales_price"]:
                    value = int(value) if value else 0
                elif field.lower() == "above_ground_living_area":
                    value = int(value) if value else 0
                elif field.lower() == "land_size":
                    value = float(value) if value else 0

                value_map[field] = value

        # Apply derived fields
        value_map.update(self._calculate_derived_fields(api_response))

        # Check if ADDRESS exists and has a value, regardless of whether it was mapped
        address_value = value_map.get("ADDRESS", "").strip()
        if not address_value:
            # If ADDRESS is empty/null/missing, try fallback combination
            fallback_name1 = value_map.get("ADDRESS_FALLBACK_NAME1", "").strip()
            fallback_name2 = value_map.get("ADDRESS_FALLBACK_NAME2", "").strip()
            
            if fallback_name1 or fallback_name2:
                address_parts = [part for part in [fallback_name1, fallback_name2] if part]
                value_map["ADDRESS"] = " ".join(address_parts)
                logger.debug(f"Using fallback address for {identifier}: '{value_map['ADDRESS']}' (from '{fallback_name1}' + '{fallback_name2}')")
            else:
                logger.warning(f"No address data available for identifier {identifier}")
                value_map["ADDRESS"] = ""

        # Remove the helper fields from final output
        value_map.pop("ADDRESS_FALLBACK_NAME1", None)
        value_map.pop("ADDRESS_FALLBACK_NAME2", None)

        # Add the original identifier
        # if self.county_config.identifier_type == "parcel_id":
        #    value_map["ParcelID"] = identifier
        # else:
        #    value_map["ADDRESS"] = identifier

        return {
            "success": True,
            "identifier": identifier,
            "identifier_type": self.county_config.identifier_type,
            "data": value_map,
        }

    def _calculate_derived_fields(self, api_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate derived fields based on API data."""
        derived_values = {}

        # Calculate "VACANT LOT (Y)" - Determines if property is a vacant lot based on improvement value
        improvement_value = api_data.get(
            "current_cycle_data_improvements_value_mdp_field_names_nfmimpvl_curimpvl_and_salimpvl_sdat_field_165",
            "0",
        )
        derived_values["VACANT LOT (Y)"] = (
            "Y" if improvement_value == "0" or improvement_value == 0 else "N"
        )

        # Calculate hundred_block - Converts address number to hundreds block (eg. 1234 -> 1200)
        address_num = api_data.get(
            "premise_address_number_mdp_field_premsnum_sdat_field_20", ""
        )
        if address_num and len(str(address_num)) >= 2:
            # Handle number part with special formatting
            number, _, _ = parse_address(str(address_num))

            # Calculate hundred block
            if len(number) >= 2:
                derived_values["hundred_block"] = number[0 : len(number) - 2] + "00"
            else:
                derived_values["hundred_block"] = "0"
        else:
            derived_values["hundred_block"] = ""

        # Calculate SDAT - URL for real property search
        if (
            "real_property_search_link" in api_data
            and isinstance(api_data["real_property_search_link"], dict)
            and "url" in api_data["real_property_search_link"]
        ):
            derived_values["SDAT"] = api_data["real_property_search_link"]["url"]
        else:
            derived_values["SDAT"] = ""

        # Calculate Parcel - URL for parcel finder online
        if (
            "finder_online_link" in api_data
            and isinstance(api_data["finder_online_link"], dict)
            and "url" in api_data["finder_online_link"]
        ):
            derived_values["Parcel"] = api_data["finder_online_link"]["url"]
        else:
            derived_values["Parcel"] = ""

        # Add Status field to the derived values
        derived_values["Status"] = "Success"

        return derived_values

    def get_sample_property(self) -> Dict[str, Any]:
        """Get a sample residential property from the county's API to demonstrate data structure."""
        try:
            # Build the sample URL with residential property filter
            sample_url = f"{self.county_config.base_url}?$where=land_use_code_mdp_field_lu_desclu_sdat_field_50%20LIKE%20'Residential%20(R)'&$limit=1"
            
            logger.info(f"Fetching sample residential property data from {self.county_config.county_name} county")
            logger.debug(f"Sample URL: {sample_url}")
            
            # Make the request with retry logic
            data = self._make_request_with_retry(sample_url, "sample property query")
            
            if data is None:
                return {
                    "success": False,
                    "message": f"Failed to fetch sample data from {self.county_config.county_name} county"
                }
            
            if len(data) == 0:
                return {
                    "success": False,
                    "message": f"No sample residential data available from {self.county_config.county_name} county"
                }
            
            # Return the raw API response for the first (and only) result
            return {
                "success": True,
                "county": self.county_config.county_name,
                "base_url": self.county_config.base_url,
                "sample_data": data[0],
                "message": f"Sample residential property data from {self.county_config.county_name} county"
            }
            
        except Exception as e:
            logger.error(f"Error fetching sample property data from {self.county_config.county_name}: {e}")
            return {
                "success": False,
                "message": f"Error: {str(e)}"
            }