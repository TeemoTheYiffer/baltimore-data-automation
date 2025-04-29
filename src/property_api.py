import logging
import time
import requests
import urllib.parse
from typing import Dict, Any, Optional
from config import AppConfig
from utils.address_utils import parse_address, get_simplified_address

logger = logging.getLogger("property")

class PropertyDataAPI:
    """Client for Maryland Property Data API."""
    
    def __init__(self, 
                config: Optional[AppConfig] = None,
                county: str = "baltimore"):
        """Initialize the property data API client."""
        self.config = config or AppConfig()
        self.county_config = self.config.get_county_config(county)
        self.session = requests.Session()
        logger.info(f"Initialized PropertyDataAPI for {self.county_config.county_name}")

    def format_api_url(self, identifier: str) -> str:
        """Format the API URL for query based on county configuration."""
        # Handle different identifier types based on county configuration
        if self.county_config.identifier_type == "parcel_id":
            # PG County - search by Parcel ID
            return f"{self.county_config.base_url}?$where=record_key_account_number_sdat_field_3 LIKE '{urllib.parse.quote(identifier)}'"
        else:
            # Other counties - search by address (default)
            cleaned_address, _, _ = parse_address(identifier)
            return f"{self.county_config.base_url}?$where=mdp_street_address_mdp_field_address LIKE '{urllib.parse.quote(cleaned_address)}%25'"
    
    def format_fallback_api_url(self, identifier: str) -> str:
        """Format the fallback API URL for a more flexible query."""
        # Handle different identifier types based on county configuration
        if self.county_config.identifier_type == "parcel_id":
            # For PG County, just use a more flexible match on Parcel ID
            return f"{self.county_config.base_url}?$where=record_key_account_number_sdat_field_3 LIKE '{urllib.parse.quote(identifier)}'"
        else:
            # Parse the address into components for other counties
            _, address_number, street_name = parse_address(identifier)
            
            # If we couldn't extract a number, try a direct search
            if not address_number:
                return f"{self.county_config.base_url}?$where=mdp_street_address_mdp_field_address LIKE '{urllib.parse.quote(identifier)}%25'"
            
            # Try different number formats
            number_formats = []
            
            # Original number
            number_formats.append(address_number)
            
            # With leading zeros (try multiple variations)
            for i in range(1, 5):  # Try up to 4 leading zeros
                number_formats.append(f"{'0' * i}{address_number}")
            
            # Build query with OR conditions for multiple number formats
            conditions = []
            for num_format in number_formats:
                conditions.append(f"premise_address_number_mdp_field_premsnum_sdat_field_20='{num_format}'")
            
            number_condition = f"({' OR '.join(conditions)})"
            street_condition = f"premise_address_name_mdp_field_premsnam_sdat_field_23 LIKE '{urllib.parse.quote(street_name)}'"
            
            return f"{self.county_config.base_url}?$where={number_condition} AND {street_condition}"
    
    def get_property_data(self, identifier: str) -> Dict[str, Any]:
        """
        Get property data for an identifier (address or Parcel ID depending on county).
        
        Args:
            identifier: Address or Parcel ID according to county configuration
        """
        original_identifier = identifier
        
        try:
            # For parcel_id counties, pad with leading zeros if needed
            if self.county_config.identifier_type == "parcel_id" and self.county_config.parcel_digits > 0:
                # Check current length and pad if needed
                current_length = len(identifier)
                expected_length = self.county_config.parcel_digits
                
                if current_length < expected_length:
                    # Calculate exactly how many zeros to add
                    padding = expected_length - current_length
                    identifier = '0' * padding + identifier
                    #logger.info(f"Padded ParcelID from {original_identifier} to {identifier} ({self.county_config.county_name} requires {expected_length} digits)")
            
            # Try the API call with the properly formatted identifier
            api_url = self.format_api_url(identifier)
            #logger.info(f"Trying URL: {api_url}")
            
            response = self.session.get(api_url, timeout=self.config.REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            # If no results, try fallback approach
            if len(data) == 0:
                fallback_url = self.format_fallback_api_url(identifier)
                #logger.info(f"No results with primary query. Trying fallback URL: {fallback_url}")
                
                fallback_response = self.session.get(fallback_url, timeout=self.config.REQUEST_TIMEOUT)
                fallback_response.raise_for_status()
                data = fallback_response.json()
                
                # For address-based counties - try simplified address
                if len(data) == 0 and self.county_config.identifier_type == "address":
                    simplified = get_simplified_address(identifier)
                    if simplified != identifier:
                        logger.debug(f"Trying with simplified address: {simplified}")
                        return self.get_property_data(simplified)
            
            if len(data) == 0:
                logger.warning(f"No data found for {self.county_config.identifier_type}: {original_identifier}")
                return {
                    "success": False,
                    "message": f"No data found for {self.county_config.identifier_type}: {original_identifier}"
                }
            
            # Process the API response
            return self._process_api_response(data[0], original_identifier)
            
        except Exception as e:
            logger.error(f"Error fetching data for identifier {original_identifier}: {e}")
            return {
                "success": False,
                "message": f"Error: {str(e)}"
            }
    
    def _process_api_response(self, api_response: Dict[str, Any], identifier: str) -> Dict[str, Any]:
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
        
        # Add the original identifier
        #if self.county_config.identifier_type == "parcel_id":
        #    value_map["ParcelID"] = identifier
        #else:
        #    value_map["ADDRESS"] = identifier
        
        return {
            "success": True,
            "identifier": identifier,
            "identifier_type": self.county_config.identifier_type,
            "data": value_map
        }
    
    def _calculate_derived_fields(self, api_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate derived fields based on API data."""
        derived_values = {}
        
        # Calculate "VACANT LOT (Y)" - Determines if property is a vacant lot based on improvement value
        improvement_value = api_data.get("current_cycle_data_improvements_value_mdp_field_names_nfmimpvl_curimpvl_and_salimpvl_sdat_field_165", "0")
        derived_values["VACANT LOT (Y)"] = "Y" if improvement_value == "0" or improvement_value == 0 else "N"
        
        # Calculate hundred_block - Converts address number to hundreds block (eg. 1234 -> 1200)
        address_num = api_data.get("premise_address_number_mdp_field_premsnum_sdat_field_20", "")
        if address_num and len(str(address_num)) >= 2:
            # Handle number part with special formatting
            number, _, _ = parse_address(str(address_num))
                
            # Calculate hundred block
            if len(number) >= 2:
                derived_values["hundred_block"] = number[0:len(number)-2] + "00"
            else:
                derived_values["hundred_block"] = "0"
        else:
            derived_values["hundred_block"] = ""
        
        # Calculate SDAT - URL for real property search
        if ("real_property_search_link" in api_data and 
                isinstance(api_data["real_property_search_link"], dict) and 
                "url" in api_data["real_property_search_link"]):
            derived_values["SDAT"] = api_data["real_property_search_link"]["url"]
        else:
            derived_values["SDAT"] = ""

        # Calculate Parcel - URL for parcel finder online
        if ("finder_online_link" in api_data and 
                isinstance(api_data["finder_online_link"], dict) and 
                "url" in api_data["finder_online_link"]):
            derived_values["Parcel"] = api_data["finder_online_link"]["url"]
        else:
            derived_values["Parcel"] = ""
        
        # Add Status field to the derived values
        derived_values["Status"] = "Success"
        
        return derived_values