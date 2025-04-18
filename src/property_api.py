import logging
import time
import requests
import urllib.parse
from typing import Dict, Any, Optional

from config import Settings, MarylandPropertySettings

logger = logging.getLogger("baltimore_property")

class PropertyDataAPI:
    """Client for Maryland Property Data API."""
    
    def __init__(self, 
                 settings: Optional[Settings] = None,
                 property_settings: Optional[MarylandPropertySettings] = None):
        """Initialize the property data API client."""
        self.settings = settings or Settings()
        self.property_settings = property_settings or MarylandPropertySettings()
        self.session = requests.Session()
    
    def format_api_url(self, address: str) -> str:
        """Format the API URL for an address query."""
        base_address = address.strip()
        return f"{self.property_settings.BASE_URL}?$where=mdp_street_address_mdp_field_address LIKE '{urllib.parse.quote(base_address)}%25'"
    
    def format_fallback_api_url(self, address_number: str, street_name: str) -> str:
        """Format the fallback API URL for a more flexible query."""
        return f"{self.property_settings.BASE_URL}?$where=premise_address_number_mdp_field_premsnum_sdat_field_20='0{address_number}' AND premise_address_name_mdp_field_premsnam_sdat_field_23 LIKE '{urllib.parse.quote(street_name)}'"
    
    def get_property_data(self, address: str) -> Dict[str, Any]:
        """Get property data for an address."""
        try:
            # Try the API call
            api_url = self.format_api_url(address)
            logger.info(f"Trying URL: {api_url}")
            
            response = self.session.get(api_url, timeout=self.settings.REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            # If no results, try fallback approach
            if len(data) == 0:
                # Extract address number and street name
                address_parts = address.strip().split(' ')
                if len(address_parts) >= 2:
                    address_number = address_parts[0]
                    street_name = address_parts[1]
                    
                    fallback_url = self.format_fallback_api_url(address_number, street_name)
                    logger.info(f"Trying fallback URL: {fallback_url}")
                    
                    fallback_response = self.session.get(fallback_url, timeout=self.settings.REQUEST_TIMEOUT)
                    fallback_response.raise_for_status()
                    data = fallback_response.json()
            
            if len(data) == 0:
                logger.warning(f"No data found for address: {address}")
                return {
                    "success": False,
                    "message": f"No data found for address: {address}"
                }
            
            # Process the API response
            return self._process_api_response(data[0], address)
            
        except Exception as e:
            logger.error(f"Error fetching data for address {address}: {e}")
            return {
                "success": False,
                "message": f"Error: {str(e)}"
            }
    
    def _process_api_response(self, api_response: Dict[str, Any], address: str) -> Dict[str, Any]:
        """Process the API response to extract and transform fields."""
        # Map API response to columns
        value_map = {}
        
        # First, directly map API fields
        for field, api_field in self.property_settings.FIELD_MAPPING.items():
            if api_field and api_field in api_response:
                value = api_response[api_field]
                
                # Apply transformations from CONFIG.COLUMN_TRANSFORMS
                if field == "BLOCK" or field == "LOT":
                    value = value.strip() if value else ""
                elif field in ["sale1", "sale2", "sale3", "sales_price"]:
                    value = int(value) if value else 0
                elif field == "above_ground_living_area":
                    value = int(value) if value else 0
                elif field == "land_size":
                    value = float(value) if value else 0
                
                value_map[field] = value
        
        # Apply derived fields
        value_map.update(self._calculate_derived_fields(api_response))
        
        return {
            "success": True,
            "address": address,
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
            derived_values["hundred_block"] = str(address_num)[0:len(str(address_num))-2] + "00"
        else:
            derived_values["hundred_block"] = ""
        
        # Calculate median_av - Gets total assessment value as integer
        #total_assessment = api_data.get("current_assessment_year_total_assessment_sdat_field_172", "0")
        #derived_values["median_av"] = int(total_assessment) if total_assessment and str(total_assessment).isdigit() else 0
        
        # Calculate MAPS - URL for Google Maps location
        if ("search_google_maps_for_this_location" in api_data and 
                isinstance(api_data["search_google_maps_for_this_location"], dict) and 
                "url" in api_data["search_google_maps_for_this_location"]):
            derived_values["MAPS"] = api_data["search_google_maps_for_this_location"]["url"]
        else:
            derived_values["MAPS"] = ""
        
        # Calculate SDAT - URL for real property search
        if ("real_property_search_link" in api_data and 
                isinstance(api_data["real_property_search_link"], dict) and 
                "url" in api_data["real_property_search_link"]):
            derived_values["SDAT"] = api_data["real_property_search_link"]["url"]
        else:
            derived_values["SDAT"] = ""
        
        # Calculate URLS - URL for finder online
        if ("finder_online_link" in api_data and 
                isinstance(api_data["finder_online_link"], dict) and 
                "url" in api_data["finder_online_link"]):
            derived_values["URLS"] = api_data["finder_online_link"]["url"]
        else:
            derived_values["URLS"] = ""
        
        return derived_values