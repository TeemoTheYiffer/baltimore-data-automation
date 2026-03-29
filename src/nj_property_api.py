"""
NJ Property API Client for ArcGIS REST Services.

This module provides access to New Jersey property data via the NJOGIS
ArcGIS FeatureServer, specifically the Parcels and MOD-IV Composite dataset.
"""

import logging
import requests
import urllib.parse
from typing import Dict, Any, Optional, List
import time
import random

logger = logging.getLogger("nj_property")


# NJ Municipality codes for supported townships
NJ_MUNICIPALITIES = {
    "ocean": {
        "stafford": "1531",
        "barnegat": "1503",
        "beach_haven": "1504",
        "beachwood": "1505",
        "berkeley": "1506",
        "brick": "1507",
        "dover": "1510",  # Toms River
        "eagleswood": "1511",
        "harvey_cedars": "1513",
        "island_heights": "1514",
        "jackson": "1512",
        "lacey": "1515",
        "lakehurst": "1516",
        "lakewood": "1517",
        "lavallette": "1518",
        "little_egg_harbor": "1519",
        "long_beach": "1520",
        "manchester": "1521",
        "mantoloking": "1522",
        "ocean": "1523",
        "ocean_gate": "1524",
        "pine_beach": "1525",
        "plumsted": "1526",
        "point_pleasant": "1527",
        "point_pleasant_beach": "1528",
        "seaside_heights": "1529",
        "seaside_park": "1530",
        "ship_bottom": "1532",
        "south_toms_river": "1533",
        "surf_city": "1534",
        "tuckerton": "1535",
    },
    "gloucester": {
        "clayton": "0801",
        "deptford": "0802",
        "east_greenwich": "0803",
        "elk": "0804",
        "franklin": "0805",
        "glassboro": "0806",
        "greenwich": "0807",
        "harrison": "0808",
        "logan": "0809",
        "mantua": "0810",
        "monroe": "0811",
        "national_park": "0812",
        "newfield": "0813",
        "paulsboro": "0814",
        "pitman": "0815",
        "south_harrison": "0816",
        "swedesboro": "0817",
        "washington": "0818",
        "wenonah": "0819",
        "west_deptford": "0820",
        "westville": "0821",
        "woodbury": "0822",
        "woodbury_heights": "0823",
        "woolwich": "0824",
    },
    "camden": {
        "audubon": "0401",
        "audubon_park": "0402",
        "barrington": "0403",
        "bellmawr": "0404",
        "berlin": "0405",
        "berlin_twp": "0406",
        "brooklawn": "0407",
        "camden": "0408",
        "cherry_hill": "0409",
        "chesilhurst": "0410",
        "clementon": "0411",
        "collingswood": "0412",
        "gibbsboro": "0413",
        "gloucester_city": "0414",
        "gloucester_twp": "0415",
        "haddon_twp": "0416",
        "haddonfield": "0417",
        "haddon_heights": "0418",
        "hi_nella": "0419",
        "laurel_springs": "0420",
        "lawnside": "0421",
        "lindenwold": "0422",
        "magnolia": "0423",
        "merchantville": "0424",
        "mount_ephraim": "0425",
        "oaklyn": "0426",
        "pennsauken": "0427",
        "pine_hill": "0428",
        "pine_valley": "0429",
        "runnemede": "0430",
        "somerdale": "0431",
        "stratford": "0432",
        "tavistock": "0433",
        "voorhees": "0434",
        "waterford": "0435",
        "winslow": "0436",
        "woodlynne": "0437",
    },
}

# Field mapping from NJ ArcGIS fields to spreadsheet columns
NJ_FIELD_MAPPING = {
    # Parcel identification
    "PAMS_PIN": "PAMS_PIN",
    "Block": "PCLBLOCK",
    "Lot": "PCLLOT",
    "Qual": "PCLQCODE",

    # Address/Location
    "Address": "PROP_LOC",
    "MailingAddress": "ST_ADDRESS",
    "MailingCityState": "CITY_STATE",
    "ZipCode": "ZIP_CODE",
    "Zip5": "ZIP5",

    # Ownership (note: may be redacted due to Daniel's Law)
    "Owner": "OWNER_NAME",

    # Property classification
    "PropertyClass": "PROP_CLASS",
    "PropertyUse": "PROP_USE",
    "BuildingClass": "BLDG_CLASS",

    # Valuation
    "LandValue": "LAND_VAL",
    "ImprovementValue": "IMPRVT_VAL",
    "TotalAssessed": "NET_VALUE",
    "LastYearTax": "LAST_YR_TX",

    # Building information
    "BuildingDesc": "BLDG_DESC",
    "YearBuilt": "YR_CONSTR",
    "Dwellings": "DWELL",
    "CommDwellings": "COMM_DWELL",

    # Land information
    "LandDesc": "LAND_DESC",
    "Acreage": "CALC_ACRE",

    # Additional details
    "County": "COUNTY",
    "Municipality": "MUN_NAME",
    "DeedBook": "DEED_BOOK",
    "DeedPage": "DEED_PAGE",
    "DeedDate": "DEED_DATE",
    "SalePrice": "SALE_PRICE",
    "SalesCode": "SALES_CODE",

    # Computed/Status fields (populated by code)
    "Status": "",
    "VacantLot": "",
    "GIS_Link": "",
}

# Reverse mapping for API queries
NJ_API_FIELDS = {v: k for k, v in NJ_FIELD_MAPPING.items() if v}


class NJPropertyAPI:
    """Client for NJ Property Data via ArcGIS REST API."""

    # Base URL for NJOGIS Parcels FeatureServer
    BASE_URL = "https://services2.arcgis.com/XVOqAjTOJ5P6ngMu/arcgis/rest/services/Parcels_Composite_NJ_WM/FeatureServer/0/query"

    # Fields to retrieve from the API
    OUT_FIELDS = [
        "PAMS_PIN", "PCLBLOCK", "PCLLOT", "PCLQCODE", "PCL_MUN", "MUN_NAME", "COUNTY",
        "PROP_LOC", "ST_ADDRESS", "CITY_STATE", "ZIP_CODE", "ZIP5",
        "OWNER_NAME", "PROP_CLASS", "PROP_USE", "BLDG_CLASS",
        "LAND_VAL", "IMPRVT_VAL", "NET_VALUE", "LAST_YR_TX",
        "BLDG_DESC", "YR_CONSTR", "DWELL", "COMM_DWELL",
        "LAND_DESC", "CALC_ACRE",
        "DEED_BOOK", "DEED_PAGE", "DEED_DATE", "SALE_PRICE", "SALES_CODE"
    ]

    def __init__(self, county: str = "ocean", municipality: str = "stafford"):
        """
        Initialize the NJ property data API client.

        Args:
            county: NJ county name (default: "ocean")
            municipality: Township/municipality name (default: "stafford")
        """
        self.county = county.lower()
        self.municipality = municipality.lower().replace(" ", "_").replace("-", "_")

        # Get municipality code
        if self.county not in NJ_MUNICIPALITIES:
            raise ValueError(f"Unsupported NJ county: {self.county}. Supported: {list(NJ_MUNICIPALITIES.keys())}")

        if self.municipality not in NJ_MUNICIPALITIES[self.county]:
            raise ValueError(
                f"Unsupported municipality '{self.municipality}' in {self.county} county. "
                f"Supported: {list(NJ_MUNICIPALITIES[self.county].keys())}"
            )

        self.mun_code = NJ_MUNICIPALITIES[self.county][self.municipality]

        # Setup session with browser-like headers
        # Note: Don't include 'br' (Brotli) in Accept-Encoding as requests doesn't support it natively
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
        })

        logger.info(f"Initialized NJ Property API for {self.municipality} ({self.mun_code}) in {self.county} county")

    def _make_request_with_retry(self, url: str, description: str, max_retries: int = 3) -> Optional[Dict]:
        """
        Make HTTP request with retry logic.

        Args:
            url: The URL to request
            description: Description for logging
            max_retries: Maximum retry attempts

        Returns:
            JSON response data or None if all retries failed
        """
        for attempt in range(max_retries):
            try:
                logger.debug(f"Attempting {description} (attempt {attempt + 1}): {url}")

                response = self.session.get(url, timeout=30)

                if response.status_code == 500:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) + random.uniform(0.1, 0.5)
                        logger.warning(f"500 error on {description}, retrying in {wait_time:.2f}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"500 error persists on {description} after {max_retries} attempts")
                        return None

                response.raise_for_status()
                data = response.json()

                # Check for ArcGIS error response
                if "error" in data:
                    error_msg = data["error"].get("message", "Unknown ArcGIS error")
                    logger.error(f"ArcGIS API error: {error_msg}")
                    return None

                if attempt > 0:
                    logger.info(f"Successfully retrieved {description} after {attempt + 1} attempts")

                return data

            except requests.exceptions.HTTPError as e:
                if hasattr(e, 'response') and e.response.status_code == 500 and attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + random.uniform(0.1, 0.5)
                    logger.warning(f"HTTP 500 error on {description}, retrying in {wait_time:.2f}s")
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
                if attempt < max_retries - 1:
                    time.sleep(1)

        return None

    def _build_query_url(self, block: str, lot: str, qual: Optional[str] = None) -> str:
        """
        Build the ArcGIS query URL for a specific block/lot.

        Args:
            block: Block number
            lot: Lot number
            qual: Qualifier (optional)

        Returns:
            Formatted query URL
        """
        # Build WHERE clause
        where_parts = [
            f"PCL_MUN='{self.mun_code}'",
            f"PCLBLOCK='{block}'",
            f"PCLLOT='{lot}'"
        ]

        if qual:
            where_parts.append(f"PCLQCODE='{qual}'")

        where_clause = " AND ".join(where_parts)

        # Build full URL
        params = {
            "where": where_clause,
            "outFields": ",".join(self.OUT_FIELDS),
            "returnGeometry": "false",
            "f": "json"
        }

        query_string = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
        return f"{self.BASE_URL}?{query_string}"

    def get_property_data(self, block: str, lot: str, qual: Optional[str] = None) -> Dict[str, Any]:
        """
        Get property data for a specific block/lot in the configured municipality.

        Args:
            block: Block number
            lot: Lot number
            qual: Qualifier (optional)

        Returns:
            Dictionary with success status and property data
        """
        # Clean inputs
        block = str(block).strip()
        lot = str(lot).strip()
        qual = str(qual).strip() if qual else None

        identifier = f"{block}/{lot}" + (f"/{qual}" if qual else "")

        try:
            # Build and execute query
            url = self._build_query_url(block, lot, qual)
            logger.info(f"Querying NJ property: Block {block}, Lot {lot}" + (f", Qual {qual}" if qual else ""))
            logger.debug(f"Query URL: {url}")

            data = self._make_request_with_retry(url, f"property query for {identifier}")

            if data is None:
                return {
                    "success": False,
                    "message": f"API request failed for Block {block}, Lot {lot}",
                    "identifier": identifier
                }

            # Check if we got results
            features = data.get("features", [])

            if not features:
                # Try without qualifier if one was provided
                if qual:
                    logger.info(f"No results with qualifier, trying without qual for {identifier}")
                    url = self._build_query_url(block, lot, None)
                    data = self._make_request_with_retry(url, f"fallback query for {identifier}")

                    if data:
                        features = data.get("features", [])

                if not features:
                    logger.warning(f"No data found for Block {block}, Lot {lot}")
                    return {
                        "success": False,
                        "message": f"No property found for Block {block}, Lot {lot}",
                        "identifier": identifier
                    }

            # Process the first result
            return self._process_api_response(features[0], identifier, block, lot, qual)

        except Exception as e:
            logger.error(f"Error fetching data for {identifier}: {e}")
            return {
                "success": False,
                "message": f"Error: {str(e)}",
                "identifier": identifier
            }

    def _process_api_response(
        self,
        feature: Dict[str, Any],
        identifier: str,
        block: str,
        lot: str,
        qual: Optional[str]
    ) -> Dict[str, Any]:
        """
        Process the ArcGIS API response to extract and transform fields.

        Args:
            feature: ArcGIS feature object
            identifier: Original identifier string
            block: Block number
            lot: Lot number
            qual: Qualifier

        Returns:
            Processed result dictionary
        """
        attributes = feature.get("attributes", {})
        value_map = {}

        # Map API fields to output columns
        for output_field, api_field in NJ_FIELD_MAPPING.items():
            if api_field and api_field in attributes:
                value = attributes[api_field]

                # Apply transformations
                if output_field in ["LandValue", "ImprovementValue", "TotalAssessed", "SalePrice", "LastYearTax"]:
                    value = int(value) if value else 0
                elif output_field == "Acreage":
                    value = float(value) if value else 0.0
                elif output_field == "YearBuilt":
                    value = int(value) if value and value != 0 else None
                elif isinstance(value, str):
                    value = value.strip()

                value_map[output_field] = value

        # Calculate derived fields
        value_map.update(self._calculate_derived_fields(attributes))

        # Build GIS map link using PAMS_PIN
        pams_pin = attributes.get("PAMS_PIN", "")
        if pams_pin:
            value_map["GIS_Link"] = (
                f"https://newjersey.maps.arcgis.com/apps/webappviewer/index.html"
                f"?id=3a4290e1b3d64094a8b8a127965ab43a&find={urllib.parse.quote(str(pams_pin))}"
            )

        # Add the original identifiers
        value_map["Block"] = block
        value_map["Lot"] = lot
        if qual:
            value_map["Qual"] = qual

        return {
            "success": True,
            "identifier": identifier,
            "identifier_type": "block_lot",
            "data": value_map
        }

    def _calculate_derived_fields(self, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate derived fields based on API data."""
        derived = {}

        # Determine if vacant lot based on improvement value
        improvement_value = attributes.get("IMPRVT_VAL", 0)
        try:
            imp_val = int(improvement_value) if improvement_value else 0
        except (ValueError, TypeError):
            imp_val = 0

        derived["VacantLot"] = "Y" if imp_val == 0 else "N"

        # Status field
        derived["Status"] = "Success"

        return derived

    def get_sample_property(self) -> Dict[str, Any]:
        """Get a sample property from the municipality to demonstrate data structure."""
        try:
            # Build a simple query to get one property
            params = {
                "where": f"PCL_MUN='{self.mun_code}'",
                "outFields": ",".join(self.OUT_FIELDS),
                "returnGeometry": "false",
                "resultRecordCount": "1",
                "f": "json"
            }

            query_string = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
            url = f"{self.BASE_URL}?{query_string}"

            logger.info(f"Fetching sample property from {self.municipality}, {self.county} county")

            data = self._make_request_with_retry(url, "sample property query")

            if data is None:
                return {
                    "success": False,
                    "message": f"Failed to fetch sample data from {self.municipality}"
                }

            features = data.get("features", [])
            if not features:
                return {
                    "success": False,
                    "message": f"No sample data available from {self.municipality}"
                }

            return {
                "success": True,
                "county": self.county,
                "municipality": self.municipality,
                "mun_code": self.mun_code,
                "sample_data": features[0].get("attributes", {}),
                "message": f"Sample property from {self.municipality}, {self.county} county"
            }

        except Exception as e:
            logger.error(f"Error fetching sample property: {e}")
            return {
                "success": False,
                "message": f"Error: {str(e)}"
            }

    @staticmethod
    def get_supported_municipalities(county: str = None) -> Dict[str, Any]:
        """
        Get list of supported municipalities.

        Args:
            county: Optional county filter

        Returns:
            Dictionary of supported municipalities by county
        """
        if county:
            county = county.lower()
            if county in NJ_MUNICIPALITIES:
                return {county: NJ_MUNICIPALITIES[county]}
            else:
                return {"error": f"County '{county}' not supported"}
        return NJ_MUNICIPALITIES

    @staticmethod
    def get_field_mappings() -> Dict[str, str]:
        """Get the field mapping dictionary."""
        return NJ_FIELD_MAPPING.copy()
