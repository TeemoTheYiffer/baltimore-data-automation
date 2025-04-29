from pydantic import Field, ConfigDict
from pydantic_settings import BaseSettings
from typing import Optional, List, Dict, Any
from enum import Enum
import os

class CountyEnum(str, Enum):
    """Enum for supported counties."""
    BALTIMORE = "baltimore"
    PG = "pg"
    FREDERICK = "frederick"
    
    @classmethod
    def get_all(cls) -> List[str]:
        """Return all county values."""
        return [county.value for county in cls]


class ProcessingMode(str, Enum):
    """Enum for processing modes."""
    WATER = "water"
    PROPERTY = "property"
    BOTH = "both"
    ALL = "all"


class CountyConfig:
    """Configuration for a specific county data source."""
    
    def __init__(self, 
                county_name: str,
                base_url: str,
                identifier_type: str = "address",
                identifier_column: str = "ADDRESS",
                field_mapping: dict = None,
                parcel_digits: int = 0,
                spreadsheet_id: str = None):
        """Initialize county configuration."""
        self.county_name = county_name.lower()
        self.base_url = base_url
        self.identifier_type = identifier_type.lower()
        self.identifier_column = identifier_column
        self.field_mapping = field_mapping or {}
        self.parcel_digits = parcel_digits
        self.spreadsheet_id = spreadsheet_id

class AppConfig(BaseSettings):
    """Application configuration with improved handling of environment variables and files."""

    # County-specific spreadsheet IDs
    BALTIMORE_SPREADSHEET_ID: str = "1duAlmRNLRY_Ew0xZdd7erz4lFm_9Ku11hjx0xbILZzk"
    PG_SPREADSHEET_ID: str = "17yr2OwW4GrhgfUXLGTYOCXKlJov8hqe9Ozg_yOvNR74"
    FREDERICK_SPREADSHEET_ID: str = "1vcBR_gGoZCSmhbmA0CLrYH-2kc3O8P9ycUEokywqC20"
    
    # Current county being processed (defaults to baltimore)
    _current_county: str = "pg"
    
    # Runtime configuration
    LOG_LEVEL: str = "INFO"  # Can be DEBUG, INFO, WARNING, ERROR, CRITICAL
    VERBOSE_LOGGING: bool = False  # Set to True for detailed logs, False for summaries
    PROCESSING_MODE: ProcessingMode = ProcessingMode.PROPERTY
    COUNTIES: List[str] = Field(default_factory=lambda: ["frederick"])
    SHEET_NAME: Optional[str] = "LIENS"
    DEBUG_MODE: bool = False
    MAX_WORKERS: int = 10  # Maximum number of threads for concurrent requests
    MAX_RETRIES: int = 3  # Maximum retries for failed requests
    COUNTY_CONFIGS: Dict[str, CountyConfig] = Field(default_factory=dict)

    # Database/connection settings
    TCP_TIMEOUT: int = 300
    CACHE_ENABLED: bool = True
    CACHE_DIRECTORY: str = "cache"
    BATCH_RETRY_ATTEMPTS: int = 5
    
    # Service account settings
    SERVICE_ACCOUNT_FILE: Optional[str] = None
    IMPERSONATED_USER: Optional[str] = None
    
    # For debugging/testing
    DEBUG_SINGLE_ADDRESS: Optional[str] = None
    DEBUG_SINGLE_ACCOUNT: Optional[str] = None  
    DEBUG_SINGLE_PARCEL_ID: Optional[str] = None
    
    # Processing settings
    REQUEST_DELAY: float = 0.5
    START_ROW: int = 90
    STOP_ROW: int = 1000
    MAX_ROWS: int = 4035
    SKIP_ROW_RANGE: str = ""
    BATCH_SIZE: int = 1000
    
    # Water bill settings
    WATER_BILL_SHEET_NAME: str = "Water Bill"
    BASE_URL: str = "https://pay.baltimorecity.gov/water/"
    ACCOUNT_SEARCH_ENDPOINT: str = "_getInfoByAccountNumber"
    ADDRESS_SEARCH_ENDPOINT: str = "_getInfoByServiceAddress" 
    REQUEST_TIMEOUT: int = 30
    MAX_RETRIES: int = 3
    
    # Property settings
    PROPERTY_SHEET_NAME: str = "LIENS"
    BALTIMORE_URL: str = "https://opendata.maryland.gov/resource/3x3p-xk2v.json"
    PG_URL: str = "https://opendata.maryland.gov/resource/w3eb-4mzd.json"
    FREDERICK_URL: str = "https://opendata.maryland.gov/resource/gx8c-a963.json"
    RETRY_FAILED_ROWS: bool = True
    DELAY_BETWEEN_BATCHES: float = 2.0
    
    # Field mappings 
    FIELD_MAPPING: dict = {
        "ParcelID": "record_key_account_number_sdat_field_3",
        "ADDRESS": "mdp_street_address_mdp_field_address",
        "zip": "premise_address_zip_code_mdp_field_premzip_sdat_field_26",
        "address_number": "premise_address_number_mdp_field_premsnum_sdat_field_20",
        "street_name": "premise_address_name_mdp_field_premsnam_sdat_field_23",
        "lat": "mdp_latitude_mdp_field_digycord_converted_to_wgs84",
        "long": "mdp_longitude_mdp_field_digxcord_converted_to_wgs84",
        "above_ground_living_area": "c_a_m_a_system_data_structure_area_sq_ft_mdp_field_sqftstrc_sdat_field_241",
        "BLOCK": "block_mdp_field_block_sdat_field_40",
        "LOT": "lot_mdp_field_lot_sdat_field_41",
        "sales_price": "sales_segment_1_consideration_mdp_field_considr1_sdat_field_90",
        "sale1": "sales_segment_1_mkt_land_value_sdat_field_95",
        "sale_date1": "sales_segment_1_transfer_date_yyyy_mm_dd_mdp_field_tradate_sdat_field_89",
        "sale2": "sales_segment_2_mkt_land_value_sdat_field_115",
        "sale_date2": "sales_segment_2_transfer_date_yyyy_mm_dd_sdat_field_109",
        "sale3": "sales_segment_3_mkt_land_value_sdat_field_135",
        "sale_date3": "sales_segment_3_transfer_date_yyyy_mm_dd_sdat_field_129",
        "GRADE": "c_a_m_a_system_data_dwelling_grade_code_and_description_mdp_field_strugrad_strudesc_sdat_field_230",
        "USE": "land_use_code_mdp_field_lu_desclu_sdat_field_50",
        "OO": "record_key_owner_occupancy_code_mdp_field_ooi_sdat_field_6",
        "land_size": "c_a_m_a_system_data_land_area_mdp_field_landarea_sdat_field_242",
        "land_units": "additional_c_a_m_a_data_land_valuation_unit_sdat_field_266",
        "type1": "additional_c_a_m_a_data_dwelling_type_mdp_field_strubldg_sdat_field_265",
        "type2": "additional_c_a_m_a_data_building_style_code_and_description_mdp_field_strustyl_descstyl_sdat_field_264",
        "LEGAL DESCRIPTION 1": "legal_description_line_1_mdp_field_legal1_sdat_field_17",
        "FOLIO": "deed_reference_1_folio_mdp_field_dr1folio_sdat_field_31",
        "LIBER": "deed_reference_1_liber_mdp_field_dr1liber_sdat_field_30",
        "arms": "sales_segment_1_how_conveyed_ind_mdp_field_convey1_sdat_field_87",
        "SDAT": "", # Placeholder for SDAT field; See property_api.py for generation
        "Parcel": "", # Placeholder for Parcel field; See property_api.py for generation
        "VACANT LOT (Y)": "", # Placeholder for VACANT LOT field; See property_api.py for generation
        "Status": ""
    }
    
    model_config = ConfigDict(
        extra="ignore"
    )
    
    def __init__(self, **data):
        super().__init__(**data)
        self._init_county_configs()
        
    def _init_county_configs(self):
        """Initialize county configurations."""
        self.COUNTY_CONFIGS = {
            "baltimore": CountyConfig(
                county_name="baltimore",
                base_url=self.BALTIMORE_URL,
                identifier_type="address",
                identifier_column="ADDRESS",
                field_mapping=self.FIELD_MAPPING,
                parcel_digits=0,
                spreadsheet_id=self.BALTIMORE_SPREADSHEET_ID
            ),
            "pg": CountyConfig(
                county_name="pg",
                base_url=self.PG_URL,
                identifier_type="parcel_id",
                identifier_column="ParcelID",
                field_mapping=self.FIELD_MAPPING,
                parcel_digits=7,
                spreadsheet_id=self.PG_SPREADSHEET_ID
            ),
            "frederick": CountyConfig(
                county_name="frederick",
                base_url=self.FREDERICK_URL,
                identifier_type="parcel_id",
                identifier_column="ParcelID",
                field_mapping=self.FIELD_MAPPING,
                parcel_digits=6,
                spreadsheet_id=self.FREDERICK_SPREADSHEET_ID
            )
        }
    
    def get_county_config(self, county_name: str) -> CountyConfig:
        """Get county configuration by name (case-insensitive)."""
        county_name = county_name.lower()
        
        # Handle aliases for county names
        if county_name in ["prince_george", "prince georges", "pg_county"]:
            county_name = "pg"
        elif county_name in ["baltimore_city", "baltimore city"]:
            county_name = "baltimore"
        
        return self.COUNTY_CONFIGS.get(county_name, self.COUNTY_CONFIGS["baltimore"])
    
    def get_counties_to_process(self) -> List[str]:
        """Get list of counties to process based on configuration."""
        if "all" in self.COUNTIES:
            return CountyEnum.get_all()
        return self.COUNTIES
    
    @property
    def SPREADSHEET_ID(self):
        """Backward compatibility property that returns the spreadsheet ID for the current county"""
        return self.get_spreadsheet_id(self._current_county)
    
    def set_current_county(self, county_name: str):
        """Set the current county being processed"""
        self._current_county = county_name
    
    def get_spreadsheet_id(self, county_name: str = None) -> str:
        """Get spreadsheet ID for a specific county or the current county"""
        county = county_name or self._current_county
        
        # Normalize county name
        county = county.lower().strip()
        
        # Return the appropriate spreadsheet ID based on county
        if county in ["pg", "prince_george", "prince georges", "pg_county"]:
            return self.PG_SPREADSHEET_ID
        elif county in ["baltimore", "baltimore_city", "baltimore city"]:
            return self.BALTIMORE_SPREADSHEET_ID
        elif county in ["frederick"]:
            return self.FREDERICK_SPREADSHEET_ID
        
        # Default fallback
        return self.BALTIMORE_SPREADSHEET_ID