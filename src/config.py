from pydantic import Field, ConfigDict
from pydantic_settings import BaseSettings
from typing import Optional, List, Dict, TYPE_CHECKING
from enum import Enum
import logging

if TYPE_CHECKING:
    from web_utils.models import ProcessBatchRequestModel # noqa: F401

logger = logging.getLogger("config")

class CountyEnum(str, Enum):
    """Enum for supported counties."""

    BALTIMORE = "baltimore"
    BALTIMORE_CITY = "baltimore_city"
    PG = "pg"
    FREDERICK = "frederick"
    MONTGOMERY = "montgomery"  
    ALLEGANY = "allegany"      
    HARFORD = "harford"        
    DORCHESTER = "dorchester"
    ANNE_ARUNDEL = "anne_arundel"  
    HOWARD = "howard"              

    @classmethod
    def get_all(cls) -> List[str]:
        """Return all county values."""
        return [county.value for county in cls]

class CountyConfig:
    """Configuration for a specific county data source."""

    def __init__(
        self,
        county_name: str,
        base_url: str,
        identifier_type: str = "address",
        identifier_column: str = "ADDRESS",
        field_mapping: dict = None,
        parcel_digits: int = 0,
        spreadsheet_id: str = None,
        optional_params: dict = None,
    ):
        """Initialize county configuration."""
        self.county_name = county_name.lower()
        self.base_url = base_url
        self.identifier_type = identifier_type.lower()
        self.identifier_column = identifier_column
        self.field_mapping = field_mapping or {}
        self.parcel_digits = parcel_digits
        self.spreadsheet_id = spreadsheet_id
        self.optional_params = optional_params or {}

class AppConfig(BaseSettings):
    """Application configuration with improved handling of environment variables and files."""

    # Current county being processed (defaults to baltimore)
    _current_county: str = "baltimore"

    # Runtime configuration
    LOG_LEVEL: str = "INFO"  # Can be DEBUG, INFO, WARNING, ERROR, CRITICAL
    VERBOSE_LOGGING: bool = False  # Set to True for detailed logs, False for summaries
    PROCESSING_MODE: str = "property"
    SHEET_NAME: Optional[str] = "LIENS"
    DEBUG_MODE: bool = False  # For things like verbose API output, debug endpoints, etc.
    MAX_WORKERS: int = 10  # Maximum number of threads for concurrent requests
    MAX_RETRIES: int = 3  # Maximum retries for failed requests
    COUNTY_CONFIGS: Dict[str, CountyConfig] = Field(default_factory=dict)
    FORCE_REPROCESS: bool = False

    # Job-specific fields (set dynamically from API request)
    START_ROW: Optional[int] = Field(default=None)
    STOP_ROW: Optional[int] = Field(default=None) 
    MAX_ROWS: Optional[int] = Field(default=None)

    # Database/connection settings
    TCP_TIMEOUT: int = 300
    CACHE_ENABLED: bool = True
    CACHE_DIRECTORY: str = "cache"
    BATCH_RETRY_ATTEMPTS: int = 5

    # Service account settings
    SERVICE_ACCOUNT_FILE: Optional[str] = None
    IMPERSONATED_USER: Optional[str] = None

    # Processing settings
    REQUEST_DELAY: float = 0.5  # Increased from 0.3 to help avoid rate limiting
    BATCH_SIZE: int = 100

    # Water bill settings
    WATER_BILL_SHEET_NAME: str = "Water Bill"
    BASE_URL: str = "https://pay.baltimorecity.gov/water/"
    ACCOUNT_SEARCH_ENDPOINT: str = "_getInfoByAccountNumber"
    ADDRESS_SEARCH_ENDPOINT: str = "_getInfoByServiceAddress"
    REQUEST_TIMEOUT: int = 30
    MAX_RETRIES: int = 3

    # Property settings (Use '$limit=5' to limit data and $where=record_key_account_number_sdat_field_3 LIKE "" to search ParcelIDs)
    PROPERTY_SHEET_NAME: str = "LIENS"
    BALTIMORE_URL: str = "https://opendata.maryland.gov/resource/jpfc-qkxp.json"  # Baltimore County
    BALTIMORE_CITY_URL: str = "https://opendata.maryland.gov/resource/3x3p-xk2v.json"  # Baltimore City
    PG_URL: str = "https://opendata.maryland.gov/resource/w3eb-4mzd.json"
    FREDERICK_URL: str = "https://opendata.maryland.gov/resource/gx8c-a963.json"
    MONTGOMERY_URL: str = "https://opendata.maryland.gov/resource/kb22-is2w.json"
    ALLEGANY_URL: str = "https://opendata.maryland.gov/resource/gm7d-x6vg.json"
    HARFORD_URL: str = "https://opendata.maryland.gov/resource/ygxu-5v84.json"
    DORCHESTER_URL: str = "https://opendata.maryland.gov/resource/ye3m-tr66.json"
    ANNE_ARUNDEL_URL: str = "https://opendata.maryland.gov/resource/3w75-7rie.json"
    HOWARD_URL: str = "https://opendata.maryland.gov/resource/9t52-zebk.json"

    RETRY_FAILED_ROWS: bool = True
    DELAY_BETWEEN_BATCHES: float = 2.0

    # Field mappings
    FIELD_MAPPING: dict = {
        "ParcelID": "record_key_account_number_sdat_field_3",
        "District": "record_key_district_ward_sdat_field_2",
        "ADDRESS": "mdp_street_address_mdp_field_address",
        "ADDRESS_FALLBACK_NAME1": "premise_address_name_mdp_field_premsnam_sdat_field_23",  
        "ADDRESS_FALLBACK_NAME2": "premise_address_type_mdp_field_premstyp_sdat_field_24",
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
        "SDAT": "",  # Placeholder for SDAT field; See property_api.py for generation
        "Parcel": "",  # Placeholder for Parcel field; See property_api.py for generation
        "VACANT LOT (Y)": "",  # Placeholder for VACANT LOT field; See property_api.py for generation
        "Status": "",
    }

    model_config = ConfigDict(extra="ignore")

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
                spreadsheet_id=None,
            ),
            "baltimore_city": CountyConfig(
                county_name="baltimore_city",
                base_url=self.BALTIMORE_CITY_URL,
                identifier_type="address",
                identifier_column="ADDRESS",
                field_mapping=self.FIELD_MAPPING,
                spreadsheet_id=None,
            ),
            "pg": CountyConfig(
                county_name="pg",
                base_url=self.PG_URL,
                identifier_type="parcel_id",
                identifier_column="ParcelID",
                field_mapping=self.FIELD_MAPPING,
                spreadsheet_id=None,
            ),
            "frederick": CountyConfig(
                county_name="frederick",
                base_url=self.FREDERICK_URL,
                identifier_type="parcel_id",
                identifier_column="ParcelID",
                field_mapping=self.FIELD_MAPPING,
                spreadsheet_id=None,
            ),
            "montgomery": CountyConfig(
                county_name="montgomery",
                base_url=self.MONTGOMERY_URL,
                identifier_type="parcel_id",
                identifier_column="ParcelID",
                field_mapping=self.FIELD_MAPPING,
                spreadsheet_id=None,
            ),
            "allegany": CountyConfig(
                county_name="allegany",
                base_url=self.ALLEGANY_URL,
                identifier_type="parcel_id",
                identifier_column="ParcelID",
                field_mapping=self.FIELD_MAPPING,
                spreadsheet_id=None,
            ),
            "harford": CountyConfig(
                county_name="harford",
                base_url=self.HARFORD_URL,
                identifier_type="parcel_id",
                identifier_column="ParcelID",
                field_mapping=self.FIELD_MAPPING,
                spreadsheet_id=None,
            ),
            "dorchester": CountyConfig(
                county_name="dorchester",
                base_url=self.DORCHESTER_URL,
                identifier_type="parcel_id",
                identifier_column="ParcelID",
                field_mapping=self.FIELD_MAPPING,
                spreadsheet_id=None,
            ),
            "anne_arundel": CountyConfig(
                county_name="anne_arundel",
                base_url=self.ANNE_ARUNDEL_URL,
                identifier_type="parcel_id",
                identifier_column="ParcelID",
                field_mapping=self.FIELD_MAPPING,
                spreadsheet_id=None,
            ),
            "howard": CountyConfig(
                county_name="howard",
                base_url=self.HOWARD_URL,
                identifier_type="parcel_id",
                identifier_column="ParcelID",
                field_mapping=self.FIELD_MAPPING,
                spreadsheet_id=None,
            ),
        }
    
    @classmethod  
    def create_job_config_from_request(cls, request) -> "AppConfig":
        """Create a job-specific configuration that honors API request parameters."""
        job_config = cls()
        
        # Single county setup  
        job_config.set_current_county(request.county)
        job_config.PROCESSING_MODE = request.mode
        
        # Set spreadsheet for this county
        county_config = job_config.get_county_config(request.county)
        county_config.spreadsheet_id = request.spreadsheet_id
        logger.info(f"Job config: Using spreadsheet_id {request.spreadsheet_id} for {request.county}")
        
        # NEW: Override identifier settings from request if provided
        if hasattr(request, 'identifier_type') and request.identifier_type:
            county_config.identifier_type = request.identifier_type
            logger.info(f"Overriding identifier_type to '{request.identifier_type}' for {request.county}")
        
        if hasattr(request, 'identifier_column') and request.identifier_column:
            county_config.identifier_column = request.identifier_column
            logger.info(f"Overriding identifier_column to '{request.identifier_column}' for {request.county}")
        
        # Set parcel_digits from request, but now check the current (possibly overridden) identifier_type
        if hasattr(request, 'parcel_digits'):
            # Only set parcel_digits for counties that use parcel_id identifier
            if county_config.identifier_type == "parcel_id":
                county_config.parcel_digits = request.parcel_digits
                logger.info(f"Set parcel_digits to {request.parcel_digits} for {request.county}")
            else:
                logger.info(f"Ignoring parcel_digits for {request.county} (uses {county_config.identifier_type} identifier)")

        # Handle optional_params from request
        if hasattr(request, 'optional_params') and request.optional_params:
            county_config.optional_params = request.optional_params
            logger.info(f"Set optional_params for {request.county}: {request.optional_params}")
        else:
            county_config.optional_params = {}

        # Set parameters from request
        job_config.SHEET_NAME = getattr(request, 'sheet_name', 'LIENS')
        job_config.START_ROW = getattr(request, 'start_row', 2)
        job_config.MAX_ROWS = getattr(request, 'max_rows', 100)
        job_config.FORCE_REPROCESS = getattr(request, 'force_reprocess', False)
        job_config.BATCH_SIZE = getattr(request, 'batch_size', 1000)
        
        # Smart stop_row logic
        requested_stop_row = getattr(request, 'stop_row', 0)
        if requested_stop_row and requested_stop_row > 0:
            job_config.STOP_ROW = requested_stop_row
            job_config._stop_row_was_set = True
            logger.info(f"Using explicit stop_row: {requested_stop_row}")
        else:
            job_config.STOP_ROW = 0
            job_config._stop_row_was_set = False
            logger.info(f"Using max_rows logic: start_row={job_config.START_ROW} + max_rows={job_config.MAX_ROWS}")

        return job_config

    def get_county_config(self, county_name: str) -> CountyConfig:
        """Get county configuration by name (case-insensitive)."""
        county_name = county_name.lower()

        # Handle aliases for county names
        if county_name in ["prince_george", "prince georges", "pg_county"]:
            county_name = "pg"
        elif county_name in ["baltimore city"]:
            county_name = "baltimore_city"
        elif county_name in ["baltimore_county", "baltimore county"]:
            county_name = "baltimore"

        if county_name not in self.COUNTY_CONFIGS:
            valid_counties = list(self.COUNTY_CONFIGS.keys())
            raise ValueError(f"Unsupported county '{county_name}'. Valid counties are: {valid_counties}")
        
        config = self.COUNTY_CONFIGS[county_name]
        logger.info(f"County config for {county_name}: identifier_type={config.identifier_type}, identifier_column={config.identifier_column}")
        return config

    def set_current_county(self, county_name: str):
        """Set the current county being processed"""
        self._current_county = county_name