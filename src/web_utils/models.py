from pydantic import BaseModel, Field
from typing import Optional, Dict, List
from enum import Enum

class ProcessingMode(str, Enum):
    """Enum for processing modes."""
    WATER = "water"
    PROPERTY = "property"
    BOTH = "both"
    ALL = "all"


class NJProcessingMode(str, Enum):
    """Enum for NJ processing modes."""
    PROPERTY = "property"

class PropertyRequestModel(BaseModel):
    address: Optional[str] = None
    parcel_id: Optional[str] = None
    county: str = "baltimore"
    optional_params: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional parameters to add to the search query. Keys should match FIELD_MAPPING names (e.g., 'District')"
    )

class PropertySampleRequestModel(BaseModel):
    county: str = "baltimore"

class WaterBillRequestModel(BaseModel):
    address: Optional[str] = None
    account_number: Optional[str] = None


class ProcessBatchRequestModel(BaseModel):
    county: str
    mode: ProcessingMode = Field(default=ProcessingMode.PROPERTY)
    spreadsheet_id: str 
    sheet_name: str = "LIENS"
    identifier_type: Optional[str] = Field(default="parcel_id")
    identifier_column: Optional[str] = Field(default="ParcelID")
    force_reprocess: bool = Field(default=False)  # Process even if status is "Success"
    start_row: Optional[int] = Field(default=None)
    stop_row: Optional[int] = Field(default=0)  # 0 = use max_rows logic
    max_rows: Optional[int] = Field(default=None)
    parcel_digits: int = Field(default=6) # Amount of digits in the parcel ID
    batch_size: int = Field(default=100)  # Amount of rows to process per batch
    optional_params: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional parameters to add to the search query. Keys should match FIELD_MAPPING names (e.g., 'District')"
    )

class StatusResponse(BaseModel):
    job_id: str
    status: str
    message: str
    progress: Optional[int] = 0
    errors: Optional[List[str]] = []
    error_count: Optional[int] = 0
    success_count: Optional[int] = 0
    total_processed: Optional[int] = 0

class SheetsRequestModel(BaseModel):
    spreadsheet_id: str


# ============== NJ-specific models ==============

class NJPropertyRequestModel(BaseModel):
    """Request model for single NJ property lookup."""
    block: str = Field(..., description="Block number")
    lot: str = Field(..., description="Lot number")
    qual: Optional[str] = Field(default=None, description="Qualifier (optional)")
    county: str = Field(default="ocean", description="NJ county name")
    municipality: str = Field(default="stafford", description="Township/municipality name")


class NJPropertySampleRequestModel(BaseModel):
    """Request model for NJ sample property."""
    county: str = Field(default="ocean", description="NJ county name")
    municipality: str = Field(default="stafford", description="Township/municipality name")


class NJBatchRequestModel(BaseModel):
    """Request model for NJ batch processing."""
    county: str = Field(default="ocean", description="NJ county name")
    municipality: str = Field(..., description="Township/municipality name (e.g., 'stafford')")
    spreadsheet_id: str = Field(..., description="Google Spreadsheet ID")
    sheet_name: str = Field(default="LIENS", description="Sheet name to process")
    block_column: str = Field(default="Block", description="Column header for Block numbers")
    lot_column: str = Field(default="Lot", description="Column header for Lot numbers")
    qual_column: Optional[str] = Field(default="Qual", description="Column header for Qualifiers (optional)")
    start_row: int = Field(default=2, description="First row to process (1-indexed, after header)")
    stop_row: int = Field(default=0, description="Last row to process (0 = use max_rows)")
    max_rows: int = Field(default=100, description="Maximum rows to process")
    batch_size: int = Field(default=100, description="Rows per batch update")
    force_reprocess: bool = Field(default=False, description="Reprocess rows with Status='Success'")


class NJMunicipalitiesResponseModel(BaseModel):
    """Response model for supported NJ municipalities."""
    county: Optional[str] = None
    municipalities: Dict[str, Dict[str, str]]
