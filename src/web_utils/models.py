from pydantic import BaseModel, Field
from typing import List, Optional
from config import ProcessingMode

class PropertyRequestModel(BaseModel):
    address: Optional[str] = None
    parcel_id: Optional[str] = None
    county: str = "baltimore"


class WaterBillRequestModel(BaseModel):
    address: Optional[str] = None
    account_number: Optional[str] = None


class ProcessBatchRequestModel(BaseModel):
    counties: List[str] = Field(default=["baltimore"])
    mode: ProcessingMode = Field(default=ProcessingMode.PROPERTY)
    spreadsheet_id: Optional[str] = Field(default=None)
    sheet_name: Optional[str] = Field(default="LIENS")
    start_row: Optional[int] = Field(default=None)
    stop_row: Optional[int] = Field(default=None)
    max_rows: Optional[int] = Field(default=None)


class StatusResponse(BaseModel):
    job_id: str
    status: str
    message: str