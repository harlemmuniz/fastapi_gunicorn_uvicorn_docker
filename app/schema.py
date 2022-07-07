from pydantic import BaseModel
from typing import Optional


class DataLayer(BaseModel):
    hitType: Optional[str] = None
    page: Optional[str] = None
    clientId: Optional[str] = None
    eventCategory: Optional[str] = None
    eventAction: Optional[str] = None
    eventLabel: Optional[str] = None
    utmSource: Optional[str] = None
    utmMedium: Optional[str] = None
    utmCampaign: Optional[str] = None
    timestampGTM: Optional[str] = None
