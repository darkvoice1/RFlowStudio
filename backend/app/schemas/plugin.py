from datetime import datetime

from pydantic import BaseModel


class PluginResponse(BaseModel):
    id: str
    name: str
    version: str
    category: str
    entry_path: str
    status: str
    created_at: datetime
