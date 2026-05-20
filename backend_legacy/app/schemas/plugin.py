from datetime import datetime

from pydantic import BaseModel


class PluginResponse(BaseModel):
    id: str
    name: str
    version: str
    category: str
    entry_path: str
    executor: str
    status: str
    source: str
    created_at: datetime
    updated_at: datetime


class PluginDetailResponse(PluginResponse):
    plugin_path: str
    manifest_path: str


class PluginListResponse(BaseModel):
    items: list[PluginResponse]
    total: int


class PluginSyncResponse(BaseModel):
    items: list[PluginResponse]
    total: int


class PluginStatusUpdateRequest(BaseModel):
    status: str
