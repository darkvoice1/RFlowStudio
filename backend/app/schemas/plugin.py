"""插件节点协议模型。"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.workflow_node import WorkflowNodePortSchema


class PluginSchemaModel(BaseModel):
    """插件协议模型基类。"""

    model_config = ConfigDict(str_strip_whitespace=True)


class PluginManifestRecord(PluginSchemaModel):
    """单节点插件清单记录。"""

    id: str = Field(min_length=1)
    key: str = Field(min_length=1)
    name: str = Field(min_length=1)
    version: str = Field(min_length=1)
    category: str = Field(min_length=1)
    executor: str = Field(min_length=1)
    source: str = Field(min_length=1)
    plugin_path: str = Field(min_length=1)
    manifest_path: str = Field(min_length=1)
    description: str | None = None
    aliases: list[str] = Field(default_factory=list)
    executor_kind: str = Field(min_length=1, default="script")
    config_schema: dict[str, Any] = Field(default_factory=dict)
    input_schema: list[WorkflowNodePortSchema] = Field(default_factory=list)
    output_schema: list[WorkflowNodePortSchema] = Field(default_factory=list)
