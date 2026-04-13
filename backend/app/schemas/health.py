from typing import Literal

from pydantic import BaseModel


class HealthCheckResponse(BaseModel):
    """定义健康检查接口的响应结构。"""

    status: Literal["ok"]
    app_name: str
    version: str
    environment: str
