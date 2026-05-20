from fastapi import APIRouter

from app.api.v1.router import router as v1_router
from app.core.config import settings

api_router = APIRouter()
# 统一挂载 v1 版本接口，后续升级到 v2 时可并行维护。
api_router.include_router(v1_router, prefix=settings.api_v1_prefix)
