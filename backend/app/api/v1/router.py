from fastapi import APIRouter

from app.api.v1.health import router as health_router

router = APIRouter()
# 按功能聚合 v1 接口模块，方便后续继续追加数据集接口。
router.include_router(health_router, tags=["health"])
