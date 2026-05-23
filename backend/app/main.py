"""新后端应用入口。"""

from fastapi import FastAPI

from app.api.v1 import router as api_v1_router
from app.core.config import settings


def create_app() -> FastAPI:
    """创建新后端应用实例。"""
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        debug=settings.app_debug,
    )
    app.include_router(api_v1_router, prefix=settings.api_v1_prefix)
    return app


app = create_app()
