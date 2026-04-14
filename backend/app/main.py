from fastapi import FastAPI

from app.api.router import api_router
from app.core.config import settings


def create_app() -> FastAPI:
    """创建并配置 FastAPI 应用实例。"""
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        debug=settings.app_debug,
        docs_url=settings.docs_url,
        redoc_url=settings.redoc_url,
        openapi_url=settings.openapi_url,
    )

    # 启动时确保运行期目录已经存在，后续上传文件可直接落盘。
    settings.storage_root.mkdir(parents=True, exist_ok=True)
    settings.upload_root.mkdir(parents=True, exist_ok=True)
    settings.dataset_metadata_root.mkdir(parents=True, exist_ok=True)
    settings.dataset_cleaning_root.mkdir(parents=True, exist_ok=True)

    # 统一挂载 API 路由，避免入口文件堆积接口定义。
    app.include_router(api_router)
    return app


# 暴露给 uvicorn 的应用对象。
app = create_app()
