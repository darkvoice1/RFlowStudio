from fastapi import FastAPI

from app.core.config import settings


def create_app() -> FastAPI:
    return FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        debug=settings.app_debug,
    )


app = create_app()

