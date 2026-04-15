from pathlib import Path
from typing import Literal
from urllib.parse import quote_plus

from pydantic_settings import BaseSettings, SettingsConfigDict

BACKEND_DIR = Path(__file__).resolve().parents[2]
PROJECT_ROOT = BACKEND_DIR.parent


class Settings(BaseSettings):
    """集中管理应用运行时配置。"""

    app_name: str = "Open R Platform API"
    app_version: str = "0.1.0"
    app_env: Literal["development", "test", "production"] = "development"
    app_debug: bool = True
    api_v1_prefix: str = "/api/v1"
    docs_url: str = "/docs"
    redoc_url: str = "/redoc"
    openapi_url: str = "/openapi.json"
    storage_root: Path = PROJECT_ROOT / "storage"
    upload_root: Path = PROJECT_ROOT / "storage" / "uploads"
    dataset_metadata_root: Path = PROJECT_ROOT / "storage" / "datasets"
    max_upload_size_mb: int = 100
    default_preview_rows: int = 20
    max_preview_rows: int = 100
    database_driver: str = "postgresql+psycopg"
    database_host: str = "127.0.0.1"
    database_port: int = 5432
    database_name: str = "rflowstudio"
    database_user: str = "postgres"
    database_password: str = "postgres"

    model_config = SettingsConfigDict(
        env_file=BACKEND_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def database_url(self) -> str:
        """根据当前配置拼装 PostgreSQL 连接地址。"""
        if self.database_driver.startswith("sqlite"):
            database_name = str(self.database_name)
            return f"{self.database_driver}:///{database_name}"

        # 用户名和密码可能包含特殊字符，这里先做 URL 转义，避免连接串解析出错。
        encoded_user = quote_plus(self.database_user)
        encoded_password = quote_plus(self.database_password)
        return (
            f"{self.database_driver}://{encoded_user}:{encoded_password}"
            f"@{self.database_host}:{self.database_port}/{self.database_name}"
        )


# 在模块导入时加载配置，供全局复用。
settings = Settings()
