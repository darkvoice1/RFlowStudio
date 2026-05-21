"""新后端配置入口。"""

from pathlib import Path
from typing import Literal
from urllib.parse import quote_plus

from pydantic_settings import BaseSettings, SettingsConfigDict

BACKEND_DIR = Path(__file__).resolve().parents[2]
PROJECT_ROOT = BACKEND_DIR.parent


class Settings(BaseSettings):
    """集中管理新后端当前阶段需要的最小配置。"""

    app_name: str = "RFlowStudio Backend"
    app_version: str = "0.1.0"
    app_env: Literal["development", "test", "production"] = "development"
    app_debug: bool = True
    api_v1_prefix: str = "/api/v1"
    database_driver: str = "sqlite+pysqlite"
    database_host: str = "127.0.0.1"
    database_port: int = 5432
    database_name: str = (PROJECT_ROOT / "storage" / "rflowstudio.db").as_posix()
    database_user: str = "postgres"
    database_password: str = "postgres"

    model_config = SettingsConfigDict(
        env_file=BACKEND_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def database_url(self) -> str:
        """根据当前配置生成数据库连接地址。"""
        if self.database_driver.startswith("sqlite"):
            return f"{self.database_driver}:///{self.database_name}"

        encoded_user = quote_plus(self.database_user)
        encoded_password = quote_plus(self.database_password)
        return (
            f"{self.database_driver}://{encoded_user}:{encoded_password}"
            f"@{self.database_host}:{self.database_port}/{self.database_name}"
        )


settings = Settings()
