from pathlib import Path

import pytest

from app.core.config import settings
from app.db.session import dispose_database_engine, initialize_database


@pytest.fixture(autouse=True)
def isolate_database(tmp_path: Path) -> None:
    """为每个测试隔离 SQLite 数据库文件。"""
    original_database_driver = settings.database_driver
    original_database_name = settings.database_name

    settings.database_driver = "sqlite+pysqlite"
    settings.database_name = (tmp_path / "test.db").as_posix()
    dispose_database_engine()
    initialize_database()

    try:
        yield
    finally:
        settings.database_driver = original_database_driver
        settings.database_name = original_database_name
        dispose_database_engine()
