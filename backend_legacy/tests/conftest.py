from pathlib import Path

import pytest

from app.core.config import settings
from app.db.session import dispose_database_engine, initialize_database
from app.services.task_service import task_service


@pytest.fixture(autouse=True)
def isolate_storage(tmp_path: Path) -> None:
    """为每个测试隔离本地存储目录，避免测试之间相互污染。"""
    original_storage_root = settings.storage_root
    original_upload_root = settings.upload_root
    original_dataset_metadata_root = settings.dataset_metadata_root
    original_database_driver = settings.database_driver
    original_database_name = settings.database_name
    original_r_analysis_service_url = settings.r_analysis_service_url
    original_r_analysis_timeout_seconds = settings.r_analysis_timeout_seconds

    settings.storage_root = tmp_path / "storage"
    settings.upload_root = settings.storage_root / "uploads"
    settings.dataset_metadata_root = settings.storage_root / "datasets"
    settings.database_driver = "sqlite+pysqlite"
    settings.database_name = (tmp_path / "test.db").as_posix()
    settings.r_analysis_service_url = "http://127.0.0.1:8090"
    settings.r_analysis_timeout_seconds = 30

    # 每个测试开始前都创建独立目录，保证断言结果可预测。
    settings.storage_root.mkdir(parents=True, exist_ok=True)
    settings.upload_root.mkdir(parents=True, exist_ok=True)
    settings.dataset_metadata_root.mkdir(parents=True, exist_ok=True)
    dispose_database_engine()
    initialize_database()
    task_service.reset()

    try:
        yield
    finally:
        settings.storage_root = original_storage_root
        settings.upload_root = original_upload_root
        settings.dataset_metadata_root = original_dataset_metadata_root
        settings.database_driver = original_database_driver
        settings.database_name = original_database_name
        settings.r_analysis_service_url = original_r_analysis_service_url
        settings.r_analysis_timeout_seconds = original_r_analysis_timeout_seconds
        dispose_database_engine()
        task_service.reset()
