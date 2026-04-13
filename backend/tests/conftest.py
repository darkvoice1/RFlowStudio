from pathlib import Path

import pytest

from app.core.config import settings


@pytest.fixture(autouse=True)
def isolate_storage(tmp_path: Path) -> None:
    """为每个测试隔离本地存储目录，避免测试之间相互污染。"""
    original_storage_root = settings.storage_root
    original_upload_root = settings.upload_root
    original_dataset_metadata_root = settings.dataset_metadata_root

    settings.storage_root = tmp_path / "storage"
    settings.upload_root = settings.storage_root / "uploads"
    settings.dataset_metadata_root = settings.storage_root / "datasets"

    # 每个测试开始前都创建独立目录，保证断言结果可预测。
    settings.storage_root.mkdir(parents=True, exist_ok=True)
    settings.upload_root.mkdir(parents=True, exist_ok=True)
    settings.dataset_metadata_root.mkdir(parents=True, exist_ok=True)

    try:
        yield
    finally:
        settings.storage_root = original_storage_root
        settings.upload_root = original_upload_root
        settings.dataset_metadata_root = original_dataset_metadata_root
