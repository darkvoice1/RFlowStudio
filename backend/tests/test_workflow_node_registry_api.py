import json
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app
from app.services.workflow.node_registry_service import workflow_node_registry_service

client = TestClient(app)


def test_workflow_node_directory_lists_builtin_nodes() -> None:
    """验证统一节点目录接口默认返回内置节点。"""
    response = client.get("/api/v1/workflow-nodes")

    assert response.status_code == 200
    assert response.json()["total"] >= 3
    assert any(item["key"] == "dataset_input" for item in response.json()["items"])


def test_workflow_node_directory_includes_enabled_plugin_nodes(tmp_path: Path) -> None:
    """验证已启用插件节点会进入统一节点目录。"""
    installed_root = tmp_path / "nodes" / "installed"
    node_dir = installed_root / "dataset_preview"
    node_dir.mkdir(parents=True)
    (node_dir / "manifest.json").write_text(
        json.dumps(
            {
                "key": "dataset_preview",
                "name": "插件版数据预览",
                "version": "1.0.0",
                "category": "dataset",
                "executor": "executor.py",
                "description": "插件接管数据预览节点。",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    loader = workflow_node_registry_service.plugin_registry_service.loader
    original_installed_root = loader.installed_root
    original_disabled_root = loader.disabled_root
    loader.installed_root = installed_root
    loader.disabled_root = tmp_path / "nodes" / "disabled"

    try:
        response = client.get("/api/v1/workflow-nodes")
    finally:
        loader.installed_root = original_installed_root
        loader.disabled_root = original_disabled_root

    assert response.status_code == 200
    preview_node = next(
        item for item in response.json()["items"] if item["key"] == "dataset_preview"
    )
    assert preview_node["source"] == "plugin"
    assert preview_node["plugin_id"] == "installed-dataset_preview"


def test_workflow_node_detail_supports_plugin_backed_node(tmp_path: Path) -> None:
    """验证节点详情接口支持查询插件节点。"""
    installed_root = tmp_path / "nodes" / "installed"
    node_dir = installed_root / "dataset_profile"
    node_dir.mkdir(parents=True)
    (node_dir / "manifest.json").write_text(
        json.dumps(
            {
                "key": "dataset_profile",
                "name": "插件版数据概览",
                "version": "1.0.0",
                "category": "dataset",
                "executor": "executor.py",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    loader = workflow_node_registry_service.plugin_registry_service.loader
    original_installed_root = loader.installed_root
    original_disabled_root = loader.disabled_root
    loader.installed_root = installed_root
    loader.disabled_root = tmp_path / "nodes" / "disabled"

    try:
        response = client.get("/api/v1/workflow-nodes/dataset_profile")
    finally:
        loader.installed_root = original_installed_root
        loader.disabled_root = original_disabled_root

    assert response.status_code == 200
    assert response.json()["source"] == "plugin"
    assert response.json()["plugin_id"] == "installed-dataset_profile"
