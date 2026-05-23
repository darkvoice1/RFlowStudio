"""插件节点清单扫描服务。"""

from __future__ import annotations

import json
from pathlib import Path

from app.core.config import BACKEND_DIR
from app.schemas.plugin import PluginManifestRecord


class PluginLoaderService:
    """负责扫描用户安装的插件节点目录。"""

    def __init__(
        self,
        installed_root: Path | None = None,
        disabled_root: Path | None = None,
    ) -> None:
        nodes_root = BACKEND_DIR / "app" / "nodes"
        self.installed_root = installed_root or (nodes_root / "installed")
        self.disabled_root = disabled_root or (nodes_root / "disabled")

    def list_plugin_manifests(self) -> list[PluginManifestRecord]:
        """返回当前已安装的插件节点清单。"""
        return self._scan_plugin_root(self.installed_root, source="installed")

    def _scan_plugin_root(self, root: Path, *, source: str) -> list[PluginManifestRecord]:
        """扫描某个插件节点根目录。"""
        if not root.exists():
            return []

        manifests: list[PluginManifestRecord] = []
        for node_dir in sorted(root.iterdir()):
            if not node_dir.is_dir():
                continue

            manifest_path = node_dir / "manifest.json"
            if not manifest_path.exists():
                continue

            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifests.append(
                PluginManifestRecord(
                    id=f"{source}-{payload['key']}",
                    key=str(payload["key"]),
                    name=str(payload["name"]),
                    version=str(payload.get("version") or "1.0.0"),
                    category=str(payload.get("category") or "plugin"),
                    executor=str(payload.get("executor") or "executor.py"),
                    source=source,
                    plugin_path=node_dir.as_posix(),
                    manifest_path=manifest_path.as_posix(),
                    description=(
                        str(payload.get("description"))
                        if payload.get("description")
                        else None
                    ),
                    aliases=[str(alias) for alias in payload.get("aliases", [])],
                    executor_kind=str(payload.get("executor_kind") or "script"),
                    config_schema=dict(payload.get("config_schema") or {}),
                    input_schema=list(payload.get("input_schema") or []),
                    output_schema=list(payload.get("output_schema") or []),
                )
            )

        return manifests
