from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import desc, select

from app.core.exceptions import WorkflowNodeNotFoundError
from app.db.session import session_scope
from app.models.plugin import PluginModel
from app.schemas.plugin import (
    PluginDetailResponse,
    PluginListResponse,
    PluginResponse,
    PluginSyncResponse,
)
from app.services.platform.plugin_loader_service import PluginLoaderService, PluginManifest


class PluginRegistryService:
    """负责单节点插件注册表同步、查询和状态管理。"""

    def __init__(self) -> None:
        """初始化插件元数据加载器。"""
        self.loader = PluginLoaderService()

    def sync_plugins(self) -> PluginSyncResponse:
        """扫描本地单节点插件目录并同步到数据库。"""
        manifests = self.loader.list_plugin_manifests()
        for manifest in manifests:
            self._upsert_manifest(manifest)
        items = self.list_plugins().items
        return PluginSyncResponse(items=items, total=len(items))

    def list_plugins(self) -> PluginListResponse:
        """返回数据库中当前已注册的单节点插件列表。"""
        with session_scope() as session:
            models = session.scalars(
                select(PluginModel).order_by(desc(PluginModel.created_at))
            ).all()

        items = [self._to_plugin_response(model) for model in models]
        return PluginListResponse(items=items, total=len(items))

    def get_plugin(self, plugin_id: str) -> PluginDetailResponse:
        """返回单个节点插件详情。"""
        with session_scope() as session:
            model = session.get(PluginModel, plugin_id)
        if model is None:
            raise WorkflowNodeNotFoundError(f"插件 {plugin_id} 不存在。")

        manifest = self.loader.load_plugin_manifest(plugin_id)
        manifest_path = "" if manifest is None else manifest.manifest_path
        plugin_path = model.entry_path if manifest is None else manifest.plugin_path

        return PluginDetailResponse(
            **self._to_plugin_response(model).model_dump(),
            plugin_path=plugin_path,
            manifest_path=manifest_path,
        )

    def update_plugin_status(self, plugin_id: str, status: str) -> PluginDetailResponse:
        """更新插件启用/停用状态。"""
        normalized_status = status.strip().lower()
        if normalized_status not in {"enabled", "disabled"}:
            raise ValueError("插件状态仅支持 enabled 或 disabled。")

        with session_scope() as session:
            model = session.get(PluginModel, plugin_id)
            if model is None:
                raise WorkflowNodeNotFoundError(f"插件 {plugin_id} 不存在。")
            model.status = normalized_status
            model.updated_at = datetime.now(UTC)

        return self.get_plugin(plugin_id)

    def _upsert_manifest(self, manifest: PluginManifest) -> None:
        """把磁盘上的单节点插件 manifest 同步进数据库。"""
        with session_scope() as session:
            model = session.get(PluginModel, manifest.id)
            now = datetime.now(UTC)
            if model is None:
                session.add(
                    PluginModel(
                        id=manifest.id,
                        name=manifest.name,
                        version=manifest.version,
                        category=manifest.category,
                        entry_path=manifest.entry_path,
                        executor=manifest.executor,
                        status="enabled",
                        source=manifest.source,
                        created_at=now,
                        updated_at=now,
                    )
                )
                return

            model.name = manifest.name
            model.version = manifest.version
            model.category = manifest.category
            model.entry_path = manifest.entry_path
            model.executor = manifest.executor
            model.source = manifest.source
            model.updated_at = now

    def _to_plugin_response(self, model: PluginModel) -> PluginResponse:
        """把数据库模型转换成节点插件响应。"""
        return PluginResponse(
            id=model.id,
            name=model.name,
            version=model.version,
            category=model.category,
            entry_path=model.entry_path,
            executor=model.executor,
            status=model.status,
            source=model.source,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )
