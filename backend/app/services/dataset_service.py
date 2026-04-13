from threading import Thread

from fastapi import UploadFile

from app.schemas.dataset import (
    DatasetDetailResponse,
    DatasetListResponse,
    DatasetPreviewResponse,
    DatasetProfileResponse,
    DatasetUploadCapabilitiesResponse,
    DatasetUploadResponse,
)
from app.schemas.task import TaskResponse
from app.services.dataset_preview_service import DatasetPreviewService
from app.services.dataset_upload_service import DatasetUploadService
from app.services.task_service import task_service


class DatasetService:
    """协调数据集存储、预览和字段分析的统一入口。"""

    def __init__(self) -> None:
        """初始化数据集领域服务。"""
        self.upload_service = DatasetUploadService()
        self.preview_service = DatasetPreviewService()

    def list_datasets(self) -> DatasetListResponse:
        """返回当前已保存的数据集列表。"""
        return self.upload_service.list_datasets()

    def get_upload_capabilities(self) -> DatasetUploadCapabilitiesResponse:
        """返回当前阶段支持的上传能力说明。"""
        return self.upload_service.get_upload_capabilities()

    def save_uploaded_file(self, upload_file: UploadFile) -> DatasetUploadResponse:
        """保存上传文件并返回上传结果摘要。"""
        return self.upload_service.save_uploaded_file(upload_file)

    def get_dataset_detail(self, dataset_id: str) -> DatasetDetailResponse:
        """按数据集 ID 返回详情信息。"""
        return self.upload_service.get_dataset_detail(dataset_id)

    def get_dataset_preview(
        self,
        dataset_id: str,
        offset: int,
        limit: int,
    ) -> DatasetPreviewResponse:
        """按数据集 ID 返回当前支持格式的预览结果。"""
        record = self.upload_service.load_record(dataset_id)
        data_file_path = self.upload_service.resolve_data_file(
            record=record,
            supported_extensions={".csv", ".xlsx"},
            unsupported_message="当前预览接口暂仅支持 CSV 和 XLSX 文件。",
            missing_file_message="原始数据文件不存在，暂时无法预览。",
        )
        return self.preview_service.get_dataset_preview(
            record=record,
            data_file_path=data_file_path,
            offset=offset,
            limit=limit,
        )

    def get_dataset_profile(self, dataset_id: str) -> DatasetProfileResponse:
        """按数据集 ID 返回当前支持格式的字段元信息统计结果。"""
        record = self.upload_service.load_record(dataset_id)
        data_file_path = self.upload_service.resolve_data_file(
            record=record,
            supported_extensions={".csv", ".xlsx"},
            unsupported_message="当前字段分析接口暂仅支持 CSV 和 XLSX 文件。",
            missing_file_message="原始数据文件不存在，暂时无法分析字段信息。",
        )
        return self.preview_service.get_dataset_profile(
            record=record,
            data_file_path=data_file_path,
        )

    def create_dataset_profile_task(self, dataset_id: str) -> TaskResponse:
        """创建字段分析异步任务并在后台执行。"""
        task = task_service.create_task(task_type="dataset_profile", dataset_id=dataset_id)

        # 用后台线程执行字段分析，先把同步耗时逻辑从主请求里挪出去。
        worker = Thread(
            target=self._run_dataset_profile_task,
            args=(task.id, dataset_id),
            daemon=True,
        )
        worker.start()
        return task

    def get_task(self, task_id: str) -> TaskResponse:
        """返回指定异步任务的当前状态。"""
        return task_service.get_task(task_id)

    def _run_dataset_profile_task(self, task_id: str, dataset_id: str) -> None:
        """在后台执行字段分析任务并更新状态。"""
        try:
            task_service.mark_running(task_id)
            profile = self.get_dataset_profile(dataset_id)
            task_service.mark_completed(task_id, profile.model_dump(mode="json"))
        except Exception as exc:
            # 当前阶段先把错误收敛成任务失败信息，避免线程异常直接丢失。
            task_service.mark_failed(task_id, str(exc))


# 提供默认服务实例，后续接入依赖注入时可以平滑替换。
dataset_service = DatasetService()
