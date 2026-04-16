from pathlib import Path
from threading import Thread

from fastapi import UploadFile

from app.schemas.analysis import (
    DatasetAnalysisCreateRequest,
    DatasetAnalysisPreparedRequest,
    DatasetAnalysisRecordListResponse,
    DatasetAnalysisReportDraftResponse,
    DatasetAnalysisReportTemplateKey,
    DatasetAnalysisScriptResponse,
)
from app.schemas.dataset import (
    DatasetCleaningRScriptResponse,
    DatasetCleaningStepCreateRequest,
    DatasetCleaningStepListResponse,
    DatasetCleaningStepRecord,
    DatasetCleaningStepResponse,
    DatasetDetailResponse,
    DatasetListResponse,
    DatasetPreviewResponse,
    DatasetProfileResponse,
    DatasetRecord,
    DatasetUploadCapabilitiesResponse,
    DatasetUploadResponse,
)
from app.schemas.task import TaskListResponse, TaskResponse
from app.services.dataset.analysis.dataset_analysis_service import DatasetAnalysisService
from app.services.dataset.cleaning.dataset_cleaning_manage_service import (
    DatasetCleaningManageService,
)
from app.services.dataset.cleaning.dataset_cleaning_r_script_service import (
    DatasetCleaningRScriptService,
)
from app.services.dataset.dataset_preview_service import DatasetPreviewService
from app.services.dataset.dataset_upload_service import DatasetUploadService
from app.services.task_service import task_service


class DatasetService:
    """协调数据集存储、预览和字段分析的统一入口。"""

    def __init__(self) -> None:
        """初始化数据集领域服务。"""
        self.upload_service = DatasetUploadService()
        self.preview_service = DatasetPreviewService()
        self.cleaning_manage_service = DatasetCleaningManageService(
            upload_service=self.upload_service
        )
        self.cleaning_r_script_service = DatasetCleaningRScriptService()
        self.analysis_service = DatasetAnalysisService()

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
        cleaning_steps = self.cleaning_manage_service.list_enabled_steps(dataset_id)
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
            cleaning_steps=cleaning_steps,
        )

    def get_dataset_profile(self, dataset_id: str) -> DatasetProfileResponse:
        """按数据集 ID 返回当前支持格式的字段元信息统计结果。"""
        record = self.upload_service.load_record(dataset_id)
        cleaning_steps = self.cleaning_manage_service.list_enabled_steps(dataset_id)
        data_file_path = self.upload_service.resolve_data_file(
            record=record,
            supported_extensions={".csv", ".xlsx"},
            unsupported_message="当前字段分析接口暂仅支持 CSV 和 XLSX 文件。",
            missing_file_message="原始数据文件不存在，暂时无法分析字段信息。",
        )
        return self.preview_service.get_dataset_profile(
            record=record,
            data_file_path=data_file_path,
            cleaning_steps=cleaning_steps,
        )

    def create_dataset_profile_task(self, dataset_id: str) -> TaskResponse:
        """创建字段分析异步任务并在后台执行。"""
        self.upload_service.load_record(dataset_id)
        task = task_service.create_task(task_type="dataset_profile", dataset_id=dataset_id)
        self.upload_service.update_dataset_status(dataset_id, "processing")

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

    def list_dataset_tasks(self, dataset_id: str) -> TaskListResponse:
        """返回指定数据集关联的异步任务列表。"""
        # 先校验数据集存在，避免对非法数据集 ID 返回空任务列表造成误导。
        self.upload_service.load_record(dataset_id)
        return task_service.list_tasks(dataset_id=dataset_id)

    def list_dataset_cleaning_steps(self, dataset_id: str) -> DatasetCleaningStepListResponse:
        """返回指定数据集当前已记录的清洗步骤列表。"""
        return self.cleaning_manage_service.list_cleaning_steps(dataset_id)

    def create_dataset_cleaning_step(
        self,
        dataset_id: str,
        payload: DatasetCleaningStepCreateRequest,
    ) -> DatasetCleaningStepResponse:
        """为指定数据集记录一条新的清洗步骤。"""
        return self.cleaning_manage_service.create_cleaning_step(dataset_id, payload)

    def get_dataset_cleaning_r_script(
        self,
        dataset_id: str,
    ) -> DatasetCleaningRScriptResponse:
        """返回指定数据集当前清洗步骤对应的 R 代码草稿。"""
        record = self.upload_service.load_record(dataset_id)
        cleaning_steps = self.cleaning_manage_service.list_all_steps(dataset_id)
        script = self.cleaning_r_script_service.build_script(record, cleaning_steps)
        return DatasetCleaningRScriptResponse(
            dataset_id=record.id,
            file_name=record.file_name,
            step_count=len(cleaning_steps),
            script=script,
        )

    def create_dataset_analysis_task(
        self,
        dataset_id: str,
        payload: DatasetAnalysisCreateRequest,
    ) -> TaskResponse:
        """创建统计分析异步任务并在后台执行。"""
        record = self.upload_service.load_record(dataset_id)
        data_file_path = self.upload_service.resolve_data_file(
            record=record,
            supported_extensions={".csv", ".xlsx"},
            unsupported_message="当前统计分析接口暂仅支持 CSV 和 XLSX 文件。",
            missing_file_message="原始数据文件不存在，暂时无法发起统计分析。",
        )
        prepared_request = self.analysis_service.prepare_request(
            record=record,
            data_file_path=data_file_path,
            payload=payload,
        )
        cleaning_steps = self.cleaning_manage_service.list_enabled_steps(dataset_id)
        task = task_service.create_task(task_type="dataset_analysis", dataset_id=dataset_id)

        # 先把分析任务骨架接入后台线程，后续具体统计方法可以沿同一入口继续扩展。
        worker = Thread(
            target=self._run_dataset_analysis_task,
            args=(task.id, record, data_file_path, prepared_request, cleaning_steps),
            daemon=True,
        )
        worker.start()
        return task

    def list_dataset_analysis_records(self, dataset_id: str) -> DatasetAnalysisRecordListResponse:
        """返回指定数据集当前已保存的统计分析历史记录。"""
        self.upload_service.load_record(dataset_id)
        return self.analysis_service.list_analysis_records(dataset_id)

    def rerun_dataset_analysis_record(
        self,
        dataset_id: str,
        analysis_record_id: str,
    ) -> TaskResponse:
        """基于指定历史记录重新创建一次统计分析任务。"""
        self.upload_service.load_record(dataset_id)
        analysis_record = self.analysis_service.get_analysis_record(dataset_id, analysis_record_id)
        payload = DatasetAnalysisCreateRequest(
            analysis_type=analysis_record.analysis_type,
            variables=list(analysis_record.variables),
            group_variable=analysis_record.group_variable,
            options=dict(analysis_record.options),
        )
        return self.create_dataset_analysis_task(dataset_id, payload)

    def get_dataset_analysis_script(
        self,
        dataset_id: str,
        analysis_record_id: str,
    ) -> DatasetAnalysisScriptResponse:
        """返回指定统计分析历史记录对应的完整脚本。"""
        self.upload_service.load_record(dataset_id)
        return self.analysis_service.get_analysis_script(dataset_id, analysis_record_id)

    def get_dataset_analysis_report_draft(
        self,
        dataset_id: str,
        analysis_record_id: str,
        template_key: DatasetAnalysisReportTemplateKey = "general",
    ) -> DatasetAnalysisReportDraftResponse:
        """返回指定统计分析历史记录对应的中文报告草稿。"""
        self.upload_service.load_record(dataset_id)
        return self.analysis_service.get_analysis_report_draft(
            dataset_id,
            analysis_record_id,
            template_key=template_key,
        )

    def get_dataset_analysis_report_html(
        self,
        dataset_id: str,
        analysis_record_id: str,
        template_key: DatasetAnalysisReportTemplateKey = "general",
    ) -> str:
        """返回指定统计分析历史记录对应的中文 HTML 报告。"""
        self.upload_service.load_record(dataset_id)
        return self.analysis_service.get_analysis_report_html(
            dataset_id,
            analysis_record_id,
            template_key=template_key,
        )

    def _run_dataset_profile_task(self, task_id: str, dataset_id: str) -> None:
        """在后台执行字段分析任务并更新状态。"""
        try:
            task_service.mark_running(task_id)
            profile = self.get_dataset_profile(dataset_id)
            task_service.mark_completed(task_id, profile.model_dump(mode="json"))
            self.upload_service.update_dataset_status(dataset_id, "ready")
        except Exception as exc:
            # 当前阶段先把错误收敛成任务失败信息，避免线程异常直接丢失。
            task_service.mark_failed(task_id, str(exc))
            try:
                self.upload_service.update_dataset_status(dataset_id, "failed")
            except Exception:
                # 如果数据集本身已不可读，任务失败状态仍然需要保留下来。
                pass

    def _run_dataset_analysis_task(
        self,
        task_id: str,
        record: DatasetRecord,
        data_file_path: Path,
        prepared_request: DatasetAnalysisPreparedRequest,
        cleaning_steps: list[DatasetCleaningStepRecord],
    ) -> None:
        """在后台执行统计分析任务并更新状态。"""
        try:
            task_service.mark_running(task_id)
            result = self.analysis_service.build_result(
                record=record,
                data_file_path=data_file_path,
                prepared_request=prepared_request,
                cleaning_steps=cleaning_steps,
            )
            self.analysis_service.save_analysis_record(
                dataset_id=record.id,
                task_id=task_id,
                prepared_request=prepared_request,
                result=result,
            )
            task_service.mark_completed(task_id, result.model_dump(mode="json"))
        except Exception as exc:
            # 当前阶段先统一把分析任务异常收敛到任务失败状态，避免线程异常直接丢失。
            task_service.mark_failed(task_id, str(exc))


# 提供默认服务实例，后续接入依赖注入时可以平滑替换。
dataset_service = DatasetService()
