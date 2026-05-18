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
from app.services.resources.analysis.analysis_resource_service import DatasetAnalysisService
from app.services.resources.datasets.cleaning.dataset_cleaning_manage_service import (
    DatasetCleaningManageService,
)
from app.services.resources.datasets.cleaning.dataset_cleaning_r_script_service import (
    DatasetCleaningRScriptService,
)
from app.services.resources.datasets.dataset_preview_service import DatasetPreviewService
from app.services.resources.datasets.dataset_store_service import DatasetStoreService
from app.services.task_service import task_service


class DatasetResourceService:
    """Coordinate shared dataset resource operations."""

    def __init__(self) -> None:
        self.store_service = DatasetStoreService()
        self.preview_service = DatasetPreviewService()
        self.cleaning_manage_service = DatasetCleaningManageService(
            upload_service=self.store_service
        )
        self.cleaning_r_script_service = DatasetCleaningRScriptService()
        self.analysis_service = DatasetAnalysisService()

    def list_datasets(self) -> DatasetListResponse:
        return self.store_service.list_datasets()

    def get_upload_capabilities(self) -> DatasetUploadCapabilitiesResponse:
        return self.store_service.get_upload_capabilities()

    def save_uploaded_file(self, upload_file: UploadFile) -> DatasetUploadResponse:
        return self.store_service.save_uploaded_file(upload_file)

    def get_dataset_detail(self, dataset_id: str) -> DatasetDetailResponse:
        return self.store_service.get_dataset_detail(dataset_id)

    def get_dataset_preview(
        self,
        dataset_id: str,
        offset: int,
        limit: int,
    ) -> DatasetPreviewResponse:
        record = self.store_service.load_record(dataset_id)
        cleaning_steps = self.cleaning_manage_service.list_enabled_steps(dataset_id)
        data_file_path = self.store_service.resolve_data_file(
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
        record = self.store_service.load_record(dataset_id)
        cleaning_steps = self.cleaning_manage_service.list_enabled_steps(dataset_id)
        data_file_path = self.store_service.resolve_data_file(
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
        self.store_service.load_record(dataset_id)
        task = task_service.create_task(task_type="dataset_profile", dataset_id=dataset_id)
        self.store_service.update_dataset_status(dataset_id, "processing")

        worker = Thread(
            target=self._run_dataset_profile_task,
            args=(task.id, dataset_id),
            daemon=True,
        )
        worker.start()
        return task

    def get_task(self, task_id: str) -> TaskResponse:
        return task_service.get_task(task_id)

    def list_dataset_tasks(self, dataset_id: str) -> TaskListResponse:
        self.store_service.load_record(dataset_id)
        return task_service.list_tasks(dataset_id=dataset_id)

    def list_dataset_cleaning_steps(self, dataset_id: str) -> DatasetCleaningStepListResponse:
        return self.cleaning_manage_service.list_cleaning_steps(dataset_id)

    def create_dataset_cleaning_step(
        self,
        dataset_id: str,
        payload: DatasetCleaningStepCreateRequest,
    ) -> DatasetCleaningStepResponse:
        return self.cleaning_manage_service.create_cleaning_step(dataset_id, payload)

    def get_dataset_cleaning_r_script(
        self,
        dataset_id: str,
    ) -> DatasetCleaningRScriptResponse:
        record = self.store_service.load_record(dataset_id)
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
        record = self.store_service.load_record(dataset_id)
        data_file_path = self.store_service.resolve_data_file(
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

        worker = Thread(
            target=self._run_dataset_analysis_task,
            args=(task.id, record, data_file_path, prepared_request, cleaning_steps),
            daemon=True,
        )
        worker.start()
        return task

    def list_dataset_analysis_records(self, dataset_id: str) -> DatasetAnalysisRecordListResponse:
        self.store_service.load_record(dataset_id)
        return self.analysis_service.list_analysis_records(dataset_id)

    def rerun_dataset_analysis_record(
        self,
        dataset_id: str,
        analysis_record_id: str,
    ) -> TaskResponse:
        self.store_service.load_record(dataset_id)
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
        self.store_service.load_record(dataset_id)
        return self.analysis_service.get_analysis_script(dataset_id, analysis_record_id)

    def get_dataset_analysis_report_draft(
        self,
        dataset_id: str,
        analysis_record_id: str,
        template_key: DatasetAnalysisReportTemplateKey = "general",
    ) -> DatasetAnalysisReportDraftResponse:
        self.store_service.load_record(dataset_id)
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
        self.store_service.load_record(dataset_id)
        return self.analysis_service.get_analysis_report_html(
            dataset_id,
            analysis_record_id,
            template_key=template_key,
        )

    def _run_dataset_profile_task(self, task_id: str, dataset_id: str) -> None:
        try:
            task_service.mark_running(task_id)
            profile = self.get_dataset_profile(dataset_id)
            task_service.mark_completed(task_id, profile.model_dump(mode="json"))
            self.store_service.update_dataset_status(dataset_id, "ready")
        except Exception as exc:
            task_service.mark_failed(task_id, str(exc))
            try:
                self.store_service.update_dataset_status(dataset_id, "failed")
            except Exception:
                pass

    def _run_dataset_analysis_task(
        self,
        task_id: str,
        record: DatasetRecord,
        data_file_path: Path,
        prepared_request: DatasetAnalysisPreparedRequest,
        cleaning_steps: list[DatasetCleaningStepRecord],
    ) -> None:
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
            task_service.mark_failed(task_id, str(exc))


DatasetService = DatasetResourceService
dataset_service = DatasetResourceService()

__all__ = ["DatasetResourceService", "DatasetService", "dataset_service"]
