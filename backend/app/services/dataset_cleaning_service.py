import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from app.core.config import settings
from app.schemas.dataset import (
    DatasetCleaningFlowRecord,
    DatasetCleaningStepCreateRequest,
    DatasetCleaningStepListResponse,
    DatasetCleaningStepRecord,
    DatasetCleaningStepResponse,
)
from app.services.dataset_upload_service import DatasetUploadService


class DatasetCleaningService:
    """管理数据清洗步骤的记录和读取。"""

    def __init__(self, upload_service: DatasetUploadService) -> None:
        """初始化数据清洗步骤服务。"""
        self.upload_service = upload_service

    def list_cleaning_steps(self, dataset_id: str) -> DatasetCleaningStepListResponse:
        """返回指定数据集当前已记录的清洗步骤。"""
        self.upload_service.load_record(dataset_id)
        flow_record = self._load_flow_record(dataset_id)
        ordered_steps = sorted(flow_record.steps, key=lambda item: item.order)

        return DatasetCleaningStepListResponse(
            dataset_id=dataset_id,
            items=[DatasetCleaningStepResponse(**step.model_dump()) for step in ordered_steps],
            total=len(ordered_steps),
        )

    def create_cleaning_step(
        self,
        dataset_id: str,
        payload: DatasetCleaningStepCreateRequest,
    ) -> DatasetCleaningStepResponse:
        """为指定数据集追加一条新的清洗步骤记录。"""
        self.upload_service.load_record(dataset_id)
        flow_record = self._load_flow_record(dataset_id)
        next_order = len(flow_record.steps) + 1

        # 先把步骤按顺序落盘，后续真正执行筛选或清洗时可以直接复用。
        step_record = DatasetCleaningStepRecord(
            id=uuid4().hex,
            step_type=payload.step_type,
            name=payload.name,
            description=payload.description,
            enabled=payload.enabled,
            order=next_order,
            parameters=dict(payload.parameters),
            created_at=datetime.now(UTC),
        )
        flow_record.steps.append(step_record)
        self._save_flow_record(flow_record)

        return DatasetCleaningStepResponse(**step_record.model_dump())

    def _load_flow_record(self, dataset_id: str) -> DatasetCleaningFlowRecord:
        """读取数据集清洗步骤流水，不存在时返回空流水。"""
        record_path = self._build_flow_record_path(dataset_id)
        if not record_path.exists():
            return DatasetCleaningFlowRecord(dataset_id=dataset_id, steps=[])

        return DatasetCleaningFlowRecord.model_validate_json(
            record_path.read_text(encoding="utf-8")
        )

    def _save_flow_record(self, flow_record: DatasetCleaningFlowRecord) -> None:
        """把数据清洗步骤流水写入本地 JSON 文件。"""
        record_path = self._build_flow_record_path(flow_record.dataset_id)
        record_path.write_text(
            json.dumps(flow_record.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _build_flow_record_path(self, dataset_id: str) -> Path:
        """构造数据清洗步骤流水文件路径。"""
        return settings.dataset_cleaning_root / f"{dataset_id}.json"
