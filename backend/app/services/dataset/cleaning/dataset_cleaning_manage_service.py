import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from app.core.config import settings
from app.core.exceptions import DatasetCleaningError
from app.schemas.dataset import (
    DatasetCleaningFlowRecord,
    DatasetCleaningStepCreateRequest,
    DatasetCleaningStepListResponse,
    DatasetCleaningStepRecord,
    DatasetCleaningStepResponse,
)
from app.services.dataset.dataset_upload_service import DatasetUploadService


class DatasetCleaningManageService:
    """管理数据清洗步骤的记录、校验和读取。"""

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
        validated_parameters = self._validate_step_parameters(payload)

        # 先把步骤按顺序落盘，后续真正执行筛选或清洗时可以直接复用。
        step_record = DatasetCleaningStepRecord(
            id=uuid4().hex,
            step_type=payload.step_type,
            name=payload.name,
            description=payload.description,
            enabled=payload.enabled,
            order=next_order,
            parameters=validated_parameters,
            created_at=datetime.now(UTC),
        )
        flow_record.steps.append(step_record)
        self._save_flow_record(flow_record)

        return DatasetCleaningStepResponse(**step_record.model_dump())

    def list_enabled_steps(self, dataset_id: str) -> list[DatasetCleaningStepRecord]:
        """返回指定数据集当前已启用的清洗步骤记录。"""
        self.upload_service.load_record(dataset_id)
        flow_record = self._load_flow_record(dataset_id)
        return [
            step
            for step in sorted(flow_record.steps, key=lambda item: item.order)
            if step.enabled
        ]

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

    def _validate_step_parameters(
        self,
        payload: DatasetCleaningStepCreateRequest,
    ) -> dict[str, object]:
        """校验不同清洗步骤的参数结构。"""
        parameters = dict(payload.parameters)
        if payload.step_type == "filter":
            return self._validate_filter_parameters(parameters)
        if payload.step_type == "missing_value":
            return self._validate_missing_value_parameters(parameters)
        if payload.step_type == "sort":
            return self._validate_sort_parameters(parameters)
        if payload.step_type == "recode":
            return self._validate_recode_parameters(parameters)
        if payload.step_type == "derive_variable":
            return self._validate_derive_variable_parameters(parameters)

        return parameters

    def _validate_filter_parameters(
        self,
        parameters: dict[str, object],
    ) -> dict[str, object]:
        """校验筛选步骤的参数结构。"""
        operator = parameters.get("operator")
        column = parameters.get("column")
        supported_operators = {
            "eq",
            "neq",
            "gt",
            "gte",
            "lt",
            "lte",
            "between",
            "contains",
            "is_empty",
            "is_not_empty",
        }

        if not isinstance(column, str) or not column.strip():
            raise DatasetCleaningError("筛选步骤缺少有效的字段名。")
        if operator not in supported_operators:
            raise DatasetCleaningError("筛选步骤的操作符不受支持。")

        # 根据操作符类型校验参数，先保证当前第一版筛选能力的输入稳定。
        if operator in {"eq", "neq", "gt", "gte", "lt", "lte", "contains"}:
            if "value" not in parameters:
                raise DatasetCleaningError("当前筛选步骤缺少 value 参数。")
        if operator == "between":
            if "start" not in parameters or "end" not in parameters:
                raise DatasetCleaningError("区间筛选必须同时提供 start 和 end 参数。")

        return parameters

    def _validate_missing_value_parameters(
        self,
        parameters: dict[str, object],
    ) -> dict[str, object]:
        """校验缺失值处理步骤的参数结构。"""
        method = parameters.get("method")
        supported_methods = {"drop_rows", "fill_value", "mark_values"}

        if method not in supported_methods:
            raise DatasetCleaningError("缺失值处理步骤的 method 不受支持。")

        if method == "fill_value":
            column = parameters.get("column")
            if not isinstance(column, str) or not column.strip():
                raise DatasetCleaningError("缺失值替换步骤缺少有效的字段名。")

            fill_value = parameters.get("value")
            if fill_value is None or not str(fill_value).strip():
                raise DatasetCleaningError("缺失值替换步骤必须提供非空的 value 参数。")

        if method == "mark_values":
            column = parameters.get("column")
            if not isinstance(column, str) or not column.strip():
                raise DatasetCleaningError("缺失值标记步骤缺少有效的字段名。")

            raw_values = parameters.get("values")
            if not isinstance(raw_values, list) or not raw_values:
                raise DatasetCleaningError("缺失值标记步骤必须提供非空的 values 列表。")

            normalized_values: list[str] = []
            for item in raw_values:
                if item is None:
                    continue

                normalized_item = str(item).strip()
                if normalized_item:
                    normalized_values.append(normalized_item)

            if not normalized_values:
                raise DatasetCleaningError("缺失值标记步骤必须提供至少一个有效标记值。")

            parameters["values"] = normalized_values

        return parameters

    def _validate_sort_parameters(
        self,
        parameters: dict[str, object],
    ) -> dict[str, object]:
        """校验排序步骤的参数结构。"""
        column = parameters.get("column")
        direction = parameters.get("direction")
        supported_directions = {"asc", "desc"}

        if not isinstance(column, str) or not column.strip():
            raise DatasetCleaningError("排序步骤缺少有效的字段名。")
        if direction not in supported_directions:
            raise DatasetCleaningError("排序步骤的 direction 只支持 asc 或 desc。")

        return parameters

    def _validate_recode_parameters(
        self,
        parameters: dict[str, object],
    ) -> dict[str, object]:
        """校验字段重编码步骤的参数结构。"""
        column = parameters.get("column")
        raw_mapping = parameters.get("mapping")

        if not isinstance(column, str) or not column.strip():
            raise DatasetCleaningError("重编码步骤缺少有效的字段名。")
        if not isinstance(raw_mapping, dict) or not raw_mapping:
            raise DatasetCleaningError("重编码步骤必须提供非空的 mapping 映射。")

        normalized_mapping: dict[str, str] = {}
        for source_value, target_value in raw_mapping.items():
            normalized_source = str(source_value).strip()
            normalized_target = str(target_value).strip()
            if not normalized_source:
                raise DatasetCleaningError("重编码步骤的原始值不能为空。")
            if not normalized_target:
                raise DatasetCleaningError("重编码步骤的目标值不能为空。")

            normalized_mapping[normalized_source] = normalized_target

        parameters["mapping"] = normalized_mapping
        return parameters

    def _validate_derive_variable_parameters(
        self,
        parameters: dict[str, object],
    ) -> dict[str, object]:
        """校验新变量生成步骤的参数结构。"""
        method = parameters.get("method")
        new_column = parameters.get("new_column")
        supported_methods = {"binary_operation", "concat"}

        if method not in supported_methods:
            raise DatasetCleaningError("新变量生成步骤的 method 不受支持。")
        if not isinstance(new_column, str) or not new_column.strip():
            raise DatasetCleaningError("新变量生成步骤缺少有效的新字段名。")

        parameters["new_column"] = new_column.strip()

        if method == "binary_operation":
            left_column = parameters.get("left_column")
            right_column = parameters.get("right_column")
            operator = parameters.get("operator")
            supported_operators = {"add", "subtract", "multiply", "divide"}

            if not isinstance(left_column, str) or not left_column.strip():
                raise DatasetCleaningError("新变量计算步骤缺少有效的 left_column。")
            if not isinstance(right_column, str) or not right_column.strip():
                raise DatasetCleaningError("新变量计算步骤缺少有效的 right_column。")
            if operator not in supported_operators:
                raise DatasetCleaningError("新变量计算步骤的 operator 不受支持。")

            parameters["left_column"] = left_column.strip()
            parameters["right_column"] = right_column.strip()
            return parameters

        raw_source_columns = parameters.get("source_columns")
        if not isinstance(raw_source_columns, list) or not raw_source_columns:
            raise DatasetCleaningError("字段拼接步骤必须提供非空的 source_columns 列表。")

        normalized_source_columns: list[str] = []
        for column in raw_source_columns:
            if not isinstance(column, str) or not column.strip():
                raise DatasetCleaningError("字段拼接步骤包含无效的来源字段名。")
            normalized_source_columns.append(column.strip())

        parameters["source_columns"] = normalized_source_columns
        separator = parameters.get("separator", "")
        parameters["separator"] = "" if separator is None else str(separator)
        return parameters
