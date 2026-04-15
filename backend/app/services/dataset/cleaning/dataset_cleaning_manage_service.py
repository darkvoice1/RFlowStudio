from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.exceptions import DatasetCleaningError, DatasetNotFoundError
from app.db.session import session_scope
from app.models.dataset import DatasetCleaningStepModel, DatasetRecordModel
from app.schemas.dataset import (
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
        ordered_steps = self.list_all_steps(dataset_id)

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
        with session_scope() as session:
            # 同一数据集新增清洗步骤时，先锁定数据集记录，降低并发顺序号冲突风险。
            dataset_model = session.get(
                DatasetRecordModel,
                dataset_id,
                with_for_update=True,
            )
            if dataset_model is None:
                raise DatasetNotFoundError("请求的数据集不存在。")

            validated_parameters = self._validate_step_parameters(payload)
            next_order = self._get_next_step_order(dataset_id, session)
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

            # 清洗步骤现在优先入库，后续预览、字段分析和 R 草稿都会复用同一套流水。
            session.add(self._to_step_model(dataset_id, step_record))

        return DatasetCleaningStepResponse(**step_record.model_dump())

    def list_enabled_steps(self, dataset_id: str) -> list[DatasetCleaningStepRecord]:
        """返回指定数据集当前已启用的清洗步骤记录。"""
        all_steps = self.list_all_steps(dataset_id)
        return [step for step in all_steps if step.enabled]

    def list_all_steps(self, dataset_id: str) -> list[DatasetCleaningStepRecord]:
        """返回指定数据集当前全部清洗步骤记录。"""
        self.upload_service.load_record(dataset_id)
        with session_scope() as session:
            step_models = self._list_step_models_from_database(dataset_id, session)
            return [self._to_step_record(step_model) for step_model in step_models]

    def _list_step_models_from_database(
        self,
        dataset_id: str,
        session: Session,
    ) -> list[DatasetCleaningStepModel]:
        """按顺序从数据库读取指定数据集的清洗步骤。"""
        return session.scalars(
            select(DatasetCleaningStepModel)
            .where(DatasetCleaningStepModel.dataset_id == dataset_id)
            .order_by(DatasetCleaningStepModel.order)
        ).all()

    def _get_next_step_order(self, dataset_id: str, session: Session) -> int:
        """计算当前数据集下一条清洗步骤的顺序号。"""
        current_max_order = session.scalar(
            select(func.max(DatasetCleaningStepModel.order)).where(
                DatasetCleaningStepModel.dataset_id == dataset_id
            )
        )
        return (current_max_order or 0) + 1

    def _to_step_record(self, model: DatasetCleaningStepModel) -> DatasetCleaningStepRecord:
        """把数据库模型转成系统内部使用的清洗步骤记录。"""
        return DatasetCleaningStepRecord(
            id=model.id,
            step_type=model.step_type,  # type: ignore[arg-type]
            name=model.name,
            description=model.description,
            enabled=model.enabled,
            order=model.order,
            parameters=dict(model.parameters),
            created_at=model.created_at,
        )

    def _to_step_model(
        self,
        dataset_id: str,
        step_record: DatasetCleaningStepRecord,
    ) -> DatasetCleaningStepModel:
        """把系统内部的清洗步骤记录转成数据库模型。"""
        return DatasetCleaningStepModel(
            id=step_record.id,
            dataset_id=dataset_id,
            step_type=step_record.step_type,
            name=step_record.name,
            description=step_record.description,
            enabled=step_record.enabled,
            order=step_record.order,
            parameters=dict(step_record.parameters),
            created_at=step_record.created_at,
        )

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
