import json
import socket
from urllib import error, request

from app.core.config import settings
from app.core.exceptions import DatasetAnalysisError
from app.schemas.analysis import DatasetAnalysisPreparedRequest, DatasetAnalysisResult


class DatasetAnalysisRExecutionService:
    """负责调用独立的 R 统计服务容器。"""

    def is_available(self) -> bool:
        """判断当前 R 统计服务是否可访问。"""
        health_url = self._build_service_url("/health")
        health_request = request.Request(
            health_url,
            headers={"Accept": "application/json"},
            method="GET",
        )

        try:
            with request.urlopen(
                health_request,
                timeout=min(settings.r_analysis_timeout_seconds, 5),
            ) as response:
                if response.status != 200:
                    return False

                response_body = response.read().decode("utf-8")
        except (error.URLError, TimeoutError, socket.timeout, ValueError):
            return False

        try:
            payload = json.loads(response_body)
        except json.JSONDecodeError:
            return False

        return payload.get("status") == "ok"

    def build_result(
        self,
        prepared_request: DatasetAnalysisPreparedRequest,
        columns: list[str],
        rows: list[dict[str, str | None]],
        raw_row_count: int,
    ) -> DatasetAnalysisResult:
        """调用 R 统计服务并解析返回的分析结果。"""
        payload = {
            "dataset_id": prepared_request.dataset_id,
            "dataset_name": prepared_request.dataset_name,
            "file_name": prepared_request.file_name,
            "analysis_type": prepared_request.analysis_type,
            "variables": prepared_request.variables,
            "group_variable": prepared_request.group_variable,
            "options": prepared_request.options,
            "columns": columns,
            "rows": rows,
            "raw_row_count": raw_row_count,
        }
        response_body = self._post_analysis_payload(payload)
        return DatasetAnalysisResult.model_validate_json(response_body)

    def _post_analysis_payload(self, payload: dict[str, object]) -> str:
        """向 R 统计服务发送分析请求。"""
        analysis_url = self._build_service_url("/analysis")
        encoded_payload = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        analysis_request = request.Request(
            analysis_url,
            data=encoded_payload,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
            },
            method="POST",
        )

        try:
            with request.urlopen(
                analysis_request,
                timeout=settings.r_analysis_timeout_seconds,
            ) as response:
                response_body = response.read().decode("utf-8")
                if response.status != 200:
                    raise DatasetAnalysisError(
                        self._extract_error_detail(
                            response_body,
                            "R 统计服务返回了非预期状态码。",
                        )
                    )
                return response_body
        except error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise DatasetAnalysisError(
                self._extract_error_detail(error_body, "R 统计服务执行失败。")
            ) from exc
        except (error.URLError, TimeoutError, socket.timeout) as exc:
            raise DatasetAnalysisError(
                "当前无法连接到 R 统计服务，请确认 r-analysis 容器已经启动。"
            ) from exc

    def _build_service_url(self, path: str) -> str:
        """拼接 R 统计服务地址。"""
        base_url = settings.r_analysis_service_url.strip().rstrip("/")
        if not base_url:
            raise DatasetAnalysisError("R 统计服务地址为空，暂时无法执行统计分析。")
        return f"{base_url}{path}"

    def _extract_error_detail(self, response_body: str, default_message: str) -> str:
        """从错误响应体里提取 detail 字段。"""
        if not response_body.strip():
            return default_message

        try:
            payload = json.loads(response_body)
        except json.JSONDecodeError:
            return response_body.strip() or default_message

        detail = payload.get("detail")
        if isinstance(detail, str) and detail.strip():
            return detail

        return default_message
