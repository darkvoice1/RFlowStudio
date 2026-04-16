import json
from urllib import error

import pytest

from app.core.exceptions import DatasetAnalysisError
from app.schemas.analysis import (
    DatasetAnalysisPreparedRequest,
    DatasetAnalysisResult,
    DatasetAnalysisSummary,
)
from app.services.dataset.analysis.dataset_analysis_execution_service import (
    DatasetAnalysisExecutionService,
)
from app.services.dataset.analysis.dataset_analysis_r_execution_service import (
    DatasetAnalysisRExecutionService,
)


def test_analysis_execution_service_delegates_to_r_execution_service() -> None:
    """验证统计执行入口会把请求交给 R 执行服务。"""
    execution_service = DatasetAnalysisExecutionService()
    expected_result = DatasetAnalysisResult(
        dataset_id="dataset-1",
        dataset_name="survey",
        file_name="survey.csv",
        analysis_type="descriptive_statistics",
        variables=["score"],
        group_variable=None,
        status="completed",
        summary=DatasetAnalysisSummary(
            title="描述统计",
            analysis_type="descriptive_statistics",
            effective_row_count=2,
            excluded_row_count=0,
            missing_value_strategy="r",
        ),
        tables=[],
        plots=[],
        interpretations=[],
    )

    def fake_is_available() -> bool:
        return True

    def fake_build_result(
        prepared_request: DatasetAnalysisPreparedRequest,
        columns: list[str],
        rows: list[dict[str, str | None]],
        raw_row_count: int,
    ) -> DatasetAnalysisResult:
        return expected_result

    execution_service.r_execution_service.is_available = fake_is_available  # type: ignore[method-assign]
    execution_service.r_execution_service.build_result = fake_build_result  # type: ignore[method-assign]

    result = execution_service.build_result(
        prepared_request=DatasetAnalysisPreparedRequest(
            dataset_id="dataset-1",
            dataset_name="survey",
            file_name="survey.csv",
            analysis_type="descriptive_statistics",
            variables=["score"],
            group_variable=None,
            options={},
        ),
        columns=["score"],
        rows=[{"score": "95"}, {"score": "88"}],
        raw_row_count=2,
    )

    assert result == expected_result


def test_analysis_execution_service_raises_when_r_is_unavailable() -> None:
    """验证没有可用 R 服务时会直接返回错误。"""
    execution_service = DatasetAnalysisExecutionService()

    def fake_is_available() -> bool:
        return False

    execution_service.r_execution_service.is_available = fake_is_available  # type: ignore[method-assign]

    with pytest.raises(DatasetAnalysisError, match="R 统计服务"):
        execution_service.build_result(
            prepared_request=DatasetAnalysisPreparedRequest(
                dataset_id="dataset-1",
                dataset_name="survey",
                file_name="survey.csv",
                analysis_type="descriptive_statistics",
                variables=["score"],
                group_variable=None,
                options={},
            ),
            columns=["score"],
            rows=[{"score": "95"}],
            raw_row_count=1,
        )

def test_analysis_r_execution_service_posts_http_request_and_parses_result(
    monkeypatch,
) -> None:
    """验证 R 执行服务会向 r-analysis 服务发请求并解析结果。"""
    execution_service = DatasetAnalysisRExecutionService()
    expected_result = DatasetAnalysisResult(
        dataset_id="dataset-1",
        dataset_name="survey",
        file_name="survey.csv",
        analysis_type="correlation_analysis",
        variables=["score", "age"],
        group_variable=None,
        status="completed",
        summary=DatasetAnalysisSummary(
            title="相关分析",
            analysis_type="correlation_analysis",
            effective_row_count=3,
            excluded_row_count=0,
            missing_value_strategy="r",
        ),
        tables=[],
        plots=[],
        interpretations=["ok"],
    )
    captured_request: dict[str, object] = {}

    class FakeResponse:
        def __init__(self, body_text: str, status: int = 200) -> None:
            self._body_text = body_text
            self.status = status

        def read(self) -> bytes:
            return self._body_text.encode("utf-8")

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type, exc, exc_tb) -> None:
            return None

    def fake_urlopen(req, timeout):
        captured_request["full_url"] = req.full_url
        captured_request["timeout"] = timeout
        captured_request["headers"] = dict(req.header_items())
        captured_request["body"] = json.loads(req.data.decode("utf-8"))
        return FakeResponse(
            json.dumps(expected_result.model_dump(mode="json"), ensure_ascii=False)
        )

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    result = execution_service.build_result(
        prepared_request=DatasetAnalysisPreparedRequest(
            dataset_id="dataset-1",
            dataset_name="survey",
            file_name="survey.csv",
            analysis_type="correlation_analysis",
            variables=["score", "age"],
            group_variable=None,
            options={},
        ),
        columns=["score", "age"],
        rows=[{"score": "95", "age": "20"}],
        raw_row_count=1,
    )

    assert result == expected_result
    assert captured_request["full_url"].endswith("/analysis")
    assert captured_request["body"]["analysis_type"] == "correlation_analysis"
    assert captured_request["body"]["rows"][0]["score"] == "95"


def test_analysis_r_execution_service_surfaces_remote_error_detail(monkeypatch) -> None:
    """验证 R 执行服务会透传远端服务返回的 detail。"""
    execution_service = DatasetAnalysisRExecutionService()

    class FakeHttpError(error.HTTPError):
        def __init__(self) -> None:
            super().__init__(
                url="http://127.0.0.1:8090/analysis",
                code=500,
                msg="internal error",
                hdrs=None,
                fp=None,
            )

        def read(self) -> bytes:
            return (
                b"{\"detail\": "
                b"\"R \xe6\x9c\x8d\xe5\x8a\xa1\xe6\x89\xa7\xe8\xa1\x8c\xe5\xa4\xb1\xe8\xb4\xa5\"}"
            )

    def fake_urlopen(req, timeout):
        raise FakeHttpError()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    with pytest.raises(DatasetAnalysisError, match="R 服务执行失败"):
        execution_service.build_result(
            prepared_request=DatasetAnalysisPreparedRequest(
                dataset_id="dataset-1",
                dataset_name="survey",
                file_name="survey.csv",
                analysis_type="correlation_analysis",
                variables=["score", "age"],
                group_variable=None,
                options={},
            ),
            columns=["score", "age"],
            rows=[{"score": "95", "age": "20"}],
            raw_row_count=1,
        )
