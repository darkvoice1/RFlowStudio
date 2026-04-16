from datetime import UTC, datetime

from app.schemas.analysis import DatasetAnalysisPreparedRequest
from app.schemas.dataset import DatasetRecord
from app.services.dataset.analysis.dataset_analysis_r_script_service import (
    DatasetAnalysisRScriptService,
)


def _build_record() -> DatasetRecord:
    return DatasetRecord(
        id="dataset-1",
        name="survey",
        file_name="survey.csv",
        extension=".csv",
        stored_path="storage/uploads/survey.csv",
        size_bytes=128,
        status="ready",
        created_at=datetime.now(UTC),
    )


def test_analysis_r_script_service_builds_descriptive_statistics_script() -> None:
    service = DatasetAnalysisRScriptService()

    script = service.build_script(
        record=_build_record(),
        prepared_request=DatasetAnalysisPreparedRequest(
            dataset_id="dataset-1",
            dataset_name="survey",
            file_name="survey.csv",
            analysis_type="descriptive_statistics",
            variables=["score", "age"],
            group_variable=None,
            options={},
        ),
    )

    assert "# 分析方法: 描述统计" in script
    assert 'selected_variables <- c("score", "age")' in script
    assert "descriptive_result <- data.frame(" in script
    assert "mean(rflow_num(analysis_data[[column]]), na.rm = TRUE)" in script


def test_analysis_r_script_service_builds_correlation_analysis_script() -> None:
    service = DatasetAnalysisRScriptService()

    script = service.build_script(
        record=_build_record(),
        prepared_request=DatasetAnalysisPreparedRequest(
            dataset_id="dataset-1",
            dataset_name="survey",
            file_name="survey.csv",
            analysis_type="correlation_analysis",
            variables=["score", "age"],
            group_variable=None,
            options={},
        ),
    )

    assert "# 分析方法: 相关分析" in script
    assert "analysis_data[] <- lapply(analysis_data, rflow_num)" in script
    assert "correlation_matrix <- stats::cor(" in script
    assert 'method = "pearson"' in script


def test_analysis_r_script_service_builds_chi_square_script() -> None:
    service = DatasetAnalysisRScriptService()

    script = service.build_script(
        record=_build_record(),
        prepared_request=DatasetAnalysisPreparedRequest(
            dataset_id="dataset-1",
            dataset_name="survey",
            file_name="survey.csv",
            analysis_type="chi_square_test",
            variables=["gender", "group"],
            group_variable=None,
            options={},
        ),
    )

    assert "# 分析方法: 卡方检验" in script
    assert 'left_variable <- "gender"' in script
    assert "contingency_table <- table(" in script
    assert "chi_square_result <- suppressWarnings(stats::chisq.test(contingency_table))" in script


def test_analysis_r_script_service_builds_t_test_script() -> None:
    service = DatasetAnalysisRScriptService()

    script = service.build_script(
        record=_build_record(),
        prepared_request=DatasetAnalysisPreparedRequest(
            dataset_id="dataset-1",
            dataset_name="survey",
            file_name="survey.csv",
            analysis_type="independent_samples_t_test",
            variables=["score"],
            group_variable="group",
            options={},
        ),
    )

    assert "# 分析方法: 独立样本 t 检验" in script
    assert 'target_variable <- "score"' in script
    assert 'group_variable <- "group"' in script
    assert "t_test_result <- stats::t.test(" in script
    assert "stats::reformulate(group_variable, response = target_variable)" in script


def test_analysis_r_script_service_builds_anova_script() -> None:
    service = DatasetAnalysisRScriptService()

    script = service.build_script(
        record=_build_record(),
        prepared_request=DatasetAnalysisPreparedRequest(
            dataset_id="dataset-1",
            dataset_name="survey",
            file_name="survey.csv",
            analysis_type="one_way_anova",
            variables=["score"],
            group_variable="class_name",
            options={},
        ),
    )

    assert "# 分析方法: 单因素方差分析" in script
    assert 'target_variable <- "score"' in script
    assert 'group_variable <- "class_name"' in script
    assert "anova_model <- stats::aov(" in script
    assert "anova_summary <- summary(anova_model)" in script


def test_analysis_r_script_service_builds_fragment_from_cleaned_data() -> None:
    service = DatasetAnalysisRScriptService()

    fragment = service.build_fragment(
        prepared_request=DatasetAnalysisPreparedRequest(
            dataset_id="dataset-1",
            dataset_name="survey",
            file_name="survey.csv",
            analysis_type="descriptive_statistics",
            variables=["score"],
            group_variable=None,
            options={},
        ),
        source_data_name="cleaned_data",
    )

    assert "# 统计分析步骤" in fragment
    assert "analysis_data <- cleaned_data" in fragment
    assert "descriptive_result <- data.frame(" in fragment
