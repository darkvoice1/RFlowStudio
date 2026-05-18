from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import HTMLResponse

from app.core.exceptions import (
    DatasetAnalysisError,
    DatasetAnalysisRecordNotFoundError,
    DatasetNotFoundError,
    DatasetPreviewError,
)
from app.schemas.analysis import (
    DatasetAnalysisCreateRequest,
    DatasetAnalysisRecordListResponse,
    DatasetAnalysisReportDraftResponse,
    DatasetAnalysisReportTemplateKey,
    DatasetAnalysisScriptResponse,
)
from app.schemas.task import TaskResponse
from app.services.resources.datasets.dataset_resource_service import dataset_service

router = APIRouter()


@router.post(
    "/{dataset_id}/analysis-jobs",
    response_model=TaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create dataset analysis job",
)
def create_dataset_analysis_job(
    dataset_id: str,
    payload: DatasetAnalysisCreateRequest,
) -> TaskResponse:
    """创建统计分析异步任务，返回任务状态入口。"""
    try:
        return dataset_service.create_dataset_analysis_task(dataset_id, payload)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except (DatasetAnalysisError, DatasetPreviewError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/analysis-records",
    response_model=DatasetAnalysisRecordListResponse,
    summary="List dataset analysis records",
)
def list_dataset_analysis_records(dataset_id: str) -> DatasetAnalysisRecordListResponse:
    """返回指定数据集当前已保存的统计分析历史记录。"""
    try:
        return dataset_service.list_dataset_analysis_records(dataset_id)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post(
    "/{dataset_id}/analysis-records/{analysis_record_id}/rerun",
    response_model=TaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Rerun dataset analysis record",
)
def rerun_dataset_analysis_record(dataset_id: str, analysis_record_id: str) -> TaskResponse:
    """基于一条已保存的历史分析记录重新创建统计分析任务。"""
    try:
        return dataset_service.rerun_dataset_analysis_record(dataset_id, analysis_record_id)
    except (DatasetNotFoundError, DatasetAnalysisRecordNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except (DatasetAnalysisError, DatasetPreviewError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/analysis-records/{analysis_record_id}/script",
    response_model=DatasetAnalysisScriptResponse,
    summary="Get dataset analysis script",
)
def get_dataset_analysis_script(
    dataset_id: str,
    analysis_record_id: str,
) -> DatasetAnalysisScriptResponse:
    """返回一条统计分析历史记录对应的完整脚本。"""
    try:
        return dataset_service.get_dataset_analysis_script(dataset_id, analysis_record_id)
    except (DatasetNotFoundError, DatasetAnalysisRecordNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/analysis-records/{analysis_record_id}/report-draft",
    response_model=DatasetAnalysisReportDraftResponse,
    summary="Get dataset analysis report draft",
)
def get_dataset_analysis_report_draft(
    dataset_id: str,
    analysis_record_id: str,
    template_key: DatasetAnalysisReportTemplateKey = Query(
        default="general",
        description="报告模板类型。",
    ),
) -> DatasetAnalysisReportDraftResponse:
    """返回一条统计分析历史记录对应的中文报告草稿。"""
    try:
        return dataset_service.get_dataset_analysis_report_draft(
            dataset_id,
            analysis_record_id,
            template_key=template_key,
        )
    except (DatasetNotFoundError, DatasetAnalysisRecordNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/analysis-records/{analysis_record_id}/report-html",
    response_class=HTMLResponse,
    summary="Get dataset analysis report html",
)
def get_dataset_analysis_report_html(
    dataset_id: str,
    analysis_record_id: str,
    template_key: DatasetAnalysisReportTemplateKey = Query(
        default="general",
        description="报告模板类型。",
    ),
) -> HTMLResponse:
    """返回一条统计分析历史记录对应的中文 HTML 报告。"""
    try:
        html = dataset_service.get_dataset_analysis_report_html(
            dataset_id,
            analysis_record_id,
            template_key=template_key,
        )
        return HTMLResponse(content=html)
    except (DatasetNotFoundError, DatasetAnalysisRecordNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
