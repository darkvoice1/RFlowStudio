from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status
from fastapi.responses import HTMLResponse

from app.core.config import settings
from app.core.exceptions import (
    DatasetAnalysisError,
    DatasetAnalysisRecordNotFoundError,
    DatasetCleaningError,
    DatasetNotFoundError,
    DatasetPreviewError,
    DatasetUploadError,
    DatasetWorkflowNotFoundError,
)
from app.schemas.analysis import (
    DatasetAnalysisCreateRequest,
    DatasetAnalysisRecordListResponse,
    DatasetAnalysisReportDraftResponse,
    DatasetAnalysisReportTemplateKey,
    DatasetAnalysisScriptResponse,
)
from app.schemas.dataset import (
    DatasetCleaningRScriptResponse,
    DatasetCleaningStepCreateRequest,
    DatasetCleaningStepListResponse,
    DatasetCleaningStepResponse,
    DatasetDetailResponse,
    DatasetListResponse,
    DatasetPreviewResponse,
    DatasetProfileResponse,
    DatasetUploadCapabilitiesResponse,
    DatasetUploadResponse,
)
from app.schemas.task import TaskListResponse, TaskResponse
from app.schemas.workflow import (
    DatasetWorkflowCreateRequest,
    DatasetWorkflowDetailResponse,
    DatasetWorkflowListResponse,
    DatasetWorkflowResponse,
    DatasetWorkflowVersionCreateRequest,
    DatasetWorkflowVersionListResponse,
    DatasetWorkflowVersionResponse,
)
from app.services.dataset.dataset_service import dataset_service

router = APIRouter(prefix="/datasets")


@router.get("", response_model=DatasetListResponse, summary="List datasets")
def list_datasets() -> DatasetListResponse:
    """返回数据集列表，用于承接后续的数据集首页。"""
    # 路由层只负责调用服务，不直接编排业务细节。
    return dataset_service.list_datasets()


@router.get(
    "/upload-capabilities",
    response_model=DatasetUploadCapabilitiesResponse,
    summary="Get upload capabilities",
)
def get_upload_capabilities() -> DatasetUploadCapabilitiesResponse:
    """返回当前阶段允许的上传类型和限制。"""
    return dataset_service.get_upload_capabilities()


@router.post("/upload", response_model=DatasetUploadResponse, summary="Upload dataset file")
def upload_dataset(file: UploadFile = File(...)) -> DatasetUploadResponse:
    """接收单个数据文件上传，并返回保存结果。"""
    try:
        return dataset_service.save_uploaded_file(file)
    except DatasetUploadError as exc:
        # 业务校验失败时统一转成 400，方便前端给用户展示提示。
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/preview",
    response_model=DatasetPreviewResponse,
    summary="Get dataset preview",
)
def get_dataset_preview(
    dataset_id: str,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(
        default=settings.default_preview_rows,
        ge=1,
        le=settings.max_preview_rows,
    ),
) -> DatasetPreviewResponse:
    """按数据集 ID 返回当前支持格式的预览结果。"""
    try:
        return dataset_service.get_dataset_preview(
            dataset_id=dataset_id,
            offset=offset,
            limit=limit,
        )
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except DatasetPreviewError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/profile",
    response_model=DatasetProfileResponse,
    summary="Get dataset column profile",
)
def get_dataset_profile(dataset_id: str) -> DatasetProfileResponse:
    """按数据集 ID 返回字段元信息分析结果。"""
    try:
        return dataset_service.get_dataset_profile(dataset_id)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except DatasetPreviewError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.post(
    "/{dataset_id}/profile-jobs",
    response_model=TaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create dataset profile job",
)
def create_dataset_profile_job(dataset_id: str) -> TaskResponse:
    """创建字段分析异步任务，返回任务状态入口。"""
    try:
        return dataset_service.create_dataset_profile_task(dataset_id)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


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


@router.get(
    "/{dataset_id}/tasks",
    response_model=TaskListResponse,
    summary="List dataset tasks",
)
def list_dataset_tasks(dataset_id: str) -> TaskListResponse:
    """返回指定数据集关联的异步任务列表。"""
    try:
        return dataset_service.list_dataset_tasks(dataset_id)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/cleaning-steps",
    response_model=DatasetCleaningStepListResponse,
    summary="List dataset cleaning steps",
)
def list_dataset_cleaning_steps(dataset_id: str) -> DatasetCleaningStepListResponse:
    """返回指定数据集当前已记录的清洗步骤。"""
    try:
        return dataset_service.list_dataset_cleaning_steps(dataset_id)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post(
    "/{dataset_id}/cleaning-steps",
    response_model=DatasetCleaningStepResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create dataset cleaning step",
)
def create_dataset_cleaning_step(
    dataset_id: str,
    payload: DatasetCleaningStepCreateRequest,
) -> DatasetCleaningStepResponse:
    """为指定数据集记录一条新的清洗步骤。"""
    try:
        return dataset_service.create_dataset_cleaning_step(dataset_id, payload)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except DatasetCleaningError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/cleaning-r-script",
    response_model=DatasetCleaningRScriptResponse,
    summary="Get dataset cleaning R script draft",
)
def get_dataset_cleaning_r_script(dataset_id: str) -> DatasetCleaningRScriptResponse:
    """返回指定数据集当前清洗步骤对应的 R 代码草稿。"""
    try:
        return dataset_service.get_dataset_cleaning_r_script(dataset_id)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/workflows",
    response_model=DatasetWorkflowListResponse,
    summary="List dataset workflows",
)
def list_dataset_workflows(dataset_id: str) -> DatasetWorkflowListResponse:
    """返回指定数据集下的工作流列表。"""
    try:
        return dataset_service.list_dataset_workflows(dataset_id)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post(
    "/{dataset_id}/workflows",
    response_model=DatasetWorkflowResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create dataset workflow",
)
def create_dataset_workflow(
    dataset_id: str,
    payload: DatasetWorkflowCreateRequest,
) -> DatasetWorkflowResponse:
    """为指定数据集创建一条新的工作流。"""
    try:
        return dataset_service.create_dataset_workflow(dataset_id, payload)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/workflows/{workflow_id}",
    response_model=DatasetWorkflowDetailResponse,
    summary="Get dataset workflow detail",
)
def get_dataset_workflow_detail(
    dataset_id: str,
    workflow_id: str,
) -> DatasetWorkflowDetailResponse:
    """返回指定工作流详情及其版本列表。"""
    try:
        return dataset_service.get_dataset_workflow_detail(dataset_id, workflow_id)
    except (DatasetNotFoundError, DatasetWorkflowNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/workflows/{workflow_id}/versions",
    response_model=DatasetWorkflowVersionListResponse,
    summary="List dataset workflow versions",
)
def list_dataset_workflow_versions(
    dataset_id: str,
    workflow_id: str,
) -> DatasetWorkflowVersionListResponse:
    """返回指定工作流下的版本列表。"""
    try:
        return dataset_service.list_dataset_workflow_versions(dataset_id, workflow_id)
    except (DatasetNotFoundError, DatasetWorkflowNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post(
    "/{dataset_id}/workflows/{workflow_id}/versions",
    response_model=DatasetWorkflowVersionResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create dataset workflow version",
)
def create_dataset_workflow_version(
    dataset_id: str,
    workflow_id: str,
    payload: DatasetWorkflowVersionCreateRequest,
) -> DatasetWorkflowVersionResponse:
    """为指定工作流创建一条新版本。"""
    try:
        return dataset_service.create_dataset_workflow_version(dataset_id, workflow_id, payload)
    except (DatasetNotFoundError, DatasetWorkflowNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get("/{dataset_id}", response_model=DatasetDetailResponse, summary="Get dataset detail")
def get_dataset_detail(dataset_id: str) -> DatasetDetailResponse:
    """按数据集 ID 返回元信息详情。"""
    try:
        return dataset_service.get_dataset_detail(dataset_id)
    except DatasetNotFoundError as exc:
        # 数据集不存在时返回 404，便于前端区分参数错误和系统错误。
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
