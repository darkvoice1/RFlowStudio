class DatasetUploadError(Exception):
    """表示数据集上传过程中的业务错误。"""


class DatasetNotFoundError(Exception):
    """表示请求的数据集不存在。"""


class DatasetPreviewError(Exception):
    """表示数据集预览过程中的业务错误。"""


class DatasetCleaningError(Exception):
    """表示数据清洗步骤配置或执行过程中的业务错误。"""


class DatasetAnalysisError(Exception):
    """表示统计分析任务配置或执行过程中的业务错误。"""


class DatasetAnalysisRecordNotFoundError(Exception):
    """表示请求的统计分析历史记录不存在。"""


class DatasetWorkflowNotFoundError(Exception):
    """表示请求的数据集工作流不存在。"""


class DatasetWorkflowValidationError(Exception):
    """表示工作流节点类型或配置校验失败。"""


class WorkflowNodeNotFoundError(Exception):
    """表示请求的工作流节点定义不存在。"""


class TaskNotFoundError(Exception):
    """表示请求的异步任务不存在。"""
