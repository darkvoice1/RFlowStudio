"""新后端基础异常定义。"""


class AppError(Exception):
    """新后端业务异常基类。"""


class ResourceNotFoundError(AppError):
    """资源不存在时抛出。"""


class ValidationError(AppError):
    """请求或业务校验失败时抛出。"""


class WorkflowError(AppError):
    """工作流主线相关异常基类。"""
