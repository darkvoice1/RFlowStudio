class DatasetUploadError(Exception):
    """表示数据集上传过程中的业务错误。"""


class DatasetNotFoundError(Exception):
    """表示请求的数据集不存在。"""
