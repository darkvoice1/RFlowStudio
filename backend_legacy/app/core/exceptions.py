class DatasetUploadError(Exception):
    """Raised when dataset upload validation or storage fails."""


class DatasetNotFoundError(Exception):
    """Raised when the requested dataset does not exist."""


class DatasetPreviewError(Exception):
    """Raised when building a dataset preview fails."""


class DatasetCleaningError(Exception):
    """Raised when dataset cleaning configuration or execution fails."""


class DatasetAnalysisError(Exception):
    """Raised when dataset analysis configuration or execution fails."""


class DatasetAnalysisRecordNotFoundError(Exception):
    """Raised when the requested analysis record does not exist."""


class WorkflowNodeValidationError(Exception):
    """Raised when workflow node configuration or node-type validation fails."""


class WorkflowNodeNotFoundError(Exception):
    """Raised when the requested workflow node definition does not exist."""


class WorkflowDefinitionNotFoundError(Exception):
    """Raised when the requested platform workflow definition does not exist."""


class WorkflowDefinitionValidationError(Exception):
    """Raised when a platform workflow graph fails validation."""


class TaskNotFoundError(Exception):
    """Raised when the requested asynchronous task does not exist."""
