from datetime import UTC, datetime
from threading import Lock
from uuid import uuid4

from app.core.exceptions import TaskNotFoundError
from app.schemas.task import TaskListResponse, TaskResponse


class TaskService:
    """管理异步任务的创建、状态流转和查询。"""

    def __init__(self) -> None:
        """初始化任务服务。"""
        self._tasks: dict[str, TaskResponse] = {}
        self._task_order: dict[str, int] = {}
        self._next_order = 0
        self._lock = Lock()

    def create_task(self, task_type: str, dataset_id: str | None = None) -> TaskResponse:
        """创建一个新的异步任务记录。"""
        task = TaskResponse(
            id=uuid4().hex,
            task_type=task_type,
            status="pending",
            dataset_id=dataset_id,
            created_at=datetime.now(UTC),
            started_at=None,
            finished_at=None,
            error_message=None,
            result=None,
        )

        with self._lock:
            self._next_order += 1
            self._tasks[task.id] = task
            self._task_order[task.id] = self._next_order

        return task

    def mark_running(self, task_id: str) -> TaskResponse:
        """把任务状态更新为运行中。"""
        with self._lock:
            task = self._get_task_or_raise(task_id)
            task.status = "running"
            task.started_at = datetime.now(UTC)
            task.error_message = None
            self._tasks[task_id] = task
            return task

    def mark_completed(self, task_id: str, result: dict[str, object] | None) -> TaskResponse:
        """把任务状态更新为已完成，并写入结果。"""
        with self._lock:
            task = self._get_task_or_raise(task_id)
            task.status = "completed"
            task.finished_at = datetime.now(UTC)
            task.result = result
            task.error_message = None
            self._tasks[task_id] = task
            return task

    def mark_failed(self, task_id: str, error_message: str) -> TaskResponse:
        """把任务状态更新为失败，并记录错误信息。"""
        with self._lock:
            task = self._get_task_or_raise(task_id)
            task.status = "failed"
            task.finished_at = datetime.now(UTC)
            task.error_message = error_message
            task.result = None
            self._tasks[task_id] = task
            return task

    def get_task(self, task_id: str) -> TaskResponse:
        """按任务 ID 返回当前状态。"""
        with self._lock:
            task = self._get_task_or_raise(task_id)
            return task.model_copy(deep=True)

    def list_tasks(self, dataset_id: str | None = None) -> TaskListResponse:
        """返回全部任务或指定数据集的任务列表。"""
        with self._lock:
            tasks = [task.model_copy(deep=True) for task in self._tasks.values()]

        if dataset_id is not None:
            tasks = [task for task in tasks if task.dataset_id == dataset_id]

        # 用创建时间加创建序号做双重排序，避免同一时刻创建任务时顺序不稳定。
        tasks.sort(
            key=lambda task: (task.created_at, self._task_order.get(task.id, 0)),
            reverse=True,
        )
        return TaskListResponse(items=tasks, total=len(tasks))

    def reset(self) -> None:
        """清空内存中的任务记录，供测试隔离使用。"""
        with self._lock:
            self._tasks.clear()
            self._task_order.clear()
            self._next_order = 0

    def _get_task_or_raise(self, task_id: str) -> TaskResponse:
        """返回指定任务，不存在时抛出异常。"""
        task = self._tasks.get(task_id)
        if task is None:
            raise TaskNotFoundError("请求的任务不存在。")

        return task


# 提供默认任务服务实例，供路由和领域服务复用。
task_service = TaskService()
