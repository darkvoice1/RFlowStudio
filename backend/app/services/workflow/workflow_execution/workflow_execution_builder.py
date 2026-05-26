"""工作流执行层结果构造器。"""

from app.schemas.workflow_definition import WorkflowDefinitionRecord
from app.schemas.workflow_execution_run import (
    WorkflowExecutionNodeRunResponse,
    WorkflowExecutionRunErrorResponse,
    WorkflowExecutionRunResponse,
)
from app.schemas.workflow_plan import WorkflowExecutionPlanStep


class WorkflowExecutionBuilder:
    """负责把节点执行过程组装成统一运行结果。"""

    def build_node_run(
        self,
        *,
        step: WorkflowExecutionPlanStep,
        status: str,
        inputs: dict,
        outputs: dict,
        error_message: str | None = None,
    ) -> WorkflowExecutionNodeRunResponse:
        """构造单个节点的运行结果。"""
        return WorkflowExecutionNodeRunResponse(
            node_id=step.node_id,
            node_key=step.node_key,
            node_type=step.node_type,
            node_name=step.node_name,
            sequence=step.sequence,
            status=status,
            inputs=inputs,
            outputs=outputs,
            error_message=error_message,
        )

    def build_success_response(
        self,
        *,
        workflow: WorkflowDefinitionRecord,
        node_runs: list[WorkflowExecutionNodeRunResponse],
        final_outputs: dict[str, dict],
    ) -> WorkflowExecutionRunResponse:
        """构造整次执行成功时的响应。"""
        return WorkflowExecutionRunResponse(
            workflow_id=workflow.id,
            workflow_name=workflow.name,
            status="succeeded",
            total=len(node_runs),
            completed=len(node_runs),
            node_runs=node_runs,
            final_outputs=final_outputs,
        )

    def build_failed_response(
        self,
        *,
        workflow: WorkflowDefinitionRecord,
        node_runs: list[WorkflowExecutionNodeRunResponse],
        failed_step: WorkflowExecutionPlanStep,
        error_message: str,
        final_outputs: dict[str, dict],
    ) -> WorkflowExecutionRunResponse:
        """构造整次执行失败时的响应。"""
        completed = len([item for item in node_runs if item.status == "succeeded"])
        return WorkflowExecutionRunResponse(
            workflow_id=workflow.id,
            workflow_name=workflow.name,
            status="failed",
            total=len(node_runs),
            completed=completed,
            node_runs=node_runs,
            final_outputs=final_outputs,
            error=WorkflowExecutionRunErrorResponse(
                node_id=failed_step.node_id,
                node_key=failed_step.node_key,
                node_type=failed_step.node_type,
                node_name=failed_step.node_name,
                message=error_message,
            ),
        )
