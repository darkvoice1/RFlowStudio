"""工作流执行引擎服务。"""

from __future__ import annotations

from app.schemas.workflow_definition import (
    WorkflowDefinitionNodeRecord,
    WorkflowDefinitionRecord,
)
from app.schemas.workflow_execution_run import (
    WorkflowExecutionFailure,
    WorkflowExecutionRunRequest,
    WorkflowExecutionRunResponse,
    WorkflowExecutionStepResult,
)
from app.schemas.workflow_node import WorkflowNodeDefinitionResponse
from app.schemas.workflow_plan import WorkflowExecutionPlanResponse, WorkflowExecutionPlanStep
from app.services.workflow.workflow_execution.executors.base import (
    WorkflowNodeExecutionRequest,
)
from app.services.workflow.workflow_execution.node_executor_service import (
    WorkflowNodeExecutorService,
)


class WorkflowEngineService:
    """按执行计划串行调度各个节点执行器。"""

    def __init__(
        self,
        node_executor_service: WorkflowNodeExecutorService | None = None,
    ) -> None:
        self.node_executor_service = node_executor_service or WorkflowNodeExecutorService()

    def execute_plan(
        self,
        *,
        workflow: WorkflowDefinitionRecord,
        plan: WorkflowExecutionPlanResponse,
        node_map: dict[str, WorkflowDefinitionNodeRecord],
        definition_map: dict[str, WorkflowNodeDefinitionResponse],
        payload: WorkflowExecutionRunRequest,
    ) -> WorkflowExecutionRunResponse:
        """按照计划顺序执行整张工作流图。"""
        node_outputs: dict[str, dict[str, object]] = {}
        step_results: list[WorkflowExecutionStepResult] = []

        for step in plan.steps:
            node = node_map[step.node_id]
            definition = definition_map[step.node_id]
            inputs = self._build_step_inputs(step, node_outputs)
            runtime_inputs = self._build_runtime_inputs(node, payload)
            request = WorkflowNodeExecutionRequest(
                workflow=workflow,
                node=node,
                definition=definition,
                step=step,
                inputs=inputs,
                runtime_inputs=runtime_inputs,
            )

            try:
                outputs = self.node_executor_service.execute_node(request)
            except Exception as exc:  # noqa: BLE001
                error_message = str(exc) or "节点执行失败。"
                step_results.append(
                    WorkflowExecutionStepResult(
                        node_id=node.id,
                        node_key=node.node_key,
                        node_type=node.node_type,
                        node_name=node.name,
                        sequence=step.sequence,
                        status="failed",
                        inputs=inputs,
                        outputs={},
                        error_message=error_message,
                    )
                )
                return WorkflowExecutionRunResponse(
                    workflow_id=workflow.id,
                    workflow_name=workflow.name,
                    status="failed",
                    total=len(plan.steps),
                    succeeded_steps=len(step_results) - 1,
                    steps=step_results,
                    final_outputs={},
                    failure=WorkflowExecutionFailure(
                        node_id=node.id,
                        node_key=node.node_key,
                        node_name=node.name,
                        message=error_message,
                    ),
                )

            node_outputs[node.id] = outputs
            step_results.append(
                WorkflowExecutionStepResult(
                    node_id=node.id,
                    node_key=node.node_key,
                    node_type=node.node_type,
                    node_name=node.name,
                    sequence=step.sequence,
                    status="succeeded",
                    inputs=inputs,
                    outputs=outputs,
                )
            )

        final_outputs = self._build_final_outputs(plan.steps, node_outputs)
        return WorkflowExecutionRunResponse(
            workflow_id=workflow.id,
            workflow_name=workflow.name,
            status="succeeded",
            total=len(plan.steps),
            succeeded_steps=len(step_results),
            steps=step_results,
            final_outputs=final_outputs,
            failure=None,
        )

    def _build_step_inputs(
        self,
        step: WorkflowExecutionPlanStep,
        node_outputs: dict[str, dict[str, object]],
    ) -> dict[str, object]:
        """根据连线绑定组装当前节点输入。"""
        inputs: dict[str, object] = {}
        for binding in step.incoming_bindings:
            source_outputs = node_outputs.get(binding.source_node_id)
            if source_outputs is None:
                raise ValueError(
                    f"节点 {binding.source_node_key} 尚未产出输出，无法给下游节点供数。"
                )

            if binding.source_port not in source_outputs:
                raise ValueError(
                    f"节点 {binding.source_node_key} 缺少输出端口 {binding.source_port}。"
                )

            inputs[binding.target_port] = source_outputs[binding.source_port]
        return inputs

    def _build_runtime_inputs(
        self,
        node: WorkflowDefinitionNodeRecord,
        payload: WorkflowExecutionRunRequest,
    ) -> dict[str, object]:
        """读取外部传入的节点运行时输入。"""
        runtime_inputs = payload.node_inputs.get(node.id)
        if runtime_inputs is not None:
            return runtime_inputs

        runtime_inputs = payload.node_inputs.get(node.node_key)
        if runtime_inputs is not None:
            return runtime_inputs

        return {}

    def _build_final_outputs(
        self,
        steps: list[WorkflowExecutionPlanStep],
        node_outputs: dict[str, dict[str, object]],
    ) -> dict[str, dict[str, object]]:
        """收集没有下游连线的终点节点输出。"""
        final_outputs: dict[str, dict[str, object]] = {}
        for step in steps:
            if step.outgoing_bindings:
                continue

            outputs = node_outputs.get(step.node_id)
            if outputs is None:
                continue

            final_outputs[step.node_id] = outputs
        return final_outputs
