"""工作流计划层校验器。"""

from app.core.exceptions import ResourceNotFoundError, ValidationError
from app.schemas.workflow_definition import (
    WorkflowDefinitionEdgeRecord,
    WorkflowDefinitionNodeRecord,
)
from app.schemas.workflow_node import (
    WorkflowNodeDefinitionResponse,
    WorkflowNodePortSchema,
)
from app.services.workflow.workflow_plan.workflow_plan_builder import (
    WorkflowPlanEdgeContext,
)


class WorkflowPlanValidator:
    """负责执行计划生成阶段的图校验和端口解析。"""

    def normalize_edges(
        self,
        node_map: dict[str, WorkflowDefinitionNodeRecord],
        definition_map: dict[str, WorkflowNodeDefinitionResponse],
        edges: list[WorkflowDefinitionEdgeRecord],
    ) -> list[WorkflowPlanEdgeContext]:
        """把连线补全为带端口信息的规范化结构。"""
        normalized_edges: list[WorkflowPlanEdgeContext] = []
        for edge in edges:
            source_node = node_map.get(edge.source_node_id)
            target_node = node_map.get(edge.target_node_id)
            if source_node is None or target_node is None:
                raise ValidationError("存在连线引用了未定义的节点。")

            source_definition = definition_map[source_node.id]
            target_definition = definition_map[target_node.id]
            source_port = self._resolve_source_port(source_node, source_definition, edge)
            target_port = self._resolve_target_port(target_node, target_definition, edge)
            self._validate_port_data_type(
                edge=edge,
                source_definition=source_definition,
                target_definition=target_definition,
                source_port_key=source_port,
                target_port_key=target_port,
            )

            normalized_edges.append(
                WorkflowPlanEdgeContext(
                    edge_id=edge.id,
                    edge_key=edge.edge_key,
                    source_node_id=edge.source_node_id,
                    target_node_id=edge.target_node_id,
                    source_port=source_port,
                    target_port=target_port,
                )
            )
        return normalized_edges

    def validate_required_inputs(
        self,
        nodes: list[WorkflowDefinitionNodeRecord],
        definition_map: dict[str, WorkflowNodeDefinitionResponse],
        normalized_edges: list[WorkflowPlanEdgeContext],
    ) -> None:
        """校验必填输入端口是否都已经接线。"""
        node_map = {node.id: node for node in nodes}
        incoming_port_map: dict[str, set[str]] = {}
        for edge in normalized_edges:
            node_ports = incoming_port_map.setdefault(edge.target_node_id, set())
            if edge.target_port in node_ports:
                raise ValidationError(
                    f"节点 {node_map[edge.target_node_id].name} 的输入端口 "
                    f"{edge.target_port} 被重复连接。"
                )
            node_ports.add(edge.target_port)

        for node in nodes:
            required_ports = {
                port.key
                for port in definition_map[node.id].input_schema
                if port.required
            }
            connected_ports = incoming_port_map.get(node.id, set())
            missing_ports = sorted(required_ports - connected_ports)
            if missing_ports:
                raise ValidationError(
                    f"节点 {node.name} 缺少必需输入端口：{', '.join(missing_ports)}。"
                )

    def validate_registered_node_definition(
        self,
        node: WorkflowDefinitionNodeRecord,
        exc: ResourceNotFoundError,
    ) -> None:
        """把未注册节点类型转成执行层错误。"""
        raise ValidationError(
            f"节点类型 {node.node_type} 未注册，无法生成执行计划。"
        ) from exc

    def _resolve_source_port(
        self,
        node: WorkflowDefinitionNodeRecord,
        definition: WorkflowNodeDefinitionResponse,
        edge: WorkflowDefinitionEdgeRecord,
    ) -> str:
        """解析连线的源端口。"""
        output_ports = definition.output_schema
        if not output_ports:
            raise ValidationError(f"节点 {node.name} 没有可用的 output 端口。")

        if edge.source_port is None:
            if len(output_ports) == 1:
                return output_ports[0].key
            raise ValidationError(f"节点 {node.name} 的输出端口未指定。")

        port = self._find_port(output_ports, edge.source_port)
        if port is None:
            raise ValidationError(f"节点 {node.name} 不存在输出端口 {edge.source_port}。")
        return port.key

    def _resolve_target_port(
        self,
        node: WorkflowDefinitionNodeRecord,
        definition: WorkflowNodeDefinitionResponse,
        edge: WorkflowDefinitionEdgeRecord,
    ) -> str:
        """解析连线的目标端口。"""
        input_ports = definition.input_schema
        if not input_ports:
            raise ValidationError(f"节点 {node.name} 没有可用的 input 端口。")

        if edge.target_port is None:
            if len(input_ports) == 1:
                return input_ports[0].key
            raise ValidationError(f"节点 {node.name} 的输入端口未指定。")

        port = self._find_port(input_ports, edge.target_port)
        if port is None:
            raise ValidationError(f"节点 {node.name} 不存在输入端口 {edge.target_port}。")
        return port.key

    def _validate_port_data_type(
        self,
        *,
        edge: WorkflowDefinitionEdgeRecord,
        source_definition: WorkflowNodeDefinitionResponse,
        target_definition: WorkflowNodeDefinitionResponse,
        source_port_key: str,
        target_port_key: str,
    ) -> None:
        """校验连线两侧端口类型是否兼容。"""
        source_port = self._find_port(source_definition.output_schema, source_port_key)
        target_port = self._find_port(target_definition.input_schema, target_port_key)
        if source_port is None or target_port is None:
            return
        if source_port.data_type != target_port.data_type:
            raise ValidationError(
                f"连线 {edge.edge_key} 的端口类型不匹配："
                f"{source_port.data_type} -> {target_port.data_type}。"
            )

    def _find_port(
        self,
        ports: list[WorkflowNodePortSchema],
        port_key: str,
    ) -> WorkflowNodePortSchema | None:
        """按 key 查询某个端口定义。"""
        for port in ports:
            if port.key == port_key:
                return port
        return None
