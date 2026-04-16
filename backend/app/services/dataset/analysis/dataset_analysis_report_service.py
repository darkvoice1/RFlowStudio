from html import escape

from app.schemas.analysis import (
    DatasetAnalysisRecord,
    DatasetAnalysisReportDraftResponse,
    DatasetAnalysisReportSection,
)


class DatasetAnalysisReportService:
    """根据分析历史记录生成中文报告草稿结构。"""

    def build_report_draft(
        self,
        analysis_record: DatasetAnalysisRecord,
    ) -> DatasetAnalysisReportDraftResponse:
        """把一条分析历史记录整理成中文报告草稿。"""
        result = analysis_record.result
        title = f"{result.summary.title}报告"
        sections = [
            self._build_dataset_section(analysis_record),
            self._build_summary_section(analysis_record),
            self._build_interpretation_section(analysis_record),
            self._build_tables_section(analysis_record),
            self._build_plots_section(analysis_record),
            self._build_script_section(analysis_record),
        ]
        return DatasetAnalysisReportDraftResponse(
            dataset_id=analysis_record.dataset_id,
            analysis_record_id=analysis_record.id,
            analysis_type=analysis_record.analysis_type,
            title=title,
            file_name=result.file_name,
            generated_at=analysis_record.created_at,
            sections=sections,
        )

    def build_report_html(self, analysis_record: DatasetAnalysisRecord) -> str:
        """把一条分析历史记录渲染成中文 HTML 报告。"""
        report_draft = self.build_report_draft(analysis_record)
        sections_html = "\n".join(
            self._render_section(section) for section in report_draft.sections
        )
        title = escape(report_draft.title)
        generated_at = escape(report_draft.generated_at.isoformat())
        return "\n".join(
            [
                "<!DOCTYPE html>",
                '<html lang="zh-CN">',
                "<head>",
                '  <meta charset="UTF-8" />',
                '  <meta name="viewport" content="width=device-width, initial-scale=1.0" />',
                f"  <title>{title}</title>",
                "  <style>",
                (
                    "    body { font-family: 'Microsoft YaHei', 'PingFang SC', sans-serif; "
                    "margin: 0; background: #f6f7fb; color: #1f2937; }"
                ),
                "    .page { max-width: 1040px; margin: 0 auto; padding: 40px 24px 64px; }",
                (
                    "    .hero { background: linear-gradient(135deg, #f8fafc, #e0f2fe); "
                    "border: 1px solid #dbeafe; border-radius: 20px; padding: 28px 32px; "
                    "margin-bottom: 24px; }"
                ),
                "    .hero h1 { margin: 0 0 12px; font-size: 32px; }",
                "    .hero p { margin: 6px 0; line-height: 1.7; }",
                (
                    "    .section { background: #ffffff; border: 1px solid #e5e7eb; "
                    "border-radius: 18px; padding: 24px; margin-bottom: 20px; "
                    "box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06); }"
                ),
                "    .section h2 { margin: 0 0 16px; font-size: 22px; }",
                (
                    "    .meta-list, .text-list, .plot-list { margin: 0; "
                    "padding-left: 20px; line-height: 1.8; }"
                ),
                "    .meta-list li, .text-list li, .plot-list li { margin: 4px 0; }",
                (
                    "    table { width: 100%; border-collapse: collapse; margin-top: 14px; "
                    "overflow: hidden; }"
                ),
                (
                    "    th, td { border: 1px solid #d1d5db; padding: 10px 12px; "
                    "text-align: left; vertical-align: top; }"
                ),
                "    th { background: #f3f4f6; font-weight: 600; }",
                "    .table-block { margin-top: 18px; }",
                "    .table-block h3 { margin: 0 0 10px; font-size: 18px; }",
                (
                    "    pre { white-space: pre-wrap; word-break: break-word; "
                    "background: #0f172a; color: #e2e8f0; border-radius: 14px; "
                    "padding: 18px; overflow-x: auto; line-height: 1.6; }"
                ),
                "    .muted { color: #6b7280; font-size: 14px; }",
                "  </style>",
                "</head>",
                "<body>",
                '  <main class="page">',
                '    <section class="hero">',
                f"      <h1>{title}</h1>",
                f"      <p><strong>分析类型：</strong>{escape(report_draft.analysis_type)}</p>",
                f"      <p><strong>数据文件：</strong>{escape(report_draft.file_name)}</p>",
                f'      <p class="muted">生成时间：{generated_at}</p>',
                "    </section>",
                sections_html,
                "  </main>",
                "</body>",
                "</html>",
            ]
        )

    def _build_dataset_section(
        self,
        analysis_record: DatasetAnalysisRecord,
    ) -> DatasetAnalysisReportSection:
        """生成数据与分析概览区块。"""
        result = analysis_record.result
        return DatasetAnalysisReportSection(
            key="dataset_overview",
            title="一、数据与分析概览",
            section_type="summary",
            content={
                "dataset_name": result.dataset_name,
                "file_name": result.file_name,
                "analysis_type": result.summary.title,
                "variables": list(result.variables),
                "group_variable": result.group_variable,
                "missing_value_strategy": result.summary.missing_value_strategy,
            },
        )

    def _build_summary_section(
        self,
        analysis_record: DatasetAnalysisRecord,
    ) -> DatasetAnalysisReportSection:
        """生成结果摘要区块。"""
        summary = analysis_record.result.summary
        return DatasetAnalysisReportSection(
            key="analysis_summary",
            title="二、结果摘要",
            section_type="summary",
            content={
                "title": summary.title,
                "effective_row_count": summary.effective_row_count,
                "excluded_row_count": summary.excluded_row_count,
                "note": summary.note,
            },
        )

    def _build_interpretation_section(
        self,
        analysis_record: DatasetAnalysisRecord,
    ) -> DatasetAnalysisReportSection:
        """生成结果解释区块。"""
        return DatasetAnalysisReportSection(
            key="interpretations",
            title="三、结果解释",
            section_type="text",
            content={"items": list(analysis_record.result.interpretations)},
        )

    def _build_tables_section(
        self,
        analysis_record: DatasetAnalysisRecord,
    ) -> DatasetAnalysisReportSection:
        """生成结果表区块。"""
        tables = [table.model_dump(mode="json") for table in analysis_record.result.tables]
        return DatasetAnalysisReportSection(
            key="result_tables",
            title="四、结果表",
            section_type="table",
            content={"items": tables},
        )

    def _build_plots_section(
        self,
        analysis_record: DatasetAnalysisRecord,
    ) -> DatasetAnalysisReportSection:
        """生成图形摘要区块。"""
        plots = [plot.model_dump(mode="json") for plot in analysis_record.result.plots]
        return DatasetAnalysisReportSection(
            key="result_plots",
            title="五、图形摘要",
            section_type="plot_list",
            content={"items": plots},
        )

    def _build_script_section(
        self,
        analysis_record: DatasetAnalysisRecord,
    ) -> DatasetAnalysisReportSection:
        """生成复现脚本区块。"""
        return DatasetAnalysisReportSection(
            key="reproducible_script",
            title="六、复现脚本",
            section_type="script",
            content={"script": analysis_record.result.script_draft},
        )

    def _render_section(self, section: DatasetAnalysisReportSection) -> str:
        """把单个报告区块渲染为 HTML。"""
        title = escape(section.title)
        content_html = self._render_section_content(section)
        return "\n".join(
            [
                '    <section class="section">',
                f"      <h2>{title}</h2>",
                content_html,
                "    </section>",
            ]
        )

    def _render_section_content(self, section: DatasetAnalysisReportSection) -> str:
        """根据区块类型渲染不同内容。"""
        if section.section_type == "summary":
            return self._render_summary_content(section.content)
        if section.section_type == "text":
            return self._render_text_content(section.content)
        if section.section_type == "table":
            return self._render_table_content(section.content)
        if section.section_type == "plot_list":
            return self._render_plot_list_content(section.content)
        return self._render_script_content(section.content)

    def _render_summary_content(self, content: dict[str, object]) -> str:
        """渲染摘要型区块。"""
        items = []
        for key, value in content.items():
            label = escape(str(key))
            display_value = self._format_summary_value(value)
            items.append(f"        <li><strong>{label}</strong>：{display_value}</li>")
        body = "\n".join(items)
        return f"      <ul class=\"meta-list\">\n{body}\n      </ul>"

    def _render_text_content(self, content: dict[str, object]) -> str:
        """渲染文本说明区块。"""
        items = content.get("items")
        if not isinstance(items, list) or not items:
            return '      <p class="muted">当前没有可展示的解释文本。</p>'
        lines = "\n".join(
            f"        <li>{escape(str(item))}</li>" for item in items
        )
        return f"      <ul class=\"text-list\">\n{lines}\n      </ul>"

    def _render_table_content(self, content: dict[str, object]) -> str:
        """渲染结果表区块。"""
        items = content.get("items")
        if not isinstance(items, list) or not items:
            return '      <p class="muted">当前没有可展示的结果表。</p>'

        blocks: list[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            title = escape(str(item.get("title", "结果表")))
            columns = item.get("columns")
            rows = item.get("rows")
            if not isinstance(columns, list) or not isinstance(rows, list):
                continue

            head_html = "".join(f"<th>{escape(str(column))}</th>" for column in columns)
            row_html_list: list[str] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                row_html = "".join(
                    f"<td>{escape(str(row.get(column, '')))}</td>" for column in columns
                )
                row_html_list.append(f"            <tr>{row_html}</tr>")

            rows_html = "\n".join(row_html_list)
            blocks.extend(
                [
                    '      <div class="table-block">',
                    f"        <h3>{title}</h3>",
                    "        <table>",
                    f"          <thead><tr>{head_html}</tr></thead>",
                    f"          <tbody>\n{rows_html}\n          </tbody>",
                    "        </table>",
                    "      </div>",
                ]
            )

        if blocks:
            return "\n".join(blocks)
        return '      <p class="muted">当前没有可展示的结果表。</p>'

    def _render_plot_list_content(self, content: dict[str, object]) -> str:
        """渲染图形摘要区块。"""
        items = content.get("items")
        if not isinstance(items, list) or not items:
            return '      <p class="muted">当前没有可展示的图形摘要。</p>'
        lines: list[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            title = escape(str(item.get("title", "未命名图形")))
            plot_type = escape(str(item.get("plot_type", "unknown")))
            lines.append(f"        <li><strong>{title}</strong>（类型：{plot_type}）</li>")
        body = "\n".join(lines)
        return f"      <ul class=\"plot-list\">\n{body}\n      </ul>"

    def _render_script_content(self, content: dict[str, object]) -> str:
        """渲染脚本区块。"""
        script = content.get("script")
        if not isinstance(script, str) or not script.strip():
            return '      <p class="muted">当前没有可展示的复现脚本。</p>'
        return f"      <pre>{escape(script)}</pre>"

    def _format_summary_value(self, value: object) -> str:
        """统一格式化摘要区块中的值。"""
        if value is None or value == "":
            return "无"
        if isinstance(value, list):
            if not value:
                return "无"
            return escape("、".join(str(item) for item in value))
        return escape(str(value))
