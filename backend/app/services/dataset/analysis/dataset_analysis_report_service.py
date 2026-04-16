from dataclasses import dataclass
from html import escape

from app.schemas.analysis import (
    DatasetAnalysisRecord,
    DatasetAnalysisReportDraftResponse,
    DatasetAnalysisReportSection,
    DatasetAnalysisReportTemplateInfo,
    DatasetAnalysisReportTemplateKey,
)


@dataclass(frozen=True)
class ReportTemplateDefinition:
    """定义单个中文报告模板的元信息。"""

    key: DatasetAnalysisReportTemplateKey
    name: str
    description: str
    title_prefix: str
    intro_title: str
    intro_items: list[str]
    recommendation_title: str
    recommendation_items: list[str]


class DatasetAnalysisReportService:
    """根据分析历史记录生成中文报告草稿结构。"""

    SECTION_KEY_LABELS: dict[str, str] = {
        "dataset_name": "数据集名称",
        "file_name": "数据文件",
        "analysis_type": "分析方法",
        "variables": "分析变量",
        "group_variable": "分组变量",
        "missing_value_strategy": "缺失值处理方式",
        "title": "结果标题",
        "effective_row_count": "有效样本量",
        "excluded_row_count": "剔除样本量",
        "note": "结果说明",
    }

    PLOT_TYPE_LABELS: dict[str, str] = {
        "histogram": "直方图",
        "boxplot": "箱线图",
        "bar_chart": "条形图",
        "scatter_plot": "散点图",
        "heatmap": "热力图",
        "grouped_bar_chart": "分组条形图",
        "grouped_boxplot": "分组箱线图",
    }

    ANALYSIS_TYPE_LABELS: dict[str, str] = {
        "descriptive_statistics": "描述统计",
        "independent_samples_t_test": "独立样本 t 检验",
        "one_way_anova": "单因素方差分析",
        "chi_square_test": "卡方检验",
        "correlation_analysis": "相关分析",
    }

    SECTION_NUMBER_LABELS: tuple[str, ...] = (
        "一",
        "二",
        "三",
        "四",
        "五",
        "六",
        "七",
        "八",
        "九",
        "十",
    )

    TEMPLATE_DEFINITIONS: dict[
        DatasetAnalysisReportTemplateKey,
        ReportTemplateDefinition,
    ] = {
        "general": ReportTemplateDefinition(
            key="general",
            name="通用分析模板",
            description="适合常规统计分析结果查看与留档的默认中文报告模板。",
            title_prefix="通用统计分析",
            intro_title="报告说明",
            intro_items=[
                "本模板适合常规教学、科研和日常数据分析场景。",
                "报告会自动汇总数据概况、结果摘要、结果表、图形摘要和复现脚本。",
            ],
            recommendation_title="使用建议",
            recommendation_items=[
                "建议结合研究问题重点阅读结果摘要、结果解释和关键结果表。",
                "如需复核过程，可直接查看本报告附带的复现脚本。",
            ],
        ),
        "questionnaire_analysis": ReportTemplateDefinition(
            key="questionnaire_analysis",
            name="问卷分析模板",
            description="适合问卷调查、量表统计和课堂练习讲解的中文报告模板。",
            title_prefix="问卷分析",
            intro_title="问卷分析说明",
            intro_items=[
                "本模板适合展示问卷数据、量表分布和样本基本情况。",
                "建议优先关注变量取值分布、缺失值处理方式和样本量说明。",
            ],
            recommendation_title="问卷解读建议",
            recommendation_items=[
                "如果结果涉及量表题项，建议结合频数分布和均值水平解释受试者特征。",
                "用于教学场景时，可把结果表与图形摘要配合展示，帮助理解问卷数据结构。",
            ],
        ),
        "pre_post_experiment": ReportTemplateDefinition(
            key="pre_post_experiment",
            name="实验前后测模板",
            description="适合实验前后测、干预前后比较和课程训练成效展示的中文报告模板。",
            title_prefix="实验前后测",
            intro_title="实验设计说明",
            intro_items=[
                "本模板适合整理干预前后测量或训练前后结果的统计报告。",
                "阅读时建议先确认分析变量、分组设置和有效样本量，再解释结果差异。",
            ],
            recommendation_title="前后测解读建议",
            recommendation_items=[
                "如需判断干预是否有效，建议重点关注结果摘要中的样本量与显著性信息。",
                "用于论文或课程汇报时，可直接引用结果表并结合复现脚本说明分析过程。",
            ],
        ),
        "group_comparison": ReportTemplateDefinition(
            key="group_comparison",
            name="组间比较模板",
            description="适合班级比较、实验组对照组比较和科研组间差异分析的中文报告模板。",
            title_prefix="组间比较",
            intro_title="组间比较说明",
            intro_items=[
                "本模板适合比较不同组别在目标变量上的差异情况。",
                "建议先查看分组字段、组别数量和每组样本量，再解读统计检验结果。",
            ],
            recommendation_title="组间比较建议",
            recommendation_items=[
                "如果涉及多个组别，请结合分组汇总表和均值差异图一起判断结果。",
                "用于教学展示时，可将结果解释与图形摘要配合说明组间差异方向。",
            ],
        ),
    }

    def build_report_draft(
        self,
        analysis_record: DatasetAnalysisRecord,
        template_key: DatasetAnalysisReportTemplateKey = "general",
    ) -> DatasetAnalysisReportDraftResponse:
        """把一条分析历史记录整理成中文报告草稿。"""
        result = analysis_record.result
        template = self._get_template_definition(template_key)
        title = f"{template.title_prefix}：{result.summary.title}报告"
        sections = [
            self._build_intro_section(template),
            self._build_dataset_section(analysis_record),
            self._build_summary_section(analysis_record),
            self._build_interpretation_section(analysis_record),
            self._build_tables_section(analysis_record),
            self._build_plots_section(analysis_record),
            self._build_script_section(analysis_record),
            self._build_recommendation_section(template),
        ]
        sections = self._apply_section_numbers(sections)
        return DatasetAnalysisReportDraftResponse(
            dataset_id=analysis_record.dataset_id,
            analysis_record_id=analysis_record.id,
            analysis_type=analysis_record.analysis_type,
            template_key=template.key,
            title=title,
            file_name=result.file_name,
            generated_at=analysis_record.created_at,
            available_templates=self.list_available_templates(),
            sections=sections,
        )

    def build_report_html(
        self,
        analysis_record: DatasetAnalysisRecord,
        template_key: DatasetAnalysisReportTemplateKey = "general",
    ) -> str:
        """把一条分析历史记录渲染成中文 HTML 报告。"""
        report_draft = self.build_report_draft(
            analysis_record,
            template_key=template_key,
        )
        sections_html = "\n".join(
            self._render_section(section) for section in report_draft.sections
        )
        title = escape(report_draft.title)
        generated_at = escape(
            report_draft.generated_at.strftime("%Y-%m-%d %H:%M:%S UTC")
        )
        template_name = escape(
            self._get_template_definition(report_draft.template_key).name
        )
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
                (
                    f"      <p><strong>分析类型：</strong>"
                    f"{escape(self._get_analysis_type_label(report_draft.analysis_type))}</p>"
                ),
                f"      <p><strong>数据文件：</strong>{escape(report_draft.file_name)}</p>",
                f"      <p><strong>报告模板：</strong>{template_name}</p>",
                f'      <p class="muted">生成时间：{generated_at}</p>',
                "    </section>",
                sections_html,
                "  </main>",
                "</body>",
                "</html>",
            ]
        )

    def list_available_templates(self) -> list[DatasetAnalysisReportTemplateInfo]:
        """返回当前内置的中文报告模板信息。"""
        return [
            DatasetAnalysisReportTemplateInfo(
                key=template.key,
                name=template.name,
                description=template.description,
            )
            for template in self.TEMPLATE_DEFINITIONS.values()
        ]

    def _build_intro_section(
        self,
        template: ReportTemplateDefinition,
    ) -> DatasetAnalysisReportSection:
        """生成模板说明区块。"""
        return DatasetAnalysisReportSection(
            key="template_intro",
            title=template.intro_title,
            section_type="text",
            content={"items": list(template.intro_items)},
        )

    def _build_dataset_section(
        self,
        analysis_record: DatasetAnalysisRecord,
    ) -> DatasetAnalysisReportSection:
        """生成数据与分析概览区块。"""
        result = analysis_record.result
        return DatasetAnalysisReportSection(
            key="dataset_overview",
            title="数据与分析概览",
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
            title="结果摘要",
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
            title="结果解释",
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
            title="结果表",
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
            title="图形摘要",
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
            title="复现脚本",
            section_type="script",
            content={"script": analysis_record.result.script_draft},
        )

    def _build_recommendation_section(
        self,
        template: ReportTemplateDefinition,
    ) -> DatasetAnalysisReportSection:
        """生成模板化的使用建议区块。"""
        return DatasetAnalysisReportSection(
            key="template_recommendations",
            title=template.recommendation_title,
            section_type="text",
            content={"items": list(template.recommendation_items)},
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
            label = escape(self.SECTION_KEY_LABELS.get(str(key), str(key)))
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
            plot_type = escape(
                self.PLOT_TYPE_LABELS.get(
                    str(item.get("plot_type", "unknown")),
                    str(item.get("plot_type", "unknown")),
                )
            )
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
        if isinstance(value, str) and value in self.ANALYSIS_TYPE_LABELS:
            return escape(self.ANALYSIS_TYPE_LABELS[value])
        return escape(str(value))

    def _apply_section_numbers(
        self,
        sections: list[DatasetAnalysisReportSection],
    ) -> list[DatasetAnalysisReportSection]:
        """统一给报告区块追加中文编号。"""
        numbered_sections: list[DatasetAnalysisReportSection] = []
        for index, section in enumerate(sections):
            number = self.SECTION_NUMBER_LABELS[index]
            numbered_sections.append(
                section.model_copy(update={"title": f"{number}、{section.title}"})
            )
        return numbered_sections

    def _get_analysis_type_label(self, analysis_type: str) -> str:
        """返回分析类型的中文展示名称。"""
        return self.ANALYSIS_TYPE_LABELS.get(analysis_type, analysis_type)

    def _get_template_definition(
        self,
        template_key: DatasetAnalysisReportTemplateKey,
    ) -> ReportTemplateDefinition:
        """返回指定模板定义。"""
        return self.TEMPLATE_DEFINITIONS[template_key]
