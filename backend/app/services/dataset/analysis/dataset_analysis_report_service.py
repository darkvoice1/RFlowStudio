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
