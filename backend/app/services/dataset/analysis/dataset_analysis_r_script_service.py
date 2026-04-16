from app.schemas.analysis import DatasetAnalysisPreparedRequest
from app.schemas.dataset import DatasetRecord


class DatasetAnalysisRScriptService:
    """根据统计分析请求生成对应的 R 代码草稿。"""

    def build_script(
        self,
        record: DatasetRecord,
        prepared_request: DatasetAnalysisPreparedRequest,
    ) -> str:
        """把当前统计分析请求翻译成一份 R 代码草稿。"""
        lines = [
            "# 统计分析 R 代码草稿",
            f"# 数据集名称: {record.name}",
            f"# 原始文件名: {record.file_name}",
            f"# 数据集 ID: {record.id}",
            f"# 分析方法: {self._build_analysis_title(prepared_request.analysis_type)}",
            "# 说明: 当前草稿仅包含统计分析步骤，不包含前置数据清洗步骤。",
            "",
            self._build_package_line(record.extension),
            "",
            self._build_data_path_line(record.stored_path),
            self._build_read_line(record.extension),
            "analysis_data <- raw_data",
            "",
            "# 分析辅助函数",
            "rflow_is_missing <- function(x) {",
            '  is.na(x) | trimws(as.character(x)) == ""',
            "}",
            "",
            "rflow_text <- function(x) {",
            "  trimws(as.character(x))",
            "}",
            "",
            "rflow_num <- function(x) {",
            "  suppressWarnings(as.numeric(rflow_text(x)))",
            "}",
            "",
        ]

        lines.extend(self._build_fragment_lines(prepared_request, source_data_name="raw_data"))
        return "\n".join(lines)

    def build_fragment(
        self,
        prepared_request: DatasetAnalysisPreparedRequest,
        *,
        source_data_name: str = "cleaned_data",
    ) -> str:
        """生成可拼接到完整流程脚本中的统计分析片段。"""
        return "\n".join(
            self._build_fragment_lines(
                prepared_request=prepared_request,
                source_data_name=source_data_name,
            )
        )

    def _build_fragment_lines(
        self,
        prepared_request: DatasetAnalysisPreparedRequest,
        source_data_name: str,
    ) -> list[str]:
        """生成基于已有数据框继续分析的脚本片段。"""
        lines = [
            "# 统计分析步骤",
            f"# 分析方法: {self._build_analysis_title(prepared_request.analysis_type)}",
            f"analysis_data <- {source_data_name}",
            "",
        ]
        lines.extend(self._build_analysis_lines(prepared_request))
        return lines

    def _build_analysis_lines(
        self,
        prepared_request: DatasetAnalysisPreparedRequest,
    ) -> list[str]:
        """根据分析类型生成对应的 R 代码片段。"""
        analysis_type = prepared_request.analysis_type
        if analysis_type == "descriptive_statistics":
            return self._build_descriptive_statistics_lines(prepared_request.variables)
        if analysis_type == "correlation_analysis":
            return self._build_correlation_analysis_lines(prepared_request.variables)
        if analysis_type == "chi_square_test":
            return self._build_chi_square_lines(prepared_request.variables)
        if analysis_type == "independent_samples_t_test":
            return self._build_t_test_lines(
                target_variable=prepared_request.variables[0],
                group_variable=prepared_request.group_variable or "",
            )
        return self._build_anova_lines(
            target_variable=prepared_request.variables[0],
            group_variable=prepared_request.group_variable or "",
        )

    def _build_descriptive_statistics_lines(self, variables: list[str]) -> list[str]:
        """生成描述统计的 R 代码。"""
        selected_variables = self._build_r_vector(variables)
        return [
            "# 描述统计",
            f"selected_variables <- c({selected_variables})",
            "analysis_data <- analysis_data[, selected_variables, drop = FALSE]",
            "",
            "descriptive_result <- data.frame(",
            "  variable = selected_variables,",
            "  non_missing_count = sapply(",
            "    selected_variables,",
            "    function(column) sum(!rflow_is_missing(analysis_data[[column]]))",
            "  ),",
            "  mean = sapply(",
            "    selected_variables,",
            "    function(column) mean(rflow_num(analysis_data[[column]]), na.rm = TRUE)",
            "  ),",
            "  sd = sapply(",
            "    selected_variables,",
            "    function(column) stats::sd(rflow_num(analysis_data[[column]]), na.rm = TRUE)",
            "  ),",
            "  minimum = sapply(",
            "    selected_variables,",
            "    function(column) min(rflow_num(analysis_data[[column]]), na.rm = TRUE)",
            "  ),",
            "  maximum = sapply(",
            "    selected_variables,",
            "    function(column) max(rflow_num(analysis_data[[column]]), na.rm = TRUE)",
            "  ),",
            "  row.names = NULL,",
            "  check.names = FALSE",
            ")",
            "",
            "descriptive_result",
        ]

    def _build_correlation_analysis_lines(self, variables: list[str]) -> list[str]:
        """生成相关分析的 R 代码。"""
        selected_variables = self._build_r_vector(variables)
        return [
            "# 相关分析",
            f"selected_variables <- c({selected_variables})",
            "analysis_data <- analysis_data[, selected_variables, drop = FALSE]",
            "analysis_data[] <- lapply(analysis_data, rflow_num)",
            "",
            "correlation_matrix <- stats::cor(",
            "  analysis_data,",
            '  use = "pairwise.complete.obs",',
            '  method = "pearson"',
            ")",
            "",
            "correlation_matrix",
        ]

    def _build_chi_square_lines(self, variables: list[str]) -> list[str]:
        """生成卡方检验的 R 代码。"""
        left_variable = variables[0]
        right_variable = variables[1]
        return [
            "# 卡方检验",
            f'left_variable <- {self._build_r_string(left_variable)}',
            f'right_variable <- {self._build_r_string(right_variable)}',
            "analysis_data <- analysis_data[, c(left_variable, right_variable), drop = FALSE]",
            "analysis_data[[left_variable]] <- rflow_text(analysis_data[[left_variable]])",
            "analysis_data[[right_variable]] <- rflow_text(analysis_data[[right_variable]])",
            (
                'analysis_data[[left_variable]]'
                '[analysis_data[[left_variable]] == ""] <- NA_character_'
            ),
            (
                'analysis_data[[right_variable]]'
                '[analysis_data[[right_variable]] == ""] <- NA_character_'
            ),
            "analysis_data <- analysis_data[stats::complete.cases(analysis_data), , drop = FALSE]",
            "",
            "contingency_table <- table(",
            "  analysis_data[[left_variable]],",
            "  analysis_data[[right_variable]]",
            ")",
            "chi_square_result <- suppressWarnings(stats::chisq.test(contingency_table))",
            "",
            "contingency_table",
            "chi_square_result",
        ]

    def _build_t_test_lines(self, target_variable: str, group_variable: str) -> list[str]:
        """生成独立样本 t 检验的 R 代码。"""
        return [
            "# 独立样本 t 检验",
            f'target_variable <- {self._build_r_string(target_variable)}',
            f'group_variable <- {self._build_r_string(group_variable)}',
            "analysis_data <- analysis_data[, c(target_variable, group_variable), drop = FALSE]",
            "analysis_data[[target_variable]] <- rflow_num(analysis_data[[target_variable]])",
            "analysis_data[[group_variable]] <- rflow_text(analysis_data[[group_variable]])",
            (
                'analysis_data[[group_variable]]'
                '[analysis_data[[group_variable]] == ""] <- NA_character_'
            ),
            "analysis_data <- analysis_data[",
            "  !is.na(analysis_data[[target_variable]]) & !is.na(analysis_data[[group_variable]]),",
            "  ,",
            "  drop = FALSE",
            "]",
            "analysis_data[[group_variable]] <- factor(analysis_data[[group_variable]])",
            "",
            "t_test_result <- stats::t.test(",
            "  stats::reformulate(group_variable, response = target_variable),",
            "  data = analysis_data",
            ")",
            "",
            "t_test_result",
        ]

    def _build_anova_lines(self, target_variable: str, group_variable: str) -> list[str]:
        """生成单因素方差分析的 R 代码。"""
        return [
            "# 单因素方差分析",
            f'target_variable <- {self._build_r_string(target_variable)}',
            f'group_variable <- {self._build_r_string(group_variable)}',
            "analysis_data <- analysis_data[, c(target_variable, group_variable), drop = FALSE]",
            "analysis_data[[target_variable]] <- rflow_num(analysis_data[[target_variable]])",
            "analysis_data[[group_variable]] <- rflow_text(analysis_data[[group_variable]])",
            (
                'analysis_data[[group_variable]]'
                '[analysis_data[[group_variable]] == ""] <- NA_character_'
            ),
            "analysis_data <- analysis_data[",
            "  !is.na(analysis_data[[target_variable]]) & !is.na(analysis_data[[group_variable]]),",
            "  ,",
            "  drop = FALSE",
            "]",
            "analysis_data[[group_variable]] <- factor(analysis_data[[group_variable]])",
            "",
            "anova_model <- stats::aov(",
            "  stats::reformulate(group_variable, response = target_variable),",
            "  data = analysis_data",
            ")",
            "anova_summary <- summary(anova_model)",
            "",
            "anova_summary",
        ]

    def _build_package_line(self, extension: str) -> str:
        """根据文件扩展名生成依赖包说明。"""
        if extension == ".csv":
            return "library(readr)"
        if extension == ".xlsx":
            return "library(readxl)"
        if extension == ".sav":
            return "library(haven)"
        return "# TODO: 请根据实际数据格式补充读取依赖"

    def _build_data_path_line(self, stored_path: str) -> str:
        """生成数据路径定义语句。"""
        escaped_path = stored_path.replace("\\", "/")
        return f'data_path <- "{escaped_path}"'

    def _build_read_line(self, extension: str) -> str:
        """根据文件扩展名生成读取数据语句。"""
        if extension == ".csv":
            return "raw_data <- readr::read_csv(data_path, show_col_types = FALSE)"
        if extension == ".xlsx":
            return "raw_data <- readxl::read_excel(data_path)"
        if extension == ".sav":
            return "raw_data <- haven::read_sav(data_path)"
        return "# TODO: 请补充 raw_data 的读取语句"

    def _build_analysis_title(self, analysis_type: str) -> str:
        """把分析类型编码转换为便于阅读的中文标题。"""
        mapping = {
            "descriptive_statistics": "描述统计",
            "correlation_analysis": "相关分析",
            "chi_square_test": "卡方检验",
            "independent_samples_t_test": "独立样本 t 检验",
            "one_way_anova": "单因素方差分析",
        }
        return mapping.get(analysis_type, analysis_type)

    def _build_r_vector(self, values: list[str]) -> str:
        """把字符串列表拼成 R 向量文本。"""
        return ", ".join(self._build_r_string(value) for value in values)

    def _build_r_string(self, value: object) -> str:
        """把 Python 值转成 R 字符串字面量。"""
        escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
