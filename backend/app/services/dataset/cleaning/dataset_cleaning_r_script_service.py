import json

from app.schemas.dataset import DatasetCleaningStepRecord, DatasetRecord


class DatasetCleaningRScriptService:
    """根据清洗步骤流水生成 R 代码草稿。"""

    def build_script(
        self,
        record: DatasetRecord,
        cleaning_steps: list[DatasetCleaningStepRecord],
        *,
        title: str = "# 数据清洗 R 代码草稿",
        include_result_output: bool = True,
    ) -> str:
        """把当前数据集的清洗步骤翻译成一份 R 代码草稿。"""
        lines = [
            title,
            "# 脚本用途: 根据当前数据集已记录的清洗步骤，生成可复现的 R 清洗脚本。",
            f"# 数据集名称: {record.name}",
            f"# 原始文件名: {record.file_name}",
            f"# 数据集 ID: {record.id}",
            "",
            "# 包依赖说明",
            f"# - {self._build_package_name(record.extension)}",
            "# - 如果你的本地 R 环境尚未安装对应依赖，请先执行 install.packages()。",
            "",
            self._build_package_line(record.extension),
            "",
            "# 数据来源说明",
            "# - 当前 data_path 指向平台内部保存的原始数据文件。",
            "# - 如果你要在外部环境运行，请先把 data_path 改成你自己的文件路径。",
            "# - cleaned_data 表示应用清洗步骤后的结果数据框。",
            "",
            self._build_data_path_line(record.stored_path),
            self._build_read_line(record.extension),
            "cleaned_data <- raw_data",
            "",
            "# 参数说明",
            f"# - 当前脚本共包含 {len(cleaning_steps)} 个清洗步骤。",
            "# - 步骤顺序与平台中展示的顺序保持一致。",
            "# - 如步骤被标记为禁用，脚本中会保留说明但不会执行。",
            "",
            "# 清洗辅助函数",
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
            "rflow_format_number <- function(x) {",
            "  ifelse(",
            "    is.na(x),",
            "    NA_character_,",
            "    ifelse(x %% 1 == 0, as.character(as.integer(x)), as.character(x))",
            "  )",
            "}",
            "",
            "rflow_order_index <- function(x, descending = FALSE) {",
            "  missing_key <- rflow_is_missing(x)",
            "  text_x <- rflow_text(x)",
            "  non_missing_text <- text_x[!missing_key]",
            "  numeric_candidate <- suppressWarnings(as.numeric(non_missing_text))",
            "  if (length(non_missing_text) > 0 && all(!is.na(numeric_candidate))) {",
            "    sort_key <- suppressWarnings(as.numeric(text_x))",
            "  } else {",
            "    sort_key <- xtfrm(tolower(text_x))",
            "  }",
            "  if (descending) {",
            "    sort_key <- -sort_key",
            "  }",
            "  order(missing_key, sort_key, na.last = TRUE)",
            "}",
            "",
            "rflow_concat_row <- function(values, separator = \"\") {",
            "  valid_values <- values[!rflow_is_missing(values)]",
            "  if (length(valid_values) == 0) {",
            "    return(NA_character_)",
            "  }",
            "  paste(as.character(valid_values), collapse = separator)",
            "}",
            "",
        ]

        if not cleaning_steps:
            lines.append("# 当前还没有记录任何清洗步骤")
            if include_result_output:
                lines.extend(
                    [
                        "# 清洗完成后的数据结果",
                        "cleaned_data",
                    ]
                )
            return "\n".join(lines)

        for step in cleaning_steps:
            lines.extend(self._build_step_lines(step))
            lines.append("")

        if include_result_output:
            lines.extend(
                [
                    "# 清洗完成后的数据结果",
                    "cleaned_data",
                ]
            )
        return "\n".join(lines)

    def _build_package_line(self, extension: str) -> str:
        """根据文件扩展名生成依赖包说明。"""
        if extension == ".csv":
            return "library(readr)"
        if extension == ".xlsx":
            return "library(readxl)"
        if extension == ".sav":
            return "library(haven)"
        return "# TODO: 请根据实际数据格式补充读取依赖"

    def _build_package_name(self, extension: str) -> str:
        """根据文件扩展名返回当前脚本依赖的包名。"""
        if extension == ".csv":
            return "readr"
        if extension == ".xlsx":
            return "readxl"
        if extension == ".sav":
            return "haven"
        return "请根据实际数据格式补充依赖"

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

    def _build_step_lines(self, step: DatasetCleaningStepRecord) -> list[str]:
        """根据步骤类型生成对应的 R 代码片段。"""
        lines = [
            f"# 步骤 {step.order}: {step.name}",
        ]
        if step.description:
            lines.append(f"# 说明: {step.description}")

        if not step.enabled:
            lines.append("# 当前步骤已禁用，暂不参与执行")
            return lines

        if step.step_type == "filter":
            lines.extend(self._build_filter_lines(step))
            return lines
        if step.step_type == "missing_value":
            lines.extend(self._build_missing_value_lines(step))
            return lines
        if step.step_type == "sort":
            lines.extend(self._build_sort_lines(step))
            return lines
        if step.step_type == "recode":
            lines.extend(self._build_recode_lines(step))
            return lines
        if step.step_type == "derive_variable":
            lines.extend(self._build_derive_variable_lines(step))
            return lines

        lines.append("# TODO: 当前步骤类型暂未生成对应的 R 代码")
        return lines

    def _build_filter_lines(self, step: DatasetCleaningStepRecord) -> list[str]:
        """生成筛选步骤的 R 代码。"""
        parameters = step.parameters
        column = str(parameters["column"])
        operator = str(parameters["operator"])
        column_ref = self._build_column_ref(column)

        if operator == "eq":
            condition = f'rflow_text({column_ref}) == {self._build_r_string(parameters["value"])}'
        elif operator == "neq":
            condition = f'rflow_text({column_ref}) != {self._build_r_string(parameters["value"])}'
        elif operator == "gt":
            condition = f"rflow_num({column_ref}) > {self._build_r_number(parameters['value'])}"
        elif operator == "gte":
            condition = f"rflow_num({column_ref}) >= {self._build_r_number(parameters['value'])}"
        elif operator == "lt":
            condition = f"rflow_num({column_ref}) < {self._build_r_number(parameters['value'])}"
        elif operator == "lte":
            condition = f"rflow_num({column_ref}) <= {self._build_r_number(parameters['value'])}"
        elif operator == "between":
            condition = (
                f"rflow_num({column_ref}) >= {self._build_r_number(parameters['start'])} & "
                f"rflow_num({column_ref}) <= {self._build_r_number(parameters['end'])}"
            )
        elif operator == "contains":
            expected = self._build_r_string(str(parameters["value"]).lower())
            condition = (
                f"grepl({expected}, tolower(rflow_text({column_ref})), fixed = TRUE)"
            )
        elif operator == "is_empty":
            condition = f"rflow_is_missing({column_ref})"
        else:
            condition = f"!rflow_is_missing({column_ref})"

        return [
            f"cleaned_data <- cleaned_data[{condition}, , drop = FALSE]",
        ]

    def _build_missing_value_lines(self, step: DatasetCleaningStepRecord) -> list[str]:
        """生成缺失值处理步骤的 R 代码。"""
        parameters = step.parameters
        method = str(parameters["method"])

        if method == "drop_rows":
            return [
                (
                    "cleaned_data <- cleaned_data[!apply(cleaned_data, 1, "
                    "function(row) any(rflow_is_missing(row))), , drop = FALSE]"
                ),
            ]

        column = str(parameters["column"])
        column_ref = self._build_column_ref(column)

        if method == "fill_value":
            fill_value = self._build_r_string(parameters["value"])
            return [
                (
                    f"cleaned_data[[{self._build_r_string(column)}]]"
                    f"[rflow_is_missing({column_ref})] <- {fill_value}"
                ),
            ]

        values = ", ".join(self._build_r_string(item) for item in parameters["values"])
        return [
            (
                f"cleaned_data[[{self._build_r_string(column)}]]"
                f"[rflow_text({column_ref}) %in% c({values})] <- NA"
            ),
        ]

    def _build_sort_lines(self, step: DatasetCleaningStepRecord) -> list[str]:
        """生成排序步骤的 R 代码。"""
        parameters = step.parameters
        column = str(parameters["column"])
        descending = str(parameters["direction"]) == "desc"
        column_name = self._build_r_string(column)
        descending_text = self._build_r_bool(descending)
        return [
            (
                f"order_index <- rflow_order_index(cleaned_data[[{column_name}]], "
                f"descending = {descending_text})"
            ),
            "cleaned_data <- cleaned_data[order_index, , drop = FALSE]",
        ]

    def _build_recode_lines(self, step: DatasetCleaningStepRecord) -> list[str]:
        """生成字段重编码步骤的 R 代码。"""
        parameters = step.parameters
        column = str(parameters["column"])
        mapping_json = json.dumps(parameters["mapping"], ensure_ascii=False)
        return [
            f"recode_map <- c({self._build_named_mapping(mapping_json)})",
            f"current_value <- rflow_text(cleaned_data[[{self._build_r_string(column)}]])",
            "matched_value <- unname(recode_map[current_value])",
            (
                f"cleaned_data[[{self._build_r_string(column)}]] <- ifelse("
                f"!is.na(matched_value), matched_value, "
                f"as.character(cleaned_data[[{self._build_r_string(column)}]])"
                ")"
            ),
        ]

    def _build_derive_variable_lines(self, step: DatasetCleaningStepRecord) -> list[str]:
        """生成新变量步骤的 R 代码。"""
        parameters = step.parameters
        method = str(parameters["method"])
        new_column = str(parameters["new_column"])

        if method == "binary_operation":
            left_column = str(parameters["left_column"])
            right_column = str(parameters["right_column"])
            operator = str(parameters["operator"])
            expression = self._build_binary_expression(
                left_column=left_column,
                right_column=right_column,
                operator=operator,
            )
            return [
                f"left_num <- rflow_num(cleaned_data[[{self._build_r_string(left_column)}]])",
                f"right_num <- rflow_num(cleaned_data[[{self._build_r_string(right_column)}]])",
                f"result_num <- {expression}",
                "result_num[is.na(left_num) | is.na(right_num)] <- NA",
                (
                    f"cleaned_data[[{self._build_r_string(new_column)}]] <- "
                    "rflow_format_number(result_num)"
                ),
            ]

        source_columns = ", ".join(
            self._build_r_string(column) for column in parameters["source_columns"]
        )
        separator = self._build_r_string(parameters["separator"])
        return [
            f"cleaned_data[[{self._build_r_string(new_column)}]] <- apply(",
            f"  cleaned_data[, c({source_columns}), drop = FALSE],",
            "  1,",
            f"  rflow_concat_row, separator = {separator}",
            ")",
        ]

    def _build_binary_expression(
        self,
        left_column: str,
        right_column: str,
        operator: str,
    ) -> str:
        """生成双字段数值运算表达式。"""
        if operator == "add":
            return "left_num + right_num"
        if operator == "subtract":
            return "left_num - right_num"
        if operator == "multiply":
            return "left_num * right_num"
        return "ifelse(right_num == 0, NA_real_, left_num / right_num)"

    def _build_named_mapping(self, mapping_json: str) -> str:
        """把 JSON 映射转换为 R 命名向量文本。"""
        mapping = json.loads(mapping_json)
        items = []
        for source, target in mapping.items():
            items.append(f"{self._build_r_string(target)} = {self._build_r_string(source)}")

        return ", ".join(items)

    def _build_column_ref(self, column: str) -> str:
        """生成列引用表达式。"""
        return f'cleaned_data[[{self._build_r_string(column)}]]'

    def _build_r_string(self, value: object) -> str:
        """把 Python 值转成 R 字符串字面量。"""
        escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'

    def _build_r_number(self, value: object) -> str:
        """把 Python 值转成 R 数值字面量。"""
        return str(value).strip()

    def _build_r_bool(self, value: bool) -> str:
        """把 Python 布尔值转成 R 布尔字面量。"""
        return "TRUE" if value else "FALSE"
