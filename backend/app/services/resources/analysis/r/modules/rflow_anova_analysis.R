rflow_anova_result <- function(data_frame) {
  target_variable <- variables[[1]]
  if (is.null(group_variable)) {
    stop("单因素方差分析必须提供分组字段。")
  }

  group_values <- rflow_normalize_text(data_frame[[group_variable]])
  target_values <- rflow_normalize_text(data_frame[[target_variable]])
  complete_index <- stats::complete.cases(group_values, target_values)
  group_values <- group_values[complete_index]
  target_values <- target_values[complete_index]
  numeric_target_values <- suppressWarnings(as.numeric(target_values))
  if (any(is.na(numeric_target_values))) {
    stop(paste0("单因素方差分析当前仅支持数值型目标字段：", target_variable, "。"))
  }

  group_names <- sort(unique(group_values))
  if (length(group_names) < 2) {
    stop("单因素方差分析要求有效样本中至少包含两个组别。")
  }

  grouped_lists <- lapply(group_names, function(group_name) {
    numeric_target_values[group_values == group_name]
  })
  names(grouped_lists) <- group_names
  if (sum(vapply(grouped_lists, length, integer(1))) <= length(group_names)) {
    stop("单因素方差分析要求总样本量大于组别数量。")
  }

  anova_data <- data.frame(
    target = numeric_target_values,
    group = factor(group_values, levels = group_names)
  )
  anova_result <- stats::aov(target ~ group, data = anova_data)
  anova_summary <- summary(anova_result)[[1]]
  between_row <- anova_summary[1, ]
  within_row <- anova_summary[2, ]
  total_count <- length(numeric_target_values)
  cleaned_row_count <- nrow(data_frame)
  excluded_row_count <- max(raw_row_count - cleaned_row_count, 0)

  list(
    dataset_id = payload$dataset_id,
    dataset_name = payload$dataset_name,
    file_name = payload$file_name,
    analysis_type = analysis_type,
    variables = as.list(variables),
    group_variable = group_variable,
    status = "completed",
    summary = list(
      title = "单因素方差分析",
      analysis_type = analysis_type,
      effective_row_count = cleaned_row_count,
      excluded_row_count = excluded_row_count,
      missing_value_strategy = paste0(
        "先应用当前清洗步骤，再按目标字段与分组字段都非缺失的样本执行",
        "单因素方差分析。"
      ),
      note = paste0(
        "目标字段 ", target_variable, " 按分组字段 ", group_variable,
        " 的 ", length(group_names), " 个组别进行均值差异比较。"
      )
    ),
    tables = list(
      list(
        key = "anova_group_summary",
        title = "分组样本汇总",
        columns = list("group", "count", "mean", "variance", "std_dev", "min", "max"),
        rows = lapply(group_names, function(group_name) {
          rflow_group_summary_row(group_name, grouped_lists[[group_name]])
        })
      ),
      list(
        key = "anova_summary",
        title = "方差分析表",
        columns = list(
          "source", "sum_of_squares", "degrees_of_freedom",
          "mean_square", "f_value", "p_value"
        ),
        rows = list(
          list(
            source = "between_groups",
            sum_of_squares = rflow_round_number(between_row[["Sum Sq"]]),
            degrees_of_freedom = as.integer(between_row[["Df"]]),
            mean_square = rflow_round_number(between_row[["Mean Sq"]]),
            f_value = rflow_round_number(between_row[["F value"]]),
            p_value = rflow_round_number(between_row[["Pr(>F)"]])
          ),
          list(
            source = "within_groups",
            sum_of_squares = rflow_round_number(within_row[["Sum Sq"]]),
            degrees_of_freedom = as.integer(within_row[["Df"]]),
            mean_square = rflow_round_number(within_row[["Mean Sq"]]),
            f_value = NULL,
            p_value = NULL
          ),
          list(
            source = "total",
            sum_of_squares = rflow_round_number(
              between_row[["Sum Sq"]] + within_row[["Sum Sq"]]
            ),
            degrees_of_freedom = total_count - 1,
            mean_square = NULL,
            f_value = NULL,
            p_value = NULL
          )
        )
      )
    ),
    plots = list(
      list(
        key = "anova_boxplot",
        title = paste0(target_variable, " 按 ", group_variable, " 分组箱线图"),
        plot_type = "grouped_boxplot",
        spec = list(
          target_variable = target_variable,
          group_variable = group_variable,
          groups = lapply(group_names, function(group_name) {
            list(
              name = group_name,
              values = lapply(grouped_lists[[group_name]], rflow_round_number)
            )
          })
        )
      ),
      list(
        key = "anova_mean_bar",
        title = paste0(target_variable, " 分组均值对比图"),
        plot_type = "bar_chart",
        spec = list(
          categories = as.list(group_names),
          values = lapply(
            lapply(group_names, function(group_name) mean(grouped_lists[[group_name]])),
            rflow_round_number
          )
        )
      )
    ),
    interpretations = list(
      paste0(
        "字段 ", target_variable, " 按 ", group_variable,
        " 分组后的单因素方差分析 F 统计量为 ",
        rflow_round_number(between_row[["F value"]]), "，组间自由度为 ",
        as.integer(between_row[["Df"]]), "，组内自由度为 ",
        as.integer(within_row[["Df"]]), "，p 值为 ",
        rflow_round_number(between_row[["Pr(>F)"]]), "。"
      )
    )
  )
}
