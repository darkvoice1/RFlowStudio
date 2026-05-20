rflow_t_test_result <- function(data_frame) {
  target_variable <- variables[[1]]
  if (is.null(group_variable)) {
    stop("独立样本 t 检验必须提供分组字段。")
  }

  group_values <- rflow_normalize_text(data_frame[[group_variable]])
  target_values <- rflow_normalize_text(data_frame[[target_variable]])
  complete_index <- stats::complete.cases(group_values, target_values)
  group_values <- group_values[complete_index]
  target_values <- target_values[complete_index]
  numeric_target_values <- suppressWarnings(as.numeric(target_values))
  if (any(is.na(numeric_target_values))) {
    stop(paste0("独立样本 t 检验当前仅支持数值型目标字段：", target_variable, "。"))
  }

  group_names <- sort(unique(group_values))
  if (length(group_names) != 2) {
    stop("独立样本 t 检验要求分组字段在有效样本中恰好包含两个组别。")
  }

  grouped_lists <- lapply(group_names, function(group_name) {
    numeric_target_values[group_values == group_name]
  })
  names(grouped_lists) <- group_names
  if (any(vapply(grouped_lists, length, integer(1)) < 2)) {
    stop("独立样本 t 检验要求两个组别都至少包含 2 条有效样本。")
  }

  test_data <- data.frame(
    target = numeric_target_values,
    group = factor(group_values, levels = group_names)
  )
  test_result <- stats::t.test(target ~ group, data = test_data, var.equal = TRUE)
  summary_row <- list(
    target_variable = target_variable,
    group_variable = group_variable,
    group_a = group_names[[1]],
    group_b = group_names[[2]],
    mean_difference = rflow_round_number(
      mean(grouped_lists[[group_names[[1]]]]) - mean(grouped_lists[[group_names[[2]]]])
    ),
    degrees_of_freedom = as.integer(test_result$parameter),
    t_statistic = rflow_round_number(test_result$statistic),
    p_value = rflow_round_number(test_result$p.value)
  )
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
      title = "独立样本 t 检验",
      analysis_type = analysis_type,
      effective_row_count = cleaned_row_count,
      excluded_row_count = excluded_row_count,
      missing_value_strategy = paste0(
        "先应用当前清洗步骤，再按目标字段与分组字段都非缺失的样本执行",
        "独立样本 t 检验。"
      ),
      note = paste0(
        "目标字段 ", target_variable, " 在组别 ", group_names[[1]],
        " 与 ", group_names[[2]], " 之间进行均值比较。"
      )
    ),
    tables = list(
      list(
        key = "t_test_group_summary",
        title = "分组样本汇总",
        columns = list("group", "count", "mean", "variance", "std_dev", "min", "max"),
        rows = lapply(group_names, function(group_name) {
          rflow_group_summary_row(group_name, grouped_lists[[group_name]])
        })
      ),
      list(
        key = "t_test_result",
        title = "独立样本 t 检验结果",
        columns = list(
          "target_variable", "group_variable", "group_a", "group_b",
          "mean_difference", "degrees_of_freedom", "t_statistic", "p_value"
        ),
        rows = list(summary_row)
      )
    ),
    plots = list(
      list(
        key = "t_test_boxplot",
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
        key = "t_test_mean_bar",
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
        "字段 ", target_variable, " 在组别 ", group_names[[1]], " 与 ", group_names[[2]],
        " 之间的 t 统计量为 ", summary_row$t_statistic,
        "，自由度为 ", summary_row$degrees_of_freedom,
        "，p 值为 ", summary_row$p_value, "。"
      )
    )
  )
}
