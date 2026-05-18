rflow_descriptive_result <- function(data_frame) {
  summary_rows <- list()
  extra_tables <- list()
  plots <- list()
  interpretations <- list()

  for (variable in variables) {
    values <- rflow_normalize_text(data_frame[[variable]])
    non_missing_values <- values[!is.na(values)]
    missing_count <- length(values) - length(non_missing_values)
    unique_count <- length(unique(non_missing_values))

    if (rflow_is_numeric_column(values)) {
      numeric_values <- suppressWarnings(as.numeric(non_missing_values))
      stats_row <- list(
        variable = variable,
        field_type = "numeric",
        valid_count = length(numeric_values),
        missing_count = missing_count,
        unique_count = unique_count,
        mean = if (length(numeric_values) > 0) rflow_round_number(mean(numeric_values)) else NULL,
        median = if (length(numeric_values) > 0) rflow_round_number(stats::median(numeric_values)) else NULL,
        std_dev = if (length(numeric_values) > 1) rflow_round_number(stats::sd(numeric_values)) else NULL,
        min = if (length(numeric_values) > 0) rflow_round_number(min(numeric_values)) else NULL,
        max = if (length(numeric_values) > 0) rflow_round_number(max(numeric_values)) else NULL
      )
      summary_rows[[length(summary_rows) + 1]] <- stats_row
      plots[[length(plots) + 1]] <- list(
        key = paste0(variable, "_histogram"),
        title = paste0(variable, " 直方图"),
        plot_type = "histogram",
        spec = list(
          variable = variable,
          values = lapply(numeric_values, rflow_round_number)
        )
      )
      plots[[length(plots) + 1]] <- list(
        key = paste0(variable, "_boxplot"),
        title = paste0(variable, " 箱线图"),
        plot_type = "boxplot",
        spec = list(
          variable = variable,
          values = lapply(numeric_values, rflow_round_number)
        )
      )
      interpretations[[length(interpretations) + 1]] <- paste0(
        "字段 ", variable, " 在清洗后的数据中共有 ", length(numeric_values),
        " 个有效值，均值为 ", stats_row$mean, "，中位数为 ", stats_row$median, "。"
      )
    } else {
      counts <- sort(table(non_missing_values), decreasing = TRUE)
      if (length(counts) > 0) {
        order_index <- order(-as.numeric(counts), names(counts))
        counts <- counts[order_index]
      }
      frequency_rows <- lapply(seq_along(counts), function(index) {
        count_value <- as.numeric(counts[[index]])
        list(
          value = names(counts)[[index]],
          count = count_value,
          ratio = if (length(non_missing_values) > 0) {
            rflow_round_number(count_value / length(non_missing_values))
          } else {
            0
          }
        )
      })
      summary_rows[[length(summary_rows) + 1]] <- list(
        variable = variable,
        field_type = "categorical",
        valid_count = length(non_missing_values),
        missing_count = missing_count,
        unique_count = unique_count,
        mean = NULL,
        median = NULL,
        std_dev = NULL,
        min = NULL,
        max = NULL
      )
      extra_tables[[length(extra_tables) + 1]] <- list(
        key = paste0(variable, "_frequency"),
        title = paste0(variable, " 频数分布"),
        columns = list("value", "count", "ratio"),
        rows = frequency_rows
      )
      plots[[length(plots) + 1]] <- list(
        key = paste0(variable, "_bar_chart"),
        title = paste0(variable, " 条形图"),
        plot_type = "bar_chart",
        spec = list(
          variable = variable,
          categories = as.list(names(counts)),
          counts = as.list(as.numeric(counts))
        )
      )
      top_value <- if (length(counts) > 0) names(counts)[[1]] else NULL
      top_count <- if (length(counts) > 0) as.numeric(counts[[1]]) else NULL
      interpretations[[length(interpretations) + 1]] <- paste0(
        "字段 ", variable, " 在清洗后的数据中共有 ", length(non_missing_values),
        " 个有效值，包含 ", unique_count, " 个唯一取值，最常见取值为 ",
        top_value, "（", top_count, " 次）。"
      )
    }
  }

  cleaned_row_count <- nrow(data_frame)
  excluded_row_count <- max(raw_row_count - cleaned_row_count, 0)
  note <- "各字段默认按非缺失值单独计算统计指标。"
  if (excluded_row_count > 0) {
    note <- paste0(
      "当前数据清洗步骤已先剔除 ", excluded_row_count,
      " 行记录，其余字段再按非缺失值单独计算统计指标。"
    )
  }

  list(
    dataset_id = payload$dataset_id,
    dataset_name = payload$dataset_name,
    file_name = payload$file_name,
    analysis_type = analysis_type,
    variables = as.list(variables),
    group_variable = group_variable,
    status = "completed",
    summary = list(
      title = "描述统计",
      analysis_type = analysis_type,
      effective_row_count = cleaned_row_count,
      excluded_row_count = excluded_row_count,
      missing_value_strategy = "先应用当前清洗步骤，再按各字段非缺失值计算描述统计。",
      note = note
    ),
    tables = c(
      list(
        list(
          key = "descriptive_summary",
          title = "描述统计汇总",
          columns = list(
            "variable", "field_type", "valid_count", "missing_count", "unique_count",
            "mean", "median", "std_dev", "min", "max"
          ),
          rows = summary_rows
        )
      ),
      extra_tables
    ),
    plots = plots,
    interpretations = interpretations
  )
}
