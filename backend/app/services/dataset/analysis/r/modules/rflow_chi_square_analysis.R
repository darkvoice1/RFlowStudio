rflow_chi_square_result <- function(data_frame) {
  left_variable <- variables[[1]]
  right_variable <- variables[[2]]
  left_values <- rflow_normalize_text(data_frame[[left_variable]])
  right_values <- rflow_normalize_text(data_frame[[right_variable]])
  complete_index <- stats::complete.cases(left_values, right_values)
  left_values <- left_values[complete_index]
  right_values <- right_values[complete_index]
  if (length(left_values) < 2) {
    stop("卡方检验的有效成对样本不足 2 条。")
  }

  left_levels <- sort(unique(left_values))
  right_levels <- sort(unique(right_values))
  if (length(left_levels) < 2 || length(right_levels) < 2) {
    stop("卡方检验要求两个字段在有效样本中都至少包含两个取值。")
  }

  observed_table <- table(
    factor(left_values, levels = left_levels),
    factor(right_values, levels = right_levels)
  )
  chi_result <- suppressWarnings(stats::chisq.test(observed_table, correct = FALSE))
  observed_matrix <- as.matrix(observed_table)
  expected_matrix <- chi_result$expected
  summary_row <- list(
    variable_x = left_variable,
    variable_y = right_variable,
    sample_count = length(left_values),
    degrees_of_freedom = as.integer(chi_result$parameter),
    chi_square = rflow_round_number(chi_result$statistic),
    p_value = rflow_round_number(chi_result$p.value)
  )

  build_matrix_rows <- function(levels_row, levels_col, matrix_values, round_values = FALSE) {
    lapply(seq_along(levels_row), function(index) {
      row <- list(variable = levels_row[[index]])
      for (column_name in levels_col) {
        value <- matrix_values[index, column_name]
        row[[column_name]] <- if (round_values) rflow_round_number(value) else as.integer(value)
      }
      row
    })
  }

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
      title = "卡方检验",
      analysis_type = analysis_type,
      effective_row_count = cleaned_row_count,
      excluded_row_count = excluded_row_count,
      missing_value_strategy = "先应用当前清洗步骤，再按两个字段都非缺失的成对样本执行卡方检验。",
      note = paste0(
        "卡方检验基于 ", length(left_values), " 条有效成对样本，比较字段 ",
        left_variable, " 与 ", right_variable, " 的分类分布差异。"
      )
    ),
    tables = list(
      list(
        key = "chi_square_observed",
        title = "列联表（观测频数）",
        columns = c(list("variable"), as.list(right_levels)),
        rows = build_matrix_rows(left_levels, right_levels, observed_matrix, FALSE)
      ),
      list(
        key = "chi_square_expected",
        title = "列联表（期望频数）",
        columns = c(list("variable"), as.list(right_levels)),
        rows = build_matrix_rows(left_levels, right_levels, expected_matrix, TRUE)
      ),
      list(
        key = "chi_square_summary",
        title = "卡方检验汇总",
        columns = list(
          "variable_x", "variable_y", "sample_count",
          "degrees_of_freedom", "chi_square", "p_value"
        ),
        rows = list(summary_row)
      )
    ),
    plots = list(
      list(
        key = "chi_square_grouped_bar",
        title = paste0(left_variable, " 与 ", right_variable, " 分组条形图"),
        plot_type = "grouped_bar_chart",
        spec = list(
          x_categories = as.list(left_levels),
          series = lapply(right_levels, function(level_name) {
            list(
              name = level_name,
              counts = as.list(as.integer(observed_matrix[, level_name]))
            )
          })
        )
      ),
      list(
        key = "chi_square_heatmap",
        title = paste0(left_variable, " 与 ", right_variable, " 列联热力图"),
        plot_type = "heatmap",
        spec = list(
          x_categories = as.list(right_levels),
          y_categories = as.list(left_levels),
          cells = unlist(
            lapply(seq_along(left_levels), function(row_index) {
              lapply(seq_along(right_levels), function(column_index) {
                list(
                  x = right_levels[[column_index]],
                  y = left_levels[[row_index]],
                  value = as.integer(observed_matrix[row_index, column_index])
                )
              })
            }),
            recursive = FALSE
          )
        )
      )
    ),
    interpretations = list(
      paste0(
        "字段 ", left_variable, " 与 ", right_variable,
        " 的卡方检验统计量为 ", summary_row$chi_square,
        "，自由度为 ", summary_row$degrees_of_freedom,
        "，p 值为 ", summary_row$p_value, "。"
      )
    )
  )
}
