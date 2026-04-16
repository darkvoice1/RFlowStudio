args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2) {
  stop("Usage: Rscript dataset_analysis_runner.R <input_json> <output_json>")
}

if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("The jsonlite package is required to run analysis via Rscript.")
}

input_path <- args[[1]]
output_path <- args[[2]]
payload <- jsonlite::fromJSON(input_path, simplifyVector = FALSE)

columns <- unlist(payload$columns, use.names = FALSE)
variables <- unlist(payload$variables, use.names = FALSE)
analysis_type <- payload$analysis_type
group_variable <- payload$group_variable
raw_row_count <- payload$raw_row_count
options <- payload$options
row_items <- payload$rows

rflow_round_number <- function(x) {
  rounded <- round(as.numeric(x), 4)
  if (isTRUE(all.equal(rounded, as.integer(rounded)))) {
    return(as.integer(rounded))
  }

  rounded
}

rflow_normalize_text <- function(values) {
  normalized <- trimws(as.character(values))
  normalized[is.na(values) | normalized == ""] <- NA_character_
  normalized
}

rflow_is_numeric_column <- function(values) {
  non_missing <- values[!is.na(values)]
  if (length(non_missing) == 0) {
    return(FALSE)
  }

  all(!is.na(suppressWarnings(as.numeric(non_missing))))
}

rflow_build_data_frame <- function(rows, columns) {
  if (length(columns) == 0) {
    return(data.frame())
  }

  if (is.null(rows) || length(rows) == 0) {
    empty_columns <- lapply(columns, function(column) character(0))
    names(empty_columns) <- columns
    return(data.frame(empty_columns, check.names = FALSE, stringsAsFactors = FALSE))
  }

  column_data <- lapply(columns, function(column) {
    vapply(
      rows,
      function(row) {
        value <- row[[column]]
        if (is.null(value)) {
          return(NA_character_)
        }

        as.character(value)
      },
      character(1)
    )
  })
  names(column_data) <- columns
  data.frame(column_data, check.names = FALSE, stringsAsFactors = FALSE)
}

rflow_group_summary_row <- function(group_name, values) {
  group_variance <- if (length(values) > 1) stats::var(values) else 0
  list(
    group = group_name,
    count = length(values),
    mean = rflow_round_number(mean(values)),
    variance = rflow_round_number(group_variance),
    std_dev = rflow_round_number(sqrt(group_variance)),
    min = rflow_round_number(min(values)),
    max = rflow_round_number(max(values))
  )
}

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

rflow_correlation_result <- function(data_frame) {
  method <- if (!is.null(options$method)) {
    tolower(trimws(as.character(options$method)))
  } else {
    "pearson"
  }
  if (method != "pearson") {
    stop("当前相关分析仅支持 pearson 方法。")
  }

  invalid_columns <- c()
  numeric_columns <- list()
  for (variable in variables) {
    normalized_values <- rflow_normalize_text(data_frame[[variable]])
    non_missing_values <- normalized_values[!is.na(normalized_values)]
    numeric_values <- suppressWarnings(as.numeric(non_missing_values))
    if (length(non_missing_values) > 0 && any(is.na(numeric_values))) {
      invalid_columns <- c(invalid_columns, variable)
    } else {
      numeric_columns[[variable]] <- suppressWarnings(as.numeric(normalized_values))
    }
  }
  if (length(invalid_columns) > 0) {
    stop(paste0("相关分析当前仅支持数值型字段：", paste(sort(unique(invalid_columns)), collapse = "、"), "。"))
  }

  pair_rows <- list()
  interpretations <- list()
  scatter_plots <- list()
  heatmap_cells <- list()
  correlation_lookup <- list()
  pair_count_lookup <- list()
  combinations <- combn(variables, 2, simplify = FALSE)
  for (pair in combinations) {
    left <- pair[[1]]
    right <- pair[[2]]
    complete_index <- stats::complete.cases(numeric_columns[[left]], numeric_columns[[right]])
    x_values <- numeric_columns[[left]][complete_index]
    y_values <- numeric_columns[[right]][complete_index]
    pair_count <- length(x_values)
    if (pair_count < 2) {
      stop(paste0("字段 ", left, " 和 ", right, " 的有效成对样本不足 2 条，暂时无法进行相关分析。"))
    }

    correlation_value <- NULL
    if (stats::sd(x_values) != 0 && stats::sd(y_values) != 0) {
      correlation_value <- stats::cor(x_values, y_values, method = "pearson")
    }
    key <- paste(left, right, sep = "__")
    correlation_lookup[[key]] <- correlation_value
    pair_count_lookup[[key]] <- pair_count
    pair_rows[[length(pair_rows) + 1]] <- list(
      variable_x = left,
      variable_y = right,
      method = method,
      pair_count = pair_count,
      correlation = if (is.null(correlation_value)) NULL else rflow_round_number(correlation_value)
    )
    heatmap_cells[[length(heatmap_cells) + 1]] <- list(
      x = left,
      y = right,
      value = if (is.null(correlation_value)) NULL else rflow_round_number(correlation_value)
    )
    heatmap_cells[[length(heatmap_cells) + 1]] <- list(
      x = right,
      y = left,
      value = if (is.null(correlation_value)) NULL else rflow_round_number(correlation_value)
    )
    scatter_plots[[length(scatter_plots) + 1]] <- list(
      key = paste0(left, "_", right, "_scatter"),
      title = paste0(left, " 与 ", right, " 散点图"),
      plot_type = "scatter_plot",
      spec = list(
        x = left,
        y = right,
        points = lapply(seq_along(x_values), function(index) {
          list(
            x = rflow_round_number(x_values[[index]]),
            y = rflow_round_number(y_values[[index]])
          )
        })
      )
    )
    if (is.null(correlation_value)) {
      interpretations[[length(interpretations) + 1]] <- paste0(
        "字段 ", left, " 与 ", right, " 共有 ", pair_count,
        " 条成对有效样本，但至少有一个字段方差为 0，当前无法计算有效相关系数。"
      )
    } else {
      direction <- if (correlation_value > 0) "正相关" else if (correlation_value < 0) "负相关" else "线性相关性较弱"
      interpretations[[length(interpretations) + 1]] <- paste0(
        "字段 ", left, " 与 ", right, " 基于 ", pair_count,
        " 条成对有效样本计算得到皮尔逊相关系数 ",
        rflow_round_number(correlation_value), "，表现为", direction, "。"
      )
    }
  }

  correlation_matrix_rows <- lapply(variables, function(left) {
    row <- list(variable = left)
    for (right in variables) {
      if (identical(left, right)) {
        row[[right]] <- 1
      } else {
        key <- paste(left, right, sep = "__")
        reverse_key <- paste(right, left, sep = "__")
        correlation_value <- correlation_lookup[[key]]
        if (is.null(correlation_value)) {
          correlation_value <- correlation_lookup[[reverse_key]]
        }
        row[[right]] <- if (is.null(correlation_value)) NULL else rflow_round_number(correlation_value)
      }
    }
    row
  })

  pair_count_rows <- lapply(variables, function(left) {
    row <- list(variable = left)
    for (right in variables) {
      if (identical(left, right)) {
        row[[right]] <- NULL
      } else {
        key <- paste(left, right, sep = "__")
        reverse_key <- paste(right, left, sep = "__")
        pair_count <- pair_count_lookup[[key]]
        if (is.null(pair_count)) {
          pair_count <- pair_count_lookup[[reverse_key]]
        }
        row[[right]] <- pair_count
      }
    }
    row
  })

  cleaned_row_count <- nrow(data_frame)
  excluded_row_count <- max(raw_row_count - cleaned_row_count, 0)
  note <- "当前第一版相关分析默认采用皮尔逊相关，并按成对非缺失样本计算。"
  if (excluded_row_count > 0) {
    note <- paste0(
      "当前数据清洗步骤已先剔除 ", excluded_row_count,
      " 行记录，随后按成对非缺失样本执行皮尔逊相关分析。"
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
      title = "相关分析",
      analysis_type = analysis_type,
      effective_row_count = cleaned_row_count,
      excluded_row_count = excluded_row_count,
      missing_value_strategy = "先应用当前清洗步骤，再按字段对的成对非缺失样本计算皮尔逊相关。",
      note = note
    ),
    tables = list(
      list(
        key = "correlation_matrix",
        title = "相关系数矩阵",
        columns = c(list("variable"), as.list(variables)),
        rows = correlation_matrix_rows
      ),
      list(
        key = "correlation_pair_counts",
        title = "成对样本量矩阵",
        columns = c(list("variable"), as.list(variables)),
        rows = pair_count_rows
      ),
      list(
        key = "correlation_pairs",
        title = "字段对相关结果",
        columns = list("variable_x", "variable_y", "method", "pair_count", "correlation"),
        rows = pair_rows
      )
    ),
    plots = c(
      list(
        list(
          key = "correlation_heatmap",
          title = "相关系数热力图",
          plot_type = "heatmap",
          spec = list(
            variables = as.list(variables),
            cells = c(
              lapply(variables, function(variable) {
                list(x = variable, y = variable, value = 1)
              }),
              heatmap_cells
            )
          )
        )
      ),
      scatter_plots
    ),
    interpretations = interpretations
  )
}

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

data_frame <- rflow_build_data_frame(row_items, columns)
result <- switch(
  analysis_type,
  descriptive_statistics = rflow_descriptive_result(data_frame),
  correlation_analysis = rflow_correlation_result(data_frame),
  chi_square_test = rflow_chi_square_result(data_frame),
  independent_samples_t_test = rflow_t_test_result(data_frame),
  one_way_anova = rflow_anova_result(data_frame),
  stop("当前分析类型暂不支持通过 R 执行。")
)

writeLines(
  jsonlite::toJSON(
    result,
    auto_unbox = TRUE,
    null = "null",
    na = "null",
    pretty = TRUE
  ),
  output_path,
  useBytes = TRUE
)
