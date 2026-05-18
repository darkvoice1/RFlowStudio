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
