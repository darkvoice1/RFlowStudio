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
