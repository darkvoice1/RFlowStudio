#* @get /health
function(res) {
  res$setHeader("Content-Type", "application/json")
  res$body <- jsonlite::toJSON(
    list(status = "ok"),
    auto_unbox = TRUE,
    pretty = TRUE
  )
  res
}

#* @post /analysis
function(req, res) {
  request_body <- req$postBody
  if (is.null(request_body) || !nzchar(trimws(request_body))) {
    res$status <- 400
    res$setHeader("Content-Type", "application/json")
    res$body <- jsonlite::toJSON(
      list(detail = "R 统计服务收到的请求体为空。"),
      auto_unbox = TRUE,
      pretty = TRUE
    )
    return(res)
  }

  script_dir <- dirname(normalizePath(sub("^--file=", "", commandArgs()[grep("^--file=", commandArgs())][1])))
  runner_path <- file.path(script_dir, "dataset_analysis_runner.R")
  if (!file.exists(runner_path)) {
    res$status <- 500
    res$setHeader("Content-Type", "application/json")
    res$body <- jsonlite::toJSON(
      list(detail = "R 统计执行脚本不存在。"),
      auto_unbox = TRUE,
      pretty = TRUE
    )
    return(res)
  }

  input_path <- tempfile(fileext = ".json")
  output_path <- tempfile(fileext = ".json")
  on.exit(unlink(c(input_path, output_path), force = TRUE), add = TRUE)
  writeLines(request_body, input_path, useBytes = TRUE)

  command_output <- tryCatch(
    system2(
      "Rscript",
      c(runner_path, input_path, output_path),
      stdout = TRUE,
      stderr = TRUE
    ),
    error = function(exc) {
      structure(
        character(0),
        status = 1L,
        error_message = conditionMessage(exc)
      )
    }
  )
  exit_status <- attr(command_output, "status")
  if (is.null(exit_status)) {
    exit_status <- 0L
  }

  if (exit_status != 0L) {
    detail <- attr(command_output, "error_message")
    if (is.null(detail) || !nzchar(trimws(detail))) {
      detail <- paste(command_output, collapse = "\n")
    }
    if (!nzchar(trimws(detail))) {
      detail <- "Rscript 执行失败，但没有返回具体错误信息。"
    }

    res$status <- 500
    res$setHeader("Content-Type", "application/json")
    res$body <- jsonlite::toJSON(
      list(detail = detail),
      auto_unbox = TRUE,
      pretty = TRUE
    )
    return(res)
  }

  if (!file.exists(output_path)) {
    res$status <- 500
    res$setHeader("Content-Type", "application/json")
    res$body <- jsonlite::toJSON(
      list(detail = "R 统计执行完成，但没有生成结果文件。"),
      auto_unbox = TRUE,
      pretty = TRUE
    )
    return(res)
  }

  result_json <- paste(readLines(output_path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
  res$status <- 200
  res$setHeader("Content-Type", "application/json")
  res$body <- result_json
  res
}
