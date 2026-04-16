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

script_path_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
if (length(script_path_arg) == 0) {
  stop("无法确定 dataset_analysis_runner.R 所在目录。")
}

script_dir <- dirname(normalizePath(sub("^--file=", "", script_path_arg[[1]])))
module_dir <- file.path(script_dir, "modules")

source(file.path(module_dir, "rflow_analysis_utils.R"), local = FALSE, encoding = "UTF-8")
source(file.path(module_dir, "rflow_descriptive_analysis.R"), local = FALSE, encoding = "UTF-8")
source(file.path(module_dir, "rflow_correlation_analysis.R"), local = FALSE, encoding = "UTF-8")
source(file.path(module_dir, "rflow_chi_square_analysis.R"), local = FALSE, encoding = "UTF-8")
source(file.path(module_dir, "rflow_t_test_analysis.R"), local = FALSE, encoding = "UTF-8")
source(file.path(module_dir, "rflow_anova_analysis.R"), local = FALSE, encoding = "UTF-8")

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
