if (!requireNamespace("plumber", quietly = TRUE)) {
  stop("The plumber package is required to run the R analysis API server.")
}

script_file_argument <- commandArgs()[grep("^--file=", commandArgs())][1]
script_dir <- dirname(normalizePath(sub("^--file=", "", script_file_argument)))
port_argument <- commandArgs(trailingOnly = TRUE)
port <- if (length(port_argument) >= 1) as.integer(port_argument[[1]]) else 8090L

router <- plumber::plumb(file.path(script_dir, "dataset_analysis_api.R"))
router$run(host = "0.0.0.0", port = port)
