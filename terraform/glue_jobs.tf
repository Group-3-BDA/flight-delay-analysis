resource "aws_glue_job" "bronze_job" {
  name     = "bronze-to-silver-glue-test"
  role_arn = var.glue_role_arn

  glue_version      = "5.1"
  execution_class   = "STANDARD"
  worker_type       = "G.1X"
  number_of_workers = 5
  timeout           = 480
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://flight-delay-scripts/glue/bronze_to_silver/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-metrics"                   = ""
    "--enable-spark-ui"                  = "true"
    "--extra-py-files"                   = "s3://flight-delay-scripts/glue/bronze_to_silver/dependencies.zip"
    "--spark-event-logs-path"            = "s3://aws-glue-assets-851725344261-us-east-1/sparkHistoryLogs/"
    "--enable-job-insights"              = "false"
    "--enable-observability-metrics"     = "true"
    "--conf"                             = "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-glue-datacatalog"          = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://aws-glue-assets-851725344261-us-east-1/temporary/"
  }

  execution_property {
    max_concurrent_runs = 1
  }
}

resource "aws_glue_job" "silver_job" {
  name     = "Silver_to_Gold_test"
  role_arn = var.glue_role_arn

  glue_version      = "5.1"
  execution_class   = "STANDARD"
  worker_type       = "G.1X"
  number_of_workers = 10
  timeout           = 480
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://flight-delay-scripts/glue/silver_to_gold/main.py"
    python_version  = "3"
  }

  default_arguments = {
    "--INPUT_PATH"                       = "s3://silver-demo12/Silver_Glue_Test/"
    "--GOLD_BASE_PATH"                   = "s3://gold-demo12/Gold_Glue_Test/"
    "--enable-glue-datacatalog"          = ""
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--TempDir"                          = "s3://aws-glue-assets-851725344261-us-east-1/temporary/"
    "--enable-metrics"                   = ""
    "--enable-spark-ui"                  = "true"
    "--extra-py-files"                   = "s3://flight-delay-scripts/glue/silver_to_gold/silver_to_gold_lib.zip"
    "--spark-event-logs-path"            = "s3://aws-glue-assets-851725344261-us-east-1/sparkHistoryLogs/"
    "--enable-job-insights"              = "false"
    "--enable-observability-metrics"     = "true"
    "--conf"                             = "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
  }

  execution_property {
    max_concurrent_runs = 1
  }
}
