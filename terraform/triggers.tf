resource "aws_glue_trigger" "start_bronze_silver" {
  name          = "Start_Bronze_Silver"
  workflow_name = aws_glue_workflow.flight_delay_workflow.name
  type          = "ON_DEMAND"

  enabled = true

  actions {
    job_name = aws_glue_job.bronze_job.name
  }
}

resource "aws_glue_trigger" "run_silver_gold" {
  name          = "Run_Silver_Gold"
  workflow_name = aws_glue_workflow.flight_delay_workflow.name
  type          = "CONDITIONAL"

  enabled = true

  actions {
    job_name = aws_glue_job.silver_job.name
  }

  predicate {
    logical = "AND"

    conditions {
      job_name = aws_glue_job.bronze_job.name
      state    = "SUCCEEDED"
    }
  }
}
