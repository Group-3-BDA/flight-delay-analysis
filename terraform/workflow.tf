resource "aws_glue_workflow" "flight_delay_workflow" {
  name        = "flight-delay-workflow"
  description = "Flight Delay ETL Workflow"

  default_run_properties = {}
}
