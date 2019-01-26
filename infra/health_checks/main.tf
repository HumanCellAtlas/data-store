data aws_caller_identity current {}
data aws_region current {}
data dss_canary_timer current {}

locals {
  region = "${data.aws_region.current.name}"
  account_id = "${data.aws_caller_identity.current.account_id}"
}
resource "aws_cloudwatch_event_rule" "dss-canary-delete" {
  name        = "/aws/${var.DSS_DEPLOYMENT_STAGE}-canary-delete"
  description = "deletes dss-canary file for health checks"

  schedule_expression = "rate(${data.dss_canary_timer})"
}
resource "aws_cloudwatch_event_rule" "dss-canary-create" {
  name        = "/aws/${var.DSS_DEPLOYMENT_STAGE}-canary-create"
  description = "create dss-canary file for health checks"

  schedule_expression = "rate(${data.dss_canary_timer})"
}