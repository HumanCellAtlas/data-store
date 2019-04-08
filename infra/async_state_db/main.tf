data "aws_caller_identity" "current" {}

locals {
  common_tags = "${map(
    "managedBy" , "terraform",
    "Name"      , "${var.DSS_INFRA_TAG_PROJECT}-${var.DSS_DEPLOYMENT_STAGE}-${var.DSS_INFRA_TAG_SERVICE}",
    "project"   , "${var.DSS_INFRA_TAG_PROJECT}",
    "env"       , "${var.DSS_DEPLOYMENT_STAGE}",
    "service"   , "${var.DSS_INFRA_TAG_SERVICE}",
    "owner"     , "admin@dss"
  )}"
}

resource "aws_dynamodb_table" "sfn_state" {
  name         = "dss-async-state-${var.DSS_DEPLOYMENT_STAGE}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "key"

  attribute {
    name = "key"
    type = "S"
  }

  tags = "${local.common_tags}"
}
