data "aws_caller_identity" "current" {}

locals {
  common_tags = "${map(
    "managedBy" , "terraform",
    "Name"      , "${var.DSS_INFRA_TAG_SERVICE}-asyncdynamodb",
    "project"   , "${var.DSS_INFRA_TAG_PROJECT}",
    "env"       , "${var.DSS_DEPLOYMENT_STAGE}",
    "service"   , "${var.DSS_INFRA_TAG_SERVICE}",
    "owner"     , "${var.DSS_INFRA_TAG_OWNER}"
  )}"
}

resource "aws_dynamodb_table" "sfn_state" {
  name         = "dss-async-state-${var.DSS_DEPLOYMENT_STAGE}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "hash_key"

  ttl {
    attribute_name = "TimeToExist"
    enabled        = true
  }

  attribute {
    name = "hash_key"
    type = "S"
  }

  tags = "${local.common_tags}"
}
