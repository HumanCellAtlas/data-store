data "aws_caller_identity" "current" {}
locals {
  common_tags = "${map(
    "managedBy" , "terraform",
    "Name"      , "${var.DSS_INFRA_TAG_SERVICE}-collectionsdynamodb",
    "project"   , "${var.DSS_INFRA_TAG_PROJECT}",
    "env"       , "${var.DSS_DEPLOYMENT_STAGE}",
    "service"   , "${var.DSS_INFRA_TAG_SERVICE}",
    "owner"     , "${var.DSS_INFRA_TAG_OWNER}"
  )}"
  replicas = ["aws", "gcp"]
}

resource "aws_dynamodb_table" "collections-db-aws" {
  name         = "dss-collections-db-${var.DSS_DEPLOYMENT_STAGE}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "hash_key"
  range_key    = "sort_key"

  point_in_time_recovery {
    enabled = true
  }

  attribute {
    name = "hash_key"
    type = "S"
  }

  attribute {
    name = "sort_key"
    type = "S"
  }

  tags = "${local.common_tags}"
}
