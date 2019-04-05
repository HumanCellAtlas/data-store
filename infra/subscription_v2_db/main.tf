data "aws_caller_identity" "current" {}
locals {
  common_tags = "${map(
    "managedBy" , "terraform",
    "Name"      , "${var.PROJECT}-${var.ENV}-${var.SERVICE}",
    "project"   , "${var.PROJECT}",
    "env"       , "${var.ENV}",
    "service"   , "${var.SERVICE}",
    "owner"     , "${element(split(":", "${data.aws_caller_identity.current.user_id}"),1)}"
  )}"
}

locals {
  replicas = ["aws", "gcp"]
} 

resource "aws_dynamodb_table" "subscriptions-aws" {
  count        = "${length(local.replicas)}"
  name         = "dss-subscriptions-v2-${local.replicas[count.index]}-${var.DSS_DEPLOYMENT_STAGE}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "hash_key"
  range_key    = "sort_key"

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
