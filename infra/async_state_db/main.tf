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
