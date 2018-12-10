resource "aws_dynamodb_table" "sfn_state" {
  name           = "dss-async-state-${var.DSS_DEPLOYMENT_STAGE}"
  read_capacity  = 20
  write_capacity = 20
  hash_key       = "key"

  attribute {
    name = "key"
    type = "S"
  }
  tags {
    CreatedBy = "Terraform"
    Application = "DSS"
  }
}
