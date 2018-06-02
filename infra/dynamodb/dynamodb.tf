resource "aws_dynamodb_table" "visitation-dynamodb-table" {
  name           = "dss-visitation-${var.DSS_DEPLOYMENT_STAGE}"
  read_capacity  = 1024
  write_capacity = 1024
  hash_key       = "key"

  attribute {
    name = "key"
    type = "S"
  }

  ttl {
    attribute_name = "expiration-time"
    enabled = true
  }
}
