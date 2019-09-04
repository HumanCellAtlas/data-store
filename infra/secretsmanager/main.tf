locals {
  verify_secret_name = "${var.DSS_SECRETS_STORE}/${var.DSS_DEPLOYMENT_STAGE}/${var.DSS_VERIFY_SECRETS_NAME}"
  tags = "${map(
  "Name"      , "${var.DSS_INFRA_TAG_SERVICE}-secrets",
  "owner"     , "${var.DSS_INFRA_TAG_OWNER}",
  "managedBy" , "terraform",
  "project"   , "${var.DSS_INFRA_TAG_PROJECT}",
  "env"       , "${var.DSS_DEPLOYMENT_STAGE}",
  "service"   , "${var.DSS_INFRA_TAG_SERVICE}"
  )}"
}

resource "aws_secretsmanager_secret" "last_verified" {
  name        = local.verify_secret_name
  description = "Time of last replica verification"
  tags        = local.tags
}

resource "aws_secretsmanager_secret_version" "last_verified_init" {
  secret_id      = local.verify_secret_name
  secret_string  = "1970-01-01T000000.000000Z"  # arbitrarily small
  version_stages = ["AWSCURRENT"]
  lifecycle {
    ignore_changes = [secret_string]
  }
  depends_on     = [aws_secretsmanager_secret.last_verified]
}
