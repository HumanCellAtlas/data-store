data "aws_secretsmanager_secret_version" "source_ips" {
  secret_id = "${var.DSS_SECRETS_STORE}/${var.DSS_DEPLOYMENT_STAGE}/${var.ES_ALLOWED_SOURCE_IP_SECRETS_NAME}"
}

locals {
  ips_str = data.aws_secretsmanager_secret_version.source_ips.secret_string
  access_ips = compact(split(",", local.ips_str))
}
