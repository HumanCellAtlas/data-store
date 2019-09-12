locals {
  file_secret_name   = "${var.DSS_SECRETS_STORE}/${var.DSS_DEPLOYMENT_STAGE}/${var.DSS_FILE_VERIFY_SECRETS_NAME}"
  blob_secret_name   = "${var.DSS_SECRETS_STORE}/${var.DSS_DEPLOYMENT_STAGE}/${var.DSS_BLOB_VERIFY_SECRETS_NAME}"
  bundle_secret_name = "${var.DSS_SECRETS_STORE}/${var.DSS_DEPLOYMENT_STAGE}/${var.DSS_BUNDLE_VERIFY_SECRETS_NAME}"
}

resource "aws_secretsmanager_secret" "files_last_verified" {
  name        = local.file_secret_name
  description = "Time of last files replication verification"
}

resource "aws_secretsmanager_secret" "bundles_last_verified" {
  name        = local.bundle_secret_name
  description = "Time of last bundle replication verification"
}

resource "aws_secretsmanager_secret" "blobs_last_verified" {
  name        = local.blob_secret_name
  description = "Time of last blob replication verification"
}

resource "aws_secretsmanager_secret_version" "files_last_verified_init" {
  secret_id      = local.file_secret_name
  secret_string  = "1969-12-31T00:00:00.000Z"  # arbitrarily small
  version_stages = ["AWSCURRENT"]
  lifecycle {
    ignore_changes = [secret_string]
  }
  depends_on     = [aws_secretsmanager_secret.files_last_verified]
}

resource "aws_secretsmanager_secret_version" "blobs_last_verified_init" {
  secret_id      = local.blob_secret_name
  secret_string  = "1969-12-31T00:00:00.000Z"
  version_stages = ["AWSCURRENT"]
  lifecycle {
    ignore_changes = [secret_string]
  }
  depends_on     = [aws_secretsmanager_secret.blobs_last_verified]
}

resource "aws_secretsmanager_secret_version" "bundles_last_verified_init" {
  secret_id      = local.bundle_secret_name
  secret_string  = "1969-12-31T00:00:00.000Z"
  version_stages = ["AWSCURRENT"]
  lifecycle {
    ignore_changes = [secret_string]
  }
  depends_on     = [aws_secretsmanager_secret.bundles_last_verified]
}
