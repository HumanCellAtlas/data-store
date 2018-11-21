resource aws_s3_bucket dss_audit_logs
{
  count = "${var.ENABLE_AUDIT_LOGS == 1 ? 1 : 0}"
  bucket = "${var.DSS_AUDIT_LOGS_BUCKET}"
  tags {
    managedby = "terraform"
  }
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm     = "AES256"
      }
    }
  }
  lifecycle_rule {
    id      = "logs"
    enabled = true
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    expiration {
      days          = 1096  # Three Years
    }
  }
}

resource "aws_cloudtrail" "dss" {
  count = "${var.ENABLE_AUDIT_LOGS == 1 ? 1 : 0}"
  name = "dss-audit-trail"
  s3_bucket_name = "${aws_s3_bucket.dss_audit_logs.bucket}"
  s3_key_prefix = "aws"
  enable_log_file_validation = true
  include_global_service_events = true

  event_selector {
    read_write_type = "All"
    include_management_events = true

    data_resource {
      type = "AWS::Lambda::Function"
      values = [
        "arn:aws:lambda:::function:dss-admin-${var.DSS_DEPLOYMENT_STAGE}",
        "arn:aws:lambda:::function:dss-${var.DSS_DEPLOYMENT_STAGE}"]
    }
    data_resource {
      type   = "AWS::S3::Object"
      values = ["arn:aws:s3:::${var.DSS_S3_BUCKET}"]
    }
  }
  tags {
    managedby = "terraform"
  }
}