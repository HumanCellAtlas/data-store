data "aws_caller_identity" "current" {}

locals {
  common_tags = "${map(
    "project"   , "${var.DSS_INFRA_TAG_PROJECT}",
    "env"       , "${var.DSS_DEPLOYMENT_STAGE}",
    "service"   , "${var.DSS_INFRA_TAG_SERVICE}"
  )}"
  aws_tags = "${map(
  "Name"      , "${var.DSS_INFRA_TAG_SERVICE}-s3-storage",
  "owner"     , "${var.DSS_INFRA_TAG_OWNER}",
  "managedBy" , "terraform"
  )}"
}

resource aws_s3_bucket dss_s3_bucket {
  count = "${length(var.DSS_S3_BUCKET) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_BUCKET}"
  server_side_encryption_configuration {
    rule {
	  apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }
  tags = "${merge(local.common_tags, local.aws_tags)}"
}

resource aws_s3_bucket dss_s3_bucket_test {
  count = "${var.DSS_DEPLOYMENT_STAGE == "dev" ? 1 : 0}"
  bucket = "${var.DSS_S3_BUCKET_TEST}"
  lifecycle_rule {
    id = "prune old things"
    enabled = true
    abort_incomplete_multipart_upload_days = "${var.DSS_BLOB_TTL_DAYS}"
    expiration {
      days = "${var.DSS_BLOB_TTL_DAYS}"
    }
  }
  tags = "${merge(local.common_tags, local.aws_tags)}"
}

resource aws_s3_bucket dss_s3_bucket_test_fixtures {
  count = "${var.DSS_DEPLOYMENT_STAGE == "dev" ? 1 : 0}"
  bucket = "${var.DSS_S3_BUCKET_TEST_FIXTURES}"
  tags = "${merge(local.common_tags, local.aws_tags)}"
}

resource aws_s3_bucket dss_s3_checkout_bucket {
  count = "${length(var.DSS_S3_CHECKOUT_BUCKET) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_CHECKOUT_BUCKET}"
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }
  lifecycle_rule {
    id = "dss_checkout_expiration"
    enabled = true
    tags = {
      "uncached" = "true"
    }
    expiration {
      days = "${var.DSS_BLOB_TTL_DAYS}"
    }
  }
  lifecycle_rule {
    id = "failed multipart cleanup"
    enabled = true
    abort_incomplete_multipart_upload_days = "${var.DSS_BLOB_TTL_DAYS}"
  }
  tags = "${merge(local.common_tags, local.aws_tags)}"
  cors_rule {
    allowed_methods = [
      "HEAD",
      "GET"
    ]
    allowed_origins = [
      "*"
    ]
    allowed_headers = [
      "*"
    ]
    max_age_seconds = 3000
  }
}

resource aws_s3_bucket dss_s3_checkout_bucket_test {
  count = "${var.DSS_DEPLOYMENT_STAGE == "dev" ? 1 : 0}"
  bucket = "${var.DSS_S3_CHECKOUT_BUCKET_TEST}"
  lifecycle_rule {
    id = "dss_checkout_expiration"
    enabled = true
    abort_incomplete_multipart_upload_days = "${var.DSS_BLOB_TTL_DAYS}"
    expiration {
      days = "${var.DSS_BLOB_TTL_DAYS}"
    }
  }
  tags = "${merge(local.common_tags, local.aws_tags)}"
}

resource aws_s3_bucket dss_s3_checkout_bucket_test_user {
  count = "${var.DSS_DEPLOYMENT_STAGE == "dev" ? 1 : 0}"
  bucket = "${var.DSS_S3_CHECKOUT_BUCKET_TEST_USER}"
  lifecycle_rule {
    id = "dss_checkout_expiration"
    enabled = true
    abort_incomplete_multipart_upload_days = "${var.DSS_BLOB_TTL_DAYS}"
    expiration {
      days = "${var.DSS_BLOB_TTL_DAYS}"
    }
  }
  tags = "${merge(local.common_tags, local.aws_tags)}"
}

resource aws_s3_bucket dss_s3_checkout_bucket_unwritable {
  count = "${var.DSS_DEPLOYMENT_STAGE == "dev" ? 1 : 0}"
  bucket = "${var.DSS_S3_CHECKOUT_BUCKET_UNWRITABLE}"
  tags = "${merge(local.common_tags, local.aws_tags)}"
  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:Get*",
        "s3:List*"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${var.DSS_S3_CHECKOUT_BUCKET_UNWRITABLE}",
        "arn:aws:s3:::${var.DSS_S3_CHECKOUT_BUCKET_UNWRITABLE}/*"
      ],
      "Principal": "*"
    },
    {
      "Action": [
        "s3:PutObject*"
      ],
      "Effect": "Deny",
      "Resource": [
        "arn:aws:s3:::${var.DSS_S3_CHECKOUT_BUCKET_UNWRITABLE}",
        "arn:aws:s3:::${var.DSS_S3_CHECKOUT_BUCKET_UNWRITABLE}/*"
      ],
      "Principal": "*"
    }
  ]
}
POLICY
}

resource aws_s3_bucket dss_s3_events_bucket {
  count = "${length(var.DSS_S3_BUCKET) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_EVENTS_BUCKET}"
  server_side_encryption_configuration {
    rule {apply_server_side_encryption_by_default {sse_algorithm = "AES256"}}
  }
  tags = "${local.common_tags}"
}
