resource aws_s3_bucket dss_s3_bucket {
  count = "${length(var.DSS_S3_BUCKET) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_BUCKET}"
}

resource aws_s3_bucket dss_s3_bucket_test {
  count = "${length(var.DSS_S3_BUCKET_TEST) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_BUCKET_TEST}"
  lifecycle_rule {
    id      = "prune old things"
    enabled = true
    abort_incomplete_multipart_upload_days = 7
    expiration {
      days = 7
    }
  }
}

resource aws_s3_bucket dss_s3_bucket_test_fixtures {
  count = "${length(var.DSS_S3_BUCKET_TEST_FIXTURES) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_BUCKET_TEST_FIXTURES}"
}

resource aws_s3_bucket dss_s3_checkout_bucket {
  count = "${length(var.DSS_S3_CHECKOUT_BUCKET) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_CHECKOUT_BUCKET}"
  lifecycle_rule {
    id      = "dss_checkout_expiration"
    enabled = true
    expiration {
	  days = 30
    }
  }
}

resource aws_s3_bucket dss_s3_checkout_bucket_test {
  count = "${length(var.DSS_S3_CHECKOUT_BUCKET_TEST) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_CHECKOUT_BUCKET_TEST}"
  lifecycle_rule {
    id      = "dss_checkout_expiration"
    enabled = true
    expiration {
	  days = 7
    }
  }
}

resource aws_s3_bucket dss_s3_checkout_bucket_test_fixtures {
  count = "${length(var.DSS_S3_CHECKOUT_BUCKET_TEST_FIXTURES) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_CHECKOUT_BUCKET_TEST_FIXTURES}"
}
