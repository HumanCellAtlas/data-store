resource aws_s3_bucket dss_s3_bucket {
  count = "${length(var.DSS_S3_BUCKET) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_BUCKET}"
  force_destroy = "true"
  acl = "private"
}

resource aws_s3_bucket dss_s3_checkout_bucket {
  count = "${length(var.DSS_S3_CHECKOUT_BUCKET) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_CHECKOUT_BUCKET}"
  force_destroy = "true"
  acl = "private"
}

resource aws_s3_bucket dss_s3_bucket_test {
  count = "${length(var.DSS_S3_BUCKET_TEST) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_BUCKET_TEST}"
  force_destroy = "true"
  acl = "private"
}

resource aws_s3_bucket dss_s3_bucket_test_fixtures {
  count = "${length(var.DSS_S3_BUCKET_TEST_FIXTURES) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_BUCKET_TEST_FIXTURES}"
  force_destroy = "true"
  acl = "private"
}

resource aws_s3_bucket dss_s3_checkout_bucket_test {
  count = "${length(var.DSS_S3_CHECKOUT_BUCKET_TEST) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_CHECKOUT_BUCKET_TEST}"
  force_destroy = "true"
  acl = "private"
}

resource aws_s3_bucket dss_s3_checkout_bucket_test_fixtures {
  count = "${length(var.DSS_S3_CHECKOUT_BUCKET_TEST_FIXTURES) > 0 ? 1 : 0}"
  bucket = "${var.DSS_S3_CHECKOUT_BUCKET_TEST_FIXTURES}"
  force_destroy = "true"
  acl = "private"
}
