resource google_storage_bucket dss_gs_bucket {
  count = "${length(var.DSS_GS_BUCKET) > 0 ? 1 : 0}"
  name = "${var.DSS_GS_BUCKET}"
  provider = "google"
  force_destroy = "true"
  location = "us-central1"
}

resource google_storage_bucket dss_gs_bucket_test {
  count = "${length(var.DSS_GS_BUCKET_TEST) > 0 ? 1 : 0}"
  name = "${var.DSS_GS_BUCKET_TEST}"
  provider = "google"
  force_destroy = "true"
  location = "us-central1"
}

resource google_storage_bucket dss_gs_bucket_test_fixtures {
  count = "${length(var.DSS_GS_BUCKET_TEST_FIXTURES) > 0 ? 1 : 0}"
  name = "${var.DSS_GS_BUCKET_TEST_FIXTURES}"
  provider = "google"
  force_destroy = "true"
  location = "us-central1"
}
