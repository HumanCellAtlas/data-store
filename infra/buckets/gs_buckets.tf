resource google_storage_bucket dss_gs_bucket {
  name = "${var.DSS_GS_BUCKET}"
  provider = "google"
  location = "${length(var.DSS_GS_BUCKET_REGION) > 0 ?
    "${var.DSS_GS_BUCKET_REGION}" : "${var.GCP_DEFAULT_REGION}"}"
  storage_class = "REGIONAL"
  storage_class = "${var.DSS_DEPLOYMENT_STAGE == "dev" ? "REGIONAL" : "STANDARD"}"
}

resource google_storage_bucket dss_gs_bucket_test {
  count = "${var.DSS_DEPLOYMENT_STAGE == "dev" ? 1 : 0}"
  name = "${var.DSS_GS_BUCKET_TEST}"
  provider = "google"
  location = "${length(var.DSS_GS_BUCKET_TEST_REGION) > 0 ?
    "${var.DSS_GS_BUCKET_TEST_REGION}" : "${var.GCP_DEFAULT_REGION}"}"
  storage_class = "REGIONAL"
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = "${var.DSS_BLOB_TTL_DAYS}"
      is_live = true
    }
  }
}

resource google_storage_bucket dss_gs_bucket_test_fixtures {
  count = "${var.DSS_DEPLOYMENT_STAGE == "dev" ? 1 : 0}"
  name = "${var.DSS_GS_BUCKET_TEST_FIXTURES}"
  provider = "google"
  location = "${length(var.DSS_GS_BUCKET_TEST_FIXTURES_REGION) > 0 ?
    "${var.DSS_GS_BUCKET_TEST_FIXTURES_REGION}" : "${var.GCP_DEFAULT_REGION}"}"
  storage_class = "REGIONAL"
}

resource google_storage_bucket dss_gs_checkout_bucket {
  count = "${length(var.DSS_GS_CHECKOUT_BUCKET) > 0 ? 1 : 0}"
  name = "${var.DSS_GS_CHECKOUT_BUCKET}"
  provider = "google"
  location = "${var.GCP_DEFAULT_REGION}"
  storage_class = "REGIONAL"
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = "${var.DSS_BLOB_TTL_DAYS}"
      is_live = true
    }
  }
}

resource google_storage_bucket dss_gs_checkout_bucket_test {
  count = "${var.DSS_DEPLOYMENT_STAGE == "dev" ? 1 : 0}"
  name = "${var.DSS_GS_CHECKOUT_BUCKET_TEST}"
  provider = "google"
  location = "${var.GCP_DEFAULT_REGION}"
  storage_class = "REGIONAL"
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = "${var.DSS_BLOB_TTL_DAYS}"
      is_live = true
    }
  }
}
