resource google_storage_bucket dss_gs_bucket {
  name          = "${var.DSS_GS_BUCKET}"
  provider      = "google"
  location      = "US"
  storage_class = "MULTI_REGIONAL"
  labels = "${merge(local.common_tags, local.gcp_tags)}"
}

resource google_storage_bucket dss_gs_bucket_test {
  count         = "${var.DSS_DEPLOYMENT_STAGE == "dev" ? 1 : 0}"
  name          = "${var.DSS_GS_BUCKET_TEST}"
  provider      = "google"
  location      = "US"
  storage_class = "MULTI_REGIONAL"
  labels = "${merge(local.common_tags, local.gcp_tags)}"
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = "${var.DSS_BLOB_TTL_DAYS}"
      with_state = "LIVE"
    }
  }
}

resource google_storage_bucket dss_gs_bucket_test_fixtures {
  count         = "${var.DSS_DEPLOYMENT_STAGE == "dev" ? 1 : 0}"
  name          = "${var.DSS_GS_BUCKET_TEST_FIXTURES}"
  provider      = "google"
  location      = "US"
  storage_class = "MULTI_REGIONAL"
  labels = "${merge(local.common_tags, local.gcp_tags)}"
}

resource google_storage_bucket dss_gs_checkout_bucket {
  name          = "${var.DSS_GS_CHECKOUT_BUCKET}"
  provider      = "google"
  location      = "US"
  storage_class = "MULTI_REGIONAL"
  labels = "${merge(local.common_tags, local.gcp_tags)}"
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = "${var.DSS_BLOB_TTL_DAYS}"
      matches_storage_class = ["STANDARD"]
      with_state = "LIVE"
    }
  }
}

locals {
  checkout_bucket_viewers = "${compact(split(",", var.DSS_CHECKOUT_BUCKET_OBJECT_VIEWERS))}",
  gcp_tags = "${map(
    "Name"      , "${var.DSS_INFRA_TAG_SERVICE}-gs-storage"
  )}"
}

resource "google_storage_bucket_iam_member" "checkout_viewer" {
  count  = "${length(local.checkout_bucket_viewers)}",
  bucket = "${google_storage_bucket.dss_gs_checkout_bucket.name}"
  role   = "roles/storage.objectViewer"
  member = "${local.checkout_bucket_viewers[count.index]}"
}

resource google_storage_bucket dss_gs_checkout_bucket_test {
  count         = "${var.DSS_DEPLOYMENT_STAGE == "dev" ? 1 : 0}"
  name          = "${var.DSS_GS_CHECKOUT_BUCKET_TEST}"
  provider      = "google"
  location      = "US"
  storage_class = "MULTI_REGIONAL"
  labels = "${merge(local.common_tags, local.gcp_tags)}"
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = "${var.DSS_BLOB_TTL_DAYS}"
      with_state = "LIVE"
    }
  }
}

resource google_storage_bucket dss_gs_checkout_bucket_test_user {
  count         = "${var.DSS_DEPLOYMENT_STAGE == "dev" ? 1 : 0}"
  name          = "${var.DSS_GS_CHECKOUT_BUCKET_TEST_USER}"
  provider      = "google"
  location      = "US"
  storage_class = "MULTI_REGIONAL"
  labels = "${merge(local.common_tags, local.gcp_tags)}"
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = "${var.DSS_BLOB_TTL_DAYS}"
      with_state = "LIVE"
    }
  }
}
