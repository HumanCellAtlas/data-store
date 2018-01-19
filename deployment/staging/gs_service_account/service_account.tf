resource google_service_account gcp_service_account {
  display_name = "${var.DSS_DEPLOYMENT_STAGE}"
  account_id = "${var.DSS_DEPLOYMENT_STAGE}"
}

resource google_project_iam_member cloudfunction_dev {
  role = "roles/cloudfunctions.developer"
  member = "serviceAccount:${var.DSS_DEPLOYMENT_STAGE}@${var.GCP_PROJECT}.iam.gserviceaccount.com"
}

resource google_project_iam_member service_account_actor {
  role = "roles/iam.serviceAccountActor"
  member = "serviceAccount:${var.DSS_DEPLOYMENT_STAGE}@${var.GCP_PROJECT}.iam.gserviceaccount.com"
}

resource google_project_iam_member project_owner {
  role = "roles/owner"
  member = "serviceAccount:${var.DSS_DEPLOYMENT_STAGE}@${var.GCP_PROJECT}.iam.gserviceaccount.com"
}
