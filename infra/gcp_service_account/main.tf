data "google_project" "project" {}

resource "google_service_account" "dss" {
  display_name = "${var.DSS_GCP_SERVICE_ACCOUNT_NAME}"
  account_id = "${var.DSS_GCP_SERVICE_ACCOUNT_NAME}"
}

# Useful command to discover role names (Guessing based on console titles is difficult):
# `gcloud iam list-grantable-roles //cloudresourcemanager.googleapis.com/projects/{project-id}`

resource "google_project_iam_member" "serviceaccountactor" {
  project = "${data.google_project.project.project_id}"
  role    = "roles/iam.serviceAccountActor"
  member  = "serviceAccount:${google_service_account.dss.email}"
}

resource "google_project_iam_member" "cloudruntimeconfiguratoradmin" {
  project = "${data.google_project.project.project_id}"
  role    = "roles/runtimeconfig.admin"
  member  = "serviceAccount:${google_service_account.dss.email}"
}

resource "google_project_iam_member" "storageadmin" {
  project = "${data.google_project.project.project_id}"
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.dss.email}"
}

resource "google_project_iam_member" "storageobjectadmin" {
  project = "${data.google_project.project.project_id}"
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dss.email}"
}

resource "google_project_iam_member" "cloudfunctionsdeveloper" {
  project = "${data.google_project.project.project_id}"
  role    = "roles/cloudfunctions.developer"
  member  = "serviceAccount:${google_service_account.dss.email}"
}
