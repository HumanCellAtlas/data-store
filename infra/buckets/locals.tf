locals {
  # There is a Terraform state file for each stage (dev, integration, staging, prod)
  # However, test buckets are shared. We choose to create test buckets when deploying infra
  # for the dev stage.
  create_test_buckets = "${var.DSS_DEPLOYMENT_STAGE == "dev" ? true : false}"
}
