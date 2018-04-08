terraform {
  backend "s3" {
    bucket = "org-humancellatlas-dss-config"
    key = "dss-buckets-dev.tfstate"
    region = "us-east-1"
  }
}