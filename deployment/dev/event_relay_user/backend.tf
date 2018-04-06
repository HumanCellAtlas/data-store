terraform {
  backend "s3" {
    bucket = "org-humancellatlas-dss-config"
    key = "dss-event_relay_user-dev.tfstate"
    region = "us-east-1"
  }
}