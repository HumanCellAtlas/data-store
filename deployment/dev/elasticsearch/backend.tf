terraform {
  backend "s3" {
    bucket = "org-humancellatlas-dss-config"
    key = "dss-elasticsearch-dev.tfstate"
    region = "us-east-1"
  }
}