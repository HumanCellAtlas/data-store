data "aws_caller_identity" "current" {}

locals {
  common_tags = "${map(
    "managedBy" , "terraform",
    "Name"      , "${var.DSS_INFRA_TAG_PROJECT}-${var.DSS_DEPLOYMENT_STAGE}-${var.DSS_INFRA_TAG_SERVICE}",
    "project"   , "${var.DSS_INFRA_TAG_PROJECT}",
    "env"       , "${var.DSS_DEPLOYMENT_STAGE}",
    "service"   , "${var.DSS_INFRA_TAG_SERVICE}",
    "owner"     , "${var.DSS_INFRA_TAG_OWNER}"
  )}"
}

resource "null_resource" "requirements" {
  triggers = {
    req_sha1 = "${sha1(file("${var.DSS_HOME}/requirements.txt"))}"
  }
  provisioner "local-exec" {
    command = "${var.DSS_HOME}/scripts/generate_requirements_layer.sh"
  }
}

resource "aws_lambda_layer_version" "lambda_layer" {
  s3_bucket = "${var.DSS_OPS_BUCKET}"
  s3_key = "dss-dependencies-${var.DSS_DEPLOYMENT_STAGE}.zip"
  layer_name = "dss-dependencies-${var.DSS_DEPLOYMENT_STAGE}"
  compatible_runtimes = ["python3.6", "python3.7"]
  depends_on = ["null_resource.requirements"]
}

output "lambda_layer_arn" {
  value = "aws_lambda_layer_version.lambda_layer.layer_arn"
}