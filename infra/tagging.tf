#These variables are populated from the enviornment; TF_VAR_{name}
variable "PROJECT" {}
variable "SERVICE" {}
variable "ENV" {}

data "aws_caller_identity" "current" {}

locals {
  owner = "${element(split(":", "${data.aws_caller_identity.current.user_id}"),1)}"

}

output "common_tags" {
  value = "${map(
    "managedBy" , "terraform",
    "Name"      , "${var.PROJECT}-${var.ENV}-${var.SERVICE}",
    "project"   , "${var.PROJECT}",
    "env"       , "${var.ENV}",
    "service"   , "${var.SERVICE}",
    "owner"     , "${local.owner}"
  )}"
}
