
data "aws_caller_identity" "current" {}

variable "project" {
  default = "dcp"
}

variable "service" {
  default = "dss"
}

variable "env" {
  default = "dev"
}

locals {
  owner = "${element(split(":", "${data.aws_caller_identity.current.user_id}"),1)}"
}

output "common_tags" {
  value = "${map(
    "managedBy" , "terraform",
    "Name"      , "${var.project}-${var.env}-${var.service}",
    "project"   , "${var.project}",
    "env"       , "${var.env}",
    "service"   , "${var.service}",
    "owner"     , "${local.owner}"
  )}"
}
