data aws_caller_identity current {}
data aws_region current {}

locals {
  region = "${data.aws_region.current.name}"
  account_id = "${data.aws_caller_identity.current.account_id}"
  common_tags = "${map(
    "managedBy" , "terraform",
    "Name"      , "${var.DSS_INFRA_TAG_SERVICE}-subscriptionsdynamodb",
    "project"   , "${var.DSS_INFRA_TAG_PROJECT}",
    "env"       , "${var.DSS_DEPLOYMENT_STAGE}",
    "service"   , "${var.DSS_INFRA_TAG_SERVICE}",
    "owner"     , "${var.DSS_INFRA_TAG_OWNER}"
  )}"
}

locals {
  replicas = ["aws", "gcp"]
}

data "aws_iam_policy_document" "sqs" {
  statement {
    principals {
      type = "AWS"
      identifiers = ["*"]
    }
    actions = ["sqs:SendMessage"]
    resources = ["arn:aws:sqs:${local.region}:${local.account_id}:dss-events-scribe-${var.DSS_DEPLOYMENT_STAGE}"]
    condition {
      test     = "StringEquals"
      variable = "aws:SourceArn"
      values = ["${aws_cloudwatch_event_rule.events-scribe.arn}"]
    }
  }
  statement {
    principals {
      type = "AWS"
      identifiers = ["*"]
    }
    actions = [
      "sqs:*",
    ]
    resources = ["arn:aws:sqs:${local.region}:${local.account_id}:dss-events-scribe-${var.DSS_DEPLOYMENT_STAGE}"]
    condition {
      test     = "StringEquals"
      variable = "aws:SourceArn"
      values   = ["arn:aws:lambda:${local.region}:${local.account_id}:function:dss-events-scribe-${var.DSS_DEPLOYMENT_STAGE}"]
    }
  }
}

resource "aws_sqs_queue" "dss-events-scribe-queue" {
  name   					 = "dss-events-scribe-${var.DSS_DEPLOYMENT_STAGE}"
  tags   					 = "${local.common_tags}"
  message_retention_seconds  = "3600"
  visibility_timeout_seconds = "600"
  policy 					 = "${data.aws_iam_policy_document.sqs.json}"
}

resource "aws_cloudwatch_event_rule" "events-scribe" {
  name 		  		  = "dss-events-scribe-${var.DSS_DEPLOYMENT_STAGE}"
  description 		  = "Queue event journal/update"
  schedule_expression = "rate(10 minutes)"
  tags 				  = "${local.common_tags}"
}

resource "aws_cloudwatch_event_target" "send-journal-and-update-message" {
  count = "${length(local.replicas)}"
  rule  = "${aws_cloudwatch_event_rule.events-scribe.name}"
  arn   = "${aws_sqs_queue.dss-events-scribe-queue.arn}"
  input = <<-DOC
  {
    "replica":"${local.replicas[count.index]}"
  }
  DOC
}
