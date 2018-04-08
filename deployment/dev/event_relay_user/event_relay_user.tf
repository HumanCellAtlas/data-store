data aws_caller_identity current {}
data aws_region current {}

resource "aws_iam_user" "event_relay_user" {
  name = "${var.DSS_EVENT_RELAY_AWS_USERNAME}"
  path = "/"
}

data "aws_iam_policy_document" "event_relay_user_policy_doc" {
  statement {
    actions = [
      "sns:Publish"
    ]
    resources = [
      "arn:aws:sns:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
}

resource "aws_iam_user_policy" "event_relay_user_policy" {
  name = "sns_publisher"
  user = "${aws_iam_user.event_relay_user.name}"
  policy = "${data.aws_iam_policy_document.event_relay_user_policy_doc.json}"
}
