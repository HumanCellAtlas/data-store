data aws_caller_identity current {}
data aws_region current {}


resource "aws_cloudwatch_log_group" "dss-index-log" {
  name = "${var.DSS_ES_DOMAIN}-index-logs"
}


resource "aws_cloudwatch_log_group" "dss-search-log" {
  name = "${var.DSS_ES_DOMAIN}-search-logs"
}


data "aws_iam_policy_document" "dss_es_cloudwatch_policy_document" {
  statement {
    principals {
      type = "Service"
      identifiers = ["es.amazonaws.com"]
    }
    actions = [
      "logs:PutLogEvents",
      "logs:CreateLogStream"
    ]
    resources = [
      "${aws_cloudwatch_log_group.dss-index-log.arn}",
      "${aws_cloudwatch_log_group.dss-search-log.arn}"
    ]
  }
}


resource "aws_cloudwatch_log_resource_policy" "dss-es-cloudwatch-policy" {
  policy_name = "dss-es-cloudwatch-log-policy"
  policy_document = "${data.aws_iam_policy_document.dss_es_cloudwatch_policy_document.json}"
}


data "aws_iam_policy_document" "dss_es_access_policy_documennt" {
  statement {
    principals {
      type = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
    actions = ["es:*"]
    resources = ["arn:aws:es:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:domain/${var.DSS_ES_DOMAIN}/*"]
  }

  statement {
    principals {
      type = "AWS"
      identifiers = ["*"]
    }
    actions = ["es:*"]
    resources = ["arn:aws:es:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:domain/${var.DSS_ES_DOMAIN}/*"]
    condition {
      test = "IpAddress"
      variable = "aws:SourceIp"
      values = [
      ]
    }
  }
}


resource aws_elasticsearch_domain elasticsearch {
  count = "${length(var.DSS_ES_DOMAIN) > 0 ? 1 : 0}"
  domain_name = "${var.DSS_ES_DOMAIN}"
  elasticsearch_version = "5.5"

  cluster_config = {
    instance_type = "t2.small.elasticsearch"
  }

  advanced_options = {
    rest.action.multi.allow_explicit_index = "true"
  }

  ebs_options = {
    ebs_enabled = "true"
    volume_type = "gp2"
    volume_size = "10"
  }

  log_publishing_options = {
    cloudwatch_log_group_arn = "${aws_cloudwatch_log_group.dss-index-log.arn}"
    log_type = "INDEX_SLOW_LOGS"
    enabled = "true"
  }

  log_publishing_options = {
    cloudwatch_log_group_arn = "${aws_cloudwatch_log_group.dss-search-log.arn}"
    log_type = "SEARCH_SLOW_LOGS"
    enabled = "true"
  }

  snapshot_options = {
    automated_snapshot_start_hour = 23
  }

  access_policies = "${data.aws_iam_policy_document.dss_es_access_policy_documennt.json}"
}
