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
        "98.210.239.128",
        "189.243.139.25",
        "128.114.59.150",
        "128.114.59.161",
        "128.114.59.210",
        "128.114.59.219",
        "169.233.211.173",
        "69.173.127.229",
        "73.92.146.41",
        "73.170.144.7",
        "128.114.59.195",
        "128.114.59.183",
        "12.31.108.106",
        "128.114.59.208",
        "64.71.0.146"
      ]
    }
  }
}


resource aws_elasticsearch_domain elasticsearch {
  count = "${length(var.DSS_ES_DOMAIN) > 0 ? 1 : 0}"
  domain_name = "${var.DSS_ES_DOMAIN}"
  elasticsearch_version = "5.5"

  cluster_config = {
    instance_type = "m4.2xlarge.elasticsearch"
    instance_count = 3
  }

  advanced_options = {
    rest.action.multi.allow_explicit_index = "true"
  }

  ebs_options = {
    ebs_enabled = "true"
    volume_type = "gp2"
    volume_size = "1500"
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
