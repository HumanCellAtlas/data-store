data aws_caller_identity current {}
data aws_region current {}
locals {
  region = "${data.aws_region.current.name}"
  account_id = "${data.aws_caller_identity.current.account_id}"
}


resource "aws_cloudwatch_log_group" "dss_index_log" {
  name = "/aws/aes/domains/${var.DSS_ES_DOMAIN}/index-logs"
  retention_in_days = 90
}


resource "aws_cloudwatch_log_group" "dss_search_log" {
  name = "/aws/aes/domains/${var.DSS_ES_DOMAIN}/search-logs"
  retention_in_days = 90
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
      "${aws_cloudwatch_log_group.dss_index_log.arn}",
      "${aws_cloudwatch_log_group.dss_search_log.arn}"
    ]
  }
}

resource "aws_cloudwatch_log_resource_policy" "dss_es_cloudwatch_policy" {
  policy_name = "${var.DSS_ES_DOMAIN}"
  policy_document = "${data.aws_iam_policy_document.dss_es_cloudwatch_policy_document.json}"
}


data "aws_iam_policy_document" "dss_es_access_policy_documennt" {
  statement {
    principals {
      type = "AWS"
      identifiers = ["arn:aws:iam::${local.account_id}:root"]
    }
    actions = ["es:*"]
    resources = ["arn:aws:es:${local.region}:${local.account_id}:domain/${var.DSS_ES_DOMAIN}/*"]
  }

  statement {
    principals {
      type = "AWS"
      identifiers = ["*"]
    }
    actions = ["es:*"]
    resources = ["arn:aws:es:${local.region}:${local.account_id}:domain/${var.DSS_ES_DOMAIN}/*"]
    condition {
      test = "IpAddress"
      variable = "aws:SourceIp"
      values = ["${local.access_ips}"]
    }
  }
}

resource aws_elasticsearch_domain elasticsearch {
  count = "${length(var.DSS_ES_DOMAIN) > 0 ? 1 : 0}"
  domain_name = "${var.DSS_ES_DOMAIN}"
  elasticsearch_version = "5.5"

  cluster_config = {
    instance_type = "${var.DSS_ES_INSTANCE_TYPE}"
	instance_count = "${var.DSS_ES_INSTANCE_COUNT}"
  }

  advanced_options = {
    rest.action.multi.allow_explicit_index = "true"
  }

  ebs_options = {
    ebs_enabled = "true"
    volume_type = "gp2"
    volume_size = "${var.DSS_ES_VOLUME_SIZE}"
  }

  log_publishing_options = {
    cloudwatch_log_group_arn = "${aws_cloudwatch_log_group.dss_index_log.arn}"
    log_type = "INDEX_SLOW_LOGS"
    enabled = "true"
  }

  log_publishing_options = {
    cloudwatch_log_group_arn = "${aws_cloudwatch_log_group.dss_search_log.arn}"
    log_type = "SEARCH_SLOW_LOGS"
    enabled = "true"
  }

  snapshot_options = {
    automated_snapshot_start_hour = 23
  }

  tags {
    Domain = "${var.DSS_ES_DOMAIN}"
  }

  access_policies = "${data.aws_iam_policy_document.dss_es_access_policy_documennt.json}"
}
