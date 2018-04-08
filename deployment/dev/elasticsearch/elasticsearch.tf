data aws_caller_identity current {}
data aws_region current {}

resource aws_elasticsearch_domain elasticsearch {
  count = "${length(var.DSS_ES_DOMAIN) > 0 ? 1 : 0}"
  domain_name = "${var.DSS_ES_DOMAIN}"
  elasticsearch_version = "5.5"

  cluster_config = {
    instance_type = "m4.large.elasticsearch"
  }

  advanced_options = {
    rest.action.multi.allow_explicit_index = "true"
  }

  ebs_options = {
    ebs_enabled = "true"
    volume_type = "gp2"
    volume_size = "35"
  }

  log_publishing_options = {
    cloudwatch_log_group_arn = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/aes/domains/${var.DSS_ES_DOMAIN}/index-logs"
    log_type = "INDEX_SLOW_LOGS"
    enabled = "true"
  }

  log_publishing_options = {
    cloudwatch_log_group_arn = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/aes/domains/${var.DSS_ES_DOMAIN}/search-logs"
    log_type = "SEARCH_SLOW_LOGS"
    enabled = "true"
  }

  access_policies = <<CONFIG
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
      },
      "Action": "es:*",
      "Resource": "arn:aws:es:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:domain/${var.DSS_ES_DOMAIN}/*"
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": "es:*",
      "Resource": "arn:aws:es:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:domain/${var.DSS_ES_DOMAIN}/*",
      "Condition": {
        "IpAddress": {
          "aws:SourceIp": [
          ]
        }
      }
    }
  ]
}
CONFIG
}
