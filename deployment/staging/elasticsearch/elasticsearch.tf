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
  snapshot_options = {
    automated_snapshot_start_hour = 23
  }
  tags = {
    Domain = "${var.DSS_ES_DOMAIN}"
  }
  access_policies = "{ \"Version\": \"2012-10-17\", \"Statement\": [ { \"Effect\": \"Allow\", \"Action\": [ \"logs:CreateLogGroup\", \"logs:CreateLogStream\", \"logs:PutLogEvents\" ], \"Resource\": \"arn:aws:logs:*:*:*\" }, { \"Effect\": \"Allow\", \"Principal\": { \"AWS\": \"*\" }, \"Action\": \"es:*\", \"Resource\": \"arn:aws:es:us-west-2:719818754276:domain/dss-index-hannes/*\", \"Condition\": { } } ] }"
}
