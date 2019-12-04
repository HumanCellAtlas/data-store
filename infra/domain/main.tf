data aws_caller_identity current {}
locals {account_id = data.aws_caller_identity.current.account_id}

data aws_route53_zone selected {
  name = var.DSS_ZONE_NAME
}

resource "aws_api_gateway_domain_name" "dss" {
  domain_name               = var.API_DOMAIN_NAME
  regional_certificate_arn  = "arn:aws:acm:${var.AWS_DEFAULT_REGION}:${local.account_id}:certificate/${var.ACM_CERTIFICATE_IDENTIFIER}"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_route53_record" "dss" {
  zone_id = data.aws_route53_zone.selected.zone_id
  name    = var.API_DOMAIN_NAME
  type    = "CNAME"
  ttl     = "300"
  records = [aws_api_gateway_domain_name.dss.regional_domain_name]
}
