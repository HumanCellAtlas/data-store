data aws_caller_identity current {}

data aws_route53_zone selected {
  name = "${var.DSS_ZONE_NAME}"
}

data "aws_acm_certificate" "cert" {
  domain      = "${var.DSS_CERTIFICATE_DOMAIN}"
  statuses    = ["ISSUED"]
  most_recent = true
}

resource "aws_api_gateway_domain_name" "dss" {
  domain_name               = "${var.API_DOMAIN_NAME}"
  regional_certificate_arn  = "${data.aws_acm_certificate.cert.arn}"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_route53_record" "dss" {
  zone_id = "${data.aws_route53_zone.selected.zone_id}"
  name    = "${var.API_DOMAIN_NAME}"
  type    = "CNAME"
  ttl     = "300"
  records = ["${aws_api_gateway_domain_name.dss.regional_domain_name}"]
}
