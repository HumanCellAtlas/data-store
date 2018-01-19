data aws_route53_zone dss_route53_zone {
  name = "${var.route53_zone}"
}

data aws_acm_certificate dss_domain_cert {
  domain = "${var.certificate_domain}"
  types = ["AMAZON_ISSUED"]
  most_recent = true
}

resource aws_api_gateway_domain_name dss_domain {
  domain_name = "${var.API_DOMAIN_NAME}"
  certificate_arn = "${data.aws_acm_certificate.dss_domain_cert.arn}"
  depends_on = ["data.aws_acm_certificate.dss_domain_cert"]
  provider = "aws.us-east-1"
}

resource aws_route53_record dss_route53_record {
  zone_id = "${data.aws_route53_zone.dss_route53_zone.zone_id}"
  name = "${aws_api_gateway_domain_name.dss_domain.domain_name}"
  type = "A"

  alias {
    name = "${aws_api_gateway_domain_name.dss_domain.cloudfront_domain_name}"
    zone_id = "${aws_api_gateway_domain_name.dss_domain.cloudfront_zone_id}"
    evaluate_target_health = false
  }

  depends_on = ["data.aws_acm_certificate.dss_domain_cert"]
  provider = "aws.us-east-1"
}
