resource aws_route53_zone dss_route53_zone {
  name = "${var.route53_zone}"
}

resource aws_acm_certificate dss_domain_cert {
  domain_name = "${var.certificate_domain}"
  validation_method = "DNS"
  provider = "aws.us-east-1"
}

resource aws_acm_certificate_validation cert {
  certificate_arn = "${aws_acm_certificate.dss_domain_cert.arn}"
  provider = "aws.us-east-1"
  timeouts = {
    create = "48h"
  }
}

resource aws_api_gateway_domain_name dss_domain {
  domain_name = "${var.API_DOMAIN_NAME}"
  certificate_arn = "${aws_acm_certificate.dss_domain_cert.arn}"
  depends_on = ["aws_acm_certificate.dss_domain_cert"]
}

resource aws_route53_record dss_route53_record {
  zone_id = "${aws_route53_zone.dss_route53_zone.zone_id}"
  name = "${aws_api_gateway_domain_name.dss_domain.domain_name}"
  type = "A"

  alias {
    name = "${aws_api_gateway_domain_name.dss_domain.cloudfront_domain_name}"
    zone_id = "${aws_api_gateway_domain_name.dss_domain.cloudfront_zone_id}"
    evaluate_target_health = false
  }

  depends_on = ["aws_acm_certificate.dss_domain_cert"]
  provider = "aws.us-east-1"
}
