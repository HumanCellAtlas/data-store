data aws_caller_identity current {}

data "aws_route53_zone" "org_tld" {
  name = "${var.ORG_TLD_DOMAIN}"
}

resource "aws_route53_zone" "dss" {
  name = "${var.DSS_ZONE_NAME}"
  force_destroy = "true"
}

resource "aws_route53_record" "dss_ns_record" {
  name    = "${aws_route53_zone.dss.name}"
  type    = "NS"
  zone_id = "${data.aws_route53_zone.org_tld.zone_id}"
  records = ["${aws_route53_zone.dss.name}"]
  ttl     = 30
}

resource "aws_route53_record" "cert_validation" {
  count   = "${"DNS" == var.DSS_CERTIFICATE_VALIDATION ? 1 : 0}"
  name    = "${aws_acm_certificate.cert.domain_validation_options.0.resource_record_name}"
  type    = "${aws_acm_certificate.cert.domain_validation_options.0.resource_record_type}"
  zone_id = "${aws_route53_zone.dss.zone_id}"
  records = ["${aws_acm_certificate.cert.domain_validation_options.0.resource_record_value}"]
  ttl     = 300
}

resource "aws_route53_record" "cert_validation_alt1" {
  count   = "${"DNS" == var.DSS_CERTIFICATE_VALIDATION ? 1 : 0}"
  name    = "${aws_acm_certificate.cert.domain_validation_options.1.resource_record_name}"
  type    = "${aws_acm_certificate.cert.domain_validation_options.1.resource_record_type}"
  zone_id = "${aws_route53_zone.dss.zone_id}"
  records = ["${aws_acm_certificate.cert.domain_validation_options.1.resource_record_value}"]
  ttl     = 300
}

resource "aws_api_gateway_domain_name" "dss" {
  domain_name = "${var.DSS_CERTIFICATE_DOMAIN}"

  regional_certificate_arn = "${aws_acm_certificate.cert.arn}"
  endpoint_configuration {
	types = ["REGIONAL"]
  }
}

resource "aws_acm_certificate" "cert" {
  domain_name               = "${var.DSS_CERTIFICATE_DOMAIN}"
  subject_alternative_names = "${compact(split(" ", var.DSS_CERTIFICATE_ADDITIONAL_NAMES))}"
  validation_method         = "${var.DSS_CERTIFICATE_VALIDATION}"
}

resource "aws_acm_certificate_validation" "cert_dns" {
  count   = "${"DNS" == var.DSS_CERTIFICATE_VALIDATION ? 1 : 0}"
  certificate_arn         = "${aws_acm_certificate.cert.arn}"
  validation_record_fqdns = ["${aws_route53_record.cert_validation.fqdn}",
			     "${aws_route53_record.cert_validation_alt1.fqdn}"]
}

resource "aws_acm_certificate_validation" "cert_email" {
  count   = "${"EMAIL" == var.DSS_CERTIFICATE_VALIDATION ? 1 : 0}"
  certificate_arn = "${aws_acm_certificate.cert.arn}"
}
