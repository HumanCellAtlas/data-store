data aws_caller_identity current {}

data aws_route53_zone selected {
  name = "${var.DSS_ZONE_NAME}"
}

resource "aws_acm_certificate" "cert" {
  domain_name               = "${var.DSS_CERTIFICATE_DOMAIN}"
  subject_alternative_names = "${compact(split(" ", var.DSS_CERTIFICATE_ADDITIONAL_NAMES))}"
  validation_method         = "${var.DSS_CERTIFICATE_VALIDATION}"
}

resource "aws_route53_record" "cert_validation" {
  count   = "${"DNS" == var.DSS_CERTIFICATE_VALIDATION ? 1 : 0}"
  name    = "${aws_acm_certificate.cert.domain_validation_options.0.resource_record_name}"
  type    = "${aws_acm_certificate.cert.domain_validation_options.0.resource_record_type}"
  zone_id = "${data.aws_route53_zone.selected.zone_id}"
  records = ["${aws_acm_certificate.cert.domain_validation_options.0.resource_record_value}"]
  ttl     = 300
}

resource "aws_acm_certificate_validation" "cert_dns" {
  count   = "${"DNS" == var.DSS_CERTIFICATE_VALIDATION ? 1 : 0}"
  certificate_arn         = "${aws_acm_certificate.cert.arn}"
  validation_record_fqdns = ["${aws_route53_record.cert_validation.fqdn}"]
}

resource "aws_acm_certificate_validation" "cert_email" {
  count   = "${"EMAIL" == var.DSS_CERTIFICATE_VALIDATION ? 1 : 0}"
  certificate_arn = "${aws_acm_certificate.cert.arn}"
}

# TODO: Configure regional domain name with Terraform
#   Terraform does not currently support regional API Gateway endpoints:
#   https://github.com/terraform-providers/terraform-provider-aws/issues/2195
resource null_resource dss_domain {
  provisioner "local-exec" {
    command = "./create_regional_domain_name.py --domain-name ${var.API_DOMAIN_NAME} --certificate-arn ${aws_acm_certificate.cert.arn} --zone-id ${data.aws_route53_zone.selected.zone_id}"
  }
  depends_on = ["aws_acm_certificate_validation.cert_dns", "aws_acm_certificate_validation.cert_email"]
}
