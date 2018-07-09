data aws_caller_identity current {}

data aws_route53_zone dss_route53_zone {
  name = "${var.DSS_ZONE_NAME}"
}

data aws_acm_certificate dss_domain_cert {
  domain = "${var.DSS_CERTIFICATE_DOMAIN}"
}

# TODO: Configure regional domain name with Terraform
#   Terraform does not currently support regional API Gateway endpoints:
#   https://github.com/terraform-providers/terraform-provider-aws/issues/2195
resource null_resource dss_domain {
  provisioner "local-exec" {
    command = "./create_regional_domain_name.py --domain-name ${var.API_DOMAIN_NAME} --certificate-arn ${data.aws_acm_certificate.dss_domain_cert.arn} --zone-id ${data.aws_route53_zone.dss_route53_zone.zone_id}"
  }
}
