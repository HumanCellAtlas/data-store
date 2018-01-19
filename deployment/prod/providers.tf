provider aws {
	profile = "${var.aws_profile}"
	region = "${var.AWS_DEFAULT_REGION}"
	shared_credentials_file = "${var.aws_shared_credentials_file}"
	assume_role {
		role_arn = "arn:aws:iam::109067257620:role/dcp-admin"
	}
}

provider aws {
	profile = "${var.aws_profile}"
	region = "us-east-1"
	shared_credentials_file = "${var.aws_shared_credentials_file}"
	alias = "us-east-1"
	assume_role {
		role_arn = "arn:aws:iam::109067257620:role/dcp-admin"
	}
}

provider google {
	credentials = "../gcp-credentials.json"
	project = "${var.gcp_project}"
}
