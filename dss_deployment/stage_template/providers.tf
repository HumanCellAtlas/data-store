provider aws {
	profile = "${var.aws_profile}"
	region = "${var.AWS_DEFAULT_REGION}"
	shared_credentials_file = "${var.aws_shared_credentials_file}"
}

provider aws {
	profile = "${var.aws_profile}"
	region = "us-east-1"
	shared_credentials_file = "${var.aws_shared_credentials_file}"
	alias = "us-east-1"
}

provider google {
	credentials = "../gcp-credentials.json"
	project = "${var.gcp_project}"
}

provider google {
	project = "${var.gcp_project}"
	alias = "env"
}
