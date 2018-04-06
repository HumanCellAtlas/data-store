{
	"variable": {
		"tf_backend": {
			"description": "Bucket to store terraform state files. S3 or GCP buckets may be specified with 's3://bucket-name' and 'gs://bucket-name', respectively. Enter \"none\" to store locally.",
			"default": null
		},
		"gcp_project": {
			"description": "GCP project-id  where google infrastructre will be deployed.",
			"default": null
		},
		"gcp_service_account_id": {
			"description": "ID of GCP service account for deploying infrastructure and cloud communication. If this account does not exist, it will be created for you.",
			"default": null
		},
		"route53_zone": {
			"description": "AWS Route 53 zone (e.g. 'data.humancellatlas.org.'). Hosted zones may be listed with the aws cli command 'aws route53 list-hosted-zones'",
			"default": null
		},
		"certificate_domain": {
			"description": "Enter the certficate domamain name (e.g. *.data.humancellatlas.org). It may take 48 hours for the certificate to validate. Available certificates may be listed with the aws cli command `aws --region us-east-1 acm list-certificates` (note that certificates used for an edge-optimized gateway domain name must be in the us-east-1 region.",
			"default": null
		}
	}
}
