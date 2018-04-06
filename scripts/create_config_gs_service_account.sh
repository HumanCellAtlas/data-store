#!/bin/bash -x

# This scripts deploys, configures, and aquires access keys for a GCP service account

# At some point it may be preferable to create/configure the service account with Terraform.
# However, Terraform 0.11.3 suffers from:
#    1) Application secrets will be stored in the terraform state file (either encrypted or unencrypted)
#    2) Terraform scripting does not support existing service accounts.
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done

DSS_HOME="$(cd -P "$(dirname "$SOURCE")/.." && pwd)"
GCP_PROJECT=$(cat ${DSS_HOME}/deployment/active/variables.tf | jq -r '.variable["gcp_project"]["default"]')
ID=$(cat ${DSS_HOME}/deployment/active/variables.tf | jq -r '.variable["gcp_service_account_id"]["default"]')
GOOGLE_APPLICATION_CREDENTIALS="${DSS_HOME}/deployment/active/gcp-credentials.json"
EMAIL="$ID@$GCP_PROJECT.iam.gserviceaccount.com"
MEMBER="serviceAccount:$EMAIL"

gcloud --project ${GCP_PROJECT} iam service-accounts describe ${EMAIL} >/dev/null 2>&1
if [[ ! ${?} -eq 0 ]]; then
	gcloud --project ${GCP_PROJECT} iam service-accounts create $ID --display-name=$ID
	[[ ${?} -eq 0 ]] || exit 1
fi

for role in "cloudfunctions.developer" "iam.serviceAccountActor" "owner"; do
	gcloud --project ${GCP_PROJECT} projects add-iam-policy-binding ${GCP_PROJECT} --member ${MEMBER} --role roles/${role} >/dev/null
	[[ ${?} -eq 0 ]] || exit 1
done

if [[ ! -e "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
	gcloud --project ${GCP_PROJECT} iam service-accounts keys create "${GOOGLE_APPLICATION_CREDENTIALS}" --iam-account ${EMAIL}
	[[ ${?} -eq 0 ]] || exit 1
fi
