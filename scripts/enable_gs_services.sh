#!/bin/bash -x

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done

DSS_HOME="$(cd -P "$(dirname "$SOURCE")/.." && pwd)"
GCP_PROJECT=$(cat ${DSS_HOME}/deployment/active/variables.tf | jq -r '.variable["gcp_project"]["default"]')

gcloud --project ${GCP_PROJECT} services enable cloudfunctions.googleapis.com
gcloud --project ${GCP_PROJECT} services enable runtimeconfig.googleapis.com
