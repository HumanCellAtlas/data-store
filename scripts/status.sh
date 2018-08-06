#!/bin/bash

set -euo pipefail

if [[ $# != 3 ]]; then
    echo "Given a owner, repo, and branch, output the combined check status."
    echo "GITLAB_API and GITLAB_TOKEN are expected to be available in the environment."
    echo "Usage: $(basename $0) owner repo branch"
    echo "Example: $(basename $0) HumanCellAtlas dcp integration"
    echo "Example: GITLAB_API=https://my_gitlab_domain/api/v4 $(basename $0) HumanCellAtlas dcp integration"
    exit 1
fi

owner=$1
repo=$2
branch=$3
SLUG=$(python -c "import sys, urllib.parse; print(urllib.parse.quote_plus(sys.argv[1]))" "${owner}/${repo}")

STATUS=$(http GET ${GITLAB_API}/projects/${SLUG}/pipelines ref==${branch} "Private-Token: ${GITLAB_TOKEN}")
echo ${STATUS} | jq -r .[0].status
