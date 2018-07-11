#!/bin/bash
set -euo pipefail
if [[ $# != 3 ]]; then
	echo "This script triggers allspark build for owner, repo, and branch"
    echo
    echo "Usage: $(basename $0) owner repo branch"
    echo "Example: $(basename $0) HumanCellAtlas data-store master"
    exit 1
fi
owner=$1
repo=$2
branch=$3
slug=$(python -c "import sys, urllib.parse; print(urllib.parse.quote_plus(sys.argv[1]))" "${owner}/${repo}")

for i in $(seq 3); do
	GITHUB_API=https://api.github.com
	GH_COMMITS=$(http GET ${GITHUB_API}/repos/${owner}/${repo}/commits sha==${branch})
	GH_LATEST=$(echo ${GH_COMMITS} | jq -r '.[0]["sha"]')
	
	GL_COMMITS=$(http GET ${GITLAB_API}/projects/${slug}/repository/commits ref_name==${branch} PRIVATE-TOKEN:${GITLAB_TOKEN})
	GL_LATEST=$(echo ${GL_COMMITS} | jq -r '.[0]["id"]')

	if [[ ${GL_LATEST} == ${GH_LATEST} ]]; then
		http POST ${GITLAB_API}/projects/${slug}/trigger/pipeline token=${GITLAB_DSS_TRIGGER_TOKEN} ref=${branch}
		exit 0
	fi

	http POST ${GITLAB_API}/projects/${slug}/mirror/pull "Private-Token: ${GITLAB_TOKEN}"
done

echo "GitLab repo not up-to-date" ; exit 1
