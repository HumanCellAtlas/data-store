#!/bin/bash

set -euo pipefail

if [[ $# != 3 ]]; then
    echo "Given a owner, repo, and branch, output the combined check status."
    echo "Usage: $(basename $0) owner repo branch"
    echo "Example: $(basename $0) HumanCellAtlas dcp integration"
    exit 1
fi

GITHUB_API="https://api.github.com"

OWNER=$(python -c "import sys, urllib.parse; print(urllib.parse.quote_plus(sys.argv[1]))" "${1}")
REPO=$(python -c "import sys, urllib.parse; print(urllib.parse.quote_plus(sys.argv[1]))" "${2}")
BRANCH=$(python -c "import sys, urllib.parse; print(urllib.parse.quote_plus(sys.argv[1]))" "${3}")

STATUS=$(http GET ${GITHUB_API}/repos/${OWNER}/${REPO}/commits/${BRANCH}/status Accept:application/vnd.github.full+json)
STATE=$(echo "$STATUS" | jq -r .state)

echo ${STATE}
