#!/bin/bash

source "$(dirname $0)/../environment"

set -euo pipefail

if [[ $# != 2 ]] && [[ $# != 3 ]]; then
    echo "Given a source (pre-release) branch and a destination (release) branch,"
    echo "this script checks the continuous integration status of the source branch,"
    echo "creates a Git tag, resets the head of the destination branch to the head"
    echo "of the source branch, and pushes the results to the Git remote."
    echo "If .travis.yml contains no deploy directives for the destination branch,"
    echo 'then this script runs "make deploy" after sourcing "environment.{DEST}",'
    echo "where DEST is the destination branch."
    echo
    echo "If the --force flag is given, deployment will proceed even if CI checks fail."
    echo
    echo "Usage: $(basename $0) source_branch dest_branch [--force]"
    echo "Example: $(basename $0) master staging"
    exit 1
fi

export PROMOTE_FROM_BRANCH=$1 PROMOTE_DEST_BRANCH=$2

GH_API=https://api.github.com
REPO=$(git remote get-url origin | perl -ne '/([^\/\:]+\/.+?)(\.git)?$/; print $1')
STATUS=$(http ${GH_API}/repos/${REPO}/commits/${PROMOTE_FROM_BRANCH}/status Accept:application/vnd.github.full+json)
STATE=$(echo "$STATUS" | jq -r .state)
echo "$STATUS" | jq '.statuses[]|select(.state != "success")'

if [[ "$STATE" != success ]]; then
    if [[ $# == 3 ]] && [[ $3 == "--force" ]]; then
        echo "Status checks failed on branch $PROMOTE_FROM_BRANCH. Forcing promotion and deployment anyway."
    else
        echo "Status checks failed on branch $PROMOTE_FROM_BRANCH."
        echo "Run with --force to promote $PROMOTE_FROM_BRANCH to $PROMOTE_DEST_BRANCH and deploy anyway."
        exit 1
    fi
fi

RELEASE_TAG=${PROMOTE_DEST_BRANCH}-$(date -u +"%Y-%m-%d-%H-%M-%S").deploy

if [[ "$(git --no-pager log --graph --abbrev-commit --pretty=oneline --no-merges $PROMOTE_DEST_BRANCH ^$PROMOTE_FROM_BRANCH)" != "" ]]; then
    echo "Warning: The following commits are present on $PROMOTE_DEST_BRANCH but not on $PROMOTE_FROM_BRANCH"
    git --no-pager log --graph --abbrev-commit --pretty=oneline --no-merges $PROMOTE_DEST_BRANCH ^$PROMOTE_FROM_BRANCH
    if [[ $# == 3 ]] && [[ $3 == "--force" ]]; then
        echo -e "\nThey will be overwritten on $PROMOTE_DEST_BRANCH and discarded."
    else
        echo -e "\nRun with --force to overwrite and discard these commits from $PROMOTE_DEST_BRANCH."
        exit 1
    fi
fi

if ! git --no-pager diff --ignore-submodules=untracked --exit-code; then
    echo "Working tree contains changes to tracked files. Please commit or discard your changes and try again."
    exit 1
fi

git fetch --all
git -c advice.detachedHead=false checkout origin/$PROMOTE_FROM_BRANCH
git checkout -B $PROMOTE_DEST_BRANCH
git tag $RELEASE_TAG
git push --force origin $PROMOTE_DEST_BRANCH
git push --tags

if yq -e '.deploy[] | select(.true.branch == env.PROMOTE_DEST_BRANCH)' .travis.yml; then
    echo "Found deployment config for $PROMOTE_DEST_BRANCH in Travis CI. Skipping deployment."
elif [[ -e "${DSS_HOME}/environment.${PROMOTE_DEST_BRANCH}" ]]; then
    source "${DSS_HOME}/environment.${PROMOTE_DEST_BRANCH}"
    make -C "$DSS_HOME" deploy
else
    echo "Error: Could not find environment config file ${DSS_HOME}/environment.${PROMOTE_DEST_BRANCH}. Unable to deploy."
    exit 1
fi

# We have the option to use the Github deployments API. It's not exposed through the UI, which makes it difficult.
# It also requires Github token authentication. But it has built-in CI pass/fail checks (and a bypass option for them).
# http --auth user:token ${GH_API}/repos/${REPO}/deployments ref=${PROMOTE_DEST_BRANCH} required_contexts:=[]
