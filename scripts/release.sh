#!/bin/bash

source "$(dirname $0)/../environment"

set -euo pipefail

# This block discovers the command line flags `--force` and  `--no-deploy`,
# and passes on positional arguments as $1, $2, etc.
FORCE=
NO_DEPLOY=
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --force)
    FORCE="--force"
    shift
    ;;
    --no-deploy)
    NO_DEPLOY="--no-deploy"
    shift
    ;;
    *)
    POSITIONAL+=("$1")
    shift
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

if [[ $# != 2 ]]; then
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
    echo "If the --no-deploy flag is given, the deployment step will be skipped."
    echo
    echo "Usage: $(basename $0) source_branch dest_branch [--force] [--no-deploy]"
    echo "Example: $(basename $0) master staging"
    exit 1
fi

echo "Please review and confirm your active AWS account configuration:"
aws configure list
aws sts get-caller-identity
aws iam list-account-aliases
echo "Is this correct?"
select result in Yes No; do
    if [[ $result != Yes ]]; then exit 1; else break; fi
done

if ! git diff-index --quiet HEAD --; then
    if [[ $# == 3 ]] && [[ $FORCE == "--force" ]]; then
        echo "You have uncommitted files in your Git repository. Forcing deployment anyway."
    else
        echo "You have uncommitted files in your Git repository. Please commit or stash them, or run $0 with --force."
        exit 1
    fi
fi

if ! [[ -e "$DSS_HOME/application_secrets.json" ]]; then
    http --check-status GET "https://$API_DOMAIN_NAME/internal/application_secrets" > "$DSS_HOME/application_secrets.json" || (
        echo "Failed to fetch application_secrets.json. Please create this file and try again."
        exit 1
    )
fi

if ! diff <(pip freeze) <(tail -n +2 "$DSS_HOME/requirements-dev.txt"); then
    if [[ $# == 3 ]] && [[ $FORCE == "--force" ]]; then
        echo "Your installed Python packages differ from requirements-dev.txt. Forcing deployment anyway."
    else
        echo "Your installed Python packages differ from requirements-dev.txt. Please update your virtualenv."
        echo "Run $0 with --force to deploy anyway."
        exit 1
    fi
fi

export PROMOTE_FROM_BRANCH=$1 PROMOTE_DEST_BRANCH=$2

if [[ "hca_cicd" != $(whoami) ]]; then
    # Skip when this script is executed by the GitLab runner
    GH_API=https://api.github.com
    REPO=$(git remote get-url origin | perl -ne '/github\.com.(.+?)(\.git)?$/; print $1')
    STATUS=$(http GET ${GH_API}/repos/${REPO}/commits/${PROMOTE_FROM_BRANCH}/status Accept:application/vnd.github.full+json)
    STATE=$(echo "$STATUS" | jq -r .state)
    echo "$STATUS" | jq '.statuses[]|select(.state != "success")'
    
    # TODO: (akislyuk) some CI builds no longer deploy or run a subset of tests. Find the last build that ran a deployment.
    if [[ "$STATE" != success ]]; then
        if [[ $# == 3 ]] && [[ $FORCE == "--force" ]]; then
            echo "Status checks failed on branch $PROMOTE_FROM_BRANCH. Forcing promotion and deployment anyway."
        else
            echo "Status checks failed on branch $PROMOTE_FROM_BRANCH."
            echo "Run with --force to promote $PROMOTE_FROM_BRANCH to $PROMOTE_DEST_BRANCH and deploy anyway."
            exit 1
        fi
    fi
fi

RELEASE_TAG=${PROMOTE_DEST_BRANCH}-$(date -u +"%Y-%m-%d-%H-%M-%S").release

if [[ "$(git --no-pager log --graph --abbrev-commit --pretty=oneline --no-merges $PROMOTE_DEST_BRANCH ^$PROMOTE_FROM_BRANCH)" != "" ]]; then
    echo "Warning: The following commits are present on $PROMOTE_DEST_BRANCH but not on $PROMOTE_FROM_BRANCH"
    git --no-pager log --graph --abbrev-commit --pretty=oneline --no-merges $PROMOTE_DEST_BRANCH ^$PROMOTE_FROM_BRANCH
    if [[ $# == 3 ]] && [[ $FORCE == "--force" ]]; then
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

if [[ $NO_DEPLOY == "--no-deploy" ]]; then
    echo "The --no-deploy flag is set. Skipping deployment."
elif yq -e '.stages[]
          | select(.name == "deploy")
          | .if
          | splits("\\s+AND\\s+")
          | match("branch\\s+IN\\s+\\(([^)]+)\\)").captures[]
          | .string
          | splits("\\s*,\\s*")
          | select(. == env.PROMOTE_DEST_BRANCH)' .travis.yml; then
    echo "Found deployment config for $PROMOTE_DEST_BRANCH in Travis CI. Skipping deployment."
elif [[ -e "${DSS_HOME}/environment.${PROMOTE_DEST_BRANCH}" ]]; then
    source "${DSS_HOME}/environment.${PROMOTE_DEST_BRANCH}"
    make -C "$DSS_HOME" deploy
else
    echo "Error: Could not find environment config file ${DSS_HOME}/environment.${PROMOTE_DEST_BRANCH}. Unable to deploy."
    exit 1
fi
