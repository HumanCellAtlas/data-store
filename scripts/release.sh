#!/bin/bash

set -euo pipefail

# This block discovers the command line flags `--force` and  `--no-deploy`,
# and passes on positional arguments as $1, $2, etc.
if [[ $# > 0 ]]; then
    FORCE=
    NO_DEPLOY=
    SKIP_ACCOUNT_VERIFICATION=
    POSITIONAL=()
    while [[ $# > 0 ]]; do
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
            --skip-account-verification)
            SKIP_ACCOUNT_VERIFICATION="--skip-account-verification"
            shift
            ;;
            *)
            POSITIONAL+=("$1")
            shift
            ;;
        esac
    done
    set -- "${POSITIONAL[@]}" # restore positional parameters
fi

if [[ $# != 2 ]]; then
    echo "Given a source (pre-release) branch and a destination (release) branch,"
    echo "this script checks the continuous integration status of the source branch,"
    echo "creates a Git tag, resets the head of the destination branch to the head"
    echo "of the source branch, and pushes the results to the Git remote."
    echo 'If an "environment.{DEST}" file is found in the repo (where DEST is the'
    echo 'destination branch), then this script runs "make deploy" after sourcing it.'
    echo
    echo "If the --force flag is given, deployment will proceed even if CI checks fail."
    echo
    echo "If the --no-deploy flag is given, the deployment step will be skipped."
    echo
    echo "If the --skip-account-verification flag is given, the user will not be asked to"
    echo "verify cloud account information."
    echo
    echo "Usage: $(basename $0) source_branch dest_branch [--force] [--no-deploy] [--skip-account-verification]"
    echo "Example: $(basename $0) master staging"
    exit 1
fi

if [[ $SKIP_ACCOUNT_VERIFICATION != "--skip-account-verification" ]]; then
    echo "Please review and confirm your active AWS account configuration:"
    aws configure list
    aws sts get-caller-identity
    aws iam list-account-aliases
    echo "Is this correct?"
    select result in Yes No; do
        if [[ $result != Yes ]]; then exit 1; else break; fi
    done
fi

if ! git diff-index --quiet HEAD --; then
    if [[ $FORCE == "--force" ]]; then
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
    if [[ $FORCE == "--force" ]]; then
        echo "Your installed Python packages differ from requirements-dev.txt. Forcing deployment anyway."
    else
        echo "Your installed Python packages differ from requirements-dev.txt. Please update your virtualenv."
        echo "Run $0 with --force to deploy anyway."
        exit 1
    fi
fi

export PROMOTE_FROM_BRANCH=$1 PROMOTE_DEST_BRANCH=$2

STATUS=$(${DSS_HOME}/scripts/status.py HumanCellAtlas data-store $PROMOTE_FROM_BRANCH)
if [[ "$STATUS" != success ]]; then
    if [[ $FORCE == "--force" ]]; then
        echo "Status checks failed on branch $PROMOTE_FROM_BRANCH. Forcing promotion and deployment anyway."
    else
        echo "Status checks failed on branch $PROMOTE_FROM_BRANCH."
        echo "Run with --force to promote $PROMOTE_FROM_BRANCH to $PROMOTE_DEST_BRANCH and deploy anyway."
        exit 1
    fi
fi

RELEASE_TAG=$(date -u +"%Y-%m-%d-%H-%M-%S")-${PROMOTE_DEST_BRANCH}.release

if [[ "$(git --no-pager log --graph --abbrev-commit --pretty=oneline --no-merges -- $PROMOTE_DEST_BRANCH ^$PROMOTE_FROM_BRANCH)" != "" ]]; then
    echo "Warning: The following commits are present on $PROMOTE_DEST_BRANCH but not on $PROMOTE_FROM_BRANCH"
    git --no-pager log --graph --abbrev-commit --pretty=oneline --no-merges $PROMOTE_DEST_BRANCH ^$PROMOTE_FROM_BRANCH
    if [[ $FORCE == "--force" ]]; then
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
elif [[ -e "${DSS_HOME}/environment.${PROMOTE_DEST_BRANCH}" ]]; then
    source "${DSS_HOME}/environment.${PROMOTE_DEST_BRANCH}"
    make -C "$DSS_HOME" deploy
    version_readback=$(http "https://$API_DOMAIN_NAME/version" | jq -r .version_info.version)
    if [[ "$version_readback" != $RELEASE_TAG ]]; then
        echo "Error: Unable to read back release tag from deployment (expected '$RELEASE_TAG', but got '$version_readback')"
        exit 1
    fi
else
    echo "Error: Could not find environment config file ${DSS_HOME}/environment.${PROMOTE_DEST_BRANCH}. Unable to deploy."
    exit 1
fi
