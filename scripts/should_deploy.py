#! /usr/bin/env python
import os
import json
import urllib
import requests
import argparse


"""
This script raises an exception when `stage` is not in an appropriate state for deployment.

dev may be deployed when:
    - GitLab and GitHub have the same HEAD for master
    - GitLab has not attempted to deploy `commit` before
    - `commit` is passing unit tests on Travis
    - `commit` is passing integration tests on Travis

dev may be promoted to integration when:
    - GitLab and GitHub have the same HEAD for integration
    - GitLab has not attempted to deploy `commit` before
    - `commit` is passing unit tests on Travis
    - `commit` is passing integration tests on Travis
    - DCP integration tests for integration passing on travis

integration may be promoted to staging when:
    - GitLab and GitHub have the same HEAD for staging
    - GitLab has not attempted to deploy `commit` before
    - `commit` is passing unit tests on Travis
    - `commit` is passing integration tests on Travis
    - DCP integration tests for staging are passing on travis
    - DCP ops agree to promote to staging

staging may be promoted to prod when:
    - GitLab and GitHub have the same HEAD for staging 
    - GitLab has not attempted to deploy `commit` before
    - `commit` is passing unit tests on Travis
    - `commit` is passing integration tests on Travis
    - DCP integration tests for staging are passing on travis
    - DCP ops agree to promote to staging
"""


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--stage", required=True)
args = parser.parse_args()


def parse_paging_header(headers):
    try:
        linktext = headers['Link']
    except KeyError:
        return None

    for link in linktext.split(","):
        link_dest = link.split(";")[1]
        if "next" in link_dest:
            return link.split("<")[1].split(">")[0] 

    return None


def max_items(max_items=50):
    def decorate(func):
        def wrapped(*args, **kwargs):
            count = 0
            for it in func(*args, **kwargs):
                yield it
                count += 1
                if count >= max_items:
                    break
        return wrapped
    return decorate


class GitHub: 
    url = "https://api.github.com"

    def __init__(self, owner, repo):
        self.owner = owner
        self.repo = repo

    def _request(self, href, params=None):
        token = os.environ['GITHUB_TOKEN']

        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {token}",
        }

        if params is None:
            params = dict()

        req_url = self.url + href
        while req_url:
            page = requests.get(req_url, headers=headers, params=params)
            yield json.loads(page.text)
            req_url = parse_paging_header(page.headers)

    @max_items()
    def commits(self, branch):
        owner = urllib.parse.quote_plus(self.owner)
        repo = urllib.parse.quote_plus(self.repo)
        for page in self._request(f"/repos/{owner}/{repo}/commits", dict(sha=branch)):
            for c in page:
                yield c

    def latest_commit(self, branch):
        for c in self.commits(branch):
            return c['sha']


class Travis:
    url = "https://api.travis-ci.com"

    def __init__(self, owner, repo):
        self.owner = owner
        self.repo = repo

    def _request(self, href, params=None):
        token = os.environ['TRAVIS_TOKEN']
        headers = {
            "Travis-API-Version": "3",
            "Authorization": f"token {token}"
        }

        if params is None:
            params = dict()
        
        resp = requests.get(self.url + href, headers=headers, params=params)
        return json.loads(resp.text)

    @max_items()
    def builds(self, branch=None):
        repo_slug = urllib.parse.quote_plus(f"{self.owner}/{self.repo}")
        href = f"/repo/{repo_slug}/builds"

        params = {}
        if branch is not None:
            params["branch.name"] = branch
    
        while href:
            resp = self._request(href, params)
    
            for b in resp['builds']:
                yield b
    
            try:
                href = resp['@pagination']['next']['@href']
            except KeyError:
                raise StopIteration

    def builds_for_branch(self, branch):
        return self.builds(branch)

    def builds_for_commit(self, branch, sha):
        for b in self.builds(branch):
            if b['commit']['sha'] == sha:
                yield b


class GitLab:
    url = "https://allspark.dev.data.humancellatlas.org/api/v4"

    def __init__(self, owner, repo):
        self.owner = owner
        self.repo = repo

    def _request(self, href, params=None):
        token = os.environ['GITLAB_TOKEN']
        headers = {"PRIVATE-TOKEN": token}

        if params is None:
            params = dict()
        
        req_url = self.url + href
        while req_url:
            page = requests.get(req_url, headers=headers, params=params)
            yield json.loads(page.text)
            req_url = parse_paging_header(page.headers)

    @max_items()
    def builds(self, branch):
        repo_slug = urllib.parse.quote_plus(f"{self.owner}/{self.repo}")
        for page in self._request(f"/projects/{repo_slug}/jobs", {"ref_name": branch}):
            for c in page:
                yield c

    @max_items()
    def builds_for_commit(self, branch, sha):
        for b in self.builds(branch):
            if b['commit']['id'] == sha:
                yield b

    @max_items()
    def commits(self, branch):
        repo_slug = urllib.parse.quote_plus(f"{self.owner}/{self.repo}")
        for page in self._request(f"/projects/{repo_slug}/repository/commits", {"ref_name": branch}):
            for c in page:
                yield c

    def latest_commit(self, branch):
        for c in self.commits(branch):
            return c['id']


def should_deploy_dev():
    owner = "HumanCellAtlas"
    repo = "data-store"
    branch = "master"

    gh_client = GitHub(owner, repo)
    gl_client = GitLab(owner, repo)

    gitlab_latest = gl_client.latest_commit(branch)
    github_latest = gh_client.latest_commit(branch)

    info = f"{owner} {repo} {branch}"

    if gitlab_latest != github_latest:
        raise Exception(f"GitHub and GitLab have different HEAD: {info} gitlab={gitlab_latest} github={github_latest}")

    sha = gitlab_latest

    for b in gl_client.builds_for_commit(branch, gitlab_latest):
        raise Exception(f"Build already attempted: {info} {sha}")

    integration_build_state = None
    unit_tests_build_state = None
    for b in Travis(owner, repo).builds_for_commit(branch, github_latest):
        if b['commit']['message'].startswith("Integration"):
            if integration_build_state is None:
                integration_build_state = b['state']
        else:
            if unit_tests_build_state is None:
                unit_tests_build_state = b['state']

        if integration_build_state and unit_tests_build_state:
            break

    if "passed" != unit_tests_build_state:
        raise Exception(f"Unit tests have not passed: {info} {sha}")

    if "passed" != integration_build_state:
        raise Exception(f"Integration tests have not passed: {info} {sha}")

if args.stage == "dev":
    should_deploy_dev()
else:
    raise Exception(f"Should not deploy stage {args.stage}")
