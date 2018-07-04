#! /usr/bin/env python
import os
import json
import urllib
import requests
import argparse
from functools import wraps


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


def limited_iter(limited_iter=50):
    def decorate(meth):
        @wraps(meth)
        def wrapped(*args, **kwargs):
            count = 0
            for it in meth(*args, **kwargs):
                yield it
                count += 1
                if count >= limited_iter:
                    break
        return wrapped
    return decorate


class _CommonClient:
    def __init__(self, token, owner, repo):
        self.token = token
        self.owner = owner
        self.repo = repo


class GitHub(_CommonClient): 
    url = "https://api.github.com"

    def _request(self, href, params=None):
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {self.token}",
        }

        if params is None:
            params = dict()

        req_url = self.url + href
        while req_url:
            page = requests.get(req_url, headers=headers, params=params)
            yield json.loads(page.text)
            req_url = parse_paging_header(page.headers)

    @limited_iter()
    def commits(self, branch):
        owner = urllib.parse.quote_plus(self.owner)
        repo = urllib.parse.quote_plus(self.repo)
        for page in self._request(f"/repos/{owner}/{repo}/commits", dict(sha=branch)):
            for p in page:
                yield p

    def latest_commit(self, branch):
        for c in self.commits(branch):
            return c['sha']


class Travis(_CommonClient):
    url = "https://api.travis-ci.com"

    def _request(self, href, params=None):
        headers = {
            "Travis-API-Version": "3",
            "Authorization": f"token {self.token}"
        }

        if params is None:
            params = dict()
        
        while href:
            resp = requests.get(self.url + href, headers=headers, params=params)
            resp = json.loads(resp.text)
            yield resp
            try:
                href = resp['@pagination']['next']['@href']
            except KeyError:
                raise StopIteration

    @limited_iter()
    def builds(self, branch):
        repo_slug = urllib.parse.quote_plus(f"{self.owner}/{self.repo}")
        href = f"/repo/{repo_slug}/builds"

        params = {'branch.name': branch}

        for page in self._request(href, params):
            for p in page['builds']:
                yield p

    def builds_for_branch(self, branch):
        return self.builds(branch)

    def builds_for_commit(self, branch, sha):
        for b in self.builds(branch):
            if b['commit']['sha'] == sha:
                yield b


class GitLab(_CommonClient):
    url = "https://allspark.dev.data.humancellatlas.org/api/v4"

    def _request(self, href, params=None):
        headers = {"PRIVATE-TOKEN": self.token}

        if params is None:
            params = dict()
        
        req_url = self.url + href
        while req_url:
            page = requests.get(req_url, headers=headers, params=params)
            yield json.loads(page.text)
            req_url = parse_paging_header(page.headers)

    @limited_iter()
    def builds(self, branch):
        repo_slug = urllib.parse.quote_plus(f"{self.owner}/{self.repo}")
        for page in self._request(f"/projects/{repo_slug}/jobs", {"ref_name": branch}):
            for p in page:
                yield p

    def builds_for_commit(self, branch, sha):
        for b in self.builds(branch):
            if b['commit']['id'] == sha:
                yield b

    @limited_iter()
    def commits(self, branch):
        repo_slug = urllib.parse.quote_plus(f"{self.owner}/{self.repo}")
        for page in self._request(f"/projects/{repo_slug}/repository/commits", {"ref_name": branch}):
            for p in page:
                yield p

    def latest_commit(self, branch):
        for c in self.commits(branch):
            return c['id']


def should_deploy_dev():
    owner = "HumanCellAtlas"
    repo = "data-store"
    branch = "master"

    gh_client = GitHub(os.environ['GITHUB_TOKEN'], owner, repo)
    gl_client = GitLab(os.environ['GITLAB_TOKEN'], owner, repo)
    travis_client = Travis(os.environ['TRAVIS_TOKEN'], owner, repo)

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
    for b in travis_client.builds_for_commit(branch, github_latest):
        
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
    print("OK to deploy `dev`")
else:
    raise Exception(f"Should not deploy stage {args.stage}")
