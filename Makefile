include common.mk
MODULES=dss tests

all: test

lint:
	flake8 $(MODULES) chalice/*.py daemons/*/*.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

export DSS_TEST_MODE?=standalone

tests:=$(wildcard tests/test_*.py)
serial_tests:=tests/test_search.py \
              tests/test_indexer.py \
              tests/test_subscriptions.py
parallel_tests:=$(filter-out $(serial_tests),$(tests))

# Run all standalone tests in parallel
#
test: $(tests) daemon-import-test
	coverage combine
	rm -f .coverage.*

daemon-import-test:
	$(MAKE) -C daemons import-test

# Serialize the standalone tests that start a local Elasticsearch instance in
# order to prevent more than one such instance at a time.
#
safe_test: serial_test parallel_test
	coverage combine
	rm -f .coverage.*

parallel_test: $(parallel_tests) daemon-import-test

serial_test:
	$(MAKE) -j1 $(serial_tests)

# A pattern rule that runs a single test script
#
$(tests): %.py : mypy lint
	coverage run -p --source=dss $*.py $(DSS_UNITTEST_OPTS)

# Run standalone and integration tests
#
all_test:
	$(MAKE) DSS_TEST_MODE="standalone integration" test

# Run integration tests only
#
integration_test:
	$(MAKE) DSS_TEST_MODE="integration" test

smoketest:
	$(MAKE) DSS_TEST_MODE="integration" tests/test_smoketest.py

scaletest:
	./tests/scalability/scale_test_runner.py -r 10 -d 30

deploy: deploy-chalice deploy-daemons

deploy-chalice:
	$(MAKE) -C chalice deploy

deploy-daemons: deploy-daemons-serial deploy-daemons-parallel

deploy-daemons-serial:
	$(MAKE) -j1 -C daemons deploy-serial

deploy-daemons-parallel:
	$(MAKE) -C daemons deploy-parallel

deploy-infra:
	$(MAKE) -C infra apply-all

create-github-deployment:
	$(eval REMOTE=$(shell git remote get-url origin | perl -ne '/([^\/\:]+\/.+?)(\.git)?$$/; print $$1'))
	$(eval BRANCH=$(shell git rev-parse --abbrev-ref HEAD))
	$(eval DEPLOY_API=https://api.github.com/repos/$(REMOTE)/deployments)
	http POST $(DEPLOY_API) Authorization:"Bearer $(GH_TOKEN)" ref=$(BRANCH) environment=dev auto_merge:=false

release_integration:
	scripts/release.sh master integration

release_staging:
	scripts/release.sh integration staging

release_prod:
	scripts/release.sh staging prod

clean:
	git clean -Xdf chalice daemons $(MODULES)
	git clean -df {chalice,daemons/*}/{chalicelib,domovoilib,vendor}
	git checkout $$(git status --porcelain {chalice,daemons/*}/.chalice/config.json | awk '{print $$2}')
	-rm -rf .requirements-env
	-rm -rf node_modules

refresh_all_requirements:
	@echo -n '' >| requirements.txt
	@echo -n '' >| requirements-dev.txt
	@if [ $$(uname -s) == "Darwin" ]; then sleep 1; fi  # this is require because Darwin HFS+ only has second-resolution for timestamps.
	@touch requirements.txt.in requirements-dev.txt.in
	@$(MAKE) requirements.txt requirements-dev.txt

requirements.txt requirements-dev.txt : %.txt : %.txt.in
	[ ! -e .requirements-env ] || exit 1
	virtualenv .requirements-env -p "$(which python)"
	.requirements-env/bin/pip install -r $@
	.requirements-env/bin/pip install -r $<
	echo "# You should not edit this file directly.  Instead, you should edit $<." >| $@
	.requirements-env/bin/pip freeze >> $@
	rm -rf .requirements-env
	scripts/find_missing_wheels.py requirements.txt

requirements-dev.txt : requirements.txt.in

.PHONY: all lint mypy test safe_test _serial_test all_test integration_test smoketest daemon-import-test $(tests)
.PHONY: deploy deploy-chalice deploy-daemons
