include common.mk
MODULES=dss tests

lint:
	flake8 $(MODULES) chalice/*.py daemons/*/*.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

tests:=$(wildcard tests/test_*.py)
serial_tests:=tests/test_search.py \
	          tests/test_indexer.py \
			  tests/test_subscriptions.py
parallel_tests:=$(filter-out $(serial_tests),$(tests))

# Run all standalone tests in parallel
#
test: mypy lint $(tests)
	coverage combine
	rm -f .coverage.*

# Serialize the standalone tests that start a local Elasticsearch instance in
# order to prevent more than one such instance at a time.
#
safe_test: mypy lint _serial_test $(parallel_tests)
	coverage combine
	rm -f .coverage.*

_serial_test:
	$(MAKE) -j1 $(serial_tests)

# A pattern rule that runs a single test script
#
$(tests): %.py :
	export DSS_TEST_MODE=$${DSS_TEST_MODE:-standalone} \
	&& set -o pipefail \
	&& coverage run -p --source=dss -m unittest $(DSS_UNITTEST_OPTS) $*.py 2>&1 | sed -e "s/^/$$$$ /"

# Run standalone and integration tests
#
all_test:
	DSS_TEST_MODE="standalone integration" $(MAKE) test

# Run integration tests only
#
integration_test:
	DSS_TEST_MODE="integration" $(MAKE) test

smoketest: all__tests/test_smoketest.py

scaletest:
	locust -f tests/scalability_test.py --no-web -c 1000  -r 100

deploy: deploy-chalice deploy-daemons

deploy-chalice:
	$(MAKE) -C chalice deploy

deploy-daemons: deploy-daemons-serial deploy-daemons-parallel

deploy-daemons-serial:
	$(MAKE) -j1 -C daemons deploy-serial

deploy-daemons-parallel:
	$(MAKE) -C daemons deploy-parallel

release_staging:
	scripts/release.sh master staging

release_prod:
	scripts/release.sh staging prod

clean:
	git clean -Xdf chalice daemons $(MODULES)
	git clean -df {chalice,daemons/*}/{chalicelib,domovoilib,vendor}
	git checkout $$(git status --porcelain {chalice,daemons/*}/.chalice/config.json | awk '{print $$2}')
	-rm -rf .requirements-env

refresh_all_requirements:
	@echo -n '' >| requirements.txt
	@echo -n '' >| requirements-dev.txt
	@if [ $$(uname -s) == "Darwin" ]; then sleep 1; fi  # this is require because Darwin HFS+ only has second-resolution for timestamps.
	@touch requirements.txt.in requirements-dev.txt.in
	@$(MAKE) requirements.txt requirements-dev.txt

requirements.txt requirements-dev.txt : %.txt : %.txt.in
	[ ! -e .requirements-env ] || exit 1
	virtualenv .requirements-env
	.requirements-env/bin/pip install -r $@
	.requirements-env/bin/pip install -r $<
	echo "# You should not edit this file directly.  Instead, you should edit $<." >| $@
	.requirements-env/bin/pip freeze >> $@
	rm -rf .requirements-env
	scripts/find_missing_wheels.py requirements.txt

requirements-dev.txt : requirements.txt.in

.PHONY: lint mypy test safe_test _serial_test all_test integration_test $(tests)
.PHONY: deploy deploy-chalice deploy-daemons
