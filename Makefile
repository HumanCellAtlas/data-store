include common.mk
MODULES=dss tests

lint:
	flake8 $(MODULES) chalice/*.py daemons/*/*.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

test_srcs := $(wildcard tests/test_*.py)

define test_rules

# Generate a set of rules for running test scripts, either all of them or
# individually, for a given value of DSS_TEST_MODE. Depending on that value,
# certain test cases or test methods within the test scripts will skipped.
#
# $1 is the prefix for the name of the phony target that runs all test scripts
# (can be empty)
# 
# $2 is the prefix that's added to each test script name to create a phony
# target for running that script individually (if empty, the actual script name
# is used)
# 
# $3 is the value for DSS_TEST_MODE (if empty, no test cases or methods will be
# skipped).

$(2)test_srcs := $$(addprefix $(2), $$(test_srcs))

$(1)test: lint mypy $$($(2)test_srcs)
	coverage combine
	rm -f .coverage.*

$$($(2)test_srcs): $(2)%.py :
	set -o pipefail \
	&& DSS_TEST_MODE="$(3)" coverage run -p --source=dss -m unittest $$(DSS_UNITTEST_OPTS) $$*.py 2>&1 \
	| sed -e "s/^/$$$$$$$$ /"

.PHONY: $(1)test $$($(2)test_srcs)

endef

# Define three independent sets of rules for standalone, integration and all tests
#
$(eval $(call test_rules,safe_,safe__,standalone))
$(eval $(call test_rules,integration_,integration__,integration))
$(eval $(call test_rules,all_,all__,integration standalone))

# Serialize the standalone tests that use a local Elasticsearch instance. This
# will prevent them from running at the same time, but running a test script
# individually may trigger running another one before that. To avoid this we
# also ...
#
safe__tests/test_search.py: safe__tests/test_indexer.py
safe__tests/test_indexer.py: safe__tests/test_subscriptions.py

# ... add a set of rules that doesn't serialize those tests. Be aware that when
# you run `make -j test`, all standalone tests will be scheduled in parallel and
# you will likely get three concurrent ES instances, each requiring 2GiB of RAM.
# Note that this set of rules is also used when you run a test individually
# e.g., via `make tests/test_indexer.py`.
#
$(eval $(call test_rules,,,standalone))

smoketest: all__tests/test_smoketest.py

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

.PHONY: lint mypy test
.PHONY: deploy deploy-chalice deploy-daemons
