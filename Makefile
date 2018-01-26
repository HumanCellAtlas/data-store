include common.mk
MODULES=dss tests

lint:
	flake8 $(MODULES) chalice/*.py daemons/*/*.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

test_srcs := $(wildcard tests/test_*.py)
all_test_srcs := $(addprefix all__, $(test_srcs))
integration_test_srcs := $(addprefix integration__, $(test_srcs))

test: lint mypy $(test_srcs)
	coverage combine
	rm -f .coverage.*

$(test_srcs): %.py :
	set -o pipefail \
	&& DSS_TEST_MODE="standalone" coverage run -p --source=dss -m unittest $(DSS_UNITTEST_OPTS) $@  2>&1 \
	| sed -e "s/^/$$$$ /"

integration_test: lint mypy $(integration_test_srcs)
	coverage combine
	rm -f .coverage.*

$(integration_test_srcs): integration__%.py :
	set -o pipefail \
	&& DSS_TEST_MODE="integration" coverage run -p --source=dss -m unittest $(DSS_UNITTEST_OPTS) $*.py 2>&1 \
	| sed -e "s/^/$$$$ /"

all_test: lint mypy $(all_test_srcs)
	coverage combine
	rm -f .coverage.*

$(all_test_srcs): all__%.py :
	set -o pipefail \
	&& DSS_TEST_MODE="integration standalone" coverage run -p --source=dss -m unittest $(DSS_UNITTEST_OPTS) $*.py 2>&1 \
	| sed -e "s/^/$$$$ /"

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

.PHONY: lint mypy
.PHONY: test all_test integration_test $(test_srcs) $(integration_test_srcs) $(standalone_test_srcs)
.PHONY: deploy deploy-chalice deploy-daemons
