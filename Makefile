SHELL=/bin/bash

tests:=$(wildcard tests/test_*.py)

before-test: package

lint:
	flake8 app.py fusillade

test: before-test lint $(tests)
	coverage combine
	rm -f .coverage.*

$(tests): %.py : lint
	coverage run -p --source=fusillade $*.py -v

init_docs:
	cd docs; sphinx-quickstart

docs:
	pandoc --from markdown --to rst Readme.md > README.rst
	$(MAKE) -C docs html

install: docs
	-rm -rf dist
	python setup.py bdist_wheel
	pip install --upgrade dist/*.whl

set_oauth2_config:
	cat ./oauth2_config.json | ./scripts/set_secret.py --secret-name oauth2_config

plan-infra:
	$(MAKE) -C infra plan-all

deploy-infra:
	$(MAKE) -C infra apply-all

package:
	git clean -df chalicelib vendor
	shopt -s nullglob; for wheel in vendor.in/*/*.whl; do unzip -q -o -d vendor $$wheel; done
	cat fusillade-api.yml | envsubst '$$API_DOMAIN_NAME' > chalicelib/fusillade-api.yml
	cat fusillade-internal-api.yml | envsubst '$$API_DOMAIN_NAME' > chalicelib/fusillade-internal-api.yml
	cp -R ./fusillade ./policies chalicelib

deploy: package
	./build_chalice_config.sh $(FUS_DEPLOYMENT_STAGE)
	chalice deploy --no-autogen-policy --stage $(FUS_DEPLOYMENT_STAGE) --api-gateway-stage $(FUS_DEPLOYMENT_STAGE)


refresh_all_requirements:
	@echo -n '' >| requirements.txt
	@echo -n '' >| requirements-dev.txt
	@if [ $$(uname -s) == "Darwin" ]; then sleep 1; fi  # this is require because Darwin HFS+ only has second-resolution for timestamps.
	@touch requirements.txt.in requirements-dev.txt.in
	@$(MAKE) requirements.txt requirements-dev.txt

requirements.txt requirements-dev.txt : %.txt : %.txt.in
	[ ! -e .requirements-env ] || exit 1
	virtualenv -p $(shell which python3) .$<-env
	.$<-env/bin/pip install -r $@
	.$<-env/bin/pip install -r $<
	echo "# You should not edit this file directly.  Instead, you should edit $<." >| $@
	.$<-env/bin/pip freeze >> $@
	rm -rf .$<-env
#	scripts/find_missing_wheels.py requirements.txt # Disabled by akislyuk (circular dependency issues)

requirements-dev.txt : requirements.txt.in

.PHONY: test release docs

include common.mk
