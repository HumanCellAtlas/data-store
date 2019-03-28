SHELL=/bin/bash

tests:=$(wildcard tests/test_*.py)

before-test:
	cat fusillade-api.yml | envsubst '$$API_DOMAIN_NAME' > chalicelib/swagger.yml

lint:
	flake8 app.py fusillade/*.py

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
	cat fusillade-api.yml | envsubst '$$API_DOMAIN_NAME' > chalicelib/swagger.yml
	cp -R ./fusillade ./policies chalicelib

deploy: package
	./build_chalice_config.sh $(FUS_DEPLOYMENT_STAGE)
	chalice deploy --no-autogen-policy --stage $(FUS_DEPLOYMENT_STAGE) --api-gateway-stage $(FUS_DEPLOYMENT_STAGE)

.PHONY: test release docs

include common.mk
