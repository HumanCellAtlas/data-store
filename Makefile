SHELL=/bin/bash
STAGE=dev
export API_HOST=auth.${STAGE}.data.humancellatlas.org

tests:=$(wildcard tests/test_*.py)

before-test:
	cat fusillade-api.yml | envsubst '$$API_HOST' > chalicelib/swagger.yml

lint:
	./setup.py flake8
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

deploy:
	git clean -df chalicelib vendor
	shopt -s nullglob; for wheel in vendor.in/*/*.whl; do unzip -q -o -d vendor $$wheel; done
	cat fusillade-api.yml | envsubst '$$API_HOST' > chalicelib/swagger.yml
	./build_chalice_config.sh $(STAGE)
	chalice deploy --no-autogen-policy --stage $(STAGE) --api-gateway-stage $(STAGE)

.PHONY: test release docs

include common.mk
