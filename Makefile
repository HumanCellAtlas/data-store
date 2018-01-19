SHELL=/bin/bash
STAGE=dev

lint:
	./setup.py flake8
	flake8 scripts/*

test: lint
	python ./test/test.py -v

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
	./build_chalice_config.sh $(STAGE)
	chalice deploy --no-autogen-policy --stage $(STAGE) --api-gateway-stage $(STAGE)

.PHONY: test release docs

include common.mk
