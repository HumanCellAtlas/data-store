SHELL=/bin/bash
STAGE=dev
MODULES=dss tests

lint:
	flake8 $(MODULES) chalice/*.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

test: lint mypy
	python -m unittest discover tests

deploy:
	pip install chalice
	pip install -r requirements.txt
	git clean -df chalice/chalicelib
	cp -R dss dss-api.yml chalice/chalicelib
	chalice/build_deploy_config.sh $(STAGE)
	cd chalice; chalice deploy --no-autogen-policy --stage $(STAGE)

.PHONY: test lint mypy
