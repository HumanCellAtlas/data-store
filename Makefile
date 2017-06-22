export SHELL=/bin/bash
export STAGE=dev
export EXPORT_ENV_VARS_TO_LAMBDA=DSS_S3_TEST_BUCKET DSS_GCS_TEST_BUCKET DSS_ES_ENDPOINT
MODULES=dss tests chalice daemons

lint:
	flake8 $(MODULES)

mypy:
	mypy --ignore-missing-imports $(MODULES)

test: lint mypy
	python -W ignore:ResourceWarning -m unittest discover tests -v

deploy:
	$(MAKE) -C chalice deploy
	$(MAKE) -C daemons deploy

.PHONY: test lint mypy
