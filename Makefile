include common.mk
MODULES=dss tests

lint:
	flake8 $(MODULES) chalice/*.py daemons/*/*.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

test: lint mypy
	tests/fixtures/populate.py --s3-bucket $(DSS_S3_BUCKET_TEST_FIXTURES) --gs-bucket $(DSS_GS_BUCKET_TEST_FIXTURES)
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=dss -m unittest discover tests -v

deploy:
	$(MAKE) -C chalice deploy
	$(MAKE) -C daemons deploy

clean:
	git clean -Xdf chalice daemons $(MODULES)
	git clean -df chalice/chalicelib daemons/*/domovoilib
	git checkout {chalice,daemons/*}/.chalice/{config,deployed}.json

.PHONY: test lint mypy
