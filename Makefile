include common.mk
MODULES=dss tests

lint:
	flake8 $(MODULES) chalice/*.py daemons/*/*.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

test_srcs := $(wildcard tests/test_*.py)

test: lint mypy
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=dss -m unittest discover tests -v

fast_test: lint mypy $(test_srcs)

$(test_srcs): %.py :
	PYTHONWARNINGS=ignore:ResourceWarning python -m unittest $@

deploy:
	$(MAKE) -C chalice deploy
	$(MAKE) -C daemons deploy

clean:
	git clean -Xdf chalice daemons $(MODULES)
	git clean -df chalice/chalicelib daemons/*/domovoilib
	git checkout {chalice,daemons/*}/.chalice/{config,deployed}.json

.PHONY: test lint mypy $(test_srcs)
