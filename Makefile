include common.mk
MODULES=dss tests

lint:
	flake8 $(MODULES) chalice/*.py daemons/*/*.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

test: lint mypy
	coverage run -m unittest discover tests -v

deploy:
	$(MAKE) -C chalice deploy
	$(MAKE) -C daemons deploy

.PHONY: test lint mypy
