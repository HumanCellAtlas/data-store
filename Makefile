lint:
	flake8 dss test

test: lint
	python -m unittest discover tests

.PHONY: test lint
