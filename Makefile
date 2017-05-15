lint:
	flake8

test: lint
	python -m unittest discover tests

.PHONY: test lint
