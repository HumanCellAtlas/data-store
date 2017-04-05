lint:
	flake8

test: lint
	python -m unittest discover test

.PHONY: test lint
